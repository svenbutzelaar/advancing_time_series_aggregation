#!/usr/bin/env julia

import TulipaIO as TIO
import TulipaEnergyModel as TEM
using Gurobi
using DuckDB
using DataFrames
using CSV
using JuMP
using MathOptInterface
const MOI = MathOptInterface
include("cluster/config.jl")
include("cluster/cluster_partitions.jl")
include("create_ens_experiment_db.jl")


config = @isdefined(CONFIG) ? CONFIG : ClusteringConfig()
println("Using config: ", config)
const RESULTS_CSV = "plotting/csv_data/regret/v2_$(experiment_name(config)).csv"

# ──────────────────────────────────────────
# Core experiment runner
# ──────────────────────────────────────────
function run_experiment(config, calc_ens::Bool; base_energy_problem = nothing, base_connection = nothing)
    if calc_ens && (isnothing(base_energy_problem) || isnothing(base_connection))
    error("base_energy_problem and base_connection are required when calc_ens=true")
    end

    file_name = experiment_name(config)
    if calc_ens
        file_name = "ens_" * file_name
    end

    timings = Dict{String, Float64}()
    timings["t_clustering"] = 0.0

    # 1. Create DB + clustering
    database_name = if calc_ens
        create_ens_db(config, base_energy_problem, base_connection)
    else
        local db = "db_files/$(experiment_name(config)).db"
        rm(db; force = true)
        cp("db_files/base_db.db", db; force = true)
        conn_setup = DBInterface.connect(DuckDB.DB, db)
        t0 = time()
        cluster_partitions!(conn_setup, config)
        timings["t_clustering"] = time() - t0
        TEM.populate_with_defaults!(conn_setup)
        close(conn_setup)
        db
    end

    # 2. Create model
    connection = DBInterface.connect(DuckDB.DB, database_name)
    energy_problem = TEM.EnergyProblem(connection)

    t0 = time()
    TEM.create_model!(energy_problem;
        optimizer = () -> Gurobi.Optimizer(),
        optimizer_parameters = Dict("output_flag" => true)
    )
    timings["t_create_model"] = time() - t0

    # 3. Solve
    t0 = time()
    TEM.solve_model!(energy_problem)
    timings["t_solve"] = time() - t0

    if termination_status(energy_problem.model) == MOI.INFEASIBLE
        println("Computing IIS...")
        compute_conflict!(energy_problem.model)
        for (F, S) in list_of_constraint_types(energy_problem.model)
            for con in all_constraints(energy_problem.model, F, S)
                if MOI.get(energy_problem.model, MOI.ConstraintConflictStatus(), con) == MOI.IN_CONFLICT
                    println("Conflicting constraint: ", con)
                end
            end
        end
        error("Model is infeasible — aborting.")
    end

    # 4a. Extract costs assets
    df_obj_assets = TIO.get_table(connection, "t_objective_assets")
    foreach(println, names(df_obj_assets))

    cost_cols_assets = [:investment_cost, :investment_cost_storage_energy,
                    :annualized_cost, :salvage_value]

    costs_assets = Dict(sym => sum(df_obj_assets[!, sym]) for sym in cost_cols_assets)

    # 4b. Extract costs flows
    df_obj_flows = TIO.get_table(connection, "t_objective_flows")
    foreach(println, names(df_obj_flows))

    cost_cols_flows = [:investment_cost, :operational_cost, :fuel_cost, 
                        :total_variable_cost, :annualized_cost, :salvage_value]

    costs_flows = Dict(sym => sum(df_obj_flows[!, sym]) for sym in cost_cols_flows)

    # 5. Save + export
    TEM.save_solution!(energy_problem; compute_duals = true)
    output_files = "outputs/" * file_name
    isdir(output_files) || mkdir(output_files)
    TEM.export_solution_to_csv_files(output_files, energy_problem)


    # 6. Compute ENS
    energy_not_served = 0.0
    if calc_ens
        var_flow_df = TIO.get_table(connection, "var_flow")
        flow_ens = filter(row -> occursin(r"^.._E_ENS", row.from_asset), var_flow_df)
        energy_not_served = isempty(flow_ens) ? 0.0 : sum(flow_ens.solution)
    end

    # 7. Append to results CSV
    df_row = DataFrame(
        vcat(
            [
                :method            => string(config.extreme_preservation),
                :num_clusters      => config.n_prime,
                :file_name         => file_name,
                :calc_ens          => calc_ens,
                :t_clustering      => timings["t_clustering"],
                :t_create_model    => timings["t_create_model"],
                :t_solve           => timings["t_solve"],
            ],
            [Symbol(sym, :_assets) => costs_assets[sym] for sym in cost_cols_assets],
            [Symbol(sym, :_flows)  => costs_flows[sym]  for sym in cost_cols_flows],
            [
                :energy_not_served => energy_not_served,
            ]
        )
    )

    if isfile(RESULTS_CSV)
        df_existing = CSV.read(RESULTS_CSV, DataFrame)
        df_out = vcat(df_existing, df_row; cols = :union)
    else
        mkpath(dirname(RESULTS_CSV))
        df_out = df_row
    end

    CSV.write(RESULTS_CSV, df_out)
    println("Results written to $RESULTS_CSV")
    println("Timings: ", timings)
    println("Done: $file_name")

    return energy_problem, connection
end

# ──────────────────────────────────────────
# Run both experiments
# ──────────────────────────────────────────

# first investment+dispatch with low resolution
solved_energy_problem, solved_connection = run_experiment(config, false)
# secondly only dispatch with high resolution but investment as fixed initial units
_, ens_connection = run_experiment(config, true; base_energy_problem = solved_energy_problem, base_connection = solved_connection)