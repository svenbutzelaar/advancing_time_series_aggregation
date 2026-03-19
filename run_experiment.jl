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

const RESULTS_CSV = "plotting/csv_data/regret.csv"

config = @isdefined(CONFIG) ? CONFIG : ClusteringConfig()
println("Using config: ", config)

# ──────────────────────────────────────────
# Core experiment runner
# ──────────────────────────────────────────
function run_experiment(config, calc_ens::Bool; base_energy_problem = nothing)
    file_name = experiment_name(config)
    if calc_ens
        file_name = "ens_" * file_name
    end

    timings = Dict{String, Float64}()

    # 1. Create DB + clustering
    database_name = if calc_ens
        create_ens_db(config, base_energy_problem)
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

    # 4. Extract costs
    m = energy_problem.model

    function get_cost(sym)
        try
            return JuMP.value(m[sym])
        catch
            return 0.0
        end
    end

    assets_investment_cost                = get_cost(:assets_investment_cost)
    assets_fixed_cost_compact_method      = get_cost(:assets_fixed_cost_compact_method)
    assets_fixed_cost_simple_method       = get_cost(:assets_fixed_cost_simple_method)
    storage_assets_energy_investment_cost = get_cost(:storage_assets_energy_investment_cost)
    storage_assets_energy_fixed_cost      = get_cost(:storage_assets_energy_fixed_cost)
    flows_investment_cost                 = get_cost(:flows_investment_cost)
    flows_fixed_cost                      = get_cost(:flows_fixed_cost)
    flows_operational_cost                = get_cost(:flows_operational_cost)
    vintage_flows_operational_cost        = get_cost(:vintage_flows_operational_cost)
    units_on_cost                         = get_cost(:units_on_cost)

    total_cost = (
        assets_investment_cost +
        assets_fixed_cost_compact_method +
        assets_fixed_cost_simple_method +
        storage_assets_energy_investment_cost +
        storage_assets_energy_fixed_cost +
        flows_investment_cost +
        flows_fixed_cost +
        flows_operational_cost +
        vintage_flows_operational_cost +
        units_on_cost
    )

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
        method                                = string(config.method),
        num_clusters                          = config.num_clusters,
        file_name                             = file_name,
        calc_ens                              = calc_ens,
        t_clustering                          = timings["t_clustering"],
        t_create_model                        = timings["t_create_model"],
        t_solve                               = timings["t_solve"],
        total_cost                            = total_cost,
        assets_investment_cost                = assets_investment_cost,
        assets_fixed_cost_compact_method      = assets_fixed_cost_compact_method,
        assets_fixed_cost_simple_method       = assets_fixed_cost_simple_method,
        storage_assets_energy_investment_cost = storage_assets_energy_investment_cost,
        storage_assets_energy_fixed_cost      = storage_assets_energy_fixed_cost,
        flows_investment_cost                 = flows_investment_cost,
        flows_fixed_cost                      = flows_fixed_cost,
        flows_operational_cost                = flows_operational_cost,
        vintage_flows_operational_cost        = vintage_flows_operational_cost,
        units_on_cost                         = units_on_cost,
        energy_not_served                     = energy_not_served,
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

    close(connection)
    rm(database_name; force = true)

    return energy_problem
end

# ──────────────────────────────────────────
# Run both experiments
# ──────────────────────────────────────────

# first investment+dispatch with low resolution
solved_energy_problem = run_experiment(config, false)
# secondly only dispatch with high resolution but investment as fixed initial units
run_experiment(config, true; base_energy_problem = solved_energy_problem)