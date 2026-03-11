#!/usr/bin/env julia

import TulipaIO as TIO
import TulipaEnergyModel as TEM
import TulipaClustering as TC
using Gurobi
using DuckDB
using DataFrames
using Plots
using Distances
include("cluster/config.jl")

config = @isdefined(CONFIG) ? CONFIG : ClusteringConfig()

println("Using config: ", config)
file_name = experiment_name(config)

calc_ens = @isdefined(CALC_ENS) ? CALC_ENS : false

if calc_ens
    file_name = "ens_" * file_name
end

connection = DBInterface.connect(DuckDB.DB, "db_files/$file_name.db")

# TEM.populate_with_defaults!(connection)

energy_problem = TEM.EnergyProblem(connection)
TEM.create_model!(energy_problem;
    optimizer = () -> Gurobi.Optimizer(),
    optimizer_parameters = Dict("output_flag" => true)
)

TEM.solve_model!(energy_problem)

using JuMP
if termination_status(energy_problem.model) == MOI.INFEASIBLE
    println("Computing IIS...")
    compute_conflict!(energy_problem.model)
    
    # Print conflicting constraints
    for (F, S) in list_of_constraint_types(energy_problem.model)
        for con in all_constraints(energy_problem.model, F, S)
            if MOI.get(energy_problem.model, MOI.ConstraintConflictStatus(), con) == MOI.IN_CONFLICT
                println("Conflicting constraint: ", con)
            end
        end
    end
end

TEM.save_solution!(energy_problem; compute_duals = true)

output_files = "outputs/" * file_name
isdir(output_files) || mkdir(output_files)
TEM.export_solution_to_csv_files(output_files, energy_problem)

close(connection)