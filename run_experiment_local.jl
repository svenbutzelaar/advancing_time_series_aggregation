#!/usr/bin/env julia

# using Pkg: Pkg
# Pkg.activate(".")
# Pkg.add([
#     "TulipaIO",
#     "TulipaEnergyModel",
#     "TulipaClustering",
#     "Distances",
#     "DuckDB",
#     "DataFrames",
#     "Plots",
# ])
# Pkg.instantiate()

import TulipaIO as TIO
import TulipaEnergyModel as TEM
import TulipaClustering as TC
using Gurobi
using DuckDB
using DataFrames
using Plots
using Distances
include("cluster/config.jl")


# # === Read command-line argument ===
# if length(ARGS) < 1
#     println("Usage: julia run_experiment.jl <case_name>")
#     println("Example: julia run_experiment.jl C0.5_small_ens")
#     exit(1)
# end

# case_name = ARGS[1]

case_name = "local"

calc_ens = true

config = ClusteringConfig(
    dependant_per_location = true,
    extreme_preservation = SeperateExtremes,
    high_percentile = 0.95,
    low_percentile = 0.05,
    )

num_clusters = 1500
    
file_name = experiment_name(config, num_clusters)

if calc_ens
    file_name = "ens_" * file_name
end

connection = DBInterface.connect(DuckDB.DB, "$file_name.db")

# TEM.populate_with_defaults!(connection)

energy_problem = TEM.EnergyProblem(connection)
TEM.create_model!(energy_problem;
    optimizer = () -> Gurobi.Optimizer(),
    optimizer_parameters = Dict(
        "output_flag" => true,
        )
)
TEM.solve_model!(energy_problem)
TEM.save_solution!(energy_problem; compute_duals = true)
    
output_files = "outputs_" * case_name
isdir(output_files) || mkdir(output_files)
TEM.export_solution_to_csv_files(output_files, energy_problem)

close(connection)