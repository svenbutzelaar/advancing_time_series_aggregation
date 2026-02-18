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


# # === Read command-line argument ===
# if length(ARGS) < 1
#     println("Usage: julia run_experiment.jl <case_name>")
#     println("Example: julia run_experiment.jl C0.5_small_ens")
#     exit(1)
# end

# case_name = ARGS[1]

case_name = "local"


connection = DBInterface.connect(DuckDB.DB, "obz-invest-small.db")

TEM.populate_with_defaults!(connection)

energy_problem = TEM.EnergyProblem(connection)
TEM.create_model!(energy_problem;
    optimizer = () -> Gurobi.Optimizer(),
    optimizer_parameters = Dict(
        "output_flag" => true,
        )
)
TEM.solve_model!(energy_problem)
TEM.save_solution!(energy_problem; compute_duals = true)
    
output_files = "obz-invest-small-outputs0.01" * case_name
isdir(output_files) || mkdir(output_files)
TEM.export_solution_to_csv_files(output_files, energy_problem)

close(connection)