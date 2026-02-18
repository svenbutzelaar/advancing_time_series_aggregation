using Pkg
using DuckDB: DBInterface, DuckDB
using DataFrames: DataFrame
using TulipaIO: TulipaIO
using TulipaClustering: TulipaClustering
using TulipaEnergyModel: TulipaEnergyModel as TEM
using Distances: SqEuclidean
using Random: Random
using Gurobi

user_input_dir = "../TulipaEnergyModel.jl/docs/src/data/obz/"

readdir(user_input_dir)
connection = DBInterface.connect(DuckDB.DB, "obz_small.db")


# include("tutorial7_db_creation.jl")

# SOLVING
TEM.populate_with_defaults!(connection)
energy_problem = TEM.EnergyProblem(connection)
optimizer_parameters = Dict(
    "output_flag" => true,
    "mip_rel_gap" => 0.0,
    "mip_feasibility_tolerance" => 1e-5,
)
model_file_name = "model_small.lp"

TEM.create_model!(energy_problem; model_file_name, optimizer_parameters)
TEM.solve_model!(energy_problem)
TEM.save_solution!(energy_problem; compute_duals = true)
mkdir("obz-outputs")
TEM.export_solution_to_csv_files("obz-outputs", energy_problem)
close(connection)