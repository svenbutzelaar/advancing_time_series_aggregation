using Pkg
using DuckDB: DBInterface, DuckDB
using DataFrames: DataFrame
using TulipaIO: TulipaIO
using TulipaClustering: TulipaClustering
using TulipaEnergyModel: TulipaEnergyModel as TEM
using Distances: SqEuclidean
using Random: Random
include("cluster/cluster_partitions.jl")
include("cluster/config.jl")

user_input_dir = "../TulipaEnergyModel.jl/docs/src/data/obz/"

#parameters
num_rep_periods = 3
period_duration = 168

create_obz_invest_full_resolution = false

config = @isdefined(CONFIG) ? CONFIG : ClusteringConfig()

println("Using config: ", config)
file_name = experiment_name(config)
database_name = "db_files/$file_name.db"

if create_obz_invest_full_resolution
    database_name = "db_files/obz-invest-full-resolution.db"
end
    
readdir(user_input_dir)

if isfile(database_name)
    error("Database file '$database_name' already exists. Please remove it or use a different name.")
end

base_db_file = "db_files/base_db.db"
cp(base_db_file, database_name; force = true)
connection = DBInterface.connect(DuckDB.DB, database_name)


if !create_obz_invest_full_resolution
    cluster_partitions!(connection, config)
end

TEM.populate_with_defaults!(connection)


close(connection)