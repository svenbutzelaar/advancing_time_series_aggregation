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


for dataset in instances(Dataset)
    base_db_file = dataset_db_file(dataset)
    database_name = dataset_db_full_resolution_file(dataset)
    cp(base_db_file, database_name; force = true)
    connection = DBInterface.connect(DuckDB.DB, database_name)
    TEM.populate_with_defaults!(connection)
    close(connection)
end
