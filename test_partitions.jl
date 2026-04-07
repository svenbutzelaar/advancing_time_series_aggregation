using Pkg
using DuckDB: DBInterface, DuckDB
using DataFrames: DataFrame
include("cluster/cluster_partitions.jl")
include("cluster/config.jl")

config = @isdefined(CONFIG) ? CONFIG : ClusteringConfig()
db = "db_files/$(experiment_name(config)).db"
rm(db; force = true)
cp("db_files/base_db.db", db; force = true)
conn_setup = DBInterface.connect(DuckDB.DB, db)
cluster_partitions!(conn_setup, config)