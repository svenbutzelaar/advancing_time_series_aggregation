using Pkg
using DuckDB: DBInterface, DuckDB
using DataFrames: DataFrame
include("cluster/cluster_partitions.jl")

base_db = "obz_partitions_base.db"
new_db  = "obz_partitions.db"

isfile(new_db) && rm(new_db; force=true)
cp(base_db, new_db)

connection = DBInterface.connect(DuckDB.DB, new_db)
cluster_partitions!(connection, 3000, true)