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

config = @isdefined(CONFIG) ? CONFIG : ClusteringConfig()

println("Using config: ", config)
file_name = experiment_name(config)
database_name = "db_files/$file_name.db"

if isfile(database_name)
    error("Database file '$database_name' already exists. Please remove it or use a different name.")
end

base_db_file = "db_files/base_db.db"
cp(base_db_file, database_name; force = true)
connection = DBInterface.connect(DuckDB.DB, database_name)

partition = div(8760, config.n_prime)

println("partition for UTR: ", partition)


# ------------------------------------------------------------
# also implement in run_experiment.jl
# -------------------------------------------------------

DuckDB.query(
    connection,
    "UPDATE assets_rep_periods_partitions
    SET partition = $(partition)
    WHERE 
                 LOWER(asset) LIKE '%wind_onshore%'
              OR LOWER(asset) LIKE '%wind_offshore%'
              OR LOWER(asset) LIKE '%solar%'
              OR LOWER(asset) LIKE '%e_demand%'
    ",
)

DuckDB.query(
    connection,
    "UPDATE flows_rep_periods_partitions
    SET partition = $(partition)
    WHERE 
                 LOWER(from_asset) LIKE '%wind_onshore%'
              OR LOWER(from_asset) LIKE '%wind_offshore%'
              OR LOWER(from_asset) LIKE '%solar%'
              OR LOWER(from_asset) LIKE '%e_demand%'
              OR LOWER(to_asset) LIKE '%wind_onshore%'
              OR LOWER(to_asset) LIKE '%wind_offshore%'
              OR LOWER(to_asset) LIKE '%solar%'
              OR LOWER(to_asset) LIKE '%e_demand%'
    ",
)

TEM.populate_with_defaults!(connection)

close(connection)