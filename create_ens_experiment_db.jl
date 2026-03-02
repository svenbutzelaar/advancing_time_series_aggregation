using Pkg
using DuckDB: DBInterface, DuckDB
using DataFrames: DataFrame
include("cluster/config.jl")


config = ClusteringConfig(
    dependant_per_location = true,
    extreme_preservation = SeperateExtremes,
    high_percentile = 0.95,
    low_percentile = 0.05,
    )

num_clusters = 1500
    
file_name = experiment_name(config, num_clusters)
database_name = "$file_name.db"

dataset_full_resolution = "obz-invest-full-resolution"
source_db = dataset_full_resolution * ".db"
target_db = "ens_$(file_name).db"

var_assets_investment_path  = joinpath("outputs-" * file_name, "var_assets_investment.csv")

# Copy the database (overwrite if it already exists)
cp(source_db, target_db; force = true)

# Connect to the copied database
connection = DBInterface.connect(DuckDB.DB, target_db)



DuckDB.query(
    connection,
    "
    UPDATE asset
    SET 
        investment_method = 'none',
        investment_integer = false
    "
)

DuckDB.query(
    connection,
    "
    UPDATE asset_commission
    SET 
        investment_cost = NULL,
        investment_limit = NULL
    "
)

DuckDB.query(
    connection,
    "
    UPDATE asset_milestone
    SET investable = false
    "
)

DuckDB.query(
    connection,
    "
    UPDATE asset_both
    SET initial_units = v.solution
    FROM read_csv_auto('$var_assets_investment_path') as v
    WHERE asset_both.asset = v.asset;
    "
)

close(connection)