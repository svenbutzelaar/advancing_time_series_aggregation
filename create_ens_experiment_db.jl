using Pkg
using DuckDB: DBInterface, DuckDB
using DataFrames: DataFrame

user_input_dir = "../TulipaEnergyModel.jl/docs/src/data/obz/"
dataset_full_resolution = "obz-invest"
case = "-part"
source_db = dataset_full_resolution * ".db"
target_db = dataset_full_resolution * case * "-ens.db"

var_flow_path  = joinpath("outputs-" * dataset_full_resolution * case, "var_assets_investment.csv")

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
    FROM read_csv_auto('$var_flow_path') as v
    WHERE asset_both.asset = v.asset;
    "
)

close(connection)