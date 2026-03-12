using Pkg
using DuckDB: DBInterface, DuckDB
using DataFrames: DataFrame
include("cluster/config.jl")

config = @isdefined(CONFIG) ? CONFIG : ClusteringConfig()

println("Using config: ", config)
file_name = experiment_name(config)
# file_name = "obz-invest-full-resolution"
database_name = "db_files/$file_name.db"

# remove .db for output directory name
output_dir = "inputs/" * replace(database_name, ".db" => "")

connection = DBInterface.connect(DuckDB.DB, database_name)

# create directory only if it does not exist
isdir(output_dir) || mkdir(output_dir)

tables_to_export = [
    "asset_both",
    "asset_commission",
    "asset_milestone",
    "asset",
    "assets_profiles",
    "assets_rep_periods_partitions",
    # "assets_timeframe_partitions",
    "assets_timeframe_profiles",
    "flow_both",
    "flow_commission",
    "flow_milestone",
    "flow",
    # "flows_profiles",
    "flows_rep_periods_partitions",
    "profiles_rep_periods",
    "profiles_timeframe",
    "rep_periods_data",
    "rep_periods_mapping",
    "timeframe_data",
    "year_data",
]

for tbl in tables_to_export
    # Check if table exists
    result = DBInterface.execute(
        connection,
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '$tbl';"
    ) |> DataFrame
    
    if result[1, 1] == 0
        println("Skipping '$tbl': table does not exist")
        continue
    end
    
    # replace _ with - in the output filename
    csv_name = replace(tbl, "_" => "-") * ".csv"
    DBInterface.execute(
        connection,
        "COPY \"$tbl\" TO '$output_dir/$csv_name' (HEADER, DELIMITER ',');"
    )
end

DBInterface.close!(connection)
