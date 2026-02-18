using Pkg
using DuckDB: DBInterface, DuckDB
using DataFrames: DataFrame

database_name = "obz_with_investment_no_temporal_reduction.db"

# remove .db for output directory name
output_dir = replace(database_name, ".db" => "")

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
    # replace _ with - in the output filename
    csv_name = replace(tbl, "_" => "-") * ".csv"
    DBInterface.execute(
        connection,
        "COPY \"$tbl\" TO '$output_dir/$csv_name' (HEADER, DELIMITER ',');"
    )
end

DBInterface.close!(connection)
