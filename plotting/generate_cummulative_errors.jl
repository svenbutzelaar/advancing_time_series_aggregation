using Pkg
using DuckDB: DBInterface, DuckDB
using DataFrames: DataFrame
using CSV

include("../cluster/cluster_ward.jl")
include("../cluster/cluster_partitions.jl")


base_db = "obz_partitions_base.db"
new_db  = "obz_partitions.db"

num_clusters = 500
do_extreme_preservation = true
dir = "plotting/csv_data/partitions"
mkpath(dir)
partitions_output_file = "$dir/$(num_clusters)-extreme_preservation-$do_extreme_preservation.csv"

isfile(new_db) && rm(new_db; force=true)
cp(base_db, new_db)

connection = DBInterface.connect(DuckDB.DB, new_db)

results = DataFrame(
    asset = String[],
    rep_period = Int64[],
    specification = String[],
    partition = String[],
    values = String[],
    mean_values = String[],
    year = Int64[],
    location = String[],
)

stats = DataFrame(
    asset = String[],
    location = String[],
    rep_period = Int64[],
    year = Int64[],
    errors = Vector{Float64}[],
    ldc_errors = Vector{Float64}[],
)

df = DataFrame(DBInterface.execute(
        connection,
        """
        SELECT 
            profile_name, 
            SUBSTRING(profile_name, 1, 2) AS location,
            rep_period, 
            timestep, 
            year, 
            value
        FROM profiles_rep_periods
        WHERE 
                value IS NOT NULL
            AND
                NOT(LOWER(profile_name) LIKE '%hydro%')
        ORDER BY profile_name, rep_period, year, timestep
        """
    ))

# cluster_partitions_per_location!(deepcopy(df), results, stats, num_clusters; calc_stats=true)
# CSV.write("plotting/csv_data/errors_per_location_extreme_preservation.csv",stats)   

# for writing
cluster_partitions_per_location!(deepcopy(df), results, stats, num_clusters; calc_stats=false, do_extreme_preservation=do_extreme_preservation)
CSV.write(partitions_output_file, results)