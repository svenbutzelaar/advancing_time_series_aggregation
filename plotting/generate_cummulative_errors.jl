using Pkg
using DuckDB: DBInterface, DuckDB
using DataFrames: DataFrame
using CSV

include("../cluster/cluster_ward.jl")
include("../cluster/cluster_partitions.jl")


base_db = "obz_partitions_base.db"
new_db  = "obz_partitions.db"

num_clusters = 200
dir = "plotting/$(num_clusters)"
mkpath(dir)
partitions_output_file = "$dir/partitions.csv"

isfile(new_db) && rm(new_db; force=true)
cp(base_db, new_db)

connection = DBInterface.connect(DuckDB.DB, new_db)

results = DataFrame(
    asset = String[],
    rep_period = Int64[],
    specification = String[],
    partition = String[],
    values = String[],
    year = Int64[],
    location = String[],
)

stats = DataFrame(
    name = String[],
    rep_period = Int64[],
    year = Int64[],
    errors = Vector{Float64}[],
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

cluster_partitions_per_profile!(df, results, stats, num_clusters)


# for writing
CSV.write("plotting/cummulative_errors_per_profile.csv",stats)   
# CSV.write(partitions_output_file, results)