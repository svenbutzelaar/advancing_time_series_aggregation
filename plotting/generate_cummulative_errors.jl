using Pkg
using DuckDB: DBInterface, DuckDB
using DataFrames: DataFrame
using CSV

include("../cluster/cluster_ward.jl")
include("../cluster/cluster_partitions.jl")
include("../cluster/config.jl")


base_db = "obz_partitions_base.db"
new_db  = "obz_partitions.db"

num_clusters = 1500
calc_stats = false

config = ClusteringConfig(
    calc_stats = calc_stats,
    dependant_per_location = true,
    extreme_preservation = SeperateExtremes,
    high_percentile = 0.95,
    low_percentile = 0.05,
)
file_name = experiment_name(config, num_clusters)

dir = "plotting/csv_data/partitions"
mkpath(dir)
partitions_output_file = "$dir/$file_name.csv"

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

    
    cluster_partitions_per_location!(deepcopy(df), results, stats, num_clusters, config)
    # for writing
    if calc_stats
        CSV.write(partitions_output_file, results)
    else
        CSV.write("plotting/csv_data/per_merge/$file_name.csv",stats)   
    end
