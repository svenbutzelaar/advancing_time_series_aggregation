using DuckDB: DBInterface
using DataFrames
using Statistics
using DuckDB

include("cluster_ward.jl")  # uses hierarchical_time_clustering_ward

"""
    cluster_partitions!(
        conn::DuckDB.DB,
        num_clusters::Int,
        dependant_per_loacation::Bool;
    )

Runs Ward clustering on profile tables stored in DuckDB.
"""
function cluster_partitions!(
    conn,
    num_clusters::Int,
    dependant_per_loacation::Bool;
)
    results = DataFrame(
        asset = String[],
        rep_period = Int64[],
        specification = String[],
        partition = String[],
        year = Int64[],
    )

    # === GENERATE PARTITIONS ===

    df = DataFrame(DBInterface.execute(
        conn,
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

    if dependant_per_loacation
        cluster_partitions_per_location!(df, results, num_clusters)
    else
        cluster_partitions_per_profile!(df, results, num_clusters)
    end

    # === UPDATE ASSET PARTITIONS ===

    tmp_table = "tmp_cluster_results"

    DuckDB.register_data_frame(conn, results, tmp_table)

    DBInterface.execute(
        conn,
        """
        UPDATE assets_rep_periods_partitions AS a
        SET
            partition     = r.partition,
            specification = r.specification
        FROM $tmp_table AS r
        WHERE
            a.asset      = r.asset
            AND a.rep_period = r.rep_period
            AND a.year   = r.year
        """
    )

    # === UPDATE FLOWS PARTITIONS ===

    DBInterface.execute(
        conn,
        """
        UPDATE flows_rep_periods_partitions AS f
        SET
            partition     = r.partition,
            specification = r.specification
        FROM $tmp_table AS r
        WHERE
            f.rep_period = r.rep_period
            AND f.year   = r.year
            AND (
                f.from_asset = r.asset
                OR f.to_asset = r.asset
            )
        """
    )
    DBInterface.execute(conn, "DROP VIEW IF EXISTS $tmp_table")

end

function cluster_partitions_per_profile!(
        df::DataFrame, 
        results::DataFrame, 
        num_clusters::Int,
    )
    
    for g in groupby(df, [:profile_name, :rep_period, :year])
        profile = first(g.profile_name)
        rep_period = first(g.rep_period)
        year = first(g.year)
        values = reshape(Vector{Float64}(g.value), :, 1)

        clusters = hierarchical_time_clustering_ward(values, num_clusters)

        push!(
            results,
            (
                asset = profile,
                rep_period = rep_period,
                specification = "explicit",
                partition = join(clusters, ";"),
                year = year,
            ),
        )
    end
    
end


function cluster_partitions_per_location!(
    df::DataFrame, 
    results::DataFrame, 
    num_clusters::Int,
)

    for group_per_location in groupby(df, [:location, :rep_period, :year])

        rep_period = first(group_per_location.rep_period)
        year = first(group_per_location.year)

        # Ensure correct order
        sort!(group_per_location, [:profile_name, :timestep])

        profiles = unique(group_per_location.profile_name)
        timesteps = unique(group_per_location.timestep)

        n_t = length(timesteps)
        n_p = length(profiles)

        # Preallocate matrix
        values = Matrix{Float64}(undef, n_t, n_p)

        # Fill matrix column by column
        for (j, profile) in enumerate(profiles)

            sub = group_per_location[group_per_location.profile_name .== profile, :]

            # If already sorted by timestep, this is safe
            values[:, j] .= sub.value

        end

        clusters = hierarchical_time_clustering_ward(values, num_clusters)

        for profile_name in profiles
            push!(
                results,
                (
                    asset = profile_name,
                    rep_period = rep_period,
                    specification = "explicit",
                    partition = join(clusters, ";"),
                    year = year,
                ),
            )
        end
    end

end