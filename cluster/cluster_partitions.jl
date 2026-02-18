using DuckDB: DBInterface
using DataFrames
using Statistics
using DuckDB

include("cluster_ward.jl")  # uses hierarchical_time_clustering_ward

"""
    cluster_partitions!(
        conn::DuckDB.DB,
        num_clusters::Int;
    )

Runs Ward clustering on profile tables stored in DuckDB.
"""
function cluster_partitions!(
    conn,
    num_clusters::Int;
)
    results = DataFrame(
        asset = String[],
        rep_period = Int64[],
        specification = String[],
        partition = String[],
        year = Int64[],
    )

    stats = DataFrame()

    # === GENERATE PARTITIONS ===

    df = DataFrame(DBInterface.execute(
        conn,
        """
        SELECT profile_name, rep_period, timestep, year, value
        FROM profiles_rep_periods
        WHERE 
                value IS NOT NULL
            AND
                NOT(LOWER(profile_name) LIKE '%hydro%')
        ORDER BY profile_name, rep_period, year, timestep
        """
    ))

    for g in groupby(df, [:profile_name, :rep_period, :year])
        profile = first(g.profile_name)
        rep_period = first(g.rep_period)
        year = first(g.year)
        values = Vector{Float64}(g.value)

        clusters, ward_stats =
            hierarchical_time_clustering_ward(values, num_clusters)

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

        # ward_stats["profile_name"] = profile
        # ward_stats["rep_period"] = rep_period
        # ward_stats["num_timesteps"] = length(values)
        # ward_stats["compression_ratio"] = length(clusters) / length(values)

        # push!(stats, DataFrame(ward_stats))
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