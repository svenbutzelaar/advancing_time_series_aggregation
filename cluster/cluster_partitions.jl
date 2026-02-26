using DuckDB: DBInterface
using DataFrames
using Statistics
using DuckDB

include("cluster_ward.jl")
include("profile_type.jl")
include("config.jl")

"""
    cluster_partitions!(
        conn,
        num_clusters::Int,
        dependant_per_location::Bool,
        config::ClusteringConfig = ClusteringConfig()
    )

Runs Ward clustering on profile tables stored in DuckDB.
"""
function cluster_partitions!(
    conn,
    num_clusters::Int,
    config::ClusteringConfig = ClusteringConfig(),
)

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

    # =========================
    # LOAD DATA
    # =========================

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

    # =========================
    # CLUSTERING
    # =========================

    if config.dependant_per_location
        cluster_partitions_per_location!(
            df, results, stats, num_clusters, config
        )
    else
        cluster_partitions_per_profile!(
            df, results, stats, num_clusters, config
        )
    end

    # =========================
    # OPTIONAL EXTREME UPDATE
    # =========================

    if config.do_extreme_preservation
        update_profiles_rep_periods_with_new_values!(conn, results)
    end

    # =========================
    # UPDATE DATABASE TABLES
    # =========================

    tmp_table = "tmp_cluster_results"
    DuckDB.register_data_frame(conn, results, tmp_table)

    DBInterface.execute(conn,
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

    DBInterface.execute(conn,
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

function update_profiles_rep_periods_with_new_values!(
    conn, 
    results;
)

    expanded_profiles = DataFrame(
        profile_name = String[],
        rep_period   = Int[],
        year         = Int[],
        timestep     = Int[],
        value        = Float64[],
    )

    for row in eachrow(results)

        profile_name = row.asset
        rep_period   = row.rep_period
        year         = row.year

        partitions = parse.(Int, split(row.partition, ";"))
        rep_values = parse.(Float64, split(row.values, ";"))
        mean_values = parse.(Float64, split(row.mean_values, ";"))

        timestep_counter = 1

        for (p, rep_v, mean_v) in zip(partitions, rep_values, mean_values)

            # Only update if representative differs from mean
            if !isapprox(rep_v, mean_v; atol=1e-10)

                for _ in 1:p
                    push!(expanded_profiles, (
                        profile_name,
                        rep_period,
                        year,
                        timestep_counter,
                        rep_v
                    ))
                    timestep_counter += 1
                end

            else
                timestep_counter += p
            end
        end
    end

    # If nothing changed → do nothing
    if nrow(expanded_profiles) == 0
        println("No extreme-preservation adjustments required.")
        return
    end

    tmp_profiles = "tmp_clustered_profiles"
    DuckDB.register_data_frame(conn, expanded_profiles, tmp_profiles)

    # --- Save old values first ---
    DBInterface.execute(
        conn,
        """
        CREATE TABLE IF NOT EXISTS profiles_rep_periods_old AS
        SELECT *
        FROM profiles_rep_periods
        """
    )

    # --- Update new values ---
    DBInterface.execute(
        conn,
        """
        UPDATE profiles_rep_periods AS p
        SET value = t.value
        FROM $tmp_profiles AS t
        WHERE
            p.profile_name = t.profile_name
            AND p.rep_period = t.rep_period
            AND p.year = t.year
            AND p.timestep = t.timestep
        """
    )

    DBInterface.execute(conn, "DROP VIEW IF EXISTS $tmp_profiles")
end

function cluster_partitions_per_profile!(
    df::DataFrame,
    results::DataFrame,
    stats::DataFrame,
    num_clusters::Int,
    config::ClusteringConfig,
)

    for g in groupby(df, [:profile_name, :rep_period, :year])

        profile_name = first(g.profile_name)
        location = first(g.location)
        rep_period = first(g.rep_period)
        year = first(g.year)

        values = reshape(Vector{Float64}(g.value), :, 1)

        profile_type = getProfileType(profile_name)

        partitions,
        partition_values,
        mean_values,
        ward_errors,
        ldc_errors = hierarchical_time_clustering_ward(
            values,
            num_clusters,
            [profile_type],
            config
        )

        partition_values_flat = first.(partition_values)
        mean_values_flat = first.(mean_values)

        if config.calc_stats
            push!(stats, (
                asset = profile_name,
                location = location,
                rep_period = rep_period,
                year = year,
                errors = [ward_errors[s][1] for s in eachindex(ward_errors)],
                ldc_errors = [ldc_errors[s][1] for s in eachindex(ldc_errors)],
            ))
        end

        push!(results, (
            asset = profile_name,
            rep_period = rep_period,
            specification = "explicit",
            partition = join(partitions, ";"),
            values = join(partition_values_flat, ";"),
            mean_values = join(mean_values_flat, ";"),
            year = year,
            location = location,
        ))
    end
end

function cluster_partitions_per_location!(
    df::DataFrame,
    results::DataFrame,
    stats::DataFrame,
    num_clusters::Int,
    config::ClusteringConfig,
)

    for group_per_location in groupby(df, [:location, :rep_period, :year])

        rep_period = first(group_per_location.rep_period)
        year = first(group_per_location.year)
        location = first(group_per_location.location)

        sort!(group_per_location, [:profile_name, :timestep])

        profiles = unique(group_per_location.profile_name)
        timesteps = unique(group_per_location.timestep)

        n_t = length(timesteps)
        n_p = length(profiles)

        values = Matrix{Float64}(undef, n_t, n_p)

        for (j, profile) in enumerate(profiles)
            sub = group_per_location[
                group_per_location.profile_name .== profile, :
            ]
            values[:, j] .= sub.value
        end

        profile_types = [getProfileType(p) for p in profiles]

        partitions,
        partition_values,
        mean_values,
        ward_errors,
        ldc_errors = hierarchical_time_clustering_ward(
            values,
            num_clusters,
            profile_types,
            config
        )

        for (j, profile_name) in enumerate(profiles)

            partition_values_per_profile = getindex.(partition_values, j)
            mean_values_per_profile = getindex.(mean_values, j)

            push!(results, (
                asset = profile_name,
                rep_period = rep_period,
                specification = "explicit",
                partition = join(partitions, ";"),
                values = join(partition_values_per_profile, ";"),
                mean_values = join(mean_values_per_profile, ";"),
                year = year,
                location = location,
            ))

            if config.calc_stats
                push!(stats, (
                    asset = profile_name,
                    location = location,
                    rep_period = rep_period,
                    year = year,
                    errors = [ward_errors[s][j] for s in eachindex(ward_errors)],
                    ldc_errors = [ldc_errors[s][j] for s in eachindex(ldc_errors)],
                ))
            end
        end
    end
end