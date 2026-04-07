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
        config::ClusteringConfig = ClusteringConfig()
    )

Runs Ward clustering on profile tables stored in DuckDB.
"""
function cluster_partitions!(
    conn,
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

    if config.clustering_method == PerLocation
        cluster_partitions_per_location!(
            df, results, stats, config
        )
    elseif config.clustering_method == PerProfile
        cluster_partitions_per_profile!(
            df, results, stats, config
        )
    elseif config.clustering_method == DemandOverAvailabilities
        @assert config.extreme_preservation == NoExtremePreservation "Currently DemandOverAvailabilities is only available without extreme preservation"
        cluster_partitions_demand_over_availabilities!(
            df, results, stats, config
        )
    else
        println("No valid config.clustering_method: ", config.clustering_method)
        exit(1)
    end

    # =========================
    # OPTIONAL EXTREME UPDATE
    # =========================

    if config.extreme_preservation != NoExtremePreservation
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


function cluster_partitions_demand_over_availabilities!(
    df::DataFrame,
    results::DataFrame,
    stats::DataFrame,
    config::ClusteringConfig,
)

    for group_per_location in groupby(df, [:location, :rep_period, :year])

        rep_period = first(group_per_location.rep_period)
        year       = first(group_per_location.year)
        location   = first(group_per_location.location)

        sort!(group_per_location, [:profile_name, :timestep])

        profiles  = unique(group_per_location.profile_name)
        timesteps = unique(group_per_location.timestep)
        n_t       = length(timesteps)

        # ── Build a name → column-vector lookup ──────────────────────────────
        profile_series = Dict{String, Vector{Float64}}()
        for profile in profiles
            sub = group_per_location[group_per_location.profile_name .== profile, :]
            profile_series[profile] = sub.value
        end

        # ── Identify profiles by type ─────────────────────────────────────────
        by_type = Dict{ProfileType, Vector{String}}(
            Demand       => String[],
            Solar        => String[],
            WindOnshore  => String[],
            WindOffshore => String[],
            Unknown      => String[],
        )
        for p in profiles
            push!(by_type[getProfileType(p)], p)
        end

        # ── Build composite signal: Demand / (sum of available renewables) ────
        demand_sum = foldl(
            (acc, p) -> acc .+ profile_series[p],
            by_type[Demand];
            init = zeros(Float64, n_t)
        )

        avail_sum = foldl(
            (acc, p) -> acc .+ profile_series[p],
            (p for type in (Solar, WindOnshore, WindOffshore) for p in by_type[type]);
            init = fill(1e-6, n_t)
        )

        composite_matrix = reshape(demand_sum ./ avail_sum, :, 1)

        partitions,
        _,
        _,
        ward_errors,
        ldc_errors = hierarchical_time_clustering_ward(
            composite_matrix,
            [Demand],
            config
        )

        # ── Write results for every profile in this location ─────────────────
        for profile_name in profiles

            series = profile_series[profile_name]

            block_means = map(Iterators.accumulate(+, partitions)) do stop
                start = stop - partitions[findfirst(==(stop), Iterators.accumulate(+, partitions) |> collect)] + 1
                mean(series[start:stop])
            end

            timestep_cursor = 1
            block_means = Float64[]
            for p in partitions
                push!(block_means, mean(series[timestep_cursor : timestep_cursor + p - 1]))
                timestep_cursor += p
            end

            mean_str = join(block_means, ";")

            push!(results, (
                asset         = profile_name,
                rep_period    = rep_period,
                specification = "explicit",
                partition     = join(partitions, ";"),
                values        = mean_str,
                mean_values   = mean_str,
                year          = year,
                location      = location,
            ))

            if config.calc_stats
                push!(stats, (
                    asset      = profile_name,
                    location   = location,
                    rep_period = rep_period,
                    year       = year,
                    errors     = [ward_errors[s][1] for s in eachindex(ward_errors)],
                    ldc_errors = [ldc_errors[s][1] for s in eachindex(ldc_errors)],
                ))
            end
        end
    end
end