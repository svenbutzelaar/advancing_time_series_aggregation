using Plots
using DataFrames
using Statistics
using DuckDB: DBInterface, DuckDB
using FilePathsBase
using Printf

# ----------------------------
# Settings
# ----------------------------

db_name = "obz_test.db"

profiles = [
    "NL_E_Demand",
    "NL_Solar",
    "NL_Wind_Offshore",
    "NL_Wind_Onshore"
]

hours = 72 * 2
rep_period = 1      # change if needed
year = 2050         # change if needed

# ----------------------------
# Connect
# ----------------------------

connection = DBInterface.connect(DuckDB.DB, db_name)

nice_query(str) = DataFrame(DuckDB.query(connection, str))

# ----------------------------
# Load time series values
# ----------------------------

# print(nice_query("SELECT DISTINCT(asset) FROM assets_rep_periods_partitions WHERE 
#         rep_period = $rep_period
#         AND year = $year
#         AND specification = 'explicit'
#         AND asset IN ('NL_E_Demand',
#                       'NL_Solar',
#                       'NL_Wind_Offshore',
#                       'NL_Wind_Onshore')
#                       "))

df = DataFrame(DBInterface.execute(
    connection,
    """
    SELECT 
        profile_name,
        rep_period,
        timestep,
        year,
        value
    FROM profiles_rep_periods
    WHERE 
            value IS NOT NULL
        AND rep_period = $rep_period
        AND year = $year
        AND profile_name IN ('NL_E_Demand',
                             'NL_Solar',
                             'NL_Wind_Offshore',
                             'NL_Wind_Onshore')
    ORDER BY profile_name, timestep
    """
))

# ----------------------------
# Load partitions
# ----------------------------

df_partitions = DataFrame(DBInterface.execute(
    connection,
    """
    SELECT 
        asset AS profile_name,
        partition
    FROM assets_rep_periods_partitions
    WHERE 
        rep_period = $rep_period
        AND year = $year
        AND specification = 'explicit'
        AND asset IN ('NL_E_Demand',
                      'NL_Solar',
                      'NL_Wind_Offshore',
                      'NL_Wind_Onshore')
    """
))


# Convert partition string to Vector{Int}
function parse_partition_string(s)
    parse.(Int, split(s, ";"))
end

partition_dict = Dict(
    row.profile_name => parse_partition_string(row.partition)
    for row in eachrow(df_partitions)
)

# ----------------------------
# Plotting function
# ----------------------------
function plot_profile_with_partitions(df, profile_name, clusters; hours=72)

    df_profile = filter(row -> row.profile_name == profile_name, df)
    sort!(df_profile, :timestep)

    df_profile = df_profile[1:hours, :]

    values = df_profile.value
    timesteps = df_profile.timestep

    p = plot(
        timesteps,
        values,
        linewidth = 2,
        label = "Value",
        title = profile_name,
        xlabel = "Hour",
        ylabel = "Value",
        legend = false
    )

    idx = 1
    for cluster_size in clusters

        cluster_end = min(idx + cluster_size - 1, hours)
        mean_val = mean(values[idx:cluster_end])

        # ---- Horizontal red partition line ----
        plot!(
            timesteps[idx:cluster_end],
            fill(mean_val, cluster_end - idx + 1),
            color = :red,
            linewidth = 4,
            label = false
        )

        # ---- Add red dot if partition size == 1 ----
        if cluster_size == 1 && idx <= hours
            scatter!(
                [timesteps[idx]],
                [values[idx]],
                color = :red,
                markersize = 4,
                markerstrokecolor = :red,
                label = false
            )
        end

        idx += cluster_size
        idx > hours && break
    end

    return p
end

function plot_profile_with_partitions_one_line(df, profile_name, clusters; hours=72)

    df_profile = filter(row -> row.profile_name == profile_name, df)
    sort!(df_profile, :timestep)

    df_profile = df_profile[1:hours, :]

    values = df_profile.value
    timesteps = df_profile.timestep

    # Base plot (no legend)
    p = plot(
        timesteps,
        values,
        linewidth = 2,
        title = profile_name,
        xlabel = "Hour",
        ylabel = "Value",
        legend = false
    )

    # -------------------------------------------------
    # Build continuous partition mean vector
    # -------------------------------------------------

    partition_mean = similar(values)

    idx = 1
    for cluster_size in clusters

        cluster_end = min(idx + cluster_size - 1, hours)

        mean_val = mean(values[idx:cluster_end])

        # Fill ALL hours in this partition
        partition_mean[idx:cluster_end] .= mean_val

        idx += cluster_size
        idx > hours && break
    end

    # -------------------------------------------------
    # Plot one continuous red line
    # -------------------------------------------------

    plot!(
        timesteps,
        partition_mean,
        color = :red,
        linewidth = 2,
        label = false
    )

    return p
end

# ----------------------------
# Generate & Save Plots
# ----------------------------
parts_dir = "plots/partitions_with_extreme_parts"
one_line_dir = "plots/partitions_with_extreme_one_line"

mkpath(parts_dir)
mkpath(one_line_dir)

individual_plots = []
individual_plots_one_line = []

for profile in profiles

    if haskey(partition_dict, profile)
        clusters = partition_dict[profile]
    else
        @warn "No partition found for $profile — skipping"
        continue
    end

    p = plot_profile_with_partitions(df, profile, clusters; hours=hours)

    # Save individual plot
    filename = joinpath(parts_dir, "$(profile)_partitions.png")
    savefig(p, filename)
    println("Saved: ", filename)

    push!(individual_plots, p)

    p = plot_profile_with_partitions_one_line(df, profile, clusters; hours=hours)

    # Save individual plot
    filename = joinpath(one_line_dir, "$(profile)_partitions.png")
    savefig(p, filename)
    println("Saved: ", filename)

    push!(individual_plots_one_line, p)
end


# ----------------------------
# Create Combined Plot
# ----------------------------

if length(individual_plots) == 4

    p_all = plot(
        individual_plots...,
        layout = (4,1),
        size = (900, 1200),
        link = :x
    )

    combined_filename = joinpath(parts_dir, "NL_all_partitions.png")
    savefig(p_all, combined_filename)

    println("Saved combined plot: ", combined_filename)
else
    @warn "Combined plot not created — not all 4 profiles available."
end

if length(individual_plots_one_line) == 4

    p_all = plot(
        individual_plots_one_line...,
        layout = (4,1),
        size = (900, 1200),
        link = :x
    )

    combined_filename = joinpath(one_line_dir, "NL_all_partitions.png")
    savefig(p_all, combined_filename)

    println("Saved combined plot: ", combined_filename)
else
    @warn "Combined plot not created — not all 4 profiles available."
end