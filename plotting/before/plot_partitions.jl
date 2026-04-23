using Plots
using DataFrames
using Statistics
using DuckDB: DBInterface, DuckDB
using FilePathsBase
using Printf
include("../../cluster/config.jl")

# ----------------------------
# Settings
# ----------------------------

config = @isdefined(CONFIG) ? CONFIG : ClusteringConfig()

println("Using config: ", config)
file_name = experiment_name(config)
db_name = "db_files/$file_name.db"


profiles = [
    "NL_E_Demand",
    "NL_Solar",
    "NL_Wind_Offshore",
    "NL_Wind_Onshore"
]

hours = 72 * 7
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

df_old = copy(df)
if config.extreme_preservation != NoExtremePreservation
    df_old = DataFrame(DBInterface.execute(
        connection,
        """
        SELECT 
            profile_name,
            rep_period,
            timestep,
            year,
            value
        FROM profiles_rep_periods_old
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
end
    

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
function plot_profile_with_partitions(df, df_old, profile_name, clusters; hours=72)

    df_profile = filter(row -> row.profile_name == profile_name, df)
    sort!(df_profile, :timestep)

    df_profile = df_profile[1:hours, :]

    values = df_profile.value
    timesteps = df_profile.timestep

    # --- Old values (blue line) ---
    df_profile_old = filter(row -> row.profile_name == profile_name, df_old)
    sort!(df_profile_old, :timestep)
    df_profile_old = df_profile_old[1:hours, :]
    values_old = df_profile_old.value

    p = plot(
        timesteps,
        values_old,
        color = :blue,
        linewidth = 2,
        label = "Old values",
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
            label = "Adjusted values"
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

function plot_profile_with_partitions_one_line(df, df_old, profile_name, clusters; hours=72)

    # --- Current values (red line) ---
    df_profile = filter(row -> row.profile_name == profile_name, df)
    sort!(df_profile, :timestep)
    df_profile = df_profile[1:hours, :]
    values = df_profile.value
    timesteps = df_profile.timestep

    # --- Old values (blue line) ---
    df_profile_old = filter(row -> row.profile_name == profile_name, df_old)
    sort!(df_profile_old, :timestep)
    df_profile_old = df_profile_old[1:hours, :]
    values_old = df_profile_old.value

    # Base plot: old values as blue line
    p = plot(
        timesteps,
        values_old,
        color = :blue,
        linewidth = 2,
        label = "Old values",
        title = profile_name,
        xlabel = "Hour",
        ylabel = "Value"
    )

    # -------------------------------------------------
    # Build continuous partition mean vector for new values
    # -------------------------------------------------
    partition_mean = similar(values)

    idx = 1
    for cluster_size in clusters
        cluster_end = min(idx + cluster_size - 1, hours)
        mean_val = mean(values[idx:cluster_end])
        partition_mean[idx:cluster_end] .= mean_val
        idx += cluster_size
        idx > hours && break
    end

    # Plot new partition-adjusted values as red line
    plot!(
        timesteps,
        partition_mean,
        color = :red,
        linewidth = 2,
        label = "Adjusted values"
    )

    return p
end

# ----------------------------
# Generate & Save Plots
# ----------------------------
parts_dir = "plots/partitions/$(db_name)_parts"
one_line_dir = "plots/partitions/$(db_name)_one_line"

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

    p = plot_profile_with_partitions(df, df_old, profile, clusters; hours=hours)

    # Save individual plot
    filename = joinpath(parts_dir, "$(profile)_partitions.png")
    savefig(p, filename)
    println("Saved: ", filename)

    push!(individual_plots, p)

    p = plot_profile_with_partitions_one_line(df, df_old, profile, clusters; hours=hours)

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


# ----------------------------
# Ratio plot: old vs adjusted (using partitions)
# ----------------------------

function get_profile(df, name; hours=72)
    df_profile = filter(row -> row.profile_name == name, df)
    sort!(df_profile, :timestep)
    return df_profile[1:hours, :]
end

function build_partition_mean(values, clusters, hours)
    out = similar(values)
    idx = 1
    for cluster_size in clusters
        cluster_end = min(idx + cluster_size - 1, hours)
        mean_val = mean(values[idx:cluster_end])
        out[idx:cluster_end] .= mean_val
        idx += cluster_size
        idx > hours && break
    end
    return out
end

# --- Load profiles (OLD = raw values) ---
df_demand = get_profile(df, "NL_E_Demand"; hours=hours)
df_solar  = get_profile(df, "NL_Solar"; hours=hours)
df_won    = get_profile(df, "NL_Wind_Onshore"; hours=hours)
df_woff   = get_profile(df, "NL_Wind_Offshore"; hours=hours)

timesteps = df_demand.timestep

demand_old = df_demand.value
solar_old  = df_solar.value
won_old    = df_won.value
woff_old   = df_woff.value

# --- Build adjusted (partition mean) series ---
clusters_demand = partition_dict["NL_E_Demand"]
clusters_solar  = partition_dict["NL_Solar"]
clusters_won    = partition_dict["NL_Wind_Onshore"]
clusters_woff   = partition_dict["NL_Wind_Offshore"]

demand_adj = build_partition_mean(demand_old, clusters_demand, hours)
solar_adj  = build_partition_mean(solar_old,  clusters_solar,  hours)
won_adj    = build_partition_mean(won_old,    clusters_won,    hours)
woff_adj   = build_partition_mean(woff_old,   clusters_woff,   hours)

# --- Compute ratios ---
ϵ = 1e-6

ratio_old = demand_old ./ (solar_old .+ won_old .+ woff_old .+ ϵ)
ratio_adj = demand_adj ./ (solar_adj .+ won_adj .+ woff_adj .+ ϵ)

# --- Plot ---
p_ratio = plot(
    timesteps,
    ratio_old,
    label = "Old values",
    linewidth = 2,
    color = :blue,
    xlabel = "Hour",
    ylabel = "Demand / Renewables",
    title = "Demand / (Solar + Wind Onshore + Wind Offshore)"
)

plot!(
    timesteps,
    ratio_adj,
    label = "Adjusted (partition means)",
    linewidth = 2,
    color = :red
)

# --- Save ---
ratio_dir = "plots/ratios"
mkpath(ratio_dir)

filename = joinpath(ratio_dir, "NL_ratio_old_vs_adjusted.png")
savefig(p_ratio, filename)

println("Saved ratio comparison plot: ", filename)