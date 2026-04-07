using CSV, DataFrames, Glob
include("../../cluster/config.jl")

# Investment costs per type (cost per unit)
const INVESTMENT_COSTS = Dict(
    "Wind_Onshore"  => 77356.32865703155,
    "Wind_Offshore" => 119732.61777406993,
    "Solar"         => 34342.98027538492,
    "Battery"       => 77577.8503057667,
    "Coal"          => 420000.0,
    "OCGT"          => 55000.0,
    "Gas"           => 95000.0,
    "Nuclear"       => 950000.0,
)
const RENEWABLE_TYPES = Set(["Wind_Onshore", "Wind_Offshore", "Solar"])

# Load asset capacities from the reference CSV
const ASSET_CAPACITY = let
    asset_df = CSV.read("inputs/db_files/obz-invest-full-resolution/asset.csv", DataFrame)
    Dict(String(row.asset) => Float64(row.capacity) for row in eachrow(asset_df))
end

"""
Determine the asset type from the asset name string.
Asset names follow the pattern: COUNTRY_Type or COUNTRY_Type_Subtype
e.g. AT_Gas, BE_Wind_Onshore, BE_Wind_Offshore
"""
function get_asset_type(asset::String)::Union{String, Nothing}
    parts = split(asset, "_")
    length(parts) < 2 && return nothing
    type_str = join(parts[2:end], "_")
    return haskey(INVESTMENT_COSTS, type_str) ? type_str : nothing
end

"""
Calculate investment costs and total capacity per technology for a single experiment CSV file.
Returns two Dicts mapping asset_type => investment_cost and asset_type => capacity.
"""
function calculate_costs_and_capacity_per_technology(filepath::String)
    df = CSV.read(filepath, DataFrame)
    costs      = Dict{String, Float64}()
    capacities = Dict{String, Float64}()
    for row in eachrow(df)
        asset_name = String(row.asset)
        asset_type = get_asset_type(asset_name)
        if asset_type === nothing
            @warn "Unknown asset type for: $asset_name — skipping"
            continue
        end
        unit_capacity = get(ASSET_CAPACITY, asset_name, 0.0)
        if unit_capacity == 0.0 && !haskey(ASSET_CAPACITY, asset_name)
            @warn "No capacity found for asset: $asset_name — capacity will be 0"
        end
        costs[asset_type]      = get(costs,      asset_type, 0.0) + INVESTMENT_COSTS[asset_type] * row.solution * unit_capacity
        capacities[asset_type] = get(capacities, asset_type, 0.0) + unit_capacity * row.solution
    end
    return costs, capacities
end

# ── Main ────────────────────────────────────────────────────────────────────
csv_files = glob("outputs/*/var_assets_investment.csv")
if isempty(csv_files)
    println("No files found matching outputs/*/var_assets_investment.csv")
    exit(1)
end

# Collect all rows first so we know the full set of technology columns
all_rows = []
for filepath in sort(csv_files)
    exp_name   = splitpath(filepath)[2]
    config     = get_config_from_experiment_name(exp_name)
    costs, capacities = calculate_costs_and_capacity_per_technology(filepath)

    total_cost     = sum(values(costs))
    renewable_cost = sum(get(costs, t, 0.0) for t in RENEWABLE_TYPES)
    total_capacity     = sum(values(capacities))
    renewable_capacity = sum(get(capacities, t, 0.0) for t in RENEWABLE_TYPES)

    println("Experiment : $exp_name")
    println("  method        : $(config.extreme_preservation)")
    println("  num_clusters  : $(config.n_prime)")
    println("  Total investment cost     : $(round(total_cost;         digits=2))")
    println("  Renewable investment cost : $(round(renewable_cost;     digits=2))")
    println("  Total capacity            : $(round(total_capacity;     digits=2))")
    println("  Renewable capacity        : $(round(renewable_capacity; digits=2))")
    for t in sort(collect(keys(costs)))
        println("    $t : cost=$(round(costs[t]; digits=2))  capacity=$(round(get(capacities, t, 0.0); digits=2))")
    end
    println()

    push!(all_rows, (
        experiment_name            = exp_name,
        method                     = string(config.extreme_preservation),
        num_clusters               = config.n_prime,
        clustering_method     = config.clustering_method,
        high_percentile            = config.high_percentile,
        low_percentile             = config.low_percentile,
        tops_window                = config.tops_window,
        investment_cost            = total_cost,
        investment_cost_renewables = renewable_cost,
        total_capacity             = total_capacity,
        renewables_capacity        = renewable_capacity,
        technology_costs           = costs,
        technology_capacities      = capacities,
    ))
end

# ── Build wide DataFrame with one column per technology ──────────────────────
all_techs = sort(collect(keys(INVESTMENT_COSTS)))  # stable, deterministic column order

results = DataFrame(
    experiment_name            = String[],
    method                     = String[],
    num_clusters               = Int[],
    clustering_method          = String[],
    high_percentile            = Float64[],
    low_percentile             = Float64[],
    tops_window                = Int[],
    investment_cost            = Float64[],
    investment_cost_renewables = Float64[],
    total_capacity             = Float64[],
    renewables_capacity        = Float64[],
)

for t in all_techs
    results[!, Symbol("cost_", t)] = Float64[]
end
for t in all_techs
    results[!, Symbol("capacity_", t)] = Float64[]
end

for row in all_rows
    push!(results, (
        row.experiment_name,
        row.method,
        row.num_clusters,
        row.clustering_method,
        row.high_percentile,
        row.low_percentile,
        row.tops_window,
        row.investment_cost,
        row.investment_cost_renewables,
        row.total_capacity,
        row.renewables_capacity,
        (get(row.technology_costs,      t, 0.0) for t in all_techs)...,
        (get(row.technology_capacities, t, 0.0) for t in all_techs)...,
    ))
end

# ── Save CSV ─────────────────────────────────────────────────────────────────
output_dir  = "plotting/csv_data"
mkpath(output_dir)
output_path = joinpath(output_dir, "investment_costs_summary.csv")
CSV.write(output_path, results)
println("Results saved to: $output_path")