using CSV, DataFrames, Glob
include("../cluster/config.jl")

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
Calculate investment costs per technology for a single experiment CSV file.
Returns a Dict mapping asset_type => investment_cost.
"""
function calculate_costs_per_technology(filepath::String)::Dict{String, Float64}
    df = CSV.read(filepath, DataFrame)
    costs = Dict{String, Float64}()
    for row in eachrow(df)
        asset_type = get_asset_type(String(row.asset))
        if asset_type === nothing
            @warn "Unknown asset type for: $(row.asset) — skipping"
            continue
        end
        costs[asset_type] = get(costs, asset_type, 0.0) + INVESTMENT_COSTS[asset_type] * row.solution
    end
    return costs
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
    exp_name = splitpath(filepath)[2]   # outputs/<experiment_name>/var_assets_investment.csv
    config   = get_config_from_experiment_name(exp_name)
    costs    = calculate_costs_per_technology(filepath)

    total_cost     = sum(values(costs))
    renewable_cost = sum(get(costs, t, 0.0) for t in RENEWABLE_TYPES)

    println("Experiment : $exp_name")
    println("  method        : $(config.extreme_preservation)")
    println("  num_clusters  : $(config.n_prime)")
    println("  Total investment cost     : $(round(total_cost;     digits=2))")
    println("  Renewable investment cost : $(round(renewable_cost; digits=2))")
    for (tech, cost) in sort(collect(costs))
        println("    $tech : $(round(cost; digits=2))")
    end
    println()

    push!(all_rows, (
        experiment_name            = exp_name,
        method                     = string(config.extreme_preservation),
        num_clusters               = config.n_prime,
        dependant_per_location     = config.dependant_per_location,
        high_percentile            = config.high_percentile,
        low_percentile             = config.low_percentile,
        tops_window                = config.tops_window,
        investment_cost            = total_cost,
        investment_cost_renewables = renewable_cost,
        technology_costs           = costs,
    ))
end

# ── Build wide DataFrame with one column per technology ──────────────────────
all_techs = sort(collect(keys(INVESTMENT_COSTS)))  # stable, deterministic column order

results = DataFrame(
    experiment_name            = String[],
    method                     = String[],
    num_clusters               = Int[],
    dependant_per_location     = Bool[],
    high_percentile            = Float64[],
    low_percentile             = Float64[],
    tops_window                = Int[],
    investment_cost            = Float64[],
    investment_cost_renewables = Float64[],
)

# Add technology columns separately
for t in all_techs
    results[!, Symbol("cost_", t)] = Float64[]
end

for row in all_rows
    push!(results, (
        row.experiment_name,
        row.method,
        row.num_clusters,
        row.dependant_per_location,
        row.high_percentile,
        row.low_percentile,
        row.tops_window,
        row.investment_cost,
        row.investment_cost_renewables,
        (get(row.technology_costs, t, 0.0) for t in all_techs)...,
    ))
end

# ── Save CSV ─────────────────────────────────────────────────────────────────
output_dir  = "plotting/csv_data"
mkpath(output_dir)
output_path = joinpath(output_dir, "investment_costs_summary.csv")
CSV.write(output_path, results)
println("Results saved to: $output_path")