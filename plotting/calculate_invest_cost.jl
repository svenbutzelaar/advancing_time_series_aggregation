using CSV, DataFrames, Glob

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
    # Remove country prefix (first part), rejoin the rest
    if length(parts) < 2
        return nothing
    end
    type_parts = parts[2:end]
    type_str = join(type_parts, "_")

    for key in keys(INVESTMENT_COSTS)
        if type_str == key
            return key
        end
    end
    return nothing
end

"""
Calculate total and renewable investment costs for a single experiment CSV file.
"""
function calculate_costs(filepath::String)
    df = CSV.read(filepath, DataFrame)

    total_cost = 0.0
    renewable_cost = 0.0

    for row in eachrow(df)
        asset_type = get_asset_type(String(row.asset))
        if asset_type === nothing
            @warn "Unknown asset type for: $(row.asset) — skipping"
            continue
        end

        cost_per_unit = INVESTMENT_COSTS[asset_type]
        investment = cost_per_unit * row.solution

        total_cost += investment
        if asset_type in RENEWABLE_TYPES
            renewable_cost += investment
        end
    end

    return total_cost, renewable_cost
end

# ── Main ────────────────────────────────────────────────────────────────────

csv_files = glob("outputs/*/var_assets_investment.csv")

if isempty(csv_files)
    println("No files found matching outputs/*/var_assets_investment.csv")
    exit(1)
end

results = DataFrame(
    experiment_name          = String[],
    investment_cost          = Float64[],
    investment_cost_renewables = Float64[],
)

println("="^60)
println("Investment Cost Summary per Experiment")
println("="^60)

for filepath in sort(csv_files)
    # Extract experiment name from path: outputs/<experiment_name>/var_assets_investment.csv
    parts = splitpath(filepath)
    experiment_name = parts[2]   # index 1 = "outputs", index 2 = experiment name

    total_cost, renewable_cost = calculate_costs(filepath)

    println("Experiment : $experiment_name")
    println("  Total investment cost     : $(round(total_cost; digits=2))")
    println("  Renewable investment cost : $(round(renewable_cost; digits=2))")
    println()

    push!(results, (experiment_name, total_cost, renewable_cost))
end

println("="^60)

# ── Save CSV ─────────────────────────────────────────────────────────────────

output_dir = "plotting/csv_data"
mkpath(output_dir)
output_path = joinpath(output_dir, "investment_costs_summary.csv")
CSV.write(output_path, results)

println("Results saved to: $output_path")