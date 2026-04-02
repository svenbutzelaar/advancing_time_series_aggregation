using CSV, DataFrames, Plots

# ── Load data ────────────────────────────────────────────────────────────────
input_path = "plotting/csv_data/investment_costs_summary.csv"
df = CSV.read(input_path, DataFrame)

# ── Choose method ────────────────────────────────────────────────────────────
available_methods = unique(df.method)
println("Available methods:")
for (i, m) in enumerate(available_methods)
    println("  $i. $m")
end
print("Choose method (number or name): ")
input = strip(readline())

chosen_method = if all(isdigit, input)
    available_methods[parse(Int, input)]
else
    input
end

@assert chosen_method in available_methods "Unknown method: $chosen_method"
println("Plotting method: $chosen_method")

# ── Filter & sort ─────────────────────────────────────────────────────────────
df_filtered = sort(filter(row -> row.method == chosen_method, df), :num_clusters)

if nrow(df_filtered) == 0
    println("No rows found for method: $chosen_method")
    exit(1)
end

# ── Identify technology cost columns ─────────────────────────────────────────
tech_cols  = filter(c -> startswith(c, "cost_"), names(df_filtered))
tech_labels = replace.(tech_cols, "cost_" => "")

# ── Build matrix: (n_experiments × n_techs), scaled to billions ──────────────
x_labels    = string.(df_filtered.num_clusters)   # categorical string labels
cost_matrix = Matrix(df_filtered[:, tech_cols]) ./ 1e6   # scale to millions

# Drop technologies with all-zero costs (keeps the legend clean)
nonzero_mask  = vec(any(cost_matrix .!= 0, dims=1))
cost_matrix   = cost_matrix[:, nonzero_mask]
tech_labels   = tech_labels[nonzero_mask]

n_experiments, n_techs = size(cost_matrix)

# ── Colour palette ────────────────────────────────────────────────────────────
tech_colors = Dict(
    "Battery"       => colorant"#f4a261",
    "Coal"          => colorant"#6d6875",
    "Gas"           => colorant"#e76f51",
    "Nuclear"       => colorant"#e9c46a",
    "OCGT"          => colorant"#264653",
    "Solar"         => colorant"#FFD166",
    "Wind_Offshore" => colorant"#118ab2",
    "Wind_Onshore"  => colorant"#06d6a0",
)

# ── Build stacked bar plot manually ──────────────────────────────────────────
x_positions = 1:n_experiments
p = plot(
    size        = (900, 550),
    dpi         = 150,
    xlabel      = "Number of clusters",
    ylabel      = "Investment cost (million €)",
    title       = "Investment costs by technology\nmethod: $chosen_method",
    legend      = :topright,
    xticks      = (x_positions, x_labels),
    margin      = 8Plots.mm,
    bar_width   = 0.7,
)

bottoms = zeros(n_experiments)
for (i, tech) in enumerate(tech_labels)
    values = cost_matrix[:, i]
    bar!(
        p,
        x_positions,
        values;
        bottom    = bottoms,
        label     = tech,
        color     = get(tech_colors, tech, colorant"grey"),
        linewidth = 0,
    )
    bottoms .+= values
end

# ── Save ──────────────────────────────────────────────────────────────────────
output_dir  = "plotting/figures"
mkpath(output_dir)
safe_method = replace(chosen_method, r"[^a-zA-Z0-9_]" => "_")
output_path = joinpath(output_dir, "investment_costs_stacked_$(safe_method).png")
savefig(p, output_path)
println("Plot saved to: $output_path")