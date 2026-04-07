using CSV, DataFrames, Plots, StatsPlots

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
tech_cols = filter(c -> startswith(string(c), "cost_"), names(df_filtered))
tech_labels = replace.(tech_cols, "cost_" => "")

# ── Build matrix: rows = num_clusters, cols = technologies ───────────────────
x_vals = df_filtered.num_clusters                          # Vector of x tick values
cost_matrix = Matrix(df_filtered[:, tech_cols])            # (n_experiments × n_techs)

# Convert to billions for readability
cost_matrix_bn = cost_matrix ./ 1e9

# ── Colour palette: one colour per technology ─────────────────────────────────
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
palette = [get(tech_colors, t, colorant"grey") for t in tech_labels]

# ── Plot ──────────────────────────────────────────────────────────────────────
p = groupedbar(
    x_vals,
    cost_matrix_bn;
    bar_position = :stack,
    label        = reshape(tech_labels, 1, :),
    color        = reshape(palette, 1, :),
    xlabel       = "Number of clusters",
    ylabel       = "Investment cost (billion €)",
    title        = "Investment costs by technology\nmethod: $chosen_method",
    legend       = :topright,
    size         = (900, 550),
    dpi          = 150,
    xticks       = (1:length(x_vals), string.(x_vals)),
    bar_width    = 0.7,
    linewidth    = 0,
    margin       = 8Plots.mm,
)

# ── Save ──────────────────────────────────────────────────────────────────────
output_dir  = "plotting/figures"
mkpath(output_dir)
safe_method = replace(chosen_method, r"[^a-zA-Z0-9_]" => "_")
output_path = joinpath(output_dir, "investment_costs_stacked_$(safe_method).png")
savefig(p, output_path)
println("Plot saved to: $output_path")