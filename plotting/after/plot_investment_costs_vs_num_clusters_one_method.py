import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

# -----------------------------
# Settings
# -----------------------------
csv_path   = Path("plotting/csv_data/investment_costs_summary.csv")
output_dir = Path("plots/invest_per_num_clusters")
output_dir.mkdir(parents=True, exist_ok=True)

TECH_COLORS = {
    "Battery":       "#f4a261",
    "Coal":          "#6d6875",
    "Gas":           "#e76f51",
    "Nuclear":       "#e9c46a",
    "OCGT":          "#264653",
    "Solar":         "#FFD166",
    "Wind_Offshore": "#118ab2",
    "Wind_Onshore":  "#06d6a0",
}

NON_RENEWABLES = ["Coal", "Gas", "Nuclear", "OCGT"]

# -----------------------------
# Load & prepare
# -----------------------------
df = pd.read_csv(csv_path)

available_methods = df["method"].unique()
print("Available methods:")
for i, m in enumerate(available_methods):
    print(f"  {i+1}. {m}")
raw = input("Choose method (number or name): ").strip()
chosen_method = available_methods[int(raw) - 1] if raw.isdigit() else raw
assert chosen_method in available_methods, f"Unknown method: {chosen_method}"
print(f"Plotting method: {chosen_method}")

df = df[df["method"] == chosen_method].sort_values("num_clusters")
df = df[df["num_clusters"] <= 2000]
df = df[df["num_clusters"] >= 1000]

safe_method = "".join(c if c.isalnum() else "_" for c in chosen_method)

# -----------------------------
# Helper: build stacked-area plot
# -----------------------------
def make_stacked_plot(df, prefix, ylabel, title_label, unit_scale, formatter, filename):
    tech_cols  = [c for c in df.columns if c.startswith(prefix)]
    # tech_cols  = [c for c in df.columns if c.startswith(prefix) and not any(c.endswith(nr) for nr in NON_RENEWABLES)]
    tech_names = [c.removeprefix(prefix) for c in tech_cols]

    # Drop technologies that are zero across all rows
    nonzero = [(col, name) for col, name in zip(tech_cols, tech_names)
               if df[col].sum() > 0]
    tech_cols, tech_names = zip(*nonzero)

    tech_data = [df[col].values / unit_scale for col in tech_cols]
    colors    = [TECH_COLORS.get(name, "#aaaaaa") for name in tech_names]
    x         = df["num_clusters"].values

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.stackplot(x, *tech_data, labels=tech_names, colors=colors, alpha=0.85)

    ax.set_xlabel("Number of clusters", fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_title(f"{title_label} — {chosen_method}", fontsize=13, fontweight="bold")
    ax.set_xticks(x)
    ax.tick_params(axis="x", rotation=45)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(formatter))
    ax.legend(title="Technology", fontsize=10, loc="upper left")
    ax.grid(True, alpha=0.3, axis="y")
    plt.tight_layout()

    out_path = output_dir / filename
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"\nSaved to {out_path}")

    # Console summary
    summary = df[["num_clusters"] + list(tech_cols)].copy()
    summary[list(tech_cols)] /= unit_scale
    summary.columns = ["num_clusters"] + list(tech_names)
    print(summary.to_string(index=False))

    return tech_cols, tech_names

# -----------------------------
# Plot 1 — Investment costs
# -----------------------------
print("\n── Investment costs ──")
make_stacked_plot(
    df,
    prefix      = "cost_",
    ylabel      = "Investment cost (million €)",
    title_label = "Investment costs by technology",
    unit_scale  = 1e6,
    formatter   = lambda val, _: f"€{val:,.1f}M",
    filename    = f"investment_costs_stacked_{safe_method}_1000-2000.png",
)

# -----------------------------
# Plot 2 — Installed capacity
# -----------------------------
print("\n── Installed capacity ──")
make_stacked_plot(
    df,
    prefix      = "capacity_",
    ylabel      = "Installed capacity (GW)",
    title_label = "Installed capacity by technology - rve",
    unit_scale  = 1e3,          # assume MW → GW; adjust to 1.0 if already in GW
    formatter   = lambda val, _: f"{val:,.1f} GW",
    filename    = f"investment_capacity_stacked_{safe_method}_1000-2000.png",
)