import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

# -----------------------------
# Settings
# -----------------------------
csv_path = Path("plotting/csv_data/regret.csv")
output_dir = Path("plots/regret")
output_dir.mkdir(parents=True, exist_ok=True)

ENS_COST_PER_UNIT = 68887

# -----------------------------
# Load & prepare data
# -----------------------------
df = pd.read_csv(csv_path)

df = df[(df["method"] == "SeperateExtremesSum") |  (df["method"] == "Afterwards") |  (df["method"] == "NoExtremePreservation") |  (df["method"] == "base_case")]

# Clean up method names if needed
df['method'] = df['method'].str.strip()

df.loc[df["method"] == "SeperateExtremesSum", "method"] = "SeperateExtremes"

df["ens_cost"] = df["energy_not_served"] * ENS_COST_PER_UNIT
df["total_cost"] = (
    df["investment_cost"]
    + df["true_operational_cost"]
    - df["ens_cost"]   # operational cost already includes ENS; add explicit ENS cost
    + df["ens_cost"]
)
# Simpler: total = investment + operational (which includes ENS implicitly) + explicit ENS penalty
# Based on original script: total_regret = ens_cost + operational_cost_without_ens + investment_cost
#   where operational_cost_without_ens = true_operational_cost - ens_cost
# So: total_regret = true_operational_cost + investment_cost  (ens_cost cancels out, then re-added)
# Actually from original: total = (true_op - ens_cost) + ens_cost + investment = true_op + investment
# But that's just true_op + investment. Let's stay consistent with original script:
df["ens_cost"] = df["energy_not_served"] * ENS_COST_PER_UNIT
df["operational_cost_without_ens"] = df["true_operational_cost"] - df["ens_cost"]
df["total_regret"] = df["ens_cost"] + df["operational_cost_without_ens"] + df["investment_cost"]

# Baseline: 8760 clusters (one row, no method ambiguity assumed)
baseline_row = df[df["num_clusters"] == 8760]
assert len(baseline_row) == 1, f"Expected 1 baseline row, got {len(baseline_row)}"
baseline_value = baseline_row["total_regret"].values[0]

df["relative_regret"] = (df["total_regret"] - baseline_value) * 100 / baseline_value

# Exclude baseline from line plots (it's a single point at 8760, no line to draw)
plot_df = df[df["num_clusters"] != 8760].copy()

methods = sorted(plot_df["method"].unique())
# x_vals = sorted(plot_df["num_clusters"].unique())
x_vals = list(range(0, 8760, 1000))

# Colour palette — one colour per method
colors = plt.cm.tab10.colors
method_colors = {m: colors[i % len(colors)] for i, m in enumerate(methods)}

no_ep = "NoExtremePreservation"
other_methods = [m for m in methods if m != no_ep]

# -----------------------------
# Figure: two-panel layout
# Main panel  — all methods except NoExtremePreservation
# Inset panel — all methods including NoExtremePreservation (log scale)
# -----------------------------
fig, (ax_main, ax_log) = plt.subplots(
    1, 2,
    figsize=(14, 6),
    gridspec_kw={"width_ratios": [2, 1]},
)

# --- Main panel ---
for method in other_methods:
    sub = plot_df[plot_df["method"] == method].sort_values("num_clusters")
    ax_main.plot(
        sub["num_clusters"],
        sub["relative_regret"],
        marker="o",
        label=method,
        color=method_colors[method],
        linewidth=2,
        markersize=6,
    )

ax_main.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
ax_main.set_xlabel("Number of clusters", fontsize=12)
ax_main.set_ylabel("Relative regret vs. baseline (%)", fontsize=12)
ax_main.set_title("Relative regret — excluding NoExtremePreservation", fontsize=13, fontweight="bold")
ax_main.set_xticks(x_vals)
ax_main.legend(title="Method", fontsize=10)
ax_main.grid(True, alpha=0.3)

# --- Log panel (all methods incl. No EP) ---
for method in methods:
    sub = plot_df[plot_df["method"] == method].sort_values("num_clusters")
    # log scale needs positive values; shift so minimum > 0
    ax_log.plot(
        sub["num_clusters"],
        sub["relative_regret"],
        marker="o",
        label=method,
        color=method_colors[method],
        linewidth=2,
        markersize=5,
        linestyle="--" if method == no_ep else "-",
    )

ax_log.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
ax_log.set_yscale("symlog", linthresh=10)   # symlog handles negative + large positive
ax_log.yaxis.set_major_formatter(mticker.ScalarFormatter())
ax_log.set_xlabel("Number of clusters", fontsize=12)
ax_log.set_ylabel("Relative regret (%, symlog scale)", fontsize=12)
ax_log.set_title("All methods\n(symlog scale)", fontsize=13, fontweight="bold")
ax_log.set_xticks(x_vals)
ax_log.tick_params(axis="x", rotation=45)
ax_log.legend(title="Method", fontsize=9)
ax_log.grid(True, alpha=0.3)

plt.tight_layout()
out_path = output_dir / "relative_regret_vs_clusters.png"
plt.savefig(out_path, dpi=150)
plt.close()

print(f"Saved to {out_path}")
print("\nRelative regret values:")
print(df[["method", "num_clusters", "relative_regret"]].to_string(index=False))