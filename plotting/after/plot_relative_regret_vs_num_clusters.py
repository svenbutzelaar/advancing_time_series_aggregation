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
# Experiment label mapping
# Edit this dict to rename experiments in the legend.
# Key:   substring matched against file_name (checked in order)
# Value: label shown in the plot
# -----------------------------
EXPERIMENT_LABELS = {
    # "demandoveravailabilities": "Demand/Avail.",
    "utr":                       "UTR",
    # "global_NoExtremePreservation": "HC (global)",
    # "perprofile_NoExtremePreservation": "HC (fully flexible)",
    "perlocation_NoExtremePreservation": "HC",
    "perlocation_SeperateExtremesSum":   "EAC",
    # "perprofile_SeperateExtremesSum":   "EAC (fully flexible)",
    "perlocation_Afterwards":   "PEC",
    "perlocation_DynamicProgramming_hp":    "DP",
    # "perprofile_DynamicProgramming":    "DP (fully flexible)",
    # "perlocation_DynamicProgramming_s672":    "DP (672)",
    # "perlocation_DynamicProgramming_s2688":    "DP (2688)",
    "base_case":                 "Base case",
}

LEGEND_ORDER = [
    "UTR",
    "HC",
    "HC (global)",
    "HC (fully flexible)",
    "PEC",
    "EAC",
    "EAC (fully flexible)",
    "DP",
    "DP (fully flexible)"
]

legend_names = [l for l in LEGEND_ORDER if l in EXPERIMENT_LABELS.values()]

assert len(set(EXPERIMENT_LABELS.values()).difference(['Base case'] + legend_names)) == 0, f"You missed defining the order of the labels: {set(EXPERIMENT_LABELS.values()).difference(['Base case'] + legend_names)}"

# Which labels should appear dashed + faded in the main panel
METHODS_ON_FOR_LOG_SCALE = {
    "HC (global)",
    "UTR",
    "EAC (fully flexible)",
    "HC",
}

def label_from_file_name(file_name: str) -> str:
    """Map a file_name to a display label using EXPERIMENT_LABELS."""
    for key, label in EXPERIMENT_LABELS.items():
        if key in file_name:
            return label
    return file_name  # fallback: show raw name


# -----------------------------
# Load & prepare data
# -----------------------------
df = pd.read_csv(csv_path)

# Derive display label from file_name
df["label"] = df["file_name"].apply(label_from_file_name)

# Keep only experiments that appear in EXPERIMENT_LABELS
known_labels = set(EXPERIMENT_LABELS.values())
df = df[df["label"].isin(known_labels)]

df["ens_cost"] = df["energy_not_served"] * ENS_COST_PER_UNIT
df["operational_cost_without_ens"] = df["true_operational_cost"] - df["ens_cost"]
df["total_regret"] = df["ens_cost"] + df["operational_cost_without_ens"] + df["investment_cost"]

baseline_row = df[df["num_clusters"] == 8760]
assert len(baseline_row) == 1, f"Expected 1 baseline row, got {len(baseline_row)}"
baseline_value = baseline_row["total_regret"].values[0]

df["relative_regret"] = (df["total_regret"] - baseline_value) * 100 / baseline_value

plot_df = df[df["num_clusters"] != 8760].copy()

labels = [l for l in LEGEND_ORDER if l in plot_df["label"].unique()]
x_vals = list(range(0, 8760, 1000))

colors = plt.cm.tab10.colors
label_colors = {lbl: colors[i % len(colors)] for i, lbl in enumerate(labels)}

# -----------------------------
# Plot
# -----------------------------
fig, (ax_main, ax_log) = plt.subplots(
    1, 2,
    figsize=(14, 6),
    gridspec_kw={"width_ratios": [2, 1]},
)

y_max = plot_df[
    ~plot_df["label"].isin(METHODS_ON_FOR_LOG_SCALE)
]["relative_regret"].max()

for ax, use_log in [(ax_main, False), (ax_log, True)]:
    for lbl in labels:
        sub = plot_df[plot_df["label"] == lbl].sort_values("num_clusters")
        is_log_only = lbl in METHODS_ON_FOR_LOG_SCALE
        ax.plot(
            sub["num_clusters"],
            sub["relative_regret"],
            marker="o",
            label=lbl,
            color=label_colors[lbl],
            linewidth=2,
            markersize=6 if not use_log else 5,
            # linestyle="--" if is_log_only else "-",
            alpha=0.7 if is_log_only else 1.0,
        )

    ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
    ax.set_xlabel("Number of clusters", fontsize=12)
    ax.set_xticks(x_vals)
    ax.tick_params(axis="x", rotation=45)
    ax.legend(title="Method", fontsize=9 if use_log else 10)
    ax.grid(True, alpha=0.3)

ax_main.set_ylabel("Relative regret (%)", fontsize=12)
ax_main.set_title("Relative regret (linear scale)", fontsize=13, fontweight="bold")
ax_main.set_ylim(top=y_max, bottom=-1)

ax_log.set_yscale("symlog", linthresh=10)
ax_log.set_ylim(bottom=-1)
ax_log.yaxis.set_major_formatter(mticker.ScalarFormatter())
ax_log.set_ylabel("Relative regret (%, symlog scale)", fontsize=12)
ax_log.set_title("Relative regret\n(symlog scale)", fontsize=13, fontweight="bold")

plt.tight_layout()
out_path = output_dir / "relative_regret_vs_clusters.png"
plt.savefig(out_path, dpi=150)
plt.close()

print(f"Saved to {out_path}")
print("\nRelative regret values:")
print(df[["label", "num_clusters", "relative_regret"]].to_string(index=False))