import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# -----------------------------
# Settings
# -----------------------------
csv_path = Path("plotting/csv_data/regret.csv")
output_dir = Path("plots/regret")
output_dir.mkdir(parents=True, exist_ok=True)

ENS_COST_PER_UNIT = 68887

DATASET_VARIANTS = ["lowvar", "basedataset", "highvar"]
DATASET_LABELS = {
    "lowvar": "Low variance",
    "basedataset": "Base dataset",
    "highvar": "High variance",
}

# Keep same colors as original code
colors = plt.cm.tab10.colors
HC_COLOR = colors[1]
EAC_COLOR = colors[3]

# -----------------------------
# Helpers
# -----------------------------
def dataset_from_file_name(file_name):
    for ds in DATASET_VARIANTS:
        if ds in file_name:
            return ds
    return None


def method_from_file_name(file_name):
    if "perlocation_NoExtremePreservation" in file_name:
        return "HC"
    if "perlocation_SeperateExtremesSum" in file_name:
        return "EAC"
    return None


# -----------------------------
# Load data
# -----------------------------
df = pd.read_csv(csv_path)

df["dataset"] = df["file_name"].apply(dataset_from_file_name)
df["method"] = df["file_name"].apply(method_from_file_name)

df = df[
    df["dataset"].notna()
    & df["method"].notna()
]

df["ens_cost"] = df["energy_not_served"] * ENS_COST_PER_UNIT
df["operational_cost_without_ens"] = (
    df["true_operational_cost"] - df["ens_cost"]
)
df["total_cost"] = (
    df["investment_cost"]
    + df["operational_cost_without_ens"]
    + df["ens_cost"]
)

# -----------------------------
# Relative regret
# -----------------------------
records = []

for dataset, grp in df.groupby("dataset"):
    baseline = grp.loc[
        grp["num_clusters"].idxmax(),
        "total_cost"
    ]

    tmp = grp.copy()
    tmp["relative_regret"] = (
        (tmp["total_cost"] - baseline)
        / baseline
        * 100
    )

    records.append(tmp)

df = pd.concat(records, ignore_index=True)

# Remove baseline row
df = df[df["num_clusters"] != 8760]

# Restrict x-range
df = df[df["num_clusters"] <= 2000]

# -----------------------------
# Plot
# -----------------------------
fig, axes = plt.subplots(
    1,
    3,
    figsize=(15, 4.5),
    sharey=False,   # each subplot gets its own y-scale
)

for ax, dataset in zip(axes, DATASET_VARIANTS):

    sub = (
        df[df["dataset"] == dataset]
        .sort_values("num_clusters")
    )

    for method, color in [
        ("HC", HC_COLOR),
        ("EAC", EAC_COLOR),
    ]:

        msub = sub[sub["method"] == method]

        ax.plot(
            msub["num_clusters"],
            msub["relative_regret"],
            marker="o",
            linewidth=2,
            color=color,
            label=method,
        )

    ax.set_title(DATASET_LABELS[dataset])
    ax.set_xlabel("Number of clusters")
    ax.grid(True, alpha=0.3)

axes[0].set_ylabel("Relative regret (%)")

handles, labels = axes[0].get_legend_handles_labels()
fig.legend(
    handles,
    labels,
    loc="lower center",
    ncol=2,
    frameon=False,
)

fig.suptitle(
    "Per-location clustering: HC versus EAC across dataset variants",
    fontsize=14,
    fontweight="bold",
)

plt.tight_layout(rect=[0, 0.08, 1, 1])

plt.savefig(
    output_dir / "regret_datasets.png",
    dpi=300,
    bbox_inches="tight",
)
