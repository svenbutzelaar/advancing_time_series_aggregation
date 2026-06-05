import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path
import re

# -----------------------------
# Settings
# -----------------------------
csv_path = Path("plotting/csv_data/regret.csv")
output_dir = Path("plots/regret")
output_dir.mkdir(parents=True, exist_ok=True)

ENS_COST_PER_UNIT = 68887

# -----------------------------
# Experiment label mapping
# Key: substring matched against file_name (checked in order)
# Value: label shown in the plot
# -----------------------------
EXPERIMENT_LABELS = {
    "utr":                               "UTR",
    "perlocation_NoExtremePreservation": "HC",
    "perprofile_NoExtremePreservation":  "HC",
    "perlocation_SeperateExtremesSum":   "EAC",
    "perprofile_SeperateExtremesSum":    "EAC",
    "perlocation_Afterwards":            "PEC",
    "perprofile_Afterwards":             "PEC",
    "perlocation_DynamicProgramming_hp": "DP",
    "perprofile_DynamicProgramming_hp":  "DP",
    "base_case":                         "Base case",
}

LEGEND_ORDER = ["UTR", "HC", "PEC", "EAC", "DP"]

DATASET_VARIANTS = ["basedataset", "lowvar", "highvar"]
DATASET_LABELS   = {"basedataset": "Base dataset", "lowvar": "Low variance", "highvar": "High variance"}

SCOPES = ["perlocation", "perprofile"]
SCOPE_LABELS = {"perlocation": "Per location", "perprofile": "Per profile"}

# Methods to show faded/alpha on log panel
METHODS_FADED = {"UTR"}

# -----------------------------
# Parsing helpers
# -----------------------------
def label_from_file_name(file_name: str) -> str:
    for key, label in EXPERIMENT_LABELS.items():
        if key in file_name:
            return label
    return file_name


def scope_from_file_name(file_name: str) -> str | None:
    if "perlocation" in file_name:
        return "perlocation"
    if "perprofile" in file_name:
        return "perprofile"
    if "utr" in file_name:
        return "utr"   # UTR goes into both scopes (same data)
    return None


def dataset_from_file_name(file_name: str) -> str | None:
    for ds in DATASET_VARIANTS:
        if ds in file_name:
            return ds
    return None


# -----------------------------
# Load & prepare data
# -----------------------------
df = pd.read_csv(csv_path)

df["label"]   = df["file_name"].apply(label_from_file_name)
df["scope"]   = df["file_name"].apply(scope_from_file_name)
df["dataset"] = df["file_name"].apply(dataset_from_file_name)

# Keep only rows with a known label
known_labels = set(EXPERIMENT_LABELS.values())
df = df[df["label"].isin(known_labels)]

df["ens_cost"] = df["energy_not_served"] * ENS_COST_PER_UNIT
df["operational_cost_without_ens"] = df["true_operational_cost"] - df["ens_cost"]
df["total_cost"] = df["ens_cost"] + df["operational_cost_without_ens"] + df["investment_cost"]

# -----------------------------
# Compute baseline per dataset
# (num_clusters == 8760, or fallback: max clusters)
# -----------------------------
def get_baseline(sub_df: pd.DataFrame) -> float:
    bl = sub_df[sub_df["num_clusters"] == 8760]
    if len(bl) == 1:
        return bl["total_cost"].values[0]
    # fallback: largest cluster count
    idx = sub_df["num_clusters"].idxmax()
    return sub_df.loc[idx, "total_cost"]


# Compute relative regret within each (dataset) group using its own baseline
records = []
for dataset, grp in df.groupby("dataset"):
    baseline_value = get_baseline(grp)
    grp = grp.copy()
    grp["relative_regret"] = (grp["total_cost"] - baseline_value) * 100 / baseline_value
    records.append(grp)

df = pd.concat(records, ignore_index=True)

# UTR rows: duplicate into both scopes so they appear in every plot
utr_rows = df[df["scope"] == "utr"].copy()
utr_perlocation = utr_rows.assign(scope="perlocation")
utr_perprofile  = utr_rows.assign(scope="perprofile")
df = pd.concat([df[df["scope"] != "utr"], utr_perlocation, utr_perprofile], ignore_index=True)

# Drop baseline rows from plot data
plot_df = df[df["num_clusters"] != 8760].copy()

# Colors
colors = plt.cm.tab10.colors
label_colors = {lbl: colors[i % len(colors)] for i, lbl in enumerate(LEGEND_ORDER)}

x_vals = list(range(0, 8760, 1000))

# -----------------------------
# Generate 6 plots (2 scopes × 3 datasets)
# Each plot: left = linear, right = symlog
# -----------------------------
for scope in SCOPES:
    for dataset in DATASET_VARIANTS:
        mask = (plot_df["scope"] == scope) & (plot_df["dataset"] == dataset)
        sub = plot_df[mask].copy()

        if sub.empty:
            print(f"No data for scope={scope}, dataset={dataset} — skipping")
            continue

        labels_present = [l for l in LEGEND_ORDER if l in sub["label"].unique()]

        fig, (ax_main, ax_log) = plt.subplots(
            1, 2,
            figsize=(14, 6),
            gridspec_kw={"width_ratios": [2, 1]},
        )

        non_faded = sub[~sub["label"].isin(METHODS_FADED)]
        y_max = non_faded["relative_regret"].max() if not non_faded.empty else sub["relative_regret"].max()
        y_max = max(y_max * 1.05, 1.0)

        for ax, use_log in [(ax_main, False), (ax_log, True)]:
            for lbl in labels_present:
                lsub = sub[sub["label"] == lbl].sort_values("num_clusters")
                faded = lbl in METHODS_FADED
                ax.plot(
                    lsub["num_clusters"],
                    lsub["relative_regret"],
                    marker="o",
                    label=lbl,
                    color=label_colors[lbl],
                    linewidth=2,
                    markersize=6 if not use_log else 5,
                    alpha=0.55 if faded else 1.0,
                    linestyle="--" if faded else "-",
                )

            ax.axhline(0, color="black", linewidth=0.8, linestyle="--", alpha=0.5)
            ax.set_xlabel("Number of clusters", fontsize=12)
            ax.set_xticks(x_vals)
            ax.tick_params(axis="x", rotation=45)
            ax.legend(title="Method", fontsize=9 if use_log else 10)
            ax.grid(True, alpha=0.3)

        ax_main.set_ylabel("Relative regret (%)", fontsize=12)
        ax_main.set_ylim(top=y_max, bottom=-1)

        ax_log.set_yscale("symlog", linthresh=10)
        ax_log.set_ylim(bottom=-1)
        ax_log.yaxis.set_major_formatter(mticker.ScalarFormatter())
        ax_log.set_ylabel("Relative regret (%, symlog scale)", fontsize=12)

        title = f"Relative regret — {SCOPE_LABELS[scope]}, {DATASET_LABELS[dataset]}"
        fig.suptitle(title, fontsize=14, fontweight="bold")
        ax_main.set_title("Linear scale", fontsize=12)
        ax_log.set_title("Symlog scale", fontsize=12)

        plt.tight_layout()
        fname = f"relative_regret_{scope}_{dataset}.png"
        out_path = output_dir / fname
        plt.savefig(out_path, dpi=150)
        plt.close()
        print(f"Saved: {out_path}")