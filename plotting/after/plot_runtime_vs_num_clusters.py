import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# -----------------------------
# Settings
# -----------------------------
csv_path = Path("plotting/csv_data/regret.csv")
output_dir = Path("plots/runtime")
output_dir.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Experiment label mapping
# -----------------------------
EXPERIMENT_LABELS = {
    "utr":                               "UTR",
    "perlocation_NoExtremePreservation": "HC",
    "perprofile_NoExtremePreservation":  "HC",
    "perlocation_SeperateExtremesSum":   "EAC",
    "perprofile_SeperateExtremesSum":    "EAC",
    "perlocation_Afterwards":            "PEC",
    "perprofile_Afterwards":             "PEC",
    "perlocation_DynamicProgramming_s168": "DP",
    "perprofile_DynamicProgramming_s168":  "DP",
    "base_case":                         "Base case",
}

LEGEND_ORDER = ["UTR", "HC", "PEC", "EAC", "DP"]

DATASET_VARIANTS = ["basedataset", "lowvar", "highvar"]
DATASET_LABELS = {
    "basedataset": "Base dataset",
    "lowvar": "Low variance",
    "highvar": "High variance",
}

SCOPES = ["perlocation", "perprofile"]
SCOPE_LABELS = {
    "perlocation": "Per location",
    "perprofile": "Per profile",
}

# -----------------------------
# Parsing helpers
# -----------------------------
def label_from_file_name(file_name: str) -> str:
    for key, label in EXPERIMENT_LABELS.items():
        if key in file_name:
            return label
    return file_name


def scope_from_file_name(file_name: str):
    if "perlocation" in file_name:
        return "perlocation"
    if "perprofile" in file_name:
        return "perprofile"
    if "utr" in file_name:
        return "utr"
    return None


def dataset_from_file_name(file_name: str):
    for ds in DATASET_VARIANTS:
        if ds in file_name:
            return ds
    return None


# -----------------------------
# Load data
# -----------------------------
df = pd.read_csv(csv_path)

df["label"] = df["file_name"].apply(label_from_file_name)
df["scope"] = df["file_name"].apply(scope_from_file_name)
df["dataset"] = df["file_name"].apply(dataset_from_file_name)

# Keep only known experiments
known_labels = set(EXPERIMENT_LABELS.values())
df = df[df["label"].isin(known_labels)].copy()

df["runtime"] = df["t_solve"]

# Exclude baseline from plots
plot_df = df[df["num_clusters"] != 8760].copy()

# Duplicate UTR into both scopes
utr_rows = plot_df[plot_df["scope"] == "utr"].copy()

if not utr_rows.empty:
    utr_perlocation = utr_rows.assign(scope="perlocation")
    utr_perprofile = utr_rows.assign(scope="perprofile")

    plot_df = pd.concat(
        [
            plot_df[plot_df["scope"] != "utr"],
            utr_perlocation,
            utr_perprofile,
        ],
        ignore_index=True,
    )

# -----------------------------
# Colors
# -----------------------------
colors = plt.cm.tab10.colors
label_colors = {
    lbl: colors[i % len(colors)]
    for i, lbl in enumerate(LEGEND_ORDER)
}

# -----------------------------
# Plot 1:
# Runtime comparison between methods
# -----------------------------
print("\nGenerating method comparison plots...")

for scope in SCOPES:
    for dataset in DATASET_VARIANTS:

        sub = plot_df[
            (plot_df["scope"] == scope)
            & (plot_df["dataset"] == dataset)
        ].copy()

        if sub.empty:
            print(
                f"No data for scope={scope}, "
                f"dataset={dataset} — skipping"
            )
            continue

        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Base case horizontal line
        base_sub = df[
            (df["num_clusters"] == 8760)
            & (df["dataset"] == dataset)
        ]
        if not base_sub.empty:
            base_runtime = base_sub["runtime"].mean()
            ax.axhline(
                base_runtime,
                color="grey",
                linestyle=":",
                linewidth=1.5,
                label="Base case",
            )

        labels_present = [
            l for l in LEGEND_ORDER
            if l in sub["label"].unique()
        ]

        for label in labels_present:

            lsub = (
                sub[sub["label"] == label]
                .sort_values("num_clusters")
            )

            ax.plot(
                lsub["num_clusters"],
                lsub["runtime"],
                marker="o",
                linewidth=2,
                markersize=6,
                label=label,
                color=label_colors[label],
            )

        ax.set_xlabel("Number of clusters", fontsize=12)
        ax.set_ylabel("Runtime (seconds)", fontsize=12)

        ax.set_title(
            f"Runtime — {SCOPE_LABELS[scope]}, "
            f"{DATASET_LABELS[dataset]}",
            fontsize=13,
            fontweight="bold",
        )

        ax.grid(True, alpha=0.3)
        ax.legend(title="Method")

        plt.tight_layout()

        out_path = (
            output_dir
            / f"runtime_methods_{scope}_{dataset}.png"
        )

        plt.savefig(out_path, dpi=150)
        plt.close()

        print(f"Saved: {out_path}")

# -----------------------------
# Plot 2:
# Per-profile minus per-location
# -----------------------------
print("\nGenerating scope difference plots...")

methods = ["HC", "PEC", "EAC", "DP"]

for dataset in DATASET_VARIANTS:

    fig, ax = plt.subplots(figsize=(10, 6))

    for method in methods:

        loc = (
            plot_df[
                (plot_df["dataset"] == dataset)
                & (plot_df["scope"] == "perlocation")
                & (plot_df["label"] == method)
            ][["num_clusters", "runtime"]]
            .rename(columns={"runtime": "runtime_loc"})
        )

        prof = (
            plot_df[
                (plot_df["dataset"] == dataset)
                & (plot_df["scope"] == "perprofile")
                & (plot_df["label"] == method)
            ][["num_clusters", "runtime"]]
            .rename(columns={"runtime": "runtime_prof"})
        )

        merged = (
            loc.merge(
                prof,
                on="num_clusters",
                how="inner",
            )
            .sort_values("num_clusters")
        )

        if merged.empty:
            continue

        merged["difference"] = (
            merged["runtime_prof"]
            - merged["runtime_loc"]
        )

        ax.plot(
            merged["num_clusters"],
            merged["difference"],
            marker="o",
            linewidth=2,
            markersize=6,
            label=method,
            color=label_colors[method],
        )

    ax.axhline(
        0,
        color="black",
        linestyle="--",
        linewidth=1,
        alpha=0.6,
    )

    ax.set_xlabel("Number of clusters", fontsize=12)
    ax.set_ylabel(
        "Runtime difference (seconds)",
        fontsize=12,
    )

    ax.set_title(
        "Per-profile − Per-location Runtime Difference\n"
        f"{DATASET_LABELS[dataset]}",
        fontsize=13,
        fontweight="bold",
    )

    ax.grid(True, alpha=0.3)
    ax.legend(title="Method")

    plt.tight_layout()

    out_path = (
        output_dir
        / f"runtime_scope_difference_{dataset}.png"
    )

    plt.savefig(out_path, dpi=150)
    plt.close()

    print(f"Saved: {out_path}")

print("\nDone.")
