import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

# -----------------------------
# Settings
# -----------------------------
results_csv_path = Path("plots/regret/regret_results_summary.csv")
output_dir = Path("plots/buggy_regret")
output_dir.mkdir(parents=True, exist_ok=True)

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

# Methods to show faded
METHODS_FADED = {}

# -----------------------------
# Load results
# -----------------------------
df = pd.read_csv(results_csv_path)

# Colors
colors = plt.cm.tab10.colors
label_colors = {lbl: colors[i % len(colors)] for i, lbl in enumerate(LEGEND_ORDER)}

x_vals = list(range(0, 8760, 1000))

# -----------------------------
# Generate plots
# -----------------------------
for scope in SCOPES:
    for dataset in DATASET_VARIANTS:

        sub = df[
            (df["scope"] == scope)
            & (df["dataset"] == dataset)
        ].copy()

        if sub.empty:
            print(f"No data for scope={scope}, dataset={dataset}")
            continue

        labels_present = [
            lbl for lbl in LEGEND_ORDER
            if lbl in sub["method"].unique()
        ]

        fig, (ax_main, ax_log) = plt.subplots(
            1,
            2,
            figsize=(14, 6),
            gridspec_kw={"width_ratios": [2, 1]},
        )

        non_faded = sub[~sub["method"].isin(METHODS_FADED)]
        y_max = (
            non_faded["relative_regret"].max()
            if not non_faded.empty
            else sub["relative_regret"].max()
        )
        y_max = min(max(y_max * 1.05, 1.0), 50)

        for ax, use_log in [(ax_main, False), (ax_log, True)]:

            for method in labels_present:

                msub = (
                    sub[sub["method"] == method]
                    .sort_values("num_clusters")
                )

                faded = method in METHODS_FADED

                ax.plot(
                    msub["num_clusters"],
                    msub["relative_regret"],
                    marker="o",
                    label=method,
                    color=label_colors[method],
                    linewidth=2,
                    markersize=6 if not use_log else 5,
                    alpha=0.55 if faded else 1.0,
                    linestyle="--" if faded else "-",
                )

            ax.axhline(
                0,
                color="black",
                linewidth=0.8,
                linestyle="--",
                alpha=0.5,
            )

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

        fig.suptitle(
            f"Relative regret — {SCOPE_LABELS[scope]}, {DATASET_LABELS[dataset]}",
            fontsize=14,
            fontweight="bold",
        )

        ax_main.set_title("Linear scale", fontsize=12)
        ax_log.set_title("Symlog scale", fontsize=12)

        plt.tight_layout()

        out_path = output_dir / f"relative_regret_{scope}_{dataset}.png"
        plt.savefig(out_path, dpi=150)
        plt.close()

        print(f"Saved: {out_path}")