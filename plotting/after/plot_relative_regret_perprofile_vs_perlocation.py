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

colors = plt.cm.tab10.colors
HC_COLOR = colors[1]
EAC_COLOR = colors[3]

# -----------------------------
# Helpers
# -----------------------------
def parse_method(file_name):

    if "NoExtremePreservation" in file_name:
        return "HC"

    if "SeperateExtremesSum" in file_name:
        return "EAC"

    return None


def parse_scope(file_name):

    if "perlocation" in file_name:
        return "perlocation"

    if "perprofile" in file_name:
        return "perprofile"

    return None


# -----------------------------
# Load data
# -----------------------------
df = pd.read_csv(csv_path)

df = df[df["file_name"].str.contains("basedataset")]

df["method"] = df["file_name"].apply(parse_method)
df["scope"] = df["file_name"].apply(parse_scope)

df = df[
    df["method"].isin(["HC", "EAC"])
    & df["scope"].isin(["perlocation", "perprofile"])
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
baseline = df.loc[
    df["num_clusters"].idxmax(),
    "total_cost"
]

df["relative_regret"] = (
    (df["total_cost"] - baseline)
    / baseline
    * 100
)

df = df[df["num_clusters"] != 8760]
df = df[df["num_clusters"] <= 2000]

# -----------------------------
# Plot
# -----------------------------
plt.figure(figsize=(8, 5))

for method, color in [
    ("HC", HC_COLOR),
    ("EAC", EAC_COLOR),
]:

    for scope, linestyle in [
        ("perlocation", "-"),
        ("perprofile", "--"),
    ]:

        sub = (
            df[
                (df["method"] == method)
                & (df["scope"] == scope)
            ]
            .sort_values("num_clusters")
        )

        plt.plot(
            sub["num_clusters"],
            sub["relative_regret"],
            marker="o",
            linewidth=2,
            linestyle=linestyle,
            color=color,
            label=f"{method} ({scope})",
        )

plt.xlabel("Number of clusters")
plt.ylabel("Relative regret (%)")

plt.ylim(top=40, bottom=-1)

plt.title(
    "Base dataset: per-location versus per-profile clustering"
)

plt.grid(True, alpha=0.3)
plt.legend(handlelength=4)
plt.tight_layout()

plt.savefig(
    output_dir / "regret_perprofile_vs_perlocation.png",
    dpi=300,
    bbox_inches="tight",
)
