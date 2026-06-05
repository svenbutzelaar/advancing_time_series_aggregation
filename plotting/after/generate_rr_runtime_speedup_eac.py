import pandas as pd
from pathlib import Path

# -----------------------------
# Settings
# -----------------------------
csv_path = Path("plotting/csv_data/regret.csv")

ENS_COST_PER_UNIT = 68887

# -----------------------------
# Load data
# -----------------------------
df = pd.read_csv(csv_path)

# -----------------------------
# Runtime baseline
# (ward_k8760_perlocation_NoExtremePreservation)
# -----------------------------
runtime_baseline_row = df[
    df["file_name"] == "ward_k8760_perlocation_NoExtremePreservation_hp0.95_lp0.05_basedataset_base_case"
]

if runtime_baseline_row.empty:
    raise ValueError(
        "Could not find runtime baseline "
        "(ward_k8760_perlocation_NoExtremePreservation)."
    )

baseline_runtime = runtime_baseline_row["t_solve"].iloc[0]
baseline_cost = runtime_baseline_row["true_operational_cost"].iloc[0] + runtime_baseline_row["investment_cost"].iloc[0]

baseline_ens = runtime_baseline_row["energy_not_served"].iloc[0] * ENS_COST_PER_UNIT
baseline_operational_cost_without_ens = runtime_baseline_row["true_operational_cost"].iloc[0] - baseline_ens
baseline_total_cost = runtime_baseline_row["investment_cost"].iloc[0] + baseline_ens + baseline_operational_cost_without_ens

assert baseline_total_cost == baseline_cost


# -----------------------------
# Filter:
#   - Base dataset
#   - Per-location EAC
# -----------------------------
mask = (
    df["file_name"].str.contains("basedataset", case=False, na=False)
    & df["file_name"].str.contains(
        "perlocation_SeperateExtremesSum",
        case=False,
        na=False,
    )
)

eac_df = df[mask].copy()

if eac_df.empty:
    raise ValueError("No matching rows found.")

# -----------------------------
# Compute total cost
# -----------------------------
eac_df["ens_cost"] = (
    eac_df["energy_not_served"] * ENS_COST_PER_UNIT
)

eac_df["operational_cost_without_ens"] = (
    eac_df["true_operational_cost"]
    - eac_df["ens_cost"]
)

eac_df["total_cost"] = (
    eac_df["investment_cost"]
    + eac_df["operational_cost_without_ens"]
    + eac_df["ens_cost"]
)


# -----------------------------
# Relative regret (%)
# -----------------------------
eac_df["relative_regret"] = (
    (eac_df["total_cost"] - baseline_cost)
    * 100
    / baseline_cost
)

# Speedup relative to base case
eac_df["runtime_speedup"] = (
    baseline_runtime / eac_df["t_solve"]
)

# -----------------------------
# Create final table
# -----------------------------
table = (
    eac_df[
        [
            "num_clusters",
            "relative_regret",
            "runtime_speedup",
        ]
    ]
    .sort_values("num_clusters")
    .reset_index(drop=True)
)

# Optional formatting
table["relative_regret"] = table["relative_regret"].round(3)
table["runtime_speedup"] = table["runtime_speedup"].round(2)

print("\nEAC (Per-location, Base dataset)\n")
print(table.to_string(index=False))

# Optional LaTeX table
print("\nLaTeX table:\n")
print(table.to_latex(index=False))