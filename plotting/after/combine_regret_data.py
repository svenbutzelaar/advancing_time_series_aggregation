import re
import pandas as pd
from pathlib import Path

# ── Investment cost constants (from Julia script) ────────────────────────────
INVESTMENT_COSTS = {
    "Wind_Onshore":  77356.32865703155,
    "Wind_Offshore": 119732.61777406993,
    "Solar":         34342.98027538492,
    "Battery":       77577.8503057667,
    "Coal":          420000.0,
    "OCGT":          55000.0,
    "Gas":           95000.0,
    "Nuclear":       950000.0,
}
RENEWABLE_TYPES = {"Wind_Onshore", "Wind_Offshore", "Solar"}


def get_asset_type(asset_name: str) -> str | None:
    """Determine asset type from asset name (e.g. 'BE_Wind_Onshore' -> 'Wind_Onshore')."""
    parts = asset_name.split("_")
    if len(parts) < 2:
        return None
    type_str = "_".join(parts[1:])
    return type_str if type_str in INVESTMENT_COSTS else None


def load_asset_capacities(asset_csv_path: Path) -> dict[str, float]:
    """Load asset capacities from the reference asset.csv."""
    df = pd.read_csv(asset_csv_path)
    return {str(row["asset"]): float(row["capacity"]) for _, row in df.iterrows()}


def calculate_investment_costs(investment_csv_path: Path, asset_capacities: dict) -> dict:
    """
    Calculate investment costs and capacities per technology for one experiment,
    mirroring the Julia calculate_costs_and_capacity_per_technology function.
    Returns a flat dict of cost_<Tech> and capacity_<Tech> columns.
    """
    df = pd.read_csv(investment_csv_path)
    costs = {}
    capacities = {}

    for _, row in df.iterrows():
        asset_name = str(row["asset"])
        asset_type = get_asset_type(asset_name)
        if asset_type is None:
            print(f"  [WARN] Unknown asset type for: {asset_name} — skipping")
            continue

        unit_capacity = asset_capacities.get(asset_name, 0.0)
        if unit_capacity == 0.0 and asset_name not in asset_capacities:
            print(f"  [WARN] No capacity found for asset: {asset_name} — skipping ")
        elif unit_capacity == 0.0:
            if row["solution"] > 0:
                print(f"  [ERROR] capacity found for asset is 0: {asset_name} - skipping")
            continue

        costs[asset_type]      = costs.get(asset_type, 0.0)      + INVESTMENT_COSTS[asset_type] * row["solution"] * unit_capacity
        capacities[asset_type] = capacities.get(asset_type, 0.0) + unit_capacity * row["solution"]

    total_cost           = sum(costs.values())
    renewable_cost       = sum(costs.get(t, 0.0) for t in RENEWABLE_TYPES)
    total_capacity       = sum(capacities.values())
    renewable_capacity   = sum(capacities.get(t, 0.0) for t in RENEWABLE_TYPES)

    result = {
        "investment_cost":            total_cost,
        "investment_cost_renewables": renewable_cost,
        "total_capacity":             total_capacity,
        "renewables_capacity":        renewable_capacity,
    }
    for t in sorted(INVESTMENT_COSTS):
        result[f"cost_{t}"]     = costs.get(t, 0.0)
        result[f"capacity_{t}"] = capacities.get(t, 0.0)

    return result


def extract_4th_optimal_objective(log_path: Path) -> float | None:
    """
    Search for all 'Optimal objective' lines in a log file and return the 4th value.
    Returns None if fewer than 4 are found.
    """
    pattern = re.compile(r"Optimal objective\s+([\d.e+\-]+)")
    matches = pattern.findall(log_path.read_text(errors="replace"))
    if len(matches) < 4:
        print(f"  [WARN] Only {len(matches)} 'Optimal objective' found in {log_path.name} (need 4)")
        return None
    return float(matches[3])  # 4th match (0-indexed)


def find_log_for_experiment(experiment_name: str, log_dir: Path) -> Path | None:
    """
    Find the log file(s) whose content contains the given experiment name.
    If multiple logs match, print their names and return the most recent one.
    """
    matches = []
    for log_file in sorted(log_dir.glob("*.log")):
        text = log_file.read_text(errors="replace")
        if f"Experiment: {experiment_name}" in text:
            matches.append(log_file)

    if not matches:
        print(f"  [WARN] No log found for experiment: {experiment_name}")
        return None

    if len(matches) > 1:
        print(f"  [INFO] Multiple logs found for '{experiment_name}':")
        for m in matches:
            print(f"         {m.name}")
        print(f"         → Using most recent: {matches[-1].name}")

    return matches[-1]


# ── Paths ─────────────────────────────────────────────────────────────────────
input_dir   = Path("plotting/csv_data/regret")
output_file = Path("plotting/csv_data/regret.csv")
log_dir     = Path("logs")
asset_csv   = Path("inputs/db_files/obz-invest-full-resolution/asset.csv")

# Investment CSV location pattern (same layout as the Julia script)
investment_csv_pattern = "outputs/{experiment_name}/var_assets_investment.csv"

# ── Load asset capacities once ────────────────────────────────────────────────
if asset_csv.exists():
    asset_capacities = load_asset_capacities(asset_csv)
    print(f"Loaded {len(asset_capacities)} asset capacities from {asset_csv}")
else:
    asset_capacities = {}
    print(f"[WARN] Asset CSV not found at {asset_csv} — investment costs will be 0")

# ── Columns to drop from the raw regret CSVs ─────────────────────────────────
COLS_TO_DROP = [
    "investment_cost_assets",
    "investment_cost_storage_energy_assets",
    "annualized_cost_assets",
    "salvage_value_assets",
    "investment_cost_flows",
    "operational_cost_flows",
    "fuel_cost_flows",
    "total_variable_cost_flows",
    "annualized_cost_flows",
    "salvage_value_flows",
]

# ── Read and merge regret CSVs ────────────────────────────────────────────────
csv_files = list(input_dir.glob("*.csv"))
if not csv_files:
    print(f"No CSV files found in {input_dir}")
    exit(1)

df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)

# Drop unwanted columns (ignore if already absent)
df.drop(columns=[c for c in COLS_TO_DROP if c in df.columns], inplace=True)

# Separate base runs and ENS runs, then merge
df_base = df[df["calc_ens"] == False].copy()
df_ens  = df[df["calc_ens"] == True].copy()

ens_only_cols = ["energy_not_served"]
merge_keys    = ["method", "num_clusters"]

df_merged = df_base.merge(
    df_ens[merge_keys + ens_only_cols],
    on=merge_keys,
    how="left",
    suffixes=("_drop", ""),
)
drop_cols = [c for c in df_merged.columns if c.endswith("_drop")]
df_merged.drop(columns=drop_cols, inplace=True)

df_merged.loc[
    df_merged["file_name"].str.contains("demandoveravailabilities"),
    "method"
] = "demandoveravailabilities"

df_merged.loc[
    df_merged["file_name"].str.contains("utr"),
    "method"
] = "UTR"

# ── Enrich with log data and investment costs ─────────────────────────────────
true_op_costs  = []
investment_rows = []

for _, row in df_merged.iterrows():
    exp_name = str(row["file_name"]).strip()
    # print(f"\nProcessing: {exp_name}")

    # --- 4th optimal objective from log ---
    log_file = find_log_for_experiment(exp_name, log_dir) if log_dir.exists() else None
    if log_file:
        val = extract_4th_optimal_objective(log_file)
        true_op_costs.append(val)
    else:
        true_op_costs.append(None)

    # --- Investment costs from var_assets_investment.csv ---
    inv_csv = Path(investment_csv_pattern.format(experiment_name=exp_name))
    if inv_csv.exists():
        inv_data = calculate_investment_costs(inv_csv, asset_capacities)
    else:
        print(f"  [WARN] Investment CSV not found: {inv_csv}")
        inv_data = {
            "investment_cost": None,
            "investment_cost_renewables": None,
            "total_capacity": None,
            "renewables_capacity": None,
            **{f"cost_{t}": None for t in sorted(INVESTMENT_COSTS)},
            **{f"capacity_{t}": None for t in sorted(INVESTMENT_COSTS)},
        }
    investment_rows.append(inv_data)

df_merged["true_operational_cost"] = true_op_costs

inv_df = pd.DataFrame(investment_rows)
df_final = pd.concat([df_merged.reset_index(drop=True), inv_df.reset_index(drop=True)], axis=1)

print(df_final[df_final["file_name"].str.contains("global", case=False, na=False)])
df_final.loc[df_final["file_name"].str.contains("global", case=False, na=False), "method"] = "NoExtremePreservation Global"
df_final = df_final.drop_duplicates()

# ── Save ──────────────────────────────────────────────────────────────────────
output_file.parent.mkdir(parents=True, exist_ok=True)
df_final.to_csv(output_file, index=False)
print(f"\nCombined {len(csv_files)} files → {output_file} ({len(df_final)} rows, {len(df_final.columns)} columns)")