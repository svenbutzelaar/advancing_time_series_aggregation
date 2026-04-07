import pandas as pd

df = pd.read_csv("outputs/ward_k8760_perlocation_NoExtremePreservation_hp0.95_lp0.05/cons_balance_hub.csv")

# Sort to ensure correct order
df = df.sort_values(["asset", "year", "rep_period", "time_block_start"]).reset_index(drop=True)

# Detect changes within each asset group
df["next_value"] = df.groupby(["asset", "year", "rep_period"])["dual_balance_hub"].shift(-1)

# A change occurs when next value differs and we're not at the last row of a group
df["changed"] = df["dual_balance_hub"] != df["next_value"]

# Count changes per asset (NaN at group boundaries = no change, so dropna handles it)
changes = (
    df.dropna(subset=["next_value"])
    .groupby("asset")["changed"]
    .sum()
    .astype(int)
    .reset_index()
    .rename(columns={"changed": "n_changes"})
    .sort_values("n_changes", ascending=False)
)

print(changes.to_string(index=False))