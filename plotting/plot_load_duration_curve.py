import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

num_clusters = 500
extreme_preservation = True
Path("plots/load_duration_curve").mkdir(parents=True, exist_ok=True)

# --- Load CSVs ---
df = pd.read_csv(f"plotting/csv_data/partitions/{num_clusters}{'-extreme_preservation-true' if extreme_preservation else ''}.csv")
df_full_resolution = pd.read_csv(f"plotting/csv_data/partitions/8760.csv")

# --- Filter for NL ---
df = df[df["location"] == "NL"]
df_full_resolution = df_full_resolution[df_full_resolution["location"] == "NL"]

# --- Parsing helper ---
def parse_semicolon_string(val_string, dtype=float):
    if pd.isna(val_string):
        return []
    val_string = val_string.strip().lstrip(",")
    return [dtype(v) for v in val_string.split(";") if v != ""]

# Parse clustered data
df["parsed_values"] = df["values"].apply(lambda x: parse_semicolon_string(x, float))
df["parsed_partitions"] = df["partition"].apply(lambda x: parse_semicolon_string(x, int))

# Parse full resolution data
df_full_resolution["parsed_values"] = df_full_resolution["values"].apply(
    lambda x: parse_semicolon_string(x, float)
)

# --- Get assets ---
assets = df["asset"].unique()

if len(assets) != 4:
    print(f"Warning: Found {len(assets)} assets instead of 4.")

# --- Create subplots ---
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
axes = axes.flatten()

for i, asset in enumerate(assets):
    ax = axes[i]
    
    # -------------------------
    # Clustered (weighted) data
    # -------------------------
    clustered_values = []
    
    asset_df = df[df["asset"] == asset]
    
    for vals, parts in zip(asset_df["parsed_values"], asset_df["parsed_partitions"]):
        if len(vals) != len(parts):
            raise ValueError(f"Mismatch in values and partitions length for asset {asset}")
        
        # Expand according to partition size
        expanded = np.repeat(vals, parts)
        clustered_values.extend(expanded)
    
    # -------------------------
    # Full resolution data
    # -------------------------
    full_values = []
    
    asset_full_df = df_full_resolution[df_full_resolution["asset"] == asset]
    
    for vals in asset_full_df["parsed_values"]:
        full_values.extend(vals)
    
    if len(clustered_values) == 0 or len(full_values) == 0:
        continue
    
    clustered_values = np.array(clustered_values)
    full_values = np.array(full_values)
    
    # Sort descending (Load Duration Curve)
    clustered_sorted = np.sort(clustered_values)[::-1]
    full_sorted = np.sort(full_values)[::-1]
    
    # --- Plot ---
    ax.plot(full_sorted, linestyle="--", linewidth=2, label="Full resolution (8760)")
    ax.plot(clustered_sorted, label=f"{num_clusters} clusters (expanded)")
    
    ax.set_title(f"Load Duration Curve - {asset}")
    ax.set_xlabel("Time step (sorted)")
    ax.set_ylabel("Load")
    ax.grid(True)
    ax.legend()

plt.tight_layout()
plt.savefig(f"plots/load_duration_curve/{num_clusters}{'-extreme_preservation-true' if extreme_preservation else ''}.png")
plt.show()