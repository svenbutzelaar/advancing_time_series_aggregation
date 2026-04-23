import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

num_clusters = 1000
Path("plots/load_duration_curve").mkdir(parents=True, exist_ok=True)

# --- Filenames ---
files = {
    "HC": "ward_k1000_perlocation_NoExtremePreservation_hp0.95_lp0.05",
    "PEC": "ward_k1000_perlocation_Afterwards_hp0.95_lp0.05",
    "AEC": "ward_k1000_perlocation_SeperateExtremesSum_hp0.95_lp0.05",
}

# --- Parsing helper ---
def parse_semicolon_string(val_string, dtype=float):
    if pd.isna(val_string):
        return []
    try:
        val_string = str(val_string).strip().lstrip(",")
        return [dtype(v) for v in val_string.split(";") if v != ""]
    except Exception as e:
        print(f"[ERROR] Failed parsing string: {val_string}")
        print(f"        Exception: {e}")
        return []

# --- Load full resolution ---
try:
    print("[INFO] Loading full resolution data (8760)...")
    df_full_resolution = pd.read_csv("plotting/csv_data/partitions/8760.csv")
except FileNotFoundError:
    raise FileNotFoundError("[FATAL] 8760.csv not found!")

required_cols = {"location", "asset", "values"}
if not required_cols.issubset(df_full_resolution.columns):
    raise ValueError(f"[FATAL] Missing columns in 8760.csv: {required_cols - set(df_full_resolution.columns)}")

df_full_resolution = df_full_resolution[df_full_resolution["location"] == "NL"]
print(f"[INFO] Full resolution rows after NL filter: {len(df_full_resolution)}")

df_full_resolution["parsed_values"] = df_full_resolution["values"].apply(
    lambda x: parse_semicolon_string(x, float)
)

# --- Extract full resolution demand ---
full_values = []
df_full_demand = df_full_resolution[df_full_resolution["asset"] == "NL_E_Demand"]

if df_full_demand.empty:
    raise ValueError("[FATAL] No demand data found in full resolution dataset!")

for vals in df_full_demand["parsed_values"]:
    full_values.extend(vals)

if len(full_values) == 0:
    raise ValueError("[FATAL] Full resolution demand values are empty!")

full_values = np.array(full_values)
full_sorted = np.sort(full_values)[::-1]

print(f"[DEBUG] Full resolution demand:")
print(f"        Count = {len(full_values)}")
print(f"        Min   = {full_values.min():.4f}")
print(f"        Max   = {full_values.max():.4f}")

# --- Create subplots ---
fig, axes = plt.subplots(1, 3, figsize=(18, 5), sharey=True)

for ax, (label, filename) in zip(axes, files.items()):
    print(f"\n[INFO] Processing {label} ({filename})")

    filepath = f"plotting/csv_data/partitions/{filename}.csv"

    if not Path(filepath).exists():
        print(f"[WARNING] File not found: {filepath} -> skipping")
        ax.set_title(f"{label}\n(MISSING FILE)")
        continue

    try:
        df = pd.read_csv(filepath)
    except Exception as e:
        print(f"[ERROR] Failed to read {filepath}: {e}")
        continue

    required_cols = {"location", "asset", "values", "partition"}
    if not required_cols.issubset(df.columns):
        print(f"[WARNING] Missing columns in {filename}: {required_cols - set(df.columns)} -> skipping")
        continue

    df = df[df["location"] == "NL"]
    df = df[df["asset"] == "NL_E_Demand"]

    print(f"[DEBUG] Rows after filtering (NL + demand): {len(df)}")

    if df.empty:
        print(f"[WARNING] No demand data for {label} -> skipping")
        continue

    df["parsed_values"] = df["values"].apply(lambda x: parse_semicolon_string(x, float))
    df["parsed_partitions"] = df["partition"].apply(lambda x: parse_semicolon_string(x, int))

    # --- Expand clustered values ---
    clustered_values = []

    for idx, (vals, parts) in enumerate(zip(df["parsed_values"], df["parsed_partitions"])):
        if len(vals) != len(parts):
            print(f"[WARNING] Length mismatch at row {idx}: values={len(vals)}, partitions={len(parts)} -> skipping row")
            continue

        try:
            expanded = np.repeat(vals, parts)
            clustered_values.extend(expanded)
        except Exception as e:
            print(f"[ERROR] Failed expanding row {idx}: {e}")

    if len(clustered_values) == 0:
        print(f"[WARNING] No valid clustered values for {label}")
        continue

    clustered_values = np.array(clustered_values)
    clustered_sorted = np.sort(clustered_values)[::-1]

    print(f"[DEBUG] Clustered ({label}):")
    print(f"        Count = {len(clustered_values)}")
    print(f"        Min   = {clustered_values.min():.4f}")
    print(f"        Max   = {clustered_values.max():.4f}")

    # --- Plot ---
    ax.plot(full_sorted, linestyle="--", linewidth=2, label="Full resolution (8760)")
    ax.plot(clustered_sorted, label=label)

    ax.set_title(label)
    ax.set_xlabel("Time step (sorted)")
    ax.grid(True)
    ax.legend()

# Shared y-label
axes[0].set_ylabel("Demand")

plt.tight_layout()
plt.savefig("plots/load_duration_curve/method_comparison_demand.png")
print("\n[INFO] Plot saved to plots/load_duration_curve/method_comparison_demand.png")

plt.show()