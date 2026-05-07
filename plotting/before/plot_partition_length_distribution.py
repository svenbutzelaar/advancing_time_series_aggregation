import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter

PROFILE = "NL_E_Demand"
FILE_NAME = "ward_k4000_perlocation_NoExtremePreservation_hp0.95_lp0.05"
# --- Load ---
path = f"inputs/db_files/{FILE_NAME}/assets-rep-periods-partitions.csv"
df = pd.read_csv(path)


# --- Filter ---
mask = (df["asset"] == PROFILE) & (df["specification"] == "explicit")
df_filtered = df[mask]

if df_filtered.empty:
    print(f"No explicit rows found for {PROFILE}")
else:
    # --- Parse all partitions across all matching rows ---
    all_sizes = []
    for _, row in df_filtered.iterrows():
        sizes = [int(x) for x in str(row["partition"]).split(";")]
        all_sizes.extend(sizes)

    counts = Counter(all_sizes)
    print(f"Total partitions: {len(all_sizes)}")
    print(f"Unique block sizes: {sorted(counts.keys())}")
    print("\nDistribution (size → count):")
    for size in sorted(counts):
        print(f"  {size:4d}  →  {counts[size]:5d}  ({100 * counts[size] / len(all_sizes):.1f}%)")
        
    print(f"avg block size: {sum(all_sizes) / len(all_sizes)}")

    # --- Plot ---
    sizes_sorted = sorted(counts.keys())
    freqs = [counts[s] for s in sizes_sorted]

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(sizes_sorted, freqs, color="steelblue", edgecolor="white")
    ax.set_xlabel("Partition block size (hours)")
    ax.set_ylabel("Count")
    ax.set_title(f"{PROFILE} — partition block size distribution (explicit)")
    ax.set_xticks(sizes_sorted)
    plt.tight_layout()
    plt.savefig(f"plots/partition_distribution/{FILE_NAME}_{PROFILE}.png", dpi=150)
    plt.show()
    print(f"\nplots/partition_distribution/{FILE_NAME}_{PROFILE}.png")