import pandas as pd
import numpy as np

INPUT_PATH = "inputs/db_files/ward_k4000_perlocation_NoExtremePreservation_hp0.95_lp0.05/"
partitions_path = f"{INPUT_PATH}assets-rep-periods-partitions.csv"
df = pd.read_csv(partitions_path)

mask = (df["asset"] == "NL_E_Demand") & (df["specification"] == "explicit")
df_filtered = df[mask]

if df_filtered.empty:
    print("No explicit rows found for NL_E_Demand")
else:
    # You'll need the actual time series values to know which timesteps are in the top 5%
    # Load from your DB or a CSV — adjust path as needed
    # Expected columns: timestep, value  (for NL_E_Demand, rep_period=1, year=2050)
    df_values = pd.read_csv(f"{INPUT_PATH}profiles-rep-periods.csv")
    df_values = df_values[df_values["profile_name"] == "NL_E_Demand"]
    values = df_values["value"].values

    threshold = np.percentile(values, 95)
    top_timesteps = set(np.where(values >= threshold)[0])  # 0-indexed

    results = []

    for _, row in df_filtered.iterrows():
        sizes = [int(x) for x in str(row["partition"]).split(";")]

        idx = 0
        for size in sizes:
            block_timesteps = set(range(idx, idx + size))
            if block_timesteps & top_timesteps:  # block contains at least one top-5% timestep
                results.append({
                    "rep_period": row["rep_period"],
                    "year": row["year"],
                    "block_size": size,
                })
            idx += size

    df_results = pd.DataFrame(results)

    print(f"95th percentile threshold: {threshold:.4f}")
    print(f"Blocks containing a top-5% timestep: {len(df_results)}")
    print(f"Average block size: {df_results['block_size'].mean():.2f}")
    print(f"Median block size:  {df_results['block_size'].median():.2f}")
    print(f"\nBlock size distribution among top-5% blocks:")
    print(df_results["block_size"].value_counts().sort_index())