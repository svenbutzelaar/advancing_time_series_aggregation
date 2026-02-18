# === CONFIG ===
import enum
import os
from pathlib import Path
import shutil
import time

import pandas as pd

from cluster.cluster_integral_cost import hierarchical_time_clustering_integral_cost
from cluster.cluster_peaks_and_lows import hierarchical_time_clustering_peaks_and_lows
from cluster.cluster_ward import hierarchical_time_clustering_ward
from cluster.cluster_ward_variance_penalty import hierarchical_time_clustering_penalized
from plot_integral_sorted_curve import plot_integral_sorted_curve
from cluster.cluster_ward_quantile import hierarchical_time_clustering_quantile


class ClusterMethod(enum.Enum):
    WARD = "ward"
    INTEGRAL_COST = "integral_cost"
    QUANTILE = "quantile"
    PENALIZED = "penalized"
    PEAKS_AND_LOWS = "peaks_and_lows"

CURRENT_CLUSTER_METHOD = ClusterMethod.PEAKS_AND_LOWS
PLOT_INTEGRAL_SORTED_CURVE = True
NUMBER_CLUSTERS = 672

INPUT_FILES_PROFILES = [
    "profiles-rep-periods-demand.csv",
    "profiles-rep-periods-availability.csv",
    "profiles-rep-periods-inflows.csv",
]
INPUT_FILE_FLOW = "flows-data.csv"
INPUT_PATH = "Cases/1h/"
OUTPUT_PATH = f"Cases/C{NUMBER_CLUSTERS}_{CURRENT_CLUSTER_METHOD.value}/"
ASSETS_OUTPUT_FILE = OUTPUT_PATH + "assets-rep-periods-partitions.csv"
FLOWS_OUTPUT_FILE = OUTPUT_PATH + "flows-rep-periods-partitions.csv"

# Only rank 0 creates output directory and copies files
if rank == 0:
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    for file in Path("Cases/1h").glob("*.csv"):
        shutil.copy(file, Path(OUTPUT_PATH[:-1]) / file.name)

# Wait for rank 0 to finish setup
comm.Barrier()

# === LOAD DATA AND DISTRIBUTE WORK ===
overall_start = time.time()

if rank == 0:
    # Rank 0 loads all profiles and distributes them
    all_profiles = []
    
    for input_file in INPUT_FILES_PROFILES:
        csv_path = INPUT_PATH + input_file
        df = pd.read_csv(csv_path, header=1)
        df = df.dropna(subset=["value"])
        
        for profile, group in df.groupby("profile_name"):
            values = group.sort_values("time_step")["value"].to_numpy()
            all_profiles.append((profile, values))
    
    print(f"Total profiles to process: {len(all_profiles)}")
    print(f"Using {size} MPI processes")
    
    # Distribute profiles to workers
    profiles_per_rank = [[] for _ in range(size)]
    for i, profile_data in enumerate(all_profiles):
        profiles_per_rank[i % size].append(profile_data)
else:
    profiles_per_rank = None

# Scatter profiles to all ranks
my_profiles = comm.scatter(profiles_per_rank, root=0)

# === PROCESS PROFILES IN PARALLEL ===
my_results = []
my_stats = []

for profile, values in my_profiles:
    start_time = time.time()
    
    if CURRENT_CLUSTER_METHOD == ClusterMethod.WARD:
        clusters, stats = hierarchical_time_clustering_ward(values, NUMBER_CLUSTERS)
    elif CURRENT_CLUSTER_METHOD == ClusterMethod.INTEGRAL_COST:
        clusters, stats = hierarchical_time_clustering_integral_cost(values, NUMBER_CLUSTERS)
    elif CURRENT_CLUSTER_METHOD == ClusterMethod.QUANTILE:
        alpha = 0.25 if "Solar" in str(profile) else 0.75
        clusters, stats = hierarchical_time_clustering_quantile(
            values, NUMBER_CLUSTERS, alpha=0.25
        )
    elif CURRENT_CLUSTER_METHOD == ClusterMethod.PENALIZED:
        clusters, stats = hierarchical_time_clustering_penalized(values, NUMBER_CLUSTERS, lam=10.0)
    elif CURRENT_CLUSTER_METHOD == ClusterMethod.PEAKS_AND_LOWS:
        clusters, stats = hierarchical_time_clustering_peaks_and_lows(values, NUMBER_CLUSTERS, alpha=0.03)
    else:
        raise ValueError("Unknown clustering method")
    
    if PLOT_INTEGRAL_SORTED_CURVE and str(profile).startswith("N"):
        plot_integral_sorted_curve(values, clusters, profile, f"C{NUMBER_CLUSTERS}_{CURRENT_CLUSTER_METHOD.value}")
    
    ratio = len(clusters) / len(values)
    elapsed = time.time() - start_time
    
    # Store stats
    if stats is not None:
        stats.update(
            {
                "profile_name": profile,
                "num_timesteps": len(values),
                "compression_ratio": ratio,
                "runtime_sec": elapsed,
            }
        )
        my_stats.append(stats)
    
    profile_row = [profile, "1", "explicit", ";".join(map(str, clusters))]
    my_results.append(profile_row)
    
    print(f"[Rank {rank}] [{profile}] {len(clusters)} clusters from {len(values)} steps "
          f"(runtime={elapsed:.3f}s)")

# === GATHER RESULTS FROM ALL RANKS ===
all_results = comm.gather(my_results, root=0)
all_stats = comm.gather(my_stats, root=0)

# === SAVE RESULTS (ONLY RANK 0) ===
if rank == 0:
    # Flatten results from all ranks
    all_results = [item for sublist in all_results for item in sublist] # type: ignore
    all_stats = [item for sublist in all_stats for item in sublist] # type: ignore
    
    # Save clustered results
    columns = pd.MultiIndex.from_tuples(
        [
            ("", "asset"),
            ("", "rep_period"),
            ("{uniform;explicit;math}", "specification"),
            ("", "partition"),
        ]
    )
    
    out_df = pd.DataFrame(all_results, columns=[c[1] for c in columns])
    out_df.columns = columns
    out_df.to_csv(ASSETS_OUTPUT_FILE, index=False)
    
    # Save statistics
    total_time = time.time() - overall_start
    if all_stats:
        stats_df = pd.DataFrame(all_stats)
        stats_df.to_csv(OUTPUT_PATH + "profile-stats.csv", index=False)
        print("\n=== Summary Statistics ===")
        print(
            stats_df[
                [
                    "profile_name",
                    "num_timesteps",
                    "num_clusters",
                    "compression_ratio",
                    "total_error",
                    "runtime_sec",
                ]
            ]
        )
    
    print(f"\nTotal runtime: {total_time:.3f} seconds")
    print(f"Speedup: {total_time / (total_time / size):.2f}x (theoretical)")
    print(f"Saved clustered results to: {ASSETS_OUTPUT_FILE}")
    
    # === GENERATE FLOWS OUTPUT ===
    print("start generating: " + FLOWS_OUTPUT_FILE)
    result_mapping = {p: c for p, _, _, c in all_results}
    csv_path = INPUT_PATH + INPUT_FILE_FLOW
    df = pd.read_csv(csv_path, header=1)
    flow_result = []
    for row in df.values:
        result = result_mapping.get(row[1]) or result_mapping.get(row[2])
        if result:
            flow_result.append([row[1], row[2], "1", "explicit", result])
    
    columns = pd.MultiIndex.from_tuples(
        [
            ("", "from_asset"),
            ("", "to_asset"),
            ("", "rep_period"),
            ("{uniform;explicit;math}", "specification"),
            ("", "partition"),
        ]
    )
    out_df = pd.DataFrame(flow_result, columns=[c[1] for c in columns])
    out_df.columns = columns
    out_df.to_csv(FLOWS_OUTPUT_FILE, index=False)
    
    print(f"Saved flows results to: {FLOWS_OUTPUT_FILE}")

# Ensure all processes finish before exiting
comm.Barrier()