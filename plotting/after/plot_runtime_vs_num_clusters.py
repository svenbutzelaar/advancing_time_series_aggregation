import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# -----------------------------
# Settings
# -----------------------------
csv_path = Path("plotting/csv_data/regret.csv")
output_dir = Path("plots/regret")
output_dir.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Load & prepare data
# -----------------------------
df = pd.read_csv(csv_path)
df["method"] = df["method"].str.strip()

df["runtime"] = df["t_solve"]

df = df[df["calc_ens"] == False]

# Separate baseline (8760) from clustered runs
baseline = df[df["num_clusters"] == 8760].copy()
plot_df = df[df["num_clusters"] != 8760].copy()

methods = sorted(plot_df["method"].unique())
x_vals = sorted(plot_df["num_clusters"].unique())

colors = plt.cm.tab10.colors
method_colors = {m: colors[i % len(colors)] for i, m in enumerate(methods)}

# -----------------------------
# Figure
# -----------------------------
fig, ax = plt.subplots(figsize=(10, 6))

for method in methods:
    sub = plot_df[plot_df["method"] == method].sort_values("num_clusters")
    ax.plot(
        sub["num_clusters"],
        sub["runtime"],
        marker="o",
        label=method,
        color=method_colors[method],
        linewidth=2,
        markersize=6,
    )

# Baseline as horizontal reference line
if not baseline.empty:
    baseline_runtime = baseline["runtime"].values[0]
    ax.axhline(
        baseline_runtime,
        color="black",
        linewidth=1.5,
        linestyle="--",
        alpha=0.7,
        label=f"Baseline (8760 clusters, {baseline_runtime:.0f}s)",
    )

ax.set_xlabel("Number of clusters", fontsize=12)
ax.set_ylabel("Runtime (seconds)", fontsize=12)
ax.set_title("Runtime vs. Number of Clusters", fontsize=13, fontweight="bold")
ax.set_xticks(x_vals)
ax.legend(title="Method", fontsize=10)
ax.grid(True, alpha=0.3)

plt.tight_layout()
out_path = output_dir / "runtime_vs_clusters.png"
plt.savefig(out_path, dpi=150)
plt.close()

print(f"Saved to {out_path}")
print("\nRuntimes:")
print(df[["method", "num_clusters", "runtime"]].sort_values(["method", "num_clusters"]).to_string(index=False))