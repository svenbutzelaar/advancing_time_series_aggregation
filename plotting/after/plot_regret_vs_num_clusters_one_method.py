import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

# -----------------------------
# Settings
# -----------------------------
csv_path = Path("plotting/csv_data/regret_v1.csv")
output_dir = Path("plots/regret")
output_dir.mkdir(parents=True, exist_ok=True)

ENS_COST_PER_UNIT = 68887

# -----------------------------
# Load & prepare data
# -----------------------------
df = pd.read_csv(csv_path)

# Filter to SeperateSum only
df = df[df["method"].str.strip() == "SeperateExtremesSum"].copy()

# Cost components
df["ens_cost"] = df["energy_not_served"] * ENS_COST_PER_UNIT
df["operational_cost_without_ens"] = df["true_operational_cost"] - df["ens_cost"]
df["total_cost"] = df["investment_cost"] + df["operational_cost_without_ens"] + df["ens_cost"]

# Sort by number of clusters
df = df.sort_values("num_clusters")

x = df["num_clusters"].values
invest = df["investment_cost"].values
operational = df["operational_cost_without_ens"].values
ens = df["ens_cost"].values
total = df["total_cost"].values

# -----------------------------
# Plot
# -----------------------------
fig, ax = plt.subplots(figsize=(10, 6))

# Stacked area
ax.stackplot(
    x,
    invest,
    operational,
    ens,
    labels=["Investment cost", "Operational cost (excl. ENS)", "ENS cost"],
    colors=["#4C72B0", "#55A868", "#C44E52"],
    alpha=0.75,
)

# Total cost line on top
ax.plot(
    x,
    total,
    color="black",
    linewidth=2.5,
    linestyle="--",
    marker="o",
    markersize=6,
    label="Total cost",
    zorder=5,
)

ax.set_xlabel("Number of clusters", fontsize=12)
ax.set_ylabel("Cost (€)", fontsize=12)
ax.set_title("Cost breakdown — SeperateSum", fontsize=13, fontweight="bold")
ax.set_xticks(x)
ax.tick_params(axis="x", rotation=45)
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda val, _: f"€{val:,.0f}"))
ax.legend(title="Cost component", fontsize=10)
ax.grid(True, alpha=0.3, axis="y")

plt.tight_layout()
out_path = output_dir / "seperatesum_cost_breakdown.png"
plt.savefig(out_path, dpi=150)
plt.close()

print(f"Saved to {out_path}")
print("\nCost breakdown values:")
print(
    df[["num_clusters", "investment_cost", "operational_cost_without_ens", "ens_cost", "total_cost"]]
    .to_string(index=False)
)