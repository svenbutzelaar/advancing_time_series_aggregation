import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# -----------------------------
# Settings
# -----------------------------
csv_path = Path("plotting/csv_data/regret.csv")
output_dir = Path("plots")
output_dir.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Load data
# -----------------------------
df = pd.read_csv(csv_path)

# Clean up method names if needed
df['method'] = df['method'].str.strip()

# -----------------------------
# Set style
# -----------------------------
sns.set(style="whitegrid", palette="muted", font_scale=1.2)

# -----------------------------
# Plot 1: Runtime by method
# -----------------------------
plt.figure(figsize=(8,5))
sns.barplot(x='method', y='runtime', data=df)
plt.ylabel("Runtime (seconds)")
plt.title("Runtime per Method")
plt.tight_layout()
plt.savefig(output_dir / "runtime_per_method.png")
plt.close()

# -----------------------------
# Pre-calculate cost columns
# -----------------------------
df["ens_cost"] = df["energy_not_served"] * 68887
df["operational_cost_without_ens"] = df["true_operational_cost"] - df["ens_cost"]



# -----------------------------
# Plot 2: regret
# -----------------------------
plt.figure(figsize=(8,5))
width = 0.5
x = range(len(df))
plt.bar(
    x,
    df["investment_cost"],
    width=width,
    label="Investment Cost"
)
plt.bar(
    x,
    df["operational_cost_without_ens"],
    width=width,
    bottom=df["investment_cost"],
    label="Operational Cost"
)
plt.bar(
    x,
    df["ens_cost"],
    width=width,
    bottom=df["operational_cost_without_ens"] + df["investment_cost"],
    label="ENS Cost"
)

plt.xticks(x, df['method'])
plt.ylabel("Cost")
plt.title(f"regret (#timesteps {min(df['num_clusters'])})")
plt.legend()
plt.tight_layout()
plt.savefig(output_dir / "regret.png")
plt.close()

# -----------------------------
# Plot 3: Energy Not Served
# -----------------------------
plt.figure(figsize=(8,5))
sns.barplot(x='method', y='energy_not_served', data=df, palette="pastel")
plt.ylabel("Energy Not Served")
plt.title("Energy Not Served per Method")
plt.tight_layout()
plt.savefig(output_dir / "energy_not_served.png")
plt.close()

print("Plots saved to 'plots/' directory.")