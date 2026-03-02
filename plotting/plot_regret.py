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
# Plot 2: Cost vs True Operational Cost
# -----------------------------
plt.figure(figsize=(8,5))
width = 0.35
x = range(len(df))
plt.bar(x, df['cost'], width=width, label='Cost')
plt.bar([i+width for i in x], df['true_operational_cost'], width=width, label='True Operational Cost')
plt.xticks([i + width/2 for i in x], df['method'])
plt.ylabel("Cost")
plt.title("Estimated vs True Operational Cost")
plt.legend()
plt.tight_layout()
plt.savefig(output_dir / "cost_comparison.png")
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