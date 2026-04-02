import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

# -----------------------------
# Settings
# -----------------------------
csv_path   = Path("plotting/csv_data/investment_costs_summary.csv")
output_dir = Path("plotting/figures")
output_dir.mkdir(parents=True, exist_ok=True)

TECH_COLORS = {
    "Battery":       "#f4a261",
    "Coal":          "#6d6875",
    "Gas":           "#e76f51",
    "Nuclear":       "#e9c46a",
    "OCGT":          "#264653",
    "Solar":         "#FFD166",
    "Wind_Offshore": "#118ab2",
    "Wind_Onshore":  "#06d6a0",
}

# -----------------------------
# Load & prepare
# -----------------------------
df = pd.read_csv(csv_path)

available_methods = df["method"].unique()
print("Available methods:")
for i, m in enumerate(available_methods):
    print(f"  {i+1}. {m}")

raw = input("Choose method (number or name): ").strip()
chosen_method = available_methods[int(raw) - 1] if raw.isdigit() else raw
assert chosen_method in available_methods, f"Unknown method: {chosen_method}"
print(f"Plotting method: {chosen_method}")

df = df[df["method"] == chosen_method].sort_values("num_clusters")

# -----------------------------
# Identify & filter tech columns
# -----------------------------
tech_cols = [c for c in df.columns if c.startswith("cost_")]
tech_names = [c.removeprefix("cost_") for c in tech_cols]

# Drop technologies that are zero across all rows
nonzero = [(col, name) for col, name in zip(tech_cols, tech_names)
           if df[col].sum() > 0]
tech_cols, tech_names = zip(*nonzero)

# Scale to millions
tech_data = [df[col].values / 1e6 for col in tech_cols]
colors     = [TECH_COLORS.get(name, "#aaaaaa") for name in tech_names]
x          = df["num_clusters"].values

# -----------------------------
# Plot
# -----------------------------
fig, ax = plt.subplots(figsize=(10, 6))

ax.stackplot(
    x,
    *tech_data,
    labels=tech_names,
    colors=colors,
    alpha=0.85,
)

ax.set_xlabel("Number of clusters", fontsize=12)
ax.set_ylabel("Investment cost (million €)", fontsize=12)
ax.set_title(f"Investment costs by technology — {chosen_method}",
             fontsize=13, fontweight="bold")
ax.set_xticks(x)
ax.tick_params(axis="x", rotation=45)
ax.yaxis.set_major_formatter(
    mticker.FuncFormatter(lambda val, _: f"€{val:,.1f}M")
)
ax.legend(title="Technology", fontsize=10, loc="upper left")
ax.grid(True, alpha=0.3, axis="y")

plt.tight_layout()

safe_method = "".join(c if c.isalnum() else "_" for c in chosen_method)
out_path = output_dir / f"investment_costs_stacked_{safe_method}.png"
plt.savefig(out_path, dpi=150)
plt.close()

print(f"\nSaved to {out_path}")
print("\nCost breakdown (million €):")
summary = df[["num_clusters"] + list(tech_cols)].copy()
summary[list(tech_cols)] /= 1e6
summary.columns = ["num_clusters"] + list(tech_names)
print(summary.to_string(index=False))