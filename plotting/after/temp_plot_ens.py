import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("plotting/csv_data/regret.csv")

# df = df[(df["num_clusters"] >= 100) & (df["num_clusters"] <= 2000)]
df = df.sort_values("num_clusters")

plt.figure(figsize=(10, 5))
plt.plot(df["num_clusters"], df["energy_not_served"], marker="o", linestyle="-", color="steelblue")
plt.xlabel("num_clusters")
plt.ylabel("energy_not_served")
plt.title("Energy Not Served vs Number of Clusters (100–2000)")
plt.grid(True)
plt.tight_layout()
plt.show()