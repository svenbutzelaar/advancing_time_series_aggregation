import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import ast

# --- Load CSV ---
file_location = "plotting/cummulative_errors_per_profile.csv"
df = pd.read_csv(file_location)

# --- Convert string list to actual list of floats ---
df["errors"] = df["errors"].apply(ast.literal_eval)

# --- Convert to 2D numpy array ---
error_matrix = np.array(df["errors"].tolist())

# --- Compute cumulative sums row-wise (left to right) ---
cumulative = np.cumsum(error_matrix, axis=1)

# --- Compute statistics ---
mean_cumulative = np.mean(cumulative, axis=0)
std_cumulative = np.std(cumulative, axis=0)

x = np.arange(cumulative.shape[1])

# --- Plot ---
plt.figure()

# Plot all individual cumulative lines
for i, row in enumerate(cumulative):
    if i == 0:
        plt.plot(x, row, alpha=0.2, label="Individual cumulative")
    else:
        plt.plot(x, row, alpha=0.2)

# Plot mean line
plt.plot(x, mean_cumulative, linewidth=2, label="Mean cumulative")

# Plot variance band (±1 std)
plt.fill_between(
    x,
    mean_cumulative - std_cumulative,
    mean_cumulative + std_cumulative,
    alpha=0.3,
    label="±1 Std Dev"
)

plt.xlabel("temporal resolution removed")
plt.ylabel("Cumulative Error")
plt.title("Cumulative Error Plot")

plt.legend(loc="upper left")

plt.savefig(file_location.replace("plotting", "plots").replace("csv", "png"))
plt.show()