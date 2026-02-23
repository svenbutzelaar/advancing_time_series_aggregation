import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import ast

# --- Load CSVs ---
df_location = pd.read_csv("plotting/cummulative_errors_per_location.csv")
df_per_profile = pd.read_csv("plotting/cummulative_errors_per_profile.csv")  # updated

# --- Convert string list to actual list of floats ---
df_location["errors"] = df_location["errors"].apply(ast.literal_eval)
df_per_profile["errors"] = df_per_profile["errors"].apply(ast.literal_eval)

# --- Convert to 2D numpy array ---
error_matrix_l = np.array(df_location["errors"].tolist())
error_matrix_p = np.array(df_per_profile["errors"].tolist())

# --- Compute cumulative sums row-wise (left to right) ---
cumulative_l = np.cumsum(error_matrix_l, axis=1)
cumulative_p = np.cumsum(error_matrix_p, axis=1)

# --- Compute statistics ---
mean_cumulative_l = np.mean(cumulative_l, axis=0)
std_cumulative_l = np.std(cumulative_l, axis=0)
mean_cumulative_p = np.mean(cumulative_p, axis=0)
std_cumulative_p = np.std(cumulative_p, axis=0)

x_l = np.arange(cumulative_l.shape[1])
x_p = np.arange(cumulative_p.shape[1])

# --- Plot ---
plt.figure(figsize=(10,6))

# Location cumulative errors
plt.plot(x_l, mean_cumulative_l, linewidth=2, label="Mean cumulative (Location)")
plt.fill_between(
    x_l,
    mean_cumulative_l - std_cumulative_l,
    mean_cumulative_l + std_cumulative_l,
    alpha=0.3
)

# Per-profile cumulative errors
plt.plot(x_p, mean_cumulative_p, linewidth=2, label="Mean cumulative (Per Profile)")
plt.fill_between(
    x_p,
    mean_cumulative_p - std_cumulative_p,
    mean_cumulative_p + std_cumulative_p,
    alpha=0.3
)

plt.xlabel("Temporal resolution removed")
plt.ylabel("Cumulative Error")
plt.title("Comparison of Cumulative Errors")
plt.legend(loc="upper left")
plt.grid(True)

plt.savefig("plots/cumulative_errors_comparison.png", dpi=300)
plt.show()