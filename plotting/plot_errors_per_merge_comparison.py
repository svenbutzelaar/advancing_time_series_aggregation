import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import ast
from pathlib import Path

# --- Config: CSV files and which columns to plot ---
csv_files = {
    "Per Location": "plotting/csv_data/cummulative_errors_per_location.csv",
    "Per Profile": "plotting/csv_data/cummulative_errors_per_profile.csv"
}

# Clear, academic labels
error_labels = {
    "errors": "Sum of Squared Errors (SSE)",
    "ldc_errors": "Load Duration Curve RMSE"
}

output_folder = Path("plots")
output_folder.mkdir(exist_ok=True)

# --- Helper function to load and convert a column ---
def load_column_as_matrix(df, column_name):
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in DataFrame.")
    return np.array(df[column_name].apply(ast.literal_eval).tolist())

# --- Plotting ---
for error_col in error_labels:
    plt.figure(figsize=(10, 6))

    for label, csv_file in csv_files.items():
        df = pd.read_csv(csv_file)

        try:
            matrix = load_column_as_matrix(df, error_col)
        except ValueError:
            print(f"Skipping {label} ({error_col}) because column is missing.")
            continue

        mean_error = np.mean(matrix, axis=0)
        std_error = np.std(matrix, axis=0)

        x = np.arange(matrix.shape[1])

        plt.plot(
            x,
            mean_error,
            linewidth=2,
            label=label
        )

        plt.fill_between(
            x,
            mean_error - std_error,
            mean_error + std_error,
            alpha=0.25
        )

    plt.xlabel("Number of HC Merges")
    plt.ylabel(error_labels.get(error_col, error_col))
    plt.title(f"{error_labels.get(error_col, error_col)} vs Number of Merges")
    plt.legend(loc="upper left")
    plt.grid(True)

    plt.tight_layout()
    plt.savefig(output_folder / f"{error_col}_comparison.png", dpi=300)
    plt.show()