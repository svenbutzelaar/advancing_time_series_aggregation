import pandas as pd
from pathlib import Path

# -----------------------------
# Settings
# -----------------------------
csv_path = Path("plotting/csv_data/regret.csv")

EXP_1100 = "ward_k1100_perlocation_SeperateExtremesSum_hp0.95_lp0.05"
EXP_1000 = "ward_k1000_perlocation_SeperateExtremesSum_hp0.95_lp0.05"
BASE     = "base_case"

# -----------------------------
# Load data
# -----------------------------
df = pd.read_csv(csv_path)

row_1100 = df[df["file_name"] == EXP_1100].iloc[0]
row_1000 = df[df["file_name"] == EXP_1000].iloc[0]
row_base = df[df["file_name"] == BASE].iloc[0]

# -----------------------------
# Extract technologies
# -----------------------------
cap_cols = [c for c in df.columns if c.startswith("capacity_")]
techs = [c.removeprefix("capacity_").replace("_", r"\_") for c in cap_cols]

# -----------------------------
# Compute per-technology values
# -----------------------------
rows = []

for col, tech in zip(cap_cols, techs):
    base_val = row_base[col]

    diff = row_1100[col] - row_1000[col]

    # relative closeness per technology
    closeness_1000 = abs(row_1000[col] - base_val) / base_val if base_val != 0 else 0
    closeness_1100 = abs(row_1100[col] - base_val) / base_val if base_val != 0 else 0

    rows.append({
        "Technology": tech,
        r"$\Delta ( 1100 - 1000 )$": diff,
        r"\makecell{Rel. deviation \\ base (k=1000)}": closeness_1000,
        r"\makecell{Rel. deviation \\ base (k=1100)}": closeness_1100,
    })

table_df = pd.DataFrame(rows)

# -----------------------------
# TOTAL row (system-level)
# -----------------------------
total_diff = sum(row_1100[c] - row_1000[c] for c in cap_cols)

total_base = sum(row_base[c] for c in cap_cols)

total_close_1000 = sum(abs(row_1000[c] - row_base[c]) for c in cap_cols) / total_base
total_close_1100 = sum(abs(row_1100[c] - row_base[c]) for c in cap_cols) / total_base

total_row = pd.DataFrame([{
    "Technology": r"\textbf{Total}",
    r"$\Delta ( 1100 - 1000 )$": total_diff,
    r"\makecell{Rel. deviation \\ base (k=1000)}": total_close_1000,
    r"\makecell{Rel. deviation \\ base (k=1100)}": total_close_1100,
}])

table_df = pd.concat([table_df, total_row], ignore_index=True)

# -----------------------------
# Formatting
# -----------------------------
def fmt_diff(x):
    return f"{x:,.1f}"

def fmt_pct(x):
    return f"{x:.2%}".replace("%", r"\%")

table_df[r"$\Delta ( 1100 - 1000 )$"] = table_df[r"$\Delta ( 1100 - 1000 )$"].apply(fmt_diff)
table_df[r"\makecell{Rel. deviation \\ base (k=1000)}"] = table_df[r"\makecell{Rel. deviation \\ base (k=1000)}"].apply(fmt_pct)
table_df[r"\makecell{Rel. deviation \\ base (k=1100)}"] = table_df[r"\makecell{Rel. deviation \\ base (k=1100)}"].apply(fmt_pct)

# -----------------------------
# Export to LaTeX
# -----------------------------
latex = table_df.to_latex(
    index=False,
    escape=False,
    column_format="lccc",
    caption="Per-technology capacity differences and relative deviation from the base case.",
    label="tab:capacity_detailed_comparison",
)

print(latex)