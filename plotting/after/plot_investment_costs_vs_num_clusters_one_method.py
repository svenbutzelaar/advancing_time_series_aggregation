import pandas as pd
import plotly.graph_objects as go
from pathlib import Path

# -----------------------------
# Settings
# -----------------------------
csv_path = Path("plotting/csv_data/regret.csv")
output_path = Path("plots/investment_stackplot.html")
output_path.parent.mkdir(parents=True, exist_ok=True)

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
# Function
# -----------------------------
def make_stacked_plot(df, prefix, ylabel, title):
    tech_cols = [c for c in df.columns if c.startswith(prefix)]
    tech_names = [c.removeprefix(prefix) for c in tech_cols]

    # Drop zero technologies
    nonzero = [(col, name) for col, name in zip(tech_cols, tech_names)
               if df[col].sum() > 0]

    if not nonzero:
        print("No data to plot.")
        return

    tech_cols, tech_names = zip(*nonzero)

    x = df["num_clusters"].values

    fig = go.Figure()

    for col, name in zip(tech_cols, tech_names):
        fig.add_trace(
            go.Scatter(
                x=x,
                y=df[col],
                mode="lines",
                name=name,
                stackgroup="one",   # 👈 THIS is the stackplot magic
                line=dict(width=0.5),
                fillcolor=TECH_COLORS.get(name, None),
            )
        )

    fig.update_layout(
        title=title,
        xaxis_title="Number of clusters",
        yaxis_title=ylabel,
        hovermode="x unified",
    )

    fig.write_html(output_path)
    fig.show()

    print(f"Saved to {output_path}")


# -----------------------------
# Run
# -----------------------------
df = pd.read_csv(csv_path)


df = df[df["file_name"].str.contains("perlocation_SeperateExtremesSum")].copy()

# Example: costs
make_stacked_plot(
    df,
    prefix="cost_",
    ylabel="Investment cost",
    title="Investment Costs by Technology"
)

# Example: capacities (optional second plot)
make_stacked_plot(
    df,
    prefix="capacity_",
    ylabel="Capacity",
    title="Installed Capacity by Technology"
)