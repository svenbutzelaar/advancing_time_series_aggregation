import matplotlib.pyplot as plt

# Total number of original timesteps
TOTAL_TIMESTEPS = 8760

BAR_HEIGHT = 0.8

# Define experiments
# Each experiment: (label, [(remaining_timesteps_after_step, method), ...])
experiments = [
    # --- 5 days ---
    ("5 days – Representative Periods",
     [(5 * 24, "rep")]),

    ("5 days – Partitions (HC)",
     [(5 * 24, "partition")]),

    ("1 week RP → 5 days Partitions",
     [(1 * 7 * 24, "rep"), (5 * 24, "partition")]),

    ("2 weeks RP → 5 days Partitions",
     [(2 * 7 * 24, "rep"), (5 * 24, "partition")]),

    ("5 weeks RP → 5 days Partitions",
     [(5 * 7 * 24, "rep"), (5 * 24, "partition")]),

    ("10 weeks RP → 5 days Partitions",
     [(10 * 7 * 24, "rep"), (5 * 24, "partition")]),

    ("15 weeks RP → 5 days Partitions",
     [(15 * 7 * 24, "rep"), (5 * 24, "partition")]),

    ("10 weeks Partitions → 5 days RP",
     [(10 * 7 * 24, "partition"), (5 * 24, "rep")]),

    # --- 10 days ---
    ("10 days – Representative Periods",
     [(10 * 24, "rep")]),

    ("10 days – Partitions (HC)",
     [(10 * 24, "partition")]),

    ("24 weeks RP → 10 days Partitions",
     [(24 * 7 * 24, "rep"), (10 * 24, "partition")]),

    ("24 weeks Partitions → 10 days RP",
     [(24 * 7 * 24, "partition"), (10 * 24, "rep")]),

    # --- 20 days ---
    ("20 days – Representative Periods",
     [(20 * 24, "rep")]),

    ("20 days – Partitions (HC)",
     [(20 * 24, "partition")]),

    ("24 weeks RP → 20 days Partitions",
     [(24 * 7 * 24, "rep"), (20 * 24, "partition")]),

    ("24 weeks Partitions → 20 days RP",
     [(24 * 7 * 24, "partition"), (20 * 24, "rep")]),

    # --- 40 days ---
    ("40 days – Representative Periods",
     [(40 * 24, "rep")]),

    ("40 days – Partitions (HC)",
     [(40 * 24, "partition")]),

    ("24 weeks RP → 40 days Partitions",
     [(24 * 7 * 24, "rep"), (40 * 24, "partition")]),

    ("24 weeks Partitions → 40 days RP",
     [(24 * 7 * 24, "partition"), (40 * 24, "rep")]),

    # --- 80 days ---
    ("80 days – Representative Periods",
     [(80 * 24, "rep")]),

    ("80 days – Partitions (HC)",
     [(80 * 24, "partition")]),

    ("24 weeks RP → 80 days Partitions",
     [(24 * 7 * 24, "rep"), (80 * 24, "partition")]),

    ("24 weeks Partitions → 80 days RP",
     [(24 * 7 * 24, "partition"), (80 * 24, "rep")]),
]


# Colors for methods (removed data)
COLORS = {
    "rep": "#1f77b4",        # blue
    "partition": "#ff7f0e", # orange
}

HATCHES = {
    "rep": None,
    "partition": "//"       # helps for grayscale printing
}

fig_height = 0.30 * len(experiments) + 1.5
fig, ax = plt.subplots(figsize=(16, fig_height))

y_positions = range(len(experiments))

for i, (label, steps) in enumerate(experiments):
    left = 0
    prev_remaining = TOTAL_TIMESTEPS

    # Draw removed parts (colored)
    for remaining, method in steps:
        removed = prev_remaining - remaining

        ax.barh(
            i,
            removed,
            left=left,
            color=COLORS[method],
            hatch=HATCHES[method],
            edgecolor="black",
            height=BAR_HEIGHT
        )

        left += removed
        prev_remaining = remaining

    # Draw remaining data (grey)
    ax.barh(
        i,
        prev_remaining,
        left=left,
        color="lightgray",
        edgecolor="black",
        height=BAR_HEIGHT
    )

    # Percentage annotation (remaining data)
    percent_remaining = 100 * prev_remaining / TOTAL_TIMESTEPS
    ax.text(
        left + prev_remaining + 100,
        i,
        f"{percent_remaining:.1f}%",
        va="center",
        fontsize=9
    )

# Axis formatting
ax.set_yticks(list(y_positions))
ax.set_yticklabels([exp[0] for exp in experiments])
ax.set_xlabel("Number of hourly timesteps")
ax.set_xlim(0, TOTAL_TIMESTEPS)

# Legend
legend_elements = [
    plt.Rectangle((0, 0), 1, 1, color=COLORS["rep"],
                  label="Removed by representative periods"),
    plt.Rectangle((0, 0), 1, 1, color=COLORS["partition"],
                  hatch="//", label="Removed by partition clustering"),
    plt.Rectangle((0, 0), 1, 1, color="lightgray",
                  label="Remaining data"),
]

ax.legend(
    handles=legend_elements,
    loc="center left",
    bbox_to_anchor=(1.05, 0.5),
    frameon=False
)

plt.tight_layout(rect=[0, 0, 0.85, 1])

ax.set_title("Temporal Data Removed by Resolution Reduction Methods")

plt.tight_layout()

plt.savefig("experiments_1.svg")
