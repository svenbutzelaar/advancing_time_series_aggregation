import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch
import numpy as np
import textwrap

# ── Data ──────────────────────────────────────────────────────────────────────

FLOWS_RAW = """from,to,perlocation_old,perlocation_new,perprofile_old,perprofile_new
X_Battery,X_E_Balance,1,A,1,ABCD
X_E_Balance,X_Battery,1,A,1,ABCD
X_E_Balance,X_E_Demand,A,A,A,A
X_E_Balance,X_electrolyzer,1,A,1,ABCD
X_E_Balance,Y_E_Balance,1,AE,1,ABCDEFGH
X_E_Balance,Z_E_Balance,1,AI,1,ABCDIJKL
X_E_ENS,X_E_Demand,A,A,A,A
X_Gas,X_E_Balance,1,A,1,ABCD
X_OCGT,X_E_Balance,1,A,1,ABCD
X_Nuclear,X_E_Balance,1,A,1,ABCD
X_Coal,X_E_Balance,1,A,1,ABCD
X_Hydro,X_E_Balance,1,A,1,ABCD
X_Hydro_Reservoir,X_E_Balance,1,A,1,ABCD
X_Pump_Hydro_Closed,X_E_Balance,24,A,24,ABCD
X_Pump_Hydro_Open,X_E_Balance,24,A,24,ABCD
X_E_Balance,X_Pump_Hydro_Closed,24,A,24,ABCD
X_E_Balance,X_Pump_Hydro_Open,24,A,24,ABCD
X_Solar,X_E_Balance,A,A,B,B
X_Wind_Onshore,X_E_Balance,A,A,C,C
X_Wind_Offshore,X_E_Balance,A,A,D,D
X_electrolyzer,X_H_Demand,1,A,1,ABCD"""

ASSETS_RAW = """asset,perlocation_old,perlocation_new,perprofile_old,perprofile_new
X_Battery,1,A,1,ABCD
X_E_Balance,1,A,1,ABCD
Y_E_Balance,1,E,1,EFGH
Z_E_Balance,1,I,1,IJKL
X_E_Demand,A,A,A,A
X_E_ENS,1,A,1,A
X_Gas,1,A,1,ABCD
X_OCGT,1,A,1,ABCD
X_Nuclear,1,A,1,ABCD
X_Coal,1,A,1,ABCD
X_H_Demand,1,A,1,ABCD
X_Hydro,1,A,1,ABCD
X_Hydro_Reservoir,1,A,1,ABCD
X_Pump_Hydro_Closed,24,A,24,ABCD
X_Pump_Hydro_Open,24,A,24,ABCD
X_Solar,A,A,B,B
X_Wind_Onshore,A,A,C,C
X_Wind_Offshore,A,A,D,D
X_electrolyzer,1,A,1,ABCD"""

# ── Parse ─────────────────────────────────────────────────────────────────────

def parse_csv(raw):
    lines = raw.strip().split("\n")
    header = lines[0].split(",")
    rows = []
    for line in lines[1:]:
        vals = line.split(",")
        rows.append(dict(zip(header, vals)))
    return rows

flows = parse_csv(FLOWS_RAW)
assets = parse_csv(ASSETS_RAW)

# ── Layout: hand-placed node positions ────────────────────────────────────────
# Three location clusters: X (left), Y (top-right), Z (bottom-right)
# Within X: generators on left, balance node in center, demands on right

NODE_POS = {
    # X location — generators (left column)
    "X_Solar":              (-3.5,  2.0),
    "X_Wind_Onshore":       (-3.5,  1.0),
    "X_Wind_Offshore":      (-3.5,  0.0),
    "X_Gas":                (-3.5, -1.0),
    "X_OCGT":               (-3.5, -2.0),
    "X_Nuclear":            (-3.5, -3.0),
    "X_Coal":               (-3.5, -4.0),
    "X_Hydro":              (-3.5,  3.0),
    "X_Hydro_Reservoir":    (-3.5,  4.0),
    "X_Pump_Hydro_Closed":  (-2.5,  3.5),
    "X_Pump_Hydro_Open":    (-2.5,  4.5),
    # X balance node (center)
    "X_E_Balance":          ( 0.0,  0.0),
    # X demands (right)
    "X_E_Demand":           ( 1.0,  1.0),
    "X_E_ENS":              ( 1.0,  2.0),
    "X_Battery":            ( 1.0, -1.0),
    "X_electrolyzer":       ( 1.0, -2.5),
    "X_H_Demand":           ( 1.0, -3.5),
    # Y location (top right cluster)
    "Y_E_Balance":          ( 5.5,  3.0),
    # Z location (bottom right cluster)
    "Z_E_Balance":          ( 5.5, -3.0),
}

# Location grouping for coloring
LOCATION = {n: "X" for n in NODE_POS if n.startswith("X")}
LOCATION.update({n: "Y" for n in NODE_POS if n.startswith("Y")})
LOCATION.update({n: "Z" for n in NODE_POS if n.startswith("Z")})

LOC_COLOR = {"X": "#4A90D9", "Y": "#E8834A", "Z": "#5BAD6F"}
LOC_LIGHT = {"X": "#D6E8F7", "Y": "#FCE8D8", "Z": "#D5EFD9"}

# ── Resolution color coding ───────────────────────────────────────────────────
# "1" or "24" = uniform (numeric) → grey tones
# letters = custom → warm/cool ramp per unique resolution

def resolution_color(res, palette):
    """Map a resolution string to a color."""
    if res in ("1", "24"):
        return "#AAAAAA"
    return palette.get(res, "#444444")

def build_palette(all_res_values):
    """Assign distinct colors to each unique letter-based resolution."""
    unique = sorted(set(v for v in all_res_values if v not in ("1", "24")))
    # Use a colorblind-friendly qualitative palette
    colors = [
        "#2190AC", "#645BE6", "#AA467D", "#1C7768",
        "#7A194F", "#0E4B41", "#DFDB14", "#61AAFD",
        "#0C0FC5", "#E25E5EFF", "#D6604D",
        "#0C7038", "#084623", 
    ]
    labels = [
        "A", "ABCD", "ABCDEFG", "ABCDIJKL",
        "AE", "AI", "B", "C",
        "D", "E", "EFGH",
        "I", "IJKL",
    ]
    return {res: colors[i % len(colors)] for i, res in enumerate(unique)}

# Collect all resolution values
col_pairs = [
    ("perlocation_old",  "Per-location — Old experiments"),
    ("perlocation_new",  "Per-location — New experiments"),
    ("perprofile_old",   "Per-profile — Old experiments"),
    ("perprofile_new",   "Per-profile — New experiments"),
]

all_res = []
for col, _ in col_pairs:
    for row in flows:
        all_res.append(row[col])
    for row in assets:
        all_res.append(row[col])

palette = build_palette(all_res)

# ── Short labels for nodes ─────────────────────────────────────────────────────
SHORT = {
    "X_Solar":              "Solar",
    "X_Wind_Onshore":       "Wind\nOnshore",
    "X_Wind_Offshore":      "Wind\nOffshore",
    "X_Gas":                "Gas",
    "X_OCGT":               "OCGT",
    "X_Nuclear":            "Nuclear",
    "X_Coal":               "Coal",
    "X_Hydro":              "Hydro",
    "X_Hydro_Reservoir":    "Hydro\nReservoir",
    "X_Pump_Hydro_Closed":  "Pump\nClosed",
    "X_Pump_Hydro_Open":    "Pump\nOpen",
    "X_E_Balance":          "X\nBalance",
    "X_E_Demand":           "Demand",
    "X_E_ENS":              "ENS",
    "X_Battery":            "Battery",
    "X_electrolyzer":       "Electrolyzer",
    "X_H_Demand":           "H₂\nDemand",
    "Y_E_Balance":          "Y\nBalance",
    "Z_E_Balance":          "Z\nBalance",
}

# ── Draw one panel ─────────────────────────────────────────────────────────────

def draw_panel(ax, col, asset_lookup, flow_rows):
    ax.set_aspect("equal")
    ax.axis("off")

    # Background location blobs
    loc_node_groups = {}
    for n, loc in LOCATION.items():
        loc_node_groups.setdefault(loc, []).append(n)

    for loc, nodes in loc_node_groups.items():
        xs = [NODE_POS[n][0] for n in nodes]
        ys = [NODE_POS[n][1] for n in nodes]
        pad = 0.7
        cx, cy = np.mean(xs), np.mean(ys)
        w = (max(xs) - min(xs)) / 2 + pad
        h = (max(ys) - min(ys)) / 2 + pad
        ellipse = mpatches.Ellipse(
            (cx, cy), width=w * 4, height=h * 2,
            color=LOC_LIGHT[loc], zorder=0, alpha=0.6
        )
        ax.add_patch(ellipse)
        ax.text(cx, max(ys) + pad * 0.55, f"Location {loc}",
                ha="center", va="bottom", fontsize=7,
                color=LOC_COLOR[loc], fontweight="bold")

    # Draw edges
    # Build a dict for quick resolution lookup
    flow_res = {(r["from"], r["to"]): r[col] for r in flow_rows}

    # Collect bidirectional pairs to offset
    drawn_pairs = set()
    for r in flow_rows:
        src, dst = r["from"], r["to"]
        if src not in NODE_POS or dst not in NODE_POS:
            continue
        key = (src, dst)
        reverse = (dst, src)
        if key in drawn_pairs:
            continue
        drawn_pairs.add(key)

        res = flow_res[key]
        color = resolution_color(res, palette)

        x0, y0 = NODE_POS[src]
        x1, y1 = NODE_POS[dst]

        bidirectional = reverse in flow_res
        if bidirectional:
            drawn_pairs.add(reverse)

        # Offset for bidirectional
        if bidirectional:
            dx, dy = x1 - x0, y1 - y0
            length = np.hypot(dx, dy)
            nx, ny = -dy / length * 0.12, dx / length * 0.12
            ax.annotate("", xy=(x1 + nx, y1 + ny), xytext=(x0 + nx, y0 + ny),
                        arrowprops=dict(arrowstyle="-|>", color=color,
                                        lw=1.2, mutation_scale=8))
            res2 = flow_res.get(reverse, res)
            color2 = resolution_color(res2, palette)
            ax.annotate("", xy=(x0 - nx, y0 - ny), xytext=(x1 - nx, y1 - ny),
                        arrowprops=dict(arrowstyle="-|>", color=color2,
                                        lw=1.2, mutation_scale=8))
            # Label at midpoint — only if both same res, or show both
            mid_x, mid_y = (x0 + x1) / 2, (y0 + y1) / 2
            label = res if res == res2 else f"{res}\n{res2}"
            ax.text(mid_x + nx * 1.5, mid_y + ny * 1.5, label,
                    ha="center", va="center", fontsize=5.5,
                    color=color, fontweight="bold",
                    bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none", alpha=0.7))
        else:
            ax.annotate("", xy=(x1, y1), xytext=(x0, y0),
                        arrowprops=dict(arrowstyle="-|>", color=color,
                                        lw=1.2, mutation_scale=8))
            mid_x, mid_y = (x0 + x1) / 2, (y0 + y1) / 2
            ax.text(mid_x, mid_y, res,
                    ha="center", va="center", fontsize=5.5,
                    color=color, fontweight="bold",
                    bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none", alpha=0.7))

    # Draw nodes
    for node, (x, y) in NODE_POS.items():
        res = asset_lookup.get(node, {}).get(col, "?")
        color = resolution_color(res, palette)
        loc = LOCATION.get(node, "X")

        # Node circle
        circle = plt.Circle((x, y), 0.28, color=color, zorder=3)
        ax.add_patch(circle)
        circle_border = plt.Circle((x, y), 0.28, fill=False,
                                   edgecolor="white", linewidth=1.0, zorder=4)
        ax.add_patch(circle_border)

        # Resolution label inside circle
        ax.text(x, y, res, ha="center", va="center",
                fontsize=5, color="white", fontweight="bold", zorder=5)

        # Node name below
        label = SHORT.get(node, node.replace("_", "\n"))
        ax.text(x, y - 0.38, label, ha="center", va="top",
                fontsize=5, color="#333333", zorder=5,
                multialignment="center")

    # Axis limits
    all_x = [p[0] for p in NODE_POS.values()]
    all_y = [p[1] for p in NODE_POS.values()]
    ax.set_xlim(min(all_x) - 1.2, max(all_x) + 1.2)
    ax.set_ylim(min(all_y) - 1.0, max(all_y) + 1.2)


# ── Legend ────────────────────────────────────────────────────────────────────

def draw_legend(fig, palette):
    handles = []
    # Uniform resolutions
    for label, color in [("1 (uniform hourly)", "#AAAAAA"),
                          ("24 (uniform daily)", "#AAAAAA")]:
        handles.append(mpatches.Patch(color=color, label=label, alpha=0.5))
    # Custom resolutions
    for res, color in sorted(palette.items()):
        handles.append(mpatches.Patch(color=color, label=f"Resolution {res}"))

    fig.legend(
        handles=handles,
        loc="lower center",
        ncol=min(len(handles), 6),
        fontsize=10,
        title="Temporal resolution",
        title_fontsize=8,
        frameon=True,
        bbox_to_anchor=(0.5, 0.01),
    )


# ── Main: 2×2 figure ──────────────────────────────────────────────────────────

asset_lookup = {r["asset"]: r for r in assets}

# fig, axes = plt.subplots(2, 2, figsize=(18, 14))
# fig.patch.set_facecolor("white")

# titles = [
#     ("perlocation_old",  "Per-location  |  Old experiments"),
#     ("perlocation_new",  "Per-location  |  New experiments"),
#     ("perprofile_old",   "Per-profile   |  Old experiments"),
#     ("perprofile_new",   "Per-profile   |  New experiments"),
# ]

# for ax, (col, title) in zip(axes.flat, titles):
#     draw_panel(ax, col, asset_lookup, flows)
#     ax.set_title(title, fontsize=10, fontweight="bold", pad=8)

# draw_legend(fig, palette)

# plt.suptitle("Temporal resolution assignment per configuration",
#              fontsize=10, fontweight="bold", y=0.98)
# plt.tight_layout(rect=[0, 0.07, 1, 0.97])
# plt.savefig("plots/explaining/resolution_diagrams.png",
#             dpi=180, bbox_inches="tight", facecolor="white")

# ── Main: 4 separate figures ──────────────────────────────────────────────────

plots_dir = "plots/explaining"

titles = [
    ("perlocation_old",  "Per-location | Old experiments",  "perlocation_old"),
    ("perlocation_new",  "Per-location | New experiments",  "perlocation_new"),
    ("perprofile_old",   "Per-profile | Old experiments",   "perprofile_old"),
    ("perprofile_new",   "Per-profile | New experiments",   "perprofile_new"),
]

for col, title, filename in titles:
    fig, ax = plt.subplots(figsize=(9, 7))
    fig.patch.set_facecolor("white")

    draw_panel(ax, col, asset_lookup, flows)
    
    ax.set_title(f"Temporal resolution assignment per configuration\n{title}",
                fontsize=10, fontweight="bold", y=0.98)

    draw_legend(fig, palette)

    plt.tight_layout(rect=[0, 0.07, 1, 0.97])

    output_file = f"{plots_dir}/{filename}.png"
    plt.savefig(
        output_file,
        dpi=180,
        bbox_inches="tight",
        facecolor="white"
    )
    plt.close(fig)

    print(f"Saved to {output_file}")