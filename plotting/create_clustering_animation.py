"""
extreme_preservation_animation.py

Visualizes the "after-clustering extreme preservation" method:

1.  extreme_bounds_frame.png
    The final clustering state (identical to clustering_final_frame.png)
    with threshold lines added:
      - Demand:        upper dashed line at the high-percentile threshold
      - Solar:         lower dashed line at the low-percentile threshold
      - Wind offshore: lower dashed line at the low-percentile threshold
      - Wind onshore:  lower dashed line at the low-percentile threshold

2.  extreme_preservation_animation.gif
    Starts from the bounds frame.  Cluster representative values that
    breach a threshold animate to the extreme (max or min) of that
    cluster.  All moving values move simultaneously.
    Plays once and freezes on the final frame.

3.  extreme_preservation_final_frame.png
    Static PNG of the completed extreme-adjusted state.

Usage:
    pip install numpy matplotlib Pillow
    python extreme_preservation_animation.py
"""

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
from PIL import Image
import io
import heapq

# ══════════════════════════════════════════════
# 1.  Reproduce profiles + clustering
#     (identical seed / logic to clustering_animation.py)
# ══════════════════════════════════════════════

np.random.seed(42)
N    = 144
DAYS = 6
PROFILE_LABELS = ["Demand", "Solar", "Wind offshore", "Wind onshore"]
COLORS_PROFILE = ["#185FA5", "#E8A020", "#0F6E56", "#533AB7"]

hours       = np.arange(N)
hour_of_day = hours % 24

demand = (0.5 + 0.3 * np.sin(2 * np.pi * (hour_of_day - 6) / 24)
          + 0.05 * np.random.randn(N))
demand[17] += 0.25
demand[18] += 0.20

solar_base = np.maximum(0, np.sin(np.pi * (hour_of_day - 6) / 12))
solar = (solar_base * (0.6 + 0.4 * np.sin(2 * np.pi * hours / 48))
         + 0.02 * np.abs(np.random.randn(N)))
solar = np.clip(solar, 0, None)
solar[48:72]   *= 0.05
solar[120:144] *= 0.10

wind_off = 0.4 + 0.35 * np.sin(2 * np.pi * hours / 36 + 1.2) + 0.08 * np.random.randn(N)
wind_off = np.clip(wind_off, 0, 1)
wind_off[30:36] = 0.03

wind_on = 0.35 + 0.30 * np.sin(2 * np.pi * hours / 40 + 2.5) + 0.07 * np.random.randn(N)
wind_on = np.clip(wind_on, 0, 1)

values = np.column_stack([demand, solar, wind_off, wind_on])   # (N, 4)

# ── Thresholds (mirrors hierarchical_time_clustering_ward) ──────────────

HIGH_PERCENTILE = 0.95
LOW_PERCENTILE  = 0.05

high_thresholds = np.zeros(4)
low_thresholds  = np.zeros(4)

for j in range(4):
    col_sorted = np.sort(values[:, j])
    high_thresholds[j] = col_sorted[int(np.ceil(HIGH_PERCENTILE * N)) - 1]
    low_thresholds[j]  = col_sorted[int(np.ceil(LOW_PERCENTILE  * N)) - 1]

# Profile types: 0=Demand (high extreme), 1-3=Solar/Wind (low extreme)
IS_DEMAND = [True, False, False, False]   # True → watch upper threshold

# ── Ward clustering ──────────────────────────────────────────────────────

class Cluster:
    def __init__(self, i, vals):
        self.start  = i
        self.end    = i
        self.sum_v  = vals.copy()
        self.max_v  = vals.copy()
        self.min_v  = vals.copy()
        self.count  = 1
        self.rep    = vals.copy()
        self.active = True
        self.prev = self.next = None

_ctr = [0]

def _ward(c1, c2):
    d = c1.rep - c2.rep
    return (c1.count * c2.count) / (c1.count + c2.count) * float(d @ d)

def _entry(c1, c2):
    _ctr[0] += 1
    return (_ward(c1, c2), _ctr[0], c1, c2)

N_PRIME = 12

def run_clustering():
    cs = [Cluster(i, values[i]) for i in range(N)]
    for i in range(N - 1):
        cs[i].next = cs[i + 1]
        cs[i + 1].prev = cs[i]
    heap = [_entry(cs[i], cs[i + 1]) for i in range(N - 1)]
    heapq.heapify(heap)
    for _ in range(N - N_PRIME):
        while heap:
            d, cnt, c1, c2 = heapq.heappop(heap)
            if c1.active and c2.active and c1.next is c2:
                break
        # merge c2 into c1
        c1.end    = c2.end
        c1.sum_v += c2.sum_v
        c1.max_v  = np.maximum(c1.max_v, c2.max_v)
        c1.min_v  = np.minimum(c1.min_v, c2.min_v)
        c1.count += c2.count
        c1.rep    = c1.sum_v / c1.count
        c1.next   = c2.next
        if c2.next:
            c2.next.prev = c1
        c2.active = False
        if c1.prev and c1.prev.active:
            heapq.heappush(heap, _entry(c1.prev, c1))
        if c1.next and c1.next.active:
            heapq.heappush(heap, _entry(c1, c1.next))
    return [c for c in cs if c.active]

active_clusters = run_clustering()
final_parts = [c.count for c in active_clusters]
final_reps  = [c.rep.copy() for c in active_clusters]

def build_step_array(parts, reps, col):
    arr = np.empty(N)
    idx = 0
    for p, r in zip(parts, reps):
        arr[idx:idx + p] = r[col]
        idx += p
    return arr

clustered_mean = np.column_stack([
    build_step_array(final_parts, final_reps, c) for c in range(4)
])

# ── Extreme-adjusted representatives ──────────────────────────────────
# For each cluster and each column, if the cluster's max/min breaches the
# threshold, the representative is replaced by that max or min.

extreme_reps = []
for c in active_clusters:
    rep = c.rep.copy()
    for j in range(4):
        if IS_DEMAND[j]:
            if c.max_v[j] >= high_thresholds[j]:
                rep[j] = c.max_v[j]
        else:
            if c.min_v[j] <= low_thresholds[j]:
                rep[j] = c.min_v[j]
    extreme_reps.append(rep)

clustered_extreme = np.column_stack([
    build_step_array(final_parts, extreme_reps, c) for c in range(4)
])

# Which timesteps actually move (mean ≠ extreme)?
moves_mask = ~np.isclose(clustered_mean, clustered_extreme, atol=1e-10)

# ══════════════════════════════════════════════
# 2.  Shared style helpers
# ══════════════════════════════════════════════

FIGSIZE = (9, 8)
DPI     = 110
BG      = "#F8F7F4"
YLIMS   = [(0.1, 1.1), (0.0, 1.05), (0.0, 1.05), (0.0, 1.05)]

THRESHOLD_COLORS = {
    "high": "#C0392B",   # red — demand upper bound
    "low":  "#8E44AD",   # purple — wind/solar lower bound
}

def _base_ax(ax, col, bottom=False):
    ax.set_xlim(-1, N)
    ax.set_ylim(*YLIMS[col])
    ax.set_ylabel(PROFILE_LABELS[col], fontsize=9, labelpad=4)
    ax.tick_params(labelsize=8)
    ax.set_facecolor("#FFFFFF")
    for sp in ax.spines.values():
        sp.set_linewidth(0.5)
        sp.set_color("#CCCCCC")
    if not bottom:
        ax.set_xticklabels([])
    for d in range(1, DAYS):
        ax.axvline(x=d * 24 - 0.5, color="#888", linewidth=0.5,
                   linestyle="--", alpha=0.35)


def _draw_threshold(ax, col):
    """Draw the relevant threshold line for a panel."""
    if IS_DEMAND[col]:
        ax.axhline(y=high_thresholds[col],
                   color=THRESHOLD_COLORS["high"], linewidth=1.4,
                   linestyle=":", alpha=0.85, zorder=4)
    else:
        ax.axhline(y=low_thresholds[col],
                   color=THRESHOLD_COLORS["low"], linewidth=1.4,
                   linestyle=":", alpha=0.85, zorder=4)


def _draw_clustered_steps(ax, col, rep_array):
    """Draw the clustered step function in red."""
    idx = 0
    for i, (p, r) in enumerate(zip(final_parts, rep_array)):
        seg = hours[idx:idx + p]
        ax.plot(seg, np.full(p, r[col]),
                color="#C0392B", linewidth=2.2,
                label="Clustered" if i == 0 else "")
        if p == 1:
            ax.scatter([hours[idx]], [r[col]],
                       color="#C0392B", s=18, zorder=5)
        if idx > 0:
            ax.axvline(x=hours[idx] - 0.5,
                       color="#C0392B", linewidth=0.4, alpha=0.35)
        idx += p


def _draw_original(ax, col):
    ax.plot(hours, values[:, col],
            color=COLORS_PROFILE[col], linewidth=1.4, alpha=0.55)


# ══════════════════════════════════════════════
# 3.  Static bounds frame
# ══════════════════════════════════════════════

def render_bounds_frame():
    fig = plt.figure(figsize=FIGSIZE, dpi=DPI)
    fig.patch.set_facecolor(BG)
    gs = GridSpec(4, 1, figure=fig, hspace=0.55,
                  top=0.90, bottom=0.07, left=0.10, right=0.97)

    for col in range(4):
        ax = fig.add_subplot(gs[col])
        _base_ax(ax, col, bottom=(col == 3))
        _draw_original(ax, col)
        _draw_clustered_steps(ax, col, final_reps)
        _draw_threshold(ax, col)

        if col == 3:
            ax.set_xlabel("Hour", fontsize=9)

        if col == 0:
            ax.legend(handles=[
                mpatches.Patch(color=COLORS_PROFILE[0], alpha=0.6, label="Original"),
                mpatches.Patch(color="#C0392B", label="Clustered (mean)"),
                mpatches.Patch(color=THRESHOLD_COLORS["high"],
                               label=f"Upper bound (p{int(HIGH_PERCENTILE*100)})"),
            ], fontsize=7.5, loc="upper right", framealpha=0.7, edgecolor="#CCCCCC")

        # Label the threshold line
        tval = high_thresholds[col] if IS_DEMAND[col] else low_thresholds[col]
        tcolor = THRESHOLD_COLORS["high"] if IS_DEMAND[col] else THRESHOLD_COLORS["low"]
        label = f"{'upper' if IS_DEMAND[col] else 'lower'} bound ({tval:.2f})"
        ax.text(N - 1, tval, label, fontsize=7, color=tcolor,
                ha="right", va="bottom" if IS_DEMAND[col] else "top",
                alpha=0.9)

    fig.suptitle(f"Extreme thresholds on clustered values — {N_PRIME} clusters",
                 fontsize=11, fontweight="normal", y=0.96, color="#222222")

    fig.savefig("extreme_bounds_frame.png", facecolor=BG, dpi=DPI)
    plt.close(fig)
    print("Saved → extreme_bounds_frame.png")

render_bounds_frame()


# ══════════════════════════════════════════════
# 4.  Animation frame renderer
#
#  t in [0, 1]: all extreme-breaching cluster reps animate
#               from mean value → extreme value simultaneously
#  t = 1: final state with both lines fully drawn
# ══════════════════════════════════════════════

def ease_in_out(x):
    return x * x * (3 - 2 * x)


def interpolated_reps(t):
    """Return list of rep vectors interpolated between mean and extreme."""
    e = ease_in_out(np.clip(t, 0, 1))
    reps = []
    for i, c in enumerate(active_clusters):
        r = final_reps[i] * (1 - e) + extreme_reps[i] * e
        reps.append(r)
    return reps


def render_anim_frame(t):
    """
    t in [0, 1]: interpolate cluster reps from mean to extreme.
    """
    fig = plt.figure(figsize=FIGSIZE, dpi=DPI)
    fig.patch.set_facecolor(BG)
    gs = GridSpec(4, 1, figure=fig, hspace=0.55,
                  top=0.90, bottom=0.07, left=0.10, right=0.97)

    current_reps = interpolated_reps(t)

    for col in range(4):
        ax = fig.add_subplot(gs[col])
        _base_ax(ax, col, bottom=(col == 3))
        _draw_original(ax, col)
        _draw_threshold(ax, col)

        # Draw each cluster segment; highlight moving ones
        idx = 0
        for i, (p, r) in enumerate(zip(final_parts, current_reps)):
            seg = hours[idx:idx + p]
            val = r[col]
            start_val = final_reps[i][col]
            end_val   = extreme_reps[i][col]
            is_moving = not np.isclose(start_val, end_val, atol=1e-10)

            if is_moving and 0 < t < 1:
                color = "#E67E22"   # orange while in motion
                lw    = 2.6
                alpha = 1.0
            elif is_moving and t >= 1:
                color = "#8E44AD" if not IS_DEMAND[col] else "#C0392B"
                lw    = 2.4
                alpha = 1.0
            else:
                color = "#C0392B"
                lw    = 2.0
                alpha = 0.75

            ax.plot(seg, np.full(p, val),
                    color=color, linewidth=lw, alpha=alpha, zorder=3)
            if p == 1:
                ax.scatter([hours[idx]], [val],
                           color=color, s=18, zorder=5)
            if idx > 0:
                ax.axvline(x=hours[idx] - 0.5,
                           color="#C0392B", linewidth=0.4, alpha=0.3)
            idx += p

        if col == 3:
            ax.set_xlabel("Hour", fontsize=9)

        # Legend on top panel at final frame
        if col == 0 and t >= 1.0:
            ax.legend(handles=[
                mpatches.Patch(color=COLORS_PROFILE[0], alpha=0.6, label="Original"),
                mpatches.Patch(color="#C0392B", label="Clustered (mean)"),
                mpatches.Patch(color="#C0392B",
                               label="Adjusted to peak/trough"),
            ], fontsize=7.5, loc="upper right", framealpha=0.7, edgecolor="#CCCCCC")

        # Threshold label
        tval   = high_thresholds[col] if IS_DEMAND[col] else low_thresholds[col]
        tcolor = THRESHOLD_COLORS["high"] if IS_DEMAND[col] else THRESHOLD_COLORS["low"]
        label  = f"{'upper' if IS_DEMAND[col] else 'lower'} bound ({tval:.2f})"
        ax.text(N - 1, tval, label, fontsize=7, color=tcolor,
                ha="right", va="bottom" if IS_DEMAND[col] else "top", alpha=0.9)

    pct = int(np.clip(t, 0, 1) * 100)
    if t == 0:
        title = "Extreme preservation — clusters breaching threshold will adjust"
    elif t >= 1.0:
        title = "Extreme preservation — complete"
    else:
        title = f"Adjusting extreme cluster representatives — {pct}%"

    fig.suptitle(title, fontsize=11, fontweight="normal", y=0.96, color="#222222")

    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=BG)
    plt.close(fig)
    buf.seek(0)
    return Image.open(buf).copy()


# ══════════════════════════════════════════════
# 5.  Build frames
# ══════════════════════════════════════════════

ANIM_FRAMES   = 40
PAUSE_FRAMES  = 8    # hold at t=0 before moving
FREEZE_FRAMES = 30   # hold at the end

frames    = []
durations = []

def add(img, ms):
    frames.append(img)
    durations.append(ms)

print("Rendering extreme preservation animation …")

# Hold on bounds frame
init_frame = render_anim_frame(0.0)
for _ in range(PAUSE_FRAMES):
    add(init_frame, 80)
print("  initial hold done")

# Animate t 0 → 1
for i in range(1, ANIM_FRAMES + 1):
    t = i / ANIM_FRAMES
    add(render_anim_frame(t), 55)
    if i % 10 == 0:
        print(f"  anim frame {i}/{ANIM_FRAMES}")

# Freeze on final frame
final_frame = render_anim_frame(1.0)
for _ in range(FREEZE_FRAMES):
    add(final_frame, 60)

print(f"Total frames: {len(frames)}")

# ══════════════════════════════════════════════
# 6.  Save outputs
# ══════════════════════════════════════════════

out_gif = "extreme_preservation_animation.gif"
frames[0].save(
    out_gif,
    save_all      = True,
    append_images = frames[1:],
    duration      = durations,
    loop          = 1,       # play once, freeze on last frame
    optimize      = False,
)
print(f"Saved → {out_gif}")

final_frame.save("extreme_preservation_final_frame.png")
print("Saved → extreme_preservation_final_frame.png")