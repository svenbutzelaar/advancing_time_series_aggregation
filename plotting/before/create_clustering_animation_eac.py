"""
eac_merge_by_merge_animation.py

Shows the hierarchical clustering PROCESS itself, one merge per frame,
comparing plain Ward clustering (HC, top panel) against Extreme-Aware
Clustering (EAC, bottom panel) side by side on the same demand profile.

This is the mechanism-level companion to eac_visualization.py (which only
showed the final partition + post-hoc correction). Here the point is to
make visible *why* EAC's partitions end up different: at every step we
show which merge was just performed and whether it was a "non-conflicting"
merge (both clusters agree on extreme/non-extreme status) or a
"cross-boundary" merge (an extreme cluster absorbing/being absorbed by a
non-extreme one).

Mechanism recap (mirrors hierarchical_time_clustering_ward.jl):
  - HC:  candidate merges are ranked by Ward dissimilarity only.
  - EAC: candidate merges are ranked lexicographically by
         (extreme_conflict_score, ward_dissimilarity). Every zero-conflict
         merge is exhausted before a single cross-boundary merge is
         allowed, regardless of how cheap that cross-boundary merge would
         be under Ward's criterion. A cluster's is_extreme flag is
         OR-combined into its parent on every merge.

Because both processes start from the same 48 singletons and run the same
number of merges (down to N_PRIME clusters), watching them side by side
frame-by-frame shows exactly when and how EAC "protects" extreme
timesteps that HC merges away early.

Output:
    eac_merge_by_merge.gif
    eac_merge_by_merge_final_frame.png

Usage:
    pip install numpy matplotlib Pillow
    python eac_merge_by_merge_animation.py
"""

import json

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from PIL import Image
import io
import heapq

# ══════════════════════════════════════════════
# 1.  Demand profile (single profile, for clarity)
# ══════════════════════════════════════════════

np.random.seed(44)
N       = 144
DAYS    = 6
N_PRIME = 16

hours       = np.arange(N)
hour_of_day = hours % 24

demand = (0.5 + 0.4 * np.sin(2 * np.pi * (hour_of_day - 6) / 24)
          + 0.05 * np.random.randn(N))
demand[17] += 0.25
demand[18] += 0.20
demand[36] -= 0.1

values = demand.reshape(-1, 1)   # (N, 1) — kept 2D to match the general form

HIGH_PERCENTILE = 0.90
sorted_vals = np.sort(values[:, 0])
HIGH_THRESH = sorted_vals[int(np.ceil(HIGH_PERCENTILE * N)) - 1]

TOTAL_MERGES = N - N_PRIME

# ══════════════════════════════════════════════
# 2.  Clustering with full per-merge history
# ══════════════════════════════════════════════

class Cluster:
    __slots__ = ("start", "end", "sum_v", "count", "rep", "val", "is_extreme",
                 "active", "prev", "next", "version")

    def __init__(self, i, val):
        self.start = i
        self.end = i
        self.sum_v = float(val)
        self.count = 1
        self.rep = float(val)
        self.val =  float(val)
        self.is_extreme = val >= HIGH_THRESH
        self.active = True
        self.prev = self.next = None
        self.version = 0


def _ward(c1, c2):
    d = c1.val - c2.val
    return (c1.count * c2.count) / (c1.count + c2.count) * d * d


def _conflict(c1, c2, extreme_aware):
    if not extreme_aware:
        return 0
    return int(c1.is_extreme != c2.is_extreme)


def snapshot(head):
    """Walk the active linked list from head and return a list of
    (start, end, rep, is_extreme) tuples."""
    out = []
    c = head
    while c is not None:
        out.append((c.start, c.end, c.rep, c.is_extreme))
        c = c.next
    return out


def run_clustering_history(extreme_aware):
    cs = [Cluster(i, values[i, 0]) for i in range(N)]
    for i in range(N - 1):
        cs[i].next = cs[i + 1]
        cs[i + 1].prev = cs[i]
    head = cs[0]

    ctr = [0]

    # NEW: heap entries now carry (conflict, ward, tiebreak_ctr, c1, c2,
    # c1_version, c2_version). The two version fields are the fix: they
    # are stamped at push time and checked at pop time so that an entry
    # computed against a since-mutated cluster (e.g. c1 was absorbed into
    # as part of a *different* merge, in place, without becoming inactive)
    # is detected as stale and discarded instead of being acted on.
    def entry(c1, c2):
        ctr[0] += 1
        ward = _ward(c1, c2)
        conflict = _conflict(c1, c2, extreme_aware)
        return (conflict, ward, ctr[0], c1, c2, c1.version, c2.version)

    heap = [entry(cs[i], cs[i + 1]) for i in range(N - 1)]
    heapq.heapify(heap)

    history = [{"state": snapshot(head), "merge": None}]

    for _ in range(TOTAL_MERGES):
        while heap:
            conflict, ward, cnt, c1, c2, c1_ver, c2_ver = heapq.heappop(heap)
            if (c1.active and c2.active and c1.next is c2
                    and c1.version == c1_ver and c2.version == c2_ver):
                break

        # Sanity check: should now always hold, since any entry whose
        # cached ward value no longer matches reality has already been
        # filtered out by the version check above.
        recomputed = _ward(c1, c2)
        if abs(recomputed - ward) > 1e-12:
            print("=" * 80)
            print("STALE ENTRY ACCEPTED")
            print()

            print(f"stored ward     = {ward}")
            print(f"current ward    = {recomputed}")
            print()

            print(f"c1 = [{c1.start}-{c1.end}]")
            print(f"count = {c1.count}")
            print(f"val   = {c1.val}")
            print(f"active= {c1.active}")

            print()

            print(f"c2 = [{c2.start}-{c2.end}]")
            print(f"count = {c2.count}")
            print(f"val   = {c2.val}")
            print(f"active= {c2.active}")

            print()

            print("adjacent:", c1.next is c2)
            print("prev of c2:", c2.prev is c1)

            raise RuntimeError("Found stale heap entry")

        merge_info = {
            "start": c1.start,
            "end": c2.end,
            "conflict": conflict,
            "ward": ward,
        }

        # merge c2 into c1
        c1.end = c2.end
        c1.sum_v += c2.sum_v
        c1.count += c2.count
        c1.is_extreme = c1.is_extreme or c2.is_extreme
        c1.val = c1.sum_v / c1.count

        if extreme_aware and c1.is_extreme:
            c1.rep = max(c1.rep, c2.rep)
        else:
            c1.rep = c1.val
        c1.next = c2.next
        if c2.next:
            c2.next.prev = c1
        c1.version += 1   # NEW — invalidate any stale entries referencing c1
        c2.active = False
        c2.version += 1   # NEW — belt-and-suspenders, c2 is now dead anyway

        if c1.prev and c1.prev.active:
            heapq.heappush(heap, entry(c1.prev, c1))
        if c1.next and c1.next.active:
            heapq.heappush(heap, entry(c1, c1.next))

        history.append({"state": snapshot(head), "merge": merge_info})

    return history


hc_history  = run_clustering_history(extreme_aware=False)
eac_history = run_clustering_history(extreme_aware=True)

assert len(hc_history) == len(eac_history) == TOTAL_MERGES + 1


def find_first_divergence():
    """First merge index k at which the HC and EAC partitions (their sets
    of (start, end) cluster boundaries) no longer match. Representative
    values are a deterministic function of which points are merged
    together, so comparing boundaries is sufficient to detect divergence."""
    for k in range(len(hc_history)):
        hc_segs = [(s, e) for (s, e, _, _) in hc_history[k]["state"]]
        eac_segs = [(s, e) for (s, e, _, _) in eac_history[k]["state"]]
        if hc_segs != eac_segs:
            return k
    return None

def find_first_merging_conflict():
    for k in range(len(eac_history)):
        if eac_history[k]["merge"]:
            c = eac_history[k]["merge"]["conflict"]
            if c:
                return k
    return None
    

DIVERGENCE_K = find_first_divergence()
MERGE_CONFLICT_K = find_first_merging_conflict()
print(f"First divergence between HC and EAC at merge {DIVERGENCE_K}")
print(f"First conflict at merge {MERGE_CONFLICT_K}")

# ══════════════════════════════════════════════
# 3.  Rendering
# ══════════════════════════════════════════════

BG           = "#F8F7F4"
ORIGINAL_CLR = "#185FA5"
NORMAL_CLR   = "#C0392B"
EXTREME_CLR  = "#8E44AD"
FLASH_CLR    = "#E67E22"
THRESH_CLR   = "#C0392B"

FIGSIZE = (9, 6.4)
DPI     = 120
WAITING_FRAMES = 40


def draw_panel(ax, state, merge, title, show_legend=False):
    ax.set_xlim(-1, N)
    ax.set_ylim(0.05, 1.15)
    ax.set_facecolor("#FFFFFF")
    for sp in ax.spines.values():
        sp.set_linewidth(0.5)
        sp.set_color("#CCCCCC")
    for d in range(1, DAYS):
        ax.axvline(x=d * 24 - 0.5, color="#888", linewidth=0.5, linestyle="--", alpha=0.3)

    ax.plot(hours, values[:, 0], color=ORIGINAL_CLR, linewidth=1.3, alpha=0.5,
            label="Original demand", zorder=1)
    ax.axhline(y=HIGH_THRESH, color=THRESH_CLR, linewidth=1.2,
               linestyle=":", alpha=0.8, zorder=2)

    just_merged = (merge["start"], merge["end"]) if merge else None

    for (start, end, rep, is_extreme) in state:
        w = end - start + 1
        is_flash = just_merged is not None and start == just_merged[0] and end == just_merged[1]
        if is_flash:
            color = FLASH_CLR
            lw = 3.0
        elif is_extreme:
            color = EXTREME_CLR
            lw = 2.4
        else:
            color = NORMAL_CLR
            lw = 2.0
        ax.plot([start, end + 1 - 0.02], [rep, rep], color=color, linewidth=lw, zorder=3)
        if w == 1:
            ax.scatter([start], [rep], color=color, s=16, zorder=4)
        if start > 0:
            ax.axvline(x=start - 0.5, color="#999", linewidth=0.4, alpha=0.4)

    ax.set_title(title, fontsize=9.5, loc="left", color="#222222")
    ax.tick_params(labelsize=8)

    if show_legend:
        ax.legend(handles=[
            mpatches.Patch(color=ORIGINAL_CLR, alpha=0.5, label="Original"),
            mpatches.Patch(color=NORMAL_CLR, label="Non-extreme cluster"),
            mpatches.Patch(color=EXTREME_CLR, label="Extreme-flagged cluster"),
            mpatches.Patch(color=FLASH_CLR, label="Just merged"),
        ], fontsize=7, loc="upper right", framealpha=0.75, edgecolor="#CCCCCC")


def panel_title(method_label, merge, k, total):
    if merge is None:
        return f"{method_label} — start: {N} singleton clusters"
    tag = "non-conflicting merge" if merge["conflict"] == 0 else "CROSS-BOUNDARY merge (extreme \u2194 non-extreme)"
    return (f"{method_label} — merge {k}/{total}  |  {tag}  "
            f"(conflict={merge['conflict']}, ward={merge['ward']:.4f})  "
            f"\u2192 {N - k} clusters")


def render_frame(k, highlight_divergence=False, first_conflict_merge=False):
    fig, (ax_hc, ax_eac) = plt.subplots(2, 1, figsize=FIGSIZE, dpi=DPI)
    fig.patch.set_facecolor(BG)

    hc_state, hc_merge = hc_history[k]["state"], hc_history[k]["merge"]
    eac_state, eac_merge = eac_history[k]["state"], eac_history[k]["merge"]

    draw_panel(ax_hc, hc_state, hc_merge,
               panel_title("HC (Ward only)", hc_merge, k, TOTAL_MERGES),
               show_legend=(k == 0))
    draw_panel(ax_eac, eac_state, eac_merge,
               panel_title("EAC (conflict-aware)", eac_merge, k, TOTAL_MERGES))
    ax_eac.set_xlabel("Hour", fontsize=9)

    if highlight_divergence:
        suptitle = "\u2605  First point where HC and EAC diverge  \u2605"
        suptitle_color = "#B9770E"
        fig.patch.set_edgecolor(suptitle_color)
        fig.patch.set_linewidth(4)
    elif first_conflict_merge:
        suptitle = "\u2605  First point where EAC has to merge conflicting pairs  \u2605"
        suptitle_color = "#B9770E"
        fig.patch.set_edgecolor(suptitle_color)
        fig.patch.set_linewidth(4)
    else:
        suptitle = "Hierarchical clustering, one merge at a time: HC vs. EAC"
        suptitle_color = "#222222"

    fig.suptitle(suptitle, fontsize=11.5, y=0.985, color=suptitle_color,
                 fontweight="bold" if highlight_divergence else "normal")
    fig.tight_layout(rect=[0, 0, 1, 0.96])

    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=BG,
                edgecolor=fig.patch.get_edgecolor() if highlight_divergence else "none")
    plt.close(fig)
    buf.seek(0)
    return Image.open(buf).copy()


# ══════════════════════════════════════════════
# 4.  Build + save GIF
# ══════════════════════════════════════════════

def main():
    frames = []
    durations = []

    def add(img, ms):
        frames.append(img)
        durations.append(ms)

    print("Rendering merge-by-merge frames …")

    first = render_frame(0)
    for _ in range(6):
        add(first, 120)
    first.save("plots/explaining/eac_merge_by_merge_first_frame.png")

    for k in range(1, TOTAL_MERGES + 1):
        is_divergence = (k == DIVERGENCE_K)
        is_merging_conflict = (k == MERGE_CONFLICT_K)
        frame = render_frame(k, highlight_divergence=is_divergence)
        add(frame, 220)
        if is_divergence:
            # Extra hold on the frame where HC and EAC first disagree
            for _ in range(WAITING_FRAMES):
                add(frame, 150)
            frame.save("plots/explaining/eac_merge_by_merge_diverging_frame.png")
        if is_merging_conflict:
            # Extra hold on the frame where HC and EAC first disagree
            for _ in range(WAITING_FRAMES):
                add(frame, 150)
            frame.save("plots/explaining/eac_merge_by_merge_first_conflict merge.png")
        if k % 10 == 0:
            print(f"  frame {k}/{TOTAL_MERGES}")

    last = frames[-1]
    for _ in range(30):
        add(last, 80)

    out_gif = "plots/explaining/eac_merge_by_merge.gif"
    frames[0].save(
        out_gif,
        save_all=True,
        append_images=frames[1:],
        duration=durations,
        # loop=1,
        optimize=False,
    )
    print(f"Saved → {out_gif}  ({len(frames)} frames)")

    last.save("plots/explaining/eac_merge_by_merge_final_frame.png")
    print("Saved → plots/explaining/eac_merge_by_merge_final_frame.png")


if __name__ == "__main__":
    main()