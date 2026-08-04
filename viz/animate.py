"""Plot a chaos run from the trace recorded by scripts/sample.sh.

    uv run --with matplotlib --no-project viz/animate.py
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter

HERE = Path(__file__).parent
TRACE = HERE / "chaos.csv"
KILLS = HERE / "kills.csv"

INK, MUTED, RULE, PAPER = "#10151b", "#667283", "#dbe1e8", "#ffffff"
KILLED = ["#a83c3c", "#c2603c", "#b5761a"]
SURVIVED = ["#2e7d5b", "#2c6d92"]


def load():
    """Returns (times, done_by_worker, depth, redelivered, kills)."""
    per_worker = defaultdict(dict)
    depth, redelivered = {}, {}

    with TRACE.open() as f:
        for row in csv.DictReader(f):
            t = float(row["t"])
            per_worker[row["worker"]][t] = int(row["done"])
            depth[t] = int(row["depth"])
            redelivered[t] = int(row["redelivered"])

    times = sorted(depth)
    kills = {}
    if KILLS.exists():
        with KILLS.open() as f:
            for name, at in csv.reader(f):
                kills[name] = float(at)

    # carry a killed worker's last count forward
    series = {}
    for worker, samples in per_worker.items():
        last, out = 0, []
        for t in times:
            last = samples.get(t, last)
            out.append(last)
        series[worker] = out

    return (times,
            dict(sorted(series.items())),
            [depth[t] for t in times],
            [redelivered[t] for t in times],
            kills)


def style(ax):
    ax.set_facecolor(PAPER)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(RULE)
    ax.tick_params(colors=MUTED, labelsize=8)
    ax.grid(True, color=RULE, linewidth=0.6, alpha=0.6)
    ax.set_axisbelow(True)


def build(times, series, depth, redelivered, kills):
    fig, (top, bot) = plt.subplots(
        2, 1, figsize=(10, 6.4), height_ratios=[2.3, 1],
        sharex=True, facecolor=PAPER,
    )
    fig.subplots_adjust(left=0.09, right=0.88, top=0.87, bottom=0.1, hspace=0.16)

    fig.text(0.09, 0.955, "taskq chaos run: 3 of 5 workers killed mid-flight",
             color=INK, fontsize=14, fontweight="bold", family="monospace")
    fig.text(0.09, 0.915,
             f"10,000 tasks, sampled 5x/sec from the queue itself. "
             f"{len(times)} samples over {times[-1]:.0f}s. Nothing here is drawn by hand.",
             color=MUTED, fontsize=9)

    lines, labels = {}, {}
    for i, worker in enumerate(series):
        died = worker in kills
        colour = KILLED[i % len(KILLED)] if died else SURVIVED[i % len(SURVIVED)]
        # survivor lines overlap, so dash every other one
        dash = (0, (5, 3)) if (not died and i % 2) else "solid"
        (line,) = top.plot([], [], color=colour, linewidth=2 if not died else 1.6,
                           linestyle=dash, solid_capstyle="round",
                           zorder=3 if dash != "solid" else 2)
        lines[worker] = (line, colour, died)
        labels[worker] = top.annotate(
            "", xy=(0, 0), xytext=(6, 0), textcoords="offset points",
            color=colour, fontsize=8.5, family="monospace",
            va="center", annotation_clip=False,
        )

    deaths = {}
    for worker, at in kills.items():
        colour = lines[worker][1] if worker in lines else KILLED[0]
        marker = top.plot([], [], marker="x", color=colour, markersize=9,
                          markeredgewidth=2.2, linestyle="none")[0]
        rule = top.axvline(at, color=colour, linewidth=0.9, linestyle=(0, (3, 3)),
                           alpha=0)
        deaths[worker] = (marker, rule, at)

    top.set_ylabel("tasks completed", color=MUTED, fontsize=9)
    top.set_xlim(0, times[-1] * 1.02)
    top.set_ylim(0, max(max(v) for v in series.values()) * 1.12)
    style(top)

    (depth_line,) = bot.plot([], [], color=INK, linewidth=1.6)
    depth_fill = [None]
    (redeliver_line,) = bot.plot([], [], color="#b5761a", linewidth=1.6)

    bot.set_ylabel("queue depth", color=MUTED, fontsize=9)
    bot.set_xlabel("seconds since the workers started", color=MUTED, fontsize=9)
    bot.set_xlim(0, times[-1] * 1.02)
    bot.set_ylim(0, max(depth) * 1.1)
    style(bot)

    redeliver_ax = bot.twinx()
    redeliver_ax.set_ylim(0, max(max(redelivered), 1) * 1.6)
    redeliver_ax.set_ylabel("tasks redelivered", color="#b5761a", fontsize=9)
    redeliver_ax.tick_params(colors="#b5761a", labelsize=8)
    for side in ("top", "left", "bottom"):
        redeliver_ax.spines[side].set_visible(False)
    redeliver_ax.spines["right"].set_color(RULE)
    (redeliver_real,) = redeliver_ax.plot([], [], color="#b5761a", linewidth=1.8)
    redeliver_line.remove()

    cursor_top = top.axvline(0, color=MUTED, linewidth=0.8, alpha=0.5)
    cursor_bot = bot.axvline(0, color=MUTED, linewidth=0.8, alpha=0.5)

    note = top.text(0.015, 0.97, "", transform=top.transAxes, color=MUTED,
                    fontsize=8.5, family="monospace", va="top", ha="left",
                    bbox=dict(boxstyle="round,pad=0.55", facecolor=PAPER,
                              edgecolor=RULE, linewidth=0.8))

    reclaim_note = bot.annotate(
        "", xy=(0, 0), xytext=(-16, 34), textcoords="offset points",
        color="#b5761a", fontsize=8.5, family="monospace", ha="right",
        arrowprops=dict(arrowstyle="->", color="#b5761a", linewidth=1),
        annotation_clip=False, alpha=0,
    )

    def frame(k):
        t = times[k]
        xs = times[: k + 1]

        # push overlapping end labels apart
        ends = sorted(((series[w][k], w) for w in series), reverse=True)
        gap = top.get_ylim()[1] * 0.052
        placed = {}
        for value, worker in ends:
            y = value
            for taken in placed.values():
                if abs(y - taken) < gap:
                    y = taken - gap
            placed[worker] = y

        for worker, (line, colour, died) in lines.items():
            ys = series[worker][: k + 1]
            line.set_data(xs, ys)
            labels[worker].xy = (t, placed[worker])
            labels[worker].set_text(f"{worker} {ys[-1]:,}")

        for worker, (marker, rule, at) in deaths.items():
            if t >= at:
                idx = min(range(len(times)), key=lambda j: abs(times[j] - at))
                marker.set_data([at], [series[worker][idx]])
                rule.set_alpha(0.35)

        depth_line.set_data(xs, depth[: k + 1])
        if depth_fill[0] is not None:
            depth_fill[0].remove()
        depth_fill[0] = bot.fill_between(xs, depth[: k + 1], color=INK, alpha=0.07)
        redeliver_real.set_data(xs, redelivered[: k + 1])
        if redelivered[k]:
            reclaim_note.set_alpha(1)
            reclaim_note.xy = (t, 0)
            reclaim_note.set_text(
                f"{redelivered[k]} orphaned tasks reclaimed here,\n"
                f"not when their lease expired at ~8s")

        cursor_top.set_xdata([t, t])
        cursor_bot.set_xdata([t, t])

        alive = len(series) - sum(1 for at in kills.values() if t >= at)
        note.set_text(
            f"t = {t:5.1f}s\n"
            f"workers alive   {alive}\n"
            f"completed       {sum(s[k] for s in series.values()):,}\n"
            f"remaining       {depth[k]:,}\n"
            f"redelivered     {redelivered[k]}"
        )
        return ()

    return fig, frame


def main():
    if not TRACE.exists():
        sys.exit(f"no trace at {TRACE}. Run ./scripts/chaos.sh first.")

    times, series, depth, redelivered, kills = load()
    fig, frame = build(times, series, depth, redelivered, kills)

    # every second sample keeps the gif small
    frames = range(0, len(times), 2)

    anim = FuncAnimation(fig, frame, frames=frames, interval=60, blit=False)
    anim.save(HERE / "chaos.gif", writer=PillowWriter(fps=18), dpi=80)
    print(f"wrote {HERE / 'chaos.gif'}")

    frame(len(times) - 1)
    fig.savefig(HERE / "chaos.png", dpi=140, facecolor=PAPER)
    print(f"wrote {HERE / 'chaos.png'}")


if __name__ == "__main__":
    main()
