"""Charts for multinode/bench/report.py: static PNGs in the style of bench/charts.py.

Series identity is fixed across the three charts' replica-count series: 1 node = slot 1
(blue), 2 nodes = slot 2 (orange), 3 nodes = slot 3 (aqua); the original single-node
project is a neutral grey reference with square markers. The per-node share chart colours
nodes (worker, worker2, worker3) with the same three slots and has its own legend.
Each renderer returns False, without writing, when the data for it does not exist yet.
"""

from __future__ import annotations

import re
from pathlib import Path

from matplotlib.lines import Line2D
from matplotlib.patches import Patch

from bench import charts
from bench.charts import plt

SLOTS = (charts.VLLM, charts.BASELINE, charts.AQUA)
REPLICA_COLORS = {1: charts.VLLM, 2: charts.BASELINE, 3: charts.AQUA}
ORIGINAL_COLOR = charts.TEXT_SECONDARY
IDEAL_COLOR = charts.TEXT_SECONDARY
WHITE = "#ffffff"


def _nodes(r: int) -> str:
    return f"{r} worker node{'s' if r != 1 else ''}"


def _short(node: str) -> str:
    match = re.search(r"(worker\d*|control-plane)$", node or "")
    return match.group(1) if match else node


def _mean(st: dict | None) -> float | None:
    return None if not st else st["mean"]


def _runs_note(report: dict) -> str:
    parts = []
    for s in report["sources"]["scaling_runs"]:
        if s["used"]:
            parts.append(f"run {s['run']} {s['status']}")
    for n in report["sources"]["missing_runs"]:
        parts.append(f"run {n} not run yet")
    return "; ".join(parts)


def _end_labels(ax, ends: list[tuple[float, float, str]]) -> None:
    """Direct labels at line ends; a label too close to one already placed is left to the legend."""
    y_span = ax.get_ylim()[1] - ax.get_ylim()[0]
    placed: list[tuple[float, float]] = []
    for x, y, text in sorted(ends, key=lambda e: -e[1]):
        if any(abs(x - px) < 0.5 and abs(y - py) < 0.07 * y_span for px, py in placed):
            continue
        charts.end_label(ax, x, y, text)
        placed.append((x, y))


def _finish(fig, path: Path, title: str, subtitle: str, handles=None, labels=None, ncol: int = 4) -> None:
    """Title, subtitle and legend in a fixed band above the panels (sizes in inches, so it
    holds for any figure height), then save."""
    h = fig.get_figheight()
    band = 0.62 + (0.38 if handles else 0.0)
    fig.tight_layout(rect=(0, 0, 1, 1 - band / h))
    fig.text(0.0, 1 - 0.08 / h, title, ha="left", va="top", fontsize=12, fontweight="bold", color=charts.TEXT_PRIMARY)
    fig.text(0.0, 1 - 0.36 / h, subtitle, ha="left", va="top", fontsize=9, color=charts.TEXT_SECONDARY)
    if handles:
        fig.legend(handles, labels if labels is not None else [x.get_label() for x in handles], loc="upper left",
                   ncol=ncol, bbox_to_anchor=(0.0, 1 - 0.6 / h), borderaxespad=0, handlelength=2.2)
    charts.save(fig, path)


def throughput_chart(report: dict, path: Path) -> bool:
    levels = report["levels"]
    if not levels:
        return False
    original = (report.get("original_single_node") or {}).get("levels") or []
    replicas = sorted({a["replicas"] for a in levels})
    cs = sorted({a["concurrency"] for a in levels} | {o["concurrency"] for o in original})
    pos = {c: i for i, c in enumerate(cs)}
    panels = [
        ("output_tokens_per_s", "Output throughput (tokens/s)", "{:.0f}"),
        ("latency_p95_s", "Tail latency, p95 (s)", "{:.0f} s"),
    ]
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.6))
    for ax, (metric, title, label_fmt) in zip(axes, panels, strict=True):
        ends = []
        pts = [(pos[o["concurrency"]], _mean(o.get(metric))) for o in original if _mean(o.get(metric)) is not None]
        if pts:
            ax.plot([p[0] for p in pts], [p[1] for p in pts], color=ORIGINAL_COLOR, linewidth=2, marker="s", markersize=8,
                    markeredgecolor=charts.SURFACE, markeredgewidth=2, zorder=2,
                    label="Original single-node project (1 node, static CPU manager)")
        for r in replicas:
            series = sorted((a["concurrency"], _mean(a[metric])) for a in levels if a["replicas"] == r and _mean(a[metric]) is not None)
            if not series:
                continue
            x, y = [pos[c] for c, _ in series], [v for _, v in series]
            charts.line_with_markers(ax, x, y, REPLICA_COLORS.get(r, charts.TEXT_PRIMARY), label=f"Multi-node: {_nodes(r)}")
            if x[-1] == len(cs) - 1:  # direct label only at the right edge; a mid-chart end would sit on other lines
                ends.append((x[-1], y[-1], label_fmt.format(y[-1])))
        ax.set_title(title)
        ax.set_xticks(range(len(cs)), [str(c) for c in cs])
        ax.set_xlabel("Concurrent requests in flight")
        ax.set_xlim(-0.3, len(cs) - 0.2)
        ax.set_ylim(bottom=0)
        ax.set_ylim(0, ax.get_ylim()[1] * 1.05)
        _end_labels(ax, ends)
    handles, labels = axes[0].get_legend_handles_labels()
    if original:  # multi-node series first (1, 2, 3 nodes), the original reference last
        handles, labels = handles[1:] + handles[:1], labels[1:] + labels[:1]
    _finish(
        fig, path,
        "Qwen2.5-0.5B-Instruct, CPU only: throughput and p95 latency vs. concurrency on 1–3 kind worker nodes (one laptop)",
        f"Means across runs ({_runs_note(report)}). The original single-node project measured up to c = "
        f"{max((o['concurrency'] for o in original), default='–')}.",
        handles, labels, ncol=len(handles),
    )
    return True


def scaling_chart(report: dict, path: Path) -> bool:
    curve = report["scaling_curve"]
    per = {p["replicas"]: p for p in curve["per_replicas"]}
    if 1 not in per or len(per) < 2:
        return False
    panels = [("at_top", f"At concurrency {curve['top_concurrency']}"), ("best", "At each node count's best concurrency level")]
    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.6), sharey=True)
    max_r = max(per)
    top_y = 0.0
    for ax, (key, title) in zip(axes, panels, strict=True):
        pts = [(r, p[key]) for r, p in sorted(per.items()) if p[key]]
        if len(pts) < 2:
            ax.set_title(title)
            ax.text(0.5, 0.5, "not enough data yet (needs complete sweeps for 1 and more nodes)", transform=ax.transAxes,
                    ha="center", color=charts.TEXT_SECONDARY)
            ax.set_xticks(range(1, max_r + 1), [_nodes(r) for r in range(1, max_r + 1)])
            ax.set_xlim(0.7, max_r + 0.75)
            continue
        one = per[1][key]["output_tokens_per_s"]
        ideal_x = list(range(1, max_r + 1))
        ideal_y = [r * one for r in ideal_x]
        ax.plot(ideal_x, ideal_y, color=IDEAL_COLOR, linewidth=1, zorder=1)
        ax.annotate("ideal linear (N × one node)", (ideal_x[-1], ideal_y[-1]), xytext=(-6, 6), textcoords="offset points",
                    ha="right", va="bottom", fontsize=9, color=charts.TEXT_SECONDARY)
        xs, ys = [r for r, _ in pts], [p["output_tokens_per_s"] for _, p in pts]
        charts.line_with_markers(ax, xs, ys, charts.VLLM)
        for r, p in pts:
            text = f"{p['output_tokens_per_s']:.0f} tok/s"
            if key == "best":
                text += f" (c = {p['concurrency']})"
            if r > 1 and p.get("scaling_efficiency") is not None:
                text += f"\n{p['speedup_vs_1_node']:.2f}× · {100 * p['scaling_efficiency']:.0f}% of linear"
            ax.annotate(text, (r, p["output_tokens_per_s"]), xytext=(10, -6), textcoords="offset points",
                        ha="left", va="top", fontsize=9, color=charts.TEXT_PRIMARY)
        top_y = max(top_y, max(ideal_y), max(ys))
        ax.set_title(title)
        ax.set_xticks(ideal_x, [_nodes(r) for r in ideal_x])
        ax.set_xlim(0.7, max_r + 0.75)
    axes[0].set_ylabel("Output throughput (tokens/s)")
    axes[0].set_ylim(0, top_y * 1.15 if top_y else 1)
    _finish(
        fig, path,
        "Scaling across kind worker nodes on one laptop: measured vs. ideal linear",
        f"Blue: measured means ({_runs_note(report)}). Grey line: N × the one-node value at the same level. "
        "All nodes share the laptop's physical CPU and memory.",
    )
    return True


def distribution_chart(report: dict, path: Path) -> bool:
    dist = report["distribution"]["levels"]
    if not dist:
        return False
    replicas = sorted({d["replicas"] for d in dist})
    repeats = sorted({d["repeat"] for d in dist})
    env_nodes = [n["name"] for n in ((report.get("environment") or {}).get("cluster") or {}).get("node_details") or []
                 if "control-plane" not in (n.get("roles") or [])]
    all_nodes = sorted(set(env_nodes) | {n for d in dist for n in d["served_per_node"]})
    color = {n: SLOTS[i % len(SLOTS)] for i, n in enumerate(all_nodes)}
    cpusets = {n["name"]: (n.get("docker") or {}).get("cpuset_cpus")
               for n in ((report.get("environment") or {}).get("cluster") or {}).get("node_details") or []}
    planned = report.get("planned_concurrency_levels") or sorted({d["concurrency"] for d in dist})
    n_rows = len(planned)
    panel_h = 0.42 * max(n_rows, 1) + 1.0
    fig, axes = plt.subplots(len(replicas), len(repeats), figsize=(6.2 * len(repeats), panel_h * len(replicas)), squeeze=False)
    bar_h = 0.38  # 0.42 in per row at 150 dpi -> ~24 px bars
    for i, r in enumerate(replicas):
        for j, rep in enumerate(repeats):
            ax = axes[i][j]
            by_c = {d["concurrency"]: d for d in dist if d["replicas"] == r and d["repeat"] == rep}
            ax.set_title(f"{_nodes(r)} · run {rep}")
            ax.grid(False)
            ticks = []
            for k, c in enumerate(planned):
                d = by_c.get(c)
                if d is None:
                    ax.text(1, k, "not measured", ha="left", va="center", fontsize=8.5, color=charts.TEXT_SECONDARY)
                    ticks.append(f"c = {c}")
                    continue
                ticks.append(f"c = {c}  (n = {d['served_total']})")
                left = 0.0
                for node in sorted(d["served_per_node"]):
                    share = 100 * d["shares"][node]
                    ax.barh(k, share, left=left, height=bar_h, color=color[node], edgecolor=charts.SURFACE, linewidth=1.0, zorder=2)
                    if share >= 12:
                        ax.text(left + share / 2, k, f"{share:.0f}%", ha="center", va="center", fontsize=8.5, color=WHITE, zorder=3)
                    left += share
            for q in range(1, r):
                ax.axvline(100 * q / r, color=charts.TEXT_PRIMARY, linewidth=1, zorder=2.5)
            ax.set_yticks(range(n_rows), ticks)
            ax.set_ylim(n_rows - 0.5, -0.5)
            ax.set_xlim(0, 100)
            ax.set_xticks([0, 25, 50, 75, 100], ["0%", "25%", "50%", "75%", "100%"])
            ax.tick_params(axis="y", length=0)
            ax.spines["left"].set_visible(False)
    for ax in axes[-1]:
        ax.set_xlabel("Share of the level's requests served by each node (black line: even split)")
    shown = sorted({n for d in dist for n in d["served_per_node"]})
    handles = [Patch(color=color[n], label=f"{_short(n)} (cpus {cpusets.get(n) or '?'})") for n in shown]
    handles.append(Line2D([], [], color=charts.TEXT_PRIMARY, linewidth=1, label="even split"))
    _finish(
        fig, path,
        "Requests served per node, from each vLLM pod's access log",
        "kube-proxy picks a pod at random for each new connection; the client opens one connection per request. "
        "n = requests in the level.",
        handles, ncol=len(handles),
    )
    return True


RENDERERS = {"throughput": throughput_chart, "scaling": scaling_chart, "distribution": distribution_chart}
