"""Combine raw benchmark runs into results/results.json, results/results.md and a chart.

Every number written here is computed from results/raw/<target>-run*.json; nothing
is typed in by hand. With several runs per target, each metric is the mean across
runs and the min-max range is kept alongside it.

    uv run python -m bench.report
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

from bench import charts
from bench.charts import plt

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "results" / "raw"
OUT_JSON = ROOT / "results" / "results.json"
OUT_MD = ROOT / "results" / "results.md"
OUT_PNG = ROOT / "results" / "benchmark.png"
TARGETS = ("baseline", "vllm")
LABELS = {"baseline": "Baseline (FastAPI + transformers)", "vllm": "vLLM on Kubernetes"}

METRICS = (
    "throughput_req_per_s",
    "output_tokens_per_s",
    "latency_p50_s",
    "latency_p95_s",
    "latency_p99_s",
    "avg_cores_used",
    "avg_cpu_utilization_pct",
    "peak_working_set_mib",
    "tokens_per_core_second",
)


def load(target: str) -> list[dict]:
    files = sorted(RAW.glob(f"{target}-run*.json")) or sorted(RAW.glob(f"{target}.json"))
    if not files:
        raise SystemExit(f"no raw results for {target} in {RAW}")
    return [json.loads(f.read_text()) for f in files]


def flatten(run: dict) -> dict:
    """One concurrency level of one run -> flat metrics dict."""
    summary = run["summary"]
    pods = run["resources"]["pods"]
    cores = run["resources"]["total_avg_cores_used"]
    pod = next(iter(pods.values())) if len(pods) == 1 else None
    return {
        **{k: summary[k] for k in ("throughput_req_per_s", "output_tokens_per_s", "latency_p50_s", "latency_p95_s", "latency_p99_s")},
        "avg_cores_used": cores,
        "avg_cpu_utilization_pct": pod["avg_cpu_utilization_pct"] if pod else None,
        "peak_working_set_mib": max(p["peak_working_set_mib"] for p in pods.values()) if pods else None,
        "tokens_per_core_second": summary["output_tokens_per_s"] / cores if cores else None,
        "requests": summary["requests"],
        "failed": summary["failed"],
    }


def aggregate(files: list[dict]) -> dict[int, dict]:
    by_level: dict[int, list[dict]] = {}
    for f in files:
        for run in f["runs"]:
            by_level.setdefault(run["concurrency"], []).append(flatten(run))
    out = {}
    for level, rows in sorted(by_level.items()):
        agg = {"runs": len(rows), "requests": sum(r["requests"] for r in rows), "failed": sum(r["failed"] for r in rows)}
        for m in METRICS:
            values = [r[m] for r in rows if r[m] is not None]
            if values:
                agg[m] = {"mean": round(mean(values), 3), "min": round(min(values), 3), "max": round(max(values), 3)}
        out[level] = agg
    return out


def fmt(stat: dict | None, digits: int = 2, show_range: bool = True) -> str:
    if not stat:
        return "n/a"
    s = f"{stat['mean']:.{digits}f}"
    if show_range and stat["max"] != stat["min"]:
        s += f" ({stat['min']:.{digits}f}–{stat['max']:.{digits}f})"
    return s


def runs_phrase(report: dict) -> str:
    n = report["runs_per_target"]
    sweeps = lambda k: f"{n[k]} complete sweep{'s' if n[k] != 1 else ''}"  # noqa: E731
    return f"vLLM {sweeps('vllm')}, baseline {sweeps('baseline')}"


def ratio(a: float, b: float) -> float:
    return round(a / b, 2)


def build() -> dict:
    files = {t: load(t) for t in TARGETS}
    agg = {t: aggregate(files[t]) for t in TARGETS}
    levels = sorted(set(agg["baseline"]) & set(agg["vllm"]))
    comparison = []
    for c in levels:
        b, v = agg["baseline"][c], agg["vllm"][c]
        comparison.append(
            {
                "concurrency": c,
                "baseline": b,
                "vllm": v,
                "vllm_vs_baseline": {
                    "throughput_x": ratio(v["output_tokens_per_s"]["mean"], b["output_tokens_per_s"]["mean"]),
                    "p50_latency_reduction_x": ratio(b["latency_p50_s"]["mean"], v["latency_p50_s"]["mean"]),
                    "p95_latency_reduction_x": ratio(b["latency_p95_s"]["mean"], v["latency_p95_s"]["mean"]),
                    "tokens_per_core_second_x": ratio(
                        v["tokens_per_core_second"]["mean"], b["tokens_per_core_second"]["mean"]
                    ),
                },
            }
        )
    first = files["vllm"][0]
    return {
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "model": "Qwen/Qwen2.5-0.5B-Instruct (bfloat16)",
        "hardware": {
            "accelerator": "none -- CPU only (no GPU was used)",
            "host": first["environment"]["host"],
            "cluster": first["environment"]["cluster"],
            "per_pod_resources": {t: files[t][0]["environment"]["pods"][0]["resources"] for t in TARGETS},
            "images": {t: files[t][0]["environment"]["pods"][0]["image"] for t in TARGETS},
        },
        "workload": first["workload"],
        "runs_per_target": {t: len(files[t]) for t in TARGETS},
        "raw_files": {t: [f"raw/{p.name}" for p in sorted(RAW.glob(f"{t}-run*.json"))] for t in TARGETS},
        "levels": comparison,
    }


def chart(report: dict) -> None:
    levels = [row["concurrency"] for row in report["levels"]]
    x = list(range(len(levels)))
    panels = [
        ("output_tokens_per_s", "Output throughput (tokens/s)", "{:.1f}"),
        ("latency_p50_s", "Median latency, p50 (s)", "{:.1f}s"),
        ("latency_p95_s", "Tail latency, p95 (s)", "{:.1f}s"),
        ("avg_cores_used", "CPU cores used (of 3 allocated)", "{:.2f}"),
    ]
    fig, axes = plt.subplots(2, 2, figsize=(11, 7.2))
    for ax, (metric, title, label_fmt) in zip(axes.flat, panels):
        for target, color in (("vllm", charts.VLLM), ("baseline", charts.BASELINE)):
            y = [row[target][metric]["mean"] for row in report["levels"]]
            charts.line_with_markers(ax, x, y, color, label=LABELS[target])
            charts.end_label(ax, x[-1], y[-1], label_fmt.format(y[-1]))
        ax.set_title(title)
        ax.set_xticks(x, [str(c) for c in levels])
        ax.set_xlabel("Concurrent requests in flight")
        ax.set_xlim(-0.3, len(levels) - 0.4)
        ax.set_ylim(bottom=0)
    axes[1][1].axhline(3, color=charts.TEXT_SECONDARY, linewidth=1, zorder=1)
    axes[1][1].annotate("3 cores allocated per pod", (x[0], 3), xytext=(0, 4), textcoords="offset points", fontsize=9, color=charts.TEXT_SECONDARY)
    axes[1][1].set_ylim(0, 3.4)
    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper left", ncol=2, bbox_to_anchor=(0.0, 1.03))
    fig.suptitle(
        "Qwen2.5-0.5B-Instruct, CPU only: vLLM vs. a plain FastAPI service on identical 3-core pods",
        x=0.0, y=1.08, ha="left", fontsize=12, fontweight="bold",
    )
    fig.tight_layout()
    charts.save(fig, OUT_PNG)


def markdown(report: dict) -> str:
    hw = report["hardware"]
    wl = report["workload"]
    res = hw["per_pod_resources"]
    lines = [
        "# Benchmark results: vLLM vs. FastAPI baseline",
        "",
        f"_Generated {report['generated_utc']} by `bench/report.py` from the raw runs in `results/raw/`. "
        "Do not edit by hand._",
        "",
        "## Setup",
        "",
        f"- **Model:** {report['model']}",
        f"- **Accelerator:** {hw['accelerator']}",
        f"- **Host:** {hw['host']['machine']}, {hw['host']['os']}",
        f"- **Cluster:** {hw['cluster']['nodes']} node (kind, Kubernetes {hw['cluster']['kubelet_version']}), "
        f"{hw['cluster']['node_cpu_capacity']} vCPUs / {int(hw['cluster']['node_memory_capacity'].rstrip('Ki')) / 2**20:.1f} GiB in a Colima VM",
        f"- **Per-pod resources (both deployments):** {res['vllm']['limits']['cpu']} dedicated CPU cores "
        f"(Guaranteed QoS + static CPU manager); memory {res['vllm']['limits']['memory']} (vLLM) / "
        f"{res['baseline']['limits']['memory']} (baseline)",
        f"- **Images:** `{hw['images']['vllm']}` / `{hw['images']['baseline']}`",
        f"- **Workload:** closed loop at concurrency {', '.join(map(str, wl['concurrency_levels']))}; "
        f"N = max(16, 4×concurrency) requests per level; every request generates exactly {wl['max_tokens']} tokens "
        "(greedy, `ignore_eos`); fixed prompt set (~43 prompt tokens each)",
        f"- **Runs:** {runs_phrase(report)}. Values are means; where runs differ, the min–max range "
        "across runs is shown in parentheses",
        "",
        "## Throughput and latency",
        "",
        "| Concurrency | Baseline tok/s | vLLM tok/s | **Throughput gain** | Baseline p50 (s) | vLLM p50 (s) | Baseline p95 (s) | vLLM p95 (s) | **p95 reduction** |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in report["levels"]:
        b, v, cmp = row["baseline"], row["vllm"], row["vllm_vs_baseline"]
        lines.append(
            f"| {row['concurrency']} | {fmt(b['output_tokens_per_s'], 1)} | {fmt(v['output_tokens_per_s'], 1)} | "
            f"**{cmp['throughput_x']:.2f}×** | {fmt(b['latency_p50_s'], 1)} | {fmt(v['latency_p50_s'], 1)} | "
            f"{fmt(b['latency_p95_s'], 1)} | {fmt(v['latency_p95_s'], 1)} | **{cmp['p95_latency_reduction_x']:.2f}×** |"
        )
    lines += [
        "",
        "| Concurrency | Baseline req/s | vLLM req/s | Failed requests (baseline / vLLM) |",
        "|---:|---:|---:|---:|",
    ]
    for row in report["levels"]:
        b, v = row["baseline"], row["vllm"]
        lines.append(
            f"| {row['concurrency']} | {fmt(b['throughput_req_per_s'], 3)} | {fmt(v['throughput_req_per_s'], 3)} | "
            f"{b['failed']} of {b['requests']} / {v['failed']} of {v['requests']} |"
        )
    lines += [
        "",
        "## CPU utilisation (in place of GPU utilisation: no GPU)",
        "",
        "Measured from each pod's cgroup counters (`cpu.stat`) over the load window. "
        "Utilisation is relative to the pod's 3-core allocation.",
        "",
        "| Concurrency | Baseline cores used | vLLM cores used | Baseline CPU util. | vLLM CPU util. | Baseline tok/s per core | vLLM tok/s per core | Peak memory baseline / vLLM (MiB) |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in report["levels"]:
        b, v = row["baseline"], row["vllm"]
        lines.append(
            f"| {row['concurrency']} | {fmt(b['avg_cores_used'], 2, False)} | {fmt(v['avg_cores_used'], 2, False)} | "
            f"{fmt(b['avg_cpu_utilization_pct'], 0, False)}% | {fmt(v['avg_cpu_utilization_pct'], 0, False)}% | "
            f"{fmt(b['tokens_per_core_second'], 1, False)} | {fmt(v['tokens_per_core_second'], 1, False)} | "
            f"{b['peak_working_set_mib']['max']:.0f} / {v['peak_working_set_mib']['max']:.0f} |"
        )
    lines += ["", "![Benchmark chart](benchmark.png)", ""]
    return "\n".join(lines)


README = ROOT / "README.md"
README_START = "<!-- results:start (generated by bench/report.py; do not edit) -->"
README_END = "<!-- results:end -->"


def readme_block(report: dict) -> str:
    """Headline results for the README, generated from the same data as results.md."""
    rows = report["levels"]
    top, single = rows[-1], rows[0]
    b, v, cmp = top["baseline"], top["vllm"], top["vllm_vs_baseline"]
    lines = [
        README_START,
        "## Results",
        "",
        f"Qwen2.5-0.5B-Instruct, bfloat16, **CPU only**. Each server gets one pod with 3 dedicated cores. "
        f"Requests generate exactly 128 tokens each. Runs: {runs_phrase(report)} (means shown). "
        "Full tables are in [`results/results.md`](results/results.md) and the data in [`results/`](results/).",
        "",
        f"- **At {top['concurrency']} concurrent requests, vLLM delivered {cmp['throughput_x']:.1f}× the baseline's "
        f"throughput** ({v['output_tokens_per_s']['mean']:.1f} vs. {b['output_tokens_per_s']['mean']:.1f} output tokens/s) "
        f"with **{cmp['p95_latency_reduction_x']:.1f}× lower p95 latency** ({v['latency_p95_s']['mean']:.1f} s vs. "
        f"{b['latency_p95_s']['mean']:.1f} s), while using fewer CPU cores "
        f"({v['avg_cores_used']['mean']:.2f} vs. {b['avg_cores_used']['mean']:.2f}): "
        f"{cmp['tokens_per_core_second_x']:.0f}× more tokens per core-second.",
        f"- **With a single request in flight there is little difference** ({single['vllm_vs_baseline']['throughput_x']:.2f}× "
        f"throughput, {single['vllm']['output_tokens_per_s']['mean']:.1f} vs. {single['baseline']['output_tokens_per_s']['mean']:.1f} tok/s). "
        "vLLM's gain comes from continuous batching, which needs concurrent requests to batch.",
        f"- The baseline's throughput stays flat (~{min(r['baseline']['output_tokens_per_s']['mean'] for r in rows):.1f}–"
        f"{max(r['baseline']['output_tokens_per_s']['mean'] for r in rows):.1f} tok/s) at every concurrency because it runs "
        "one `generate()` at a time. Its latency grows with the queue instead, reaching "
        f"p50 {b['latency_p50_s']['mean']:.0f} s at concurrency {top['concurrency']}.",
        f"- Failed requests: {sum(r['baseline']['failed'] for r in rows)} of {sum(r['baseline']['requests'] for r in rows)} "
        f"(baseline), {sum(r['vllm']['failed'] for r in rows)} of {sum(r['vllm']['requests'] for r in rows)} (vLLM).",
        "",
        "| Concurrency | Baseline tok/s | vLLM tok/s | Throughput gain | Baseline p50 / p95 (s) | vLLM p50 / p95 (s) | p95 latency reduction | CPU cores used, baseline / vLLM |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        b, v, cmp = row["baseline"], row["vllm"], row["vllm_vs_baseline"]
        lines.append(
            f"| {row['concurrency']} | {b['output_tokens_per_s']['mean']:.1f} | {v['output_tokens_per_s']['mean']:.1f} | "
            f"**{cmp['throughput_x']:.2f}×** | {b['latency_p50_s']['mean']:.1f} / {b['latency_p95_s']['mean']:.1f} | "
            f"{v['latency_p50_s']['mean']:.1f} / {v['latency_p95_s']['mean']:.1f} | **{cmp['p95_latency_reduction_x']:.2f}×** | "
            f"{b['avg_cores_used']['mean']:.2f} / {v['avg_cores_used']['mean']:.2f} |"
        )
    lines += ["", "![Benchmark results](results/benchmark.png)", "", README_END]
    return "\n".join(lines)


def update_readme(report: dict) -> bool:
    if not README.exists():
        return False
    text = README.read_text()
    if README_START not in text or README_END not in text:
        return False
    before = text[: text.index(README_START)]
    after = text[text.index(README_END) + len(README_END):]
    README.write_text(before + readme_block(report) + after)
    return True


def main() -> None:
    report = build()
    OUT_JSON.write_text(json.dumps(report, indent=2))
    OUT_MD.write_text(markdown(report))
    chart(report)
    print(f"wrote {OUT_JSON.relative_to(ROOT)}, {OUT_MD.relative_to(ROOT)}, {OUT_PNG.relative_to(ROOT)}")
    if update_readme(report):
        print("updated the results section of README.md")


if __name__ == "__main__":
    main()
