"""HorizontalPodAutoscaler demo: drive sustained load at vLLM and record it scaling out and back in.

Timeline:
  1. vLLM idle at 1 replica (the baseline is scaled to 0 to free its 3 cores; the
     single node has room for exactly two 3-core inference pods).
  2. LOAD_S seconds of closed-loop load at concurrency 16. The pod's CPU goes above
     the HPA's 50% target, the HPA asks for a 2nd replica, it starts, passes its
     readiness probe and the Service starts sending it new connections.
  3. Load stops; after the HPA's 120s scale-down stabilisation window it removes
     the extra replica.

Everything observed (HPA status every 5s, every request, `kubectl describe hpa`) is
written to results/hpa/, plus a chart and a summary computed from those records.

    uv run python -m bench.hpa_demo
"""

from __future__ import annotations

import asyncio
import csv
import json
import time
from pathlib import Path

from bench import charts
from bench.charts import plt
from bench.kube import NAMESPACE, kubectl, kubectl_json, list_pods, pod_is_ready, scale
from bench.loadgen import RequestRecord, records_to_dicts, run_load, summarize

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "results" / "hpa"
URL = "http://localhost:30080"
CONCURRENCY = 16
LOAD_S = 480
COOLDOWN_S = 360
POLL_S = 5


async def hpa_state() -> dict:
    hpa = await kubectl_json("get", "hpa", "vllm", "-n", NAMESPACE)
    pods = await list_pods("app=vllm")
    status = hpa.get("status", {})
    utilization = None
    for metric in status.get("currentMetrics") or []:
        if metric.get("type") == "Resource" and metric["resource"]["name"] == "cpu":
            utilization = metric["resource"]["current"].get("averageUtilization")
    return {
        "t": time.time(),
        "hpa_current_replicas": status.get("currentReplicas"),
        "hpa_desired_replicas": status.get("desiredReplicas"),
        "hpa_cpu_utilization_pct": utilization,
        "pods_total": len(pods),
        "pods_ready": sum(pod_is_ready(p) for p in pods),
        "pods": ";".join(
            f"{p['metadata']['name']}:{'Ready' if pod_is_ready(p) else p['status'].get('phase')}" for p in pods
        ),
    }


async def wait_for_single_idle_replica() -> None:
    print("waiting for vLLM to be at exactly 1 ready replica ...")
    while True:
        state = await hpa_state()
        if state["pods_total"] == 1 and state["pods_ready"] == 1 and state["hpa_current_replicas"] == 1:
            return
        await asyncio.sleep(POLL_S)


async def record_timeline(timeline: list[dict], stop: asyncio.Event) -> None:
    last = None
    while not stop.is_set():
        state = await hpa_state()
        timeline.append(state)
        key = (state["hpa_desired_replicas"], state["pods_ready"], state["pods_total"])
        if key != last:
            print(
                f"  t={state['t'] - timeline[0]['t']:6.0f}s  cpu={state['hpa_cpu_utilization_pct']}%  "
                f"desired={state['hpa_desired_replicas']}  pods ready/total={state['pods_ready']}/{state['pods_total']}"
            )
            last = key
        try:
            await asyncio.wait_for(stop.wait(), POLL_S)
        except asyncio.TimeoutError:
            pass


def first_time(timeline: list[dict], predicate, after: float = 0.0) -> float | None:
    return next((s["t"] for s in timeline if s["t"] >= after and predicate(s)), None)


def tokens_per_s(records: list[RequestRecord], start: float, end: float) -> float | None:
    """Output tokens generated inside [start, end), per second.

    Each request's tokens are spread evenly over its lifetime (decode dominates
    these requests). Counting only completions would be jumpy: with identical
    128-token requests, a batch of 16 finishes almost at the same moment.
    """
    if end <= start:
        return None
    tokens = 0.0
    for r in records:
        overlap = min(r.end, end) - max(r.start, start)
        if r.ok and overlap > 0:
            tokens += r.completion_tokens * overlap / (r.end - r.start)
    return tokens / (end - start)


def analyse(timeline: list[dict], records: list[RequestRecord], load_start: float, load_end: float) -> dict:
    scale_up_decided = first_time(timeline, lambda s: (s["hpa_desired_replicas"] or 0) >= 2)
    second_ready = first_time(timeline, lambda s: s["pods_ready"] >= 2)
    scale_down_decided = first_time(timeline, lambda s: s["hpa_desired_replicas"] == 1, after=load_end)
    back_to_one = first_time(timeline, lambda s: s["pods_total"] == 1, after=load_end)
    peak_cpu = max((s["hpa_cpu_utilization_pct"] or 0) for s in timeline if load_start <= s["t"] <= load_end)
    rel = lambda t: None if t is None else round(t - load_start, 1)  # noqa: E731
    # Throughput while one replica served vs. while two did. The 2-replica window
    # starts 30s after the second pod is ready, once requests that were already
    # queued on the first pod have drained and new ones are spread over both.
    one = tokens_per_s(records, load_start, second_ready) if second_ready else None
    two = tokens_per_s(records, second_ready + 30, load_end) if second_ready else None
    return {
        "concurrency": CONCURRENCY,
        "load_duration_s": round(load_end - load_start, 1),
        "hpa_target_cpu_utilization_pct": 50,
        "peak_hpa_cpu_utilization_pct": peak_cpu,
        "scale_up_decided_at_s": rel(scale_up_decided),
        "second_replica_ready_at_s": rel(second_ready),
        "load_stopped_at_s": rel(load_end),
        "scale_down_decided_at_s": rel(scale_down_decided),
        "back_to_one_replica_at_s": rel(back_to_one),
        "max_ready_replicas": max(s["pods_ready"] for s in timeline),
        "tokens_per_s_one_replica": round(one, 1) if one else None,
        "tokens_per_s_two_replicas": round(two, 1) if two else None,
        "requests": summarize(records),
    }


def chart(timeline: list[dict], records: list[RequestRecord], load_start: float, load_end: float, path: Path) -> None:
    t = [s["t"] - load_start for s in timeline]
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 8.4), sharex=True)

    charts.line_with_markers(ax1, t, [s["pods_ready"] for s in timeline], charts.VLLM, step=True)
    ax1.set_title("Ready vLLM replicas")
    ax1.set_ylim(0, 2.6)
    ax1.set_yticks([0, 1, 2])

    cpu = [s["hpa_cpu_utilization_pct"] for s in timeline]
    charts.line_with_markers(ax2, t, [c if c is not None else float("nan") for c in cpu], charts.VLLM, step=True)
    ax2.axhline(50, color=charts.TEXT_SECONDARY, linewidth=1, zorder=1)
    ax2.annotate("HPA target 50%", (t[0], 50), xytext=(4, 4), textcoords="offset points", fontsize=9, color=charts.TEXT_SECONDARY)
    ax2.set_title("Average CPU utilisation across vLLM pods, as seen by the HPA (% of 3-core request)")
    ax2.set_ylim(0, 110)

    bucket = 30
    starts = list(range(0, int(load_end - load_start) - bucket + 1, bucket))  # full windows only
    tput = [tokens_per_s(records, load_start + a, load_start + a + bucket) or 0 for a in starts]
    charts.line_with_markers(ax3, [a + bucket / 2 for a in starts], tput, charts.VLLM)
    ax3.set_title(f"Output throughput (tokens/s, {bucket}s windows) at concurrency {CONCURRENCY}")
    ax3.set_ylim(bottom=0)
    ax3.set_xlabel("Seconds since load started")

    for ax in (ax1, ax2, ax3):
        ax.axvline(load_end - load_start, color=charts.TEXT_SECONDARY, linewidth=1, zorder=1)
    ax1.annotate("load stops", (load_end - load_start, 2.3), xytext=(4, 0), textcoords="offset points", fontsize=9, color=charts.TEXT_SECONDARY)
    fig.suptitle("HPA scaling vLLM 1 → 2 → 1 replicas under load (single-node kind cluster, CPU only)", x=0.0, ha="left", fontsize=12, fontweight="bold")
    fig.tight_layout()
    charts.save(fig, path)


def write_outputs(timeline, records, load_start, load_end, summary, describe) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / "timeline.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["t_rel_s", *timeline[0].keys()])
        writer.writeheader()
        for s in timeline:
            writer.writerow({"t_rel_s": round(s["t"] - load_start, 1), **s})
    with open(OUT / "requests.csv", "w", newline="") as f:
        rows = records_to_dicts(records)
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2))
    (OUT / "describe_hpa.txt").write_text(describe)
    chart(timeline, records, load_start, load_end, OUT / "hpa_demo.png")


def chart_from_saved() -> None:
    """Re-render the chart from results/hpa/ without re-running the demo."""
    with open(OUT / "timeline.csv") as f:
        rows = list(csv.DictReader(f))
    num = lambda x: float(x) if x not in ("", "None") else None  # noqa: E731
    timeline = [
        {"t": float(r["t"]), "pods_ready": int(r["pods_ready"]), "hpa_cpu_utilization_pct": num(r["hpa_cpu_utilization_pct"])}
        for r in rows
    ]
    load_start = float(rows[0]["t"]) - float(rows[0]["t_rel_s"])
    load_end = load_start + json.loads((OUT / "summary.json").read_text())["load_duration_s"]
    with open(OUT / "requests.csv") as f:
        records = [
            RequestRecord(
                index=int(r["index"]), start=float(r["start"]), end=float(r["end"]), latency_s=float(r["latency_s"]),
                ok=r["ok"] == "True", status=int(r["status"]) if r["status"] else None, error=r["error"] or None,
                prompt_tokens=int(r["prompt_tokens"]), completion_tokens=int(r["completion_tokens"]),
            )
            for r in csv.DictReader(f)
        ]
    chart(timeline, records, load_start, load_end, OUT / "hpa_demo.png")
    print(f"wrote {(OUT / 'hpa_demo.png').relative_to(ROOT)}")


async def main() -> None:
    baseline_replicas = len(await list_pods("app=baseline"))
    print("scaling baseline to 0 to free its cores for a second vLLM replica")
    await scale("baseline", 0)
    await kubectl("apply", "-f", str(ROOT / "k8s" / "vllm" / "hpa.yaml"))
    await wait_for_single_idle_replica()

    timeline: list[dict] = []
    stop_recording = asyncio.Event()
    recorder = asyncio.create_task(record_timeline(timeline, stop_recording))
    await asyncio.sleep(2 * POLL_S)  # a little idle history before the load

    print(f"load: concurrency {CONCURRENCY} for {LOAD_S}s")
    load_start = time.time()
    records = await run_load(URL, concurrency=CONCURRENCY, duration_s=LOAD_S)
    load_end = time.time()
    print(f"load stopped; watching scale-down for {COOLDOWN_S}s")
    await asyncio.sleep(COOLDOWN_S)
    stop_recording.set()
    await recorder

    describe = await kubectl("describe", "hpa", "vllm", "-n", NAMESPACE)
    summary = analyse(timeline, records, load_start, load_end)
    write_outputs(timeline, records, load_start, load_end, summary, describe)
    print(json.dumps(summary, indent=2))

    if baseline_replicas:
        await scale("baseline", baseline_replicas)
    print(f"wrote {OUT.relative_to(ROOT)}/")


if __name__ == "__main__":
    import sys

    if "--chart-only" in sys.argv:
        chart_from_saved()
    else:
        asyncio.run(main())
