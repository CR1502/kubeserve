"""Rolling-update demo: roll vLLM v1 -> v2 and back (rollback) under continuous load.

While a closed-loop client keeps CONCURRENCY requests in flight, the script
  1. changes the Deployment's image (kubeserve-vllm:v1 -> :v2) and waits for the rollout,
  2. rolls it back (`kubectl rollout undo`, -> :v1) and waits again,
then reports how many requests failed. The zero-drop behaviour comes from the
manifests: maxSurge=1/maxUnavailable=0 + readiness probe (the new pod gets traffic
only once it can serve), a preStop sleep (endpoint removal propagates before
shutdown) and vLLM's --shutdown-timeout (in-flight requests drain instead of
being aborted).

Evidence written to results/rolling_update/: every request, a pod lifecycle
timeline, each pod's access log (to show which pod served what), rollout
history, a summary and a chart.

Single-node capacity note: one vLLM pod plus the surge pod uses 6 of the node's
8 allocatable cores, so the baseline is scaled to 0 and the HPA is capped at one
replica for the duration of the demo (both are restored afterwards).

    uv run python -m bench.rolling_update_demo
"""

from __future__ import annotations

import asyncio
import csv
import json
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from bench import charts
from bench.charts import plt
from bench.kube import NAMESPACE, kubectl, list_pods, pod_is_ready, scale, wait_rollout
from bench.loadgen import RequestRecord, records_to_dicts, run_load, summarize

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "results" / "rolling_update"
URL = "http://localhost:30080"
CONCURRENCY = 4
SETTLE_S = 30  # steady load before, between and after the rollouts
POLL_S = 1
ACCESS_LINE = re.compile(r'"POST /v1/chat/completions HTTP/1\.1" (\d{3})')


class PodWatcher:
    """Polls vLLM pods every second: records lifecycle transitions and tails each pod's log."""

    def __init__(self, log_dir: Path):
        self.log_dir = log_dir
        # Only count requests from this demo, not earlier traffic in a pre-existing pod's log.
        self.since = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.pods: dict[str, dict] = {}
        self.events: list[dict] = []
        self._log_procs: dict[str, subprocess.Popen] = {}
        self._stop = asyncio.Event()

    def _event(self, pod: str, what: str, t: float) -> None:
        self.events.append({"t": t, "pod": pod, "event": what})
        print(f"  {time.strftime('%H:%M:%S', time.localtime(t))}  {pod}  {what}")

    async def _tick(self) -> None:
        now = time.time()
        seen = set()
        for p in await list_pods("app=vllm"):
            name = p["metadata"]["name"]
            seen.add(name)
            info = self.pods.setdefault(
                name, {"image": p["spec"]["containers"][0]["image"], "seen": now, "running": None, "ready": None, "terminating": None, "gone": None}
            )
            if info["running"] is None and p["status"].get("phase") == "Running":
                info["running"] = now
                self._event(name, f"running ({info['image']})", now)
                self._tail_log(name)
            if info["ready"] is None and pod_is_ready(p):
                info["ready"] = now
                self._event(name, "ready -> receives traffic", now)
            if info["terminating"] is None and p["metadata"].get("deletionTimestamp"):
                info["terminating"] = now
                self._event(name, "terminating (preStop sleep, then drain)", now)
        for name, info in self.pods.items():
            if name not in seen and info["gone"] is None:
                info["gone"] = now
                self._event(name, "gone", now)

    def _tail_log(self, pod: str) -> None:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        log_file = open(self.log_dir / f"{pod}.log", "w")
        self._log_procs[pod] = subprocess.Popen(
            ["kubectl", "logs", "-f", f"--since-time={self.since}", "-n", NAMESPACE, pod], stdout=log_file, stderr=subprocess.STDOUT
        )

    async def run(self) -> None:
        while not self._stop.is_set():
            await self._tick()
            try:
                await asyncio.wait_for(self._stop.wait(), POLL_S)
            except asyncio.TimeoutError:
                pass

    async def stop(self) -> None:
        self._stop.set()
        await asyncio.sleep(2)  # let `kubectl logs -f` flush the last lines
        for proc in self._log_procs.values():
            proc.terminate()

    async def wait_until_single_pod(self) -> None:
        while True:
            pods = await list_pods("app=vllm")
            if len(pods) == 1 and pod_is_ready(pods[0]):
                return
            await asyncio.sleep(POLL_S)


def served_per_pod(log_dir: Path) -> dict[str, dict[str, int]]:
    counts = {}
    for log in sorted(log_dir.glob("*.log")):
        codes: dict[str, int] = {}
        for match in ACCESS_LINE.finditer(log.read_text(errors="replace")):
            codes[match.group(1)] = codes.get(match.group(1), 0) + 1
        counts[log.stem] = codes
    return counts


def client_lanes(requests: list[dict]) -> list[int]:
    """Assign each request to the client worker that sent it (closed loop: a worker's
    next request starts right after its previous one ends)."""
    lanes: list[float] = []  # end time of each lane's last request
    assignment = [0] * len(requests)
    for i in sorted(range(len(requests)), key=lambda i: requests[i]["start"]):
        r = requests[i]
        free = [k for k, end in enumerate(lanes) if end <= r["start"] + 0.5]
        lane = min(free, key=lambda k: r["start"] - lanes[k]) if free else len(lanes)
        if lane == len(lanes):
            lanes.append(0.0)
        lanes[lane] = r["end"]
        assignment[i] = lane
    return assignment


def chart(pods: dict[str, dict], requests: list[dict], marks: dict[str, float], path: Path) -> None:
    """pods/requests/marks use seconds relative to the start of the load."""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6.4), sharex=True, gridspec_kw={"height_ratios": [1, 1]})
    colors = {"kubeserve-vllm:v1": charts.VLLM, "kubeserve-vllm:v2": charts.AQUA}
    end_t = max(r["end"] for r in requests)
    ordered = sorted(pods.items(), key=lambda kv: kv[1]["seen"])
    for lane, (name, info) in enumerate(ordered):
        y = len(ordered) - 1 - lane
        color = colors.get(info["image"], charts.TEXT_SECONDARY)
        start = info["running"] if info["running"] is not None else info["seen"]
        ready, term = info["ready"], info["terminating"]
        gone = info["gone"] if info["gone"] is not None else end_t
        if ready is not None and ready > start:  # starting: outline only
            ax1.barh(y, ready - start, left=start, height=0.5, color="none", edgecolor=color, linewidth=1.5)
        serve_from = ready if ready is not None else start
        serve_to = term if term is not None else gone
        ax1.barh(y, serve_to - serve_from, left=serve_from, height=0.5, color=color)
        if term is not None:  # draining: hatched
            ax1.barh(y, gone - term, left=term, height=0.5, color="none", edgecolor=color, hatch="////", linewidth=1.5)
        ax1.annotate(f"{name}  ({info['image'].split(':')[1]})", (max(start, -5), y + 0.32), fontsize=8.5, color=charts.TEXT_PRIMARY)
    ax1.set_yticks([])
    ax1.set_ylim(-0.6, len(ordered) - 0.2)
    ax1.grid(False)
    ax1.set_title("vLLM pods: outline = starting, solid = ready (in Service), hatched = draining")

    lanes = client_lanes(requests)
    failed = sum(not r["ok"] for r in requests)
    for r, lane in zip(requests, lanes):
        color = charts.VLLM if r["ok"] else charts.CRITICAL
        # 2px surface-coloured edge = the gap between back-to-back requests on a lane
        ax2.barh(lane, r["end"] - r["start"], left=r["start"], height=0.6, color=color, edgecolor=charts.SURFACE, linewidth=2)
    n_lanes = max(lanes) + 1
    ax2.set_yticks(range(n_lanes), [f"client {k + 1}" for k in range(n_lanes)])
    ax2.set_ylim(-0.6, n_lanes - 0.4)
    ax2.invert_yaxis()
    ax2.grid(False)
    ax2.set_title(
        f"All {len(requests)} requests, send → response, per client connection slot: "
        f"{len(requests) - failed} succeeded, {failed} failed"
        + (" (red)" if failed else "")
    )
    ax2.set_xlabel("Seconds since load started")
    for label, t in marks.items():
        for ax in (ax1, ax2):
            ax.axvline(t, color=charts.TEXT_SECONDARY, linewidth=1, zorder=0)
        ax1.annotate(label, (t, len(ordered) - 0.35), xytext=(4, 0), textcoords="offset points", fontsize=8.5, color=charts.TEXT_SECONDARY)
    ax2.set_xlim(-5, end_t + 5)
    fig.suptitle("Rolling update v1 → v2 and rollback v2 → v1 under live traffic", x=0.0, ha="left", fontsize=12, fontweight="bold")
    fig.tight_layout()
    charts.save(fig, path)


def chart_from_saved() -> None:
    summary = json.loads((OUT / "summary.json").read_text())
    with open(OUT / "requests.csv") as f:
        rows = list(csv.DictReader(f))
    t0 = min(float(r["start"]) for r in rows)
    requests = [{"start": float(r["start"]) - t0, "end": float(r["end"]) - t0, "ok": r["ok"] == "True"} for r in rows]
    chart(summary["pods"], requests, summary["marks_s"], OUT / "rolling_update.png")
    print(f"wrote {(OUT / 'rolling_update.png').relative_to(ROOT)}")


async def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    log_dir = OUT / "pod_logs"
    for old in log_dir.glob("*.log"):
        old.unlink()

    baseline_replicas = len(await list_pods("app=baseline"))
    print("preparing: baseline -> 0 replicas, HPA capped at 1 replica, vLLM on :v1")
    await scale("baseline", 0)
    await kubectl("patch", "hpa", "vllm", "-n", NAMESPACE, "--type", "merge", "-p", '{"spec":{"minReplicas":1,"maxReplicas":1}}')
    await kubectl("set", "image", "-n", NAMESPACE, "deployment/vllm", "vllm=kubeserve-vllm:v1")
    await wait_rollout("vllm")

    watcher = PodWatcher(log_dir)
    await watcher.wait_until_single_pod()
    watch_task = asyncio.create_task(watcher.run())
    await asyncio.sleep(2)

    stop_load = asyncio.Event()
    records: list[RequestRecord] = []
    load_start = time.time()
    load_task = asyncio.create_task(run_load(URL, concurrency=CONCURRENCY, stop_event=stop_load))
    print(f"load running at concurrency {CONCURRENCY}")
    marks = {}

    await asyncio.sleep(SETTLE_S)
    marks["set image v2"] = time.time()
    print("rolling update: kubeserve-vllm:v1 -> kubeserve-vllm:v2")
    await kubectl("set", "image", "-n", NAMESPACE, "deployment/vllm", "vllm=kubeserve-vllm:v2")
    await kubectl("annotate", "-n", NAMESPACE, "deployment/vllm", "kubernetes.io/change-cause=roll out kubeserve-vllm:v2", "--overwrite")
    await wait_rollout("vllm")
    await watcher.wait_until_single_pod()

    await asyncio.sleep(SETTLE_S)
    marks["rollout undo"] = time.time()
    print("rollback: kubectl rollout undo (-> kubeserve-vllm:v1)")
    await kubectl("rollout", "undo", "-n", NAMESPACE, "deployment/vllm")
    await kubectl("annotate", "-n", NAMESPACE, "deployment/vllm", "kubernetes.io/change-cause=rollback to kubeserve-vllm:v1", "--overwrite")
    await wait_rollout("vllm")
    await watcher.wait_until_single_pod()

    await asyncio.sleep(SETTLE_S)
    stop_load.set()
    records = await load_task
    load_end = time.time()
    await watcher.stop()
    await watch_task

    history = await kubectl("rollout", "history", "-n", NAMESPACE, "deployment/vllm")
    served = served_per_pod(log_dir)
    summary = {
        "concurrency": CONCURRENCY,
        "load_duration_s": round(load_end - load_start, 1),
        "rollouts": ["kubeserve-vllm:v1 -> kubeserve-vllm:v2", "rollback -> kubeserve-vllm:v1"],
        "requests": summarize(records),
        "failed_requests": [r.__dict__ for r in records if not r.ok],
        "served_per_pod_from_access_logs": served,
        "pods": {
            name: {k: (round(v - load_start, 1) if isinstance(v, float) else v) for k, v in info.items()}
            for name, info in watcher.pods.items()
        },
        "marks_s": {k: round(v - load_start, 1) for k, v in marks.items()},
    }
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2))
    (OUT / "rollout_history.txt").write_text(history)
    with open(OUT / "requests.csv", "w", newline="") as f:
        rows = records_to_dicts(records)
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    with open(OUT / "pod_events.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["t_rel_s", "pod", "event"])
        writer.writeheader()
        for e in watcher.events:
            writer.writerow({"t_rel_s": round(e["t"] - load_start, 1), "pod": e["pod"], "event": e["event"]})
    chart_from_saved()

    print("restoring HPA and baseline")
    await kubectl("apply", "-f", str(ROOT / "k8s" / "vllm" / "hpa.yaml"))
    if baseline_replicas:
        await scale("baseline", baseline_replicas)

    s = summary["requests"]
    print(f"\n{s['requests']} requests, {s['succeeded']} succeeded, {s['failed']} failed")
    print("served per pod (access logs):", json.dumps(served))
    print(f"wrote {OUT.relative_to(ROOT)}/")


if __name__ == "__main__":
    import sys

    if "--chart-only" in sys.argv:  # re-render the chart from results/rolling_update/
        chart_from_saved()
    else:
        asyncio.run(main())
