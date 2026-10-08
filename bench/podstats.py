"""Samples CPU and memory of the pods behind a label selector during a benchmark.

Reads the container's own cgroup v2 counters via `kubectl exec`:
  * cpu.stat usage_usec       -> cumulative CPU time, so the average number of
                                 cores used between two samples is exact;
  * memory.current - inactive_file from memory.stat
                              -> working-set memory (what the kubelet uses).
Utilisation is reported relative to the pod's CPU limit (3 exclusive cores here).

This is the CPU equivalent of "GPU utilisation": this deployment has no GPU.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from bench.kube import NAMESPACE, kubectl, list_pods, parse_cpu, pod_is_ready

_READ_CMD = "cat /sys/fs/cgroup/cpu.stat /sys/fs/cgroup/memory.current; grep '^inactive_file ' /sys/fs/cgroup/memory.stat"


@dataclass
class Sample:
    t: float
    pod: str
    cpu_usage_s: float
    working_set_bytes: int


async def read_pod(pod: str, namespace: str = NAMESPACE) -> Sample | None:
    try:
        out = await kubectl("exec", "-n", namespace, pod, "--", "sh", "-c", _READ_CMD)
    except RuntimeError:
        return None  # pod went away between listing and exec
    usage_usec, memory_current, inactive_file = 0, 0, 0
    for line in out.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] == "usage_usec":
            usage_usec = int(parts[1])
        elif len(parts) == 2 and parts[0] == "inactive_file":
            inactive_file = int(parts[1])
        elif len(parts) == 1 and parts[0].isdigit():
            memory_current = int(parts[0])
    return Sample(time.time(), pod, usage_usec / 1e6, memory_current - inactive_file)


class PodStatsSampler:
    def __init__(self, selector: str, interval_s: float = 2.0, namespace: str = NAMESPACE):
        self.selector = selector
        self.interval_s = interval_s
        self.namespace = namespace
        self.samples: list[Sample] = []
        self.cpu_limits: dict[str, float] = {}
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    async def _tick(self) -> None:
        pods = [p for p in await list_pods(self.selector, self.namespace) if pod_is_ready(p)]
        for pod in pods:
            name = pod["metadata"]["name"]
            if name not in self.cpu_limits:
                limit = pod["spec"]["containers"][0]["resources"]["limits"]["cpu"]
                self.cpu_limits[name] = parse_cpu(limit)
        results = await asyncio.gather(*(read_pod(p["metadata"]["name"], self.namespace) for p in pods))
        self.samples.extend(s for s in results if s is not None)

    async def _run(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            await self._tick()
            try:
                await asyncio.wait_for(self._stop.wait(), max(0.0, self.interval_s - (time.monotonic() - started)))
            except asyncio.TimeoutError:
                pass

    async def start(self) -> None:
        await self._tick()  # baseline sample before load starts
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
        await self._tick()  # closing sample after load ends

    def summary(self) -> dict:
        """Per-pod and total CPU/memory over the sampled window."""
        per_pod = {}
        for pod in sorted({s.pod for s in self.samples}):
            series = [s for s in self.samples if s.pod == pod]
            if len(series) < 2:
                continue
            first, last = series[0], series[-1]
            window = last.t - first.t
            avg_cores = (last.cpu_usage_s - first.cpu_usage_s) / window
            interval_cores = [
                (b.cpu_usage_s - a.cpu_usage_s) / (b.t - a.t) for a, b in zip(series, series[1:]) if b.t > a.t
            ]
            limit = self.cpu_limits.get(pod)
            per_pod[pod] = {
                "cpu_limit_cores": limit,
                "avg_cores_used": round(avg_cores, 3),
                "avg_cpu_utilization_pct": round(100 * avg_cores / limit, 1) if limit else None,
                "peak_cpu_utilization_pct": round(100 * max(interval_cores) / limit, 1) if limit and interval_cores else None,
                "peak_working_set_mib": round(max(s.working_set_bytes for s in series) / 2**20),
                "window_s": round(window, 1),
            }
        return {
            "pods": per_pod,
            "total_avg_cores_used": round(sum(p["avg_cores_used"] for p in per_pod.values()), 3),
        }
