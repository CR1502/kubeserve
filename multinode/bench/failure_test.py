"""Failure-injection test: lose a worker node (or the vLLM process in one pod) under load.

Scenarios (each can also run with --retry, see below):

  node-kill      `docker kill` of a worker node container that hosts a vLLM pod. Kubelet,
                 kube-proxy, the pod and its network vanish at once (an abrupt node loss;
                 not a real power, NIC or partition failure). Kubernetes has to notice (node-monitor-grace-period), evict the pod
                 (NoExecute taint + the pod's 30 s toleration) and start a replacement on
                 the spare worker (vLLM needs ~30-50 s to become Ready).
  process-crash  SIGKILL of the vLLM server process inside one pod, sent from its node
                 (`docker exec <node> pkill -9 -f "vllm serve"`; each node runs at most one
                 vLLM pod). The kubelet restarts the container in place; the pod is out of
                 the Service until the new container passes its readiness probe.

A run:
  1. precondition: the Deployment is scaled to R and the script waits for R Ready pods on
     R distinct workers (node-kill: plus a spare worker without a vLLM pod).
  2. a 1 s watcher records every transition of the nodes, the vLLM pods and the Service's
     EndpointSlices with host timestamps, and tails every vLLM pod's log to pod_logs/.
  3. closed-loop load (new connection per request, client timeout --timeout). PRE_S later
     the fault is injected; the script waits until Kubernetes has recovered (R ready
     endpoints again, including the replacement / restarted pod; capped at
     RECOVERY_CAP_S), then keeps the load on for POST_S.
  4. the load stops (in-flight requests finish) and the cluster is restored (node-kill:
     `docker start` + pin-node-cpus.sh, then wait for the node and for exactly R Ready pods).

--retry: the client retries a failed request once, on a new connection, after a
connection error, timeout or 5xx, and records both attempts. It is a separate run that
shows how much a client retry masks; the no-retry run is the primary result.

The analysis only reads the saved files, so `--chart-only` re-runs it offline and
re-renders the chart. Outputs in --out: summary.json, summary.md, requests.csv,
attempts.csv (retry runs), timeline.csv, pod_logs/, events.txt, failure_timeline.png.

    uv run python -m multinode.bench.failure_test --scenario node-kill
    uv run python -m multinode.bench.failure_test --scenario process-crash --retry
    uv run python -m multinode.bench.failure_test --scenario node-kill --chart-only
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import os
import platform
import shlex
import shutil
import subprocess
import time
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import httpx

from bench import charts
from bench.charts import plt
from bench.hpa_demo import tokens_per_s
from bench.kube import NAMESPACE, kubectl, kubectl_json, list_pods, pod_is_ready, scale
from bench.loadgen import (
    _AWAKE_CLOCK,
    DEFAULT_MAX_TOKENS,
    MAX_SLEEP_S,
    HostSleptError,
    RequestRecord,
    _send,
    _slept_since,
    percentile,
    records_to_dicts,
    run_load,
    summarize,
)
from bench.rolling_update_demo import ACCESS_LINE, client_lanes
from multinode.bench import hostguard

ROOT = Path(__file__).resolve().parents[2]
URL = "http://localhost:30080"
CLUSTER = "kubeserve-mn"
KUBE_CONTEXT = f"kind-{CLUSTER}"
CONTROL_PLANE = f"{CLUSTER}-control-plane"  # the ingress node: never faulted
WORKERS = (f"{CLUSTER}-worker", f"{CLUSTER}-worker2", f"{CLUSTER}-worker3")
PIN_SCRIPT = ROOT / "multinode" / "scripts" / "pin-node-cpus.sh"
DEPLOYMENT = "vllm"
CONTAINER = "vllm"
SELECTOR = "app=vllm"
SERVICE = "vllm"
CRASH_PATTERN = "vllm serve"  # matches the pod's main process: ENTRYPOINT ["vllm", "serve"]

PRE_S = 60  # steady load before the fault
POST_S = 90  # steady load after recovery
RECOVERY_CAP_S = 600  # give up waiting for recovery after this long
SETUP_TIMEOUT_S = 600
NODE_READY_TIMEOUT_S = 300
RESTORE_TIMEOUT_S = 600
POLL_S = 1.0
WINDOW_S = 10

KINDS = ("node", "pod", "endpoint", "service")
FIELD_ORDER = [
    "exists", "node", "phase", "ready", "state", "restarts", "deleting", "container", "last_terminated",
    "taints", "ready_reason", "ready_since", "created", "ready_count", "ready_pods",
]
QUIET_FIELDS = {"ready_since", "created", "ready_reason", "last_terminated", "ready_pods"}
CLASS_LABELS = {
    "ok": "succeeded",
    "failed_in_flight": "failed, in flight when the fault hit",
    "failed_after_fault": "failed, sent after the fault",
    "failed_before_fault": "failed before the fault",
}
PHASE_LABELS = {
    "pre_fault": "pre-fault",
    "detection": "detection (fault → victim out of the ready endpoints)",
    "degraded": "degraded (→ recovered)",
    "post_recovery": "post-recovery",
    "whole_run": "whole run (no fault injected)",
}
KEY_LABELS = [
    ("fault", "fault injected"),
    ("node_not_ready", "victim node Ready≠True"),
    ("node_unreachable_taint", "node.kubernetes.io/unreachable taint on the victim node"),
    ("victim_pod_not_ready", "victim pod Ready=False"),
    ("victim_endpoint_removed", "victim pod no longer a ready endpoint of the Service"),
    ("ready_endpoints_below_R", "ready endpoints < R"),
    ("victim_pod_deletion", "victim pod deletionTimestamp set (evicted)"),
    ("container_restart_observed", "victim container restarted (restartCount +1)"),
    ("victim_ready_again", "victim pod Ready again"),
    ("replacement_created", "replacement pod created"),
    ("replacement_scheduled", "replacement pod scheduled"),
    ("replacement_running", "replacement container running"),
    ("replacement_ready", "replacement pod Ready"),
    ("ready_endpoints_back_to_R", "ready endpoints back to R"),
    ("recovered", "recovered (criterion met)"),
    ("load_stopped", "load stopped"),
    ("docker_start", "restore: docker start of the victim node"),
    ("node_ready_again", "restore: victim node Ready again"),
    ("victim_pod_gone", "victim pod object deleted"),
]


# --------------------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------------------


async def run_cmd(*args: str, timeout: float = 120) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(*args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        return -1, "", f"timed out after {timeout:.0f}s"
    return proc.returncode, out.decode(errors="replace"), err.decode(errors="replace")


async def docker(*args: str, timeout: float = 120) -> tuple[int, str, str]:
    return await run_cmd("docker", *args, timeout=timeout)


def assert_faultable(node: str) -> None:
    """Hard guard: only the three kind worker containers may ever be killed or exec'd into."""
    if node == CONTROL_PLANE or node not in WORKERS:
        raise RuntimeError(f"refusing to inject a fault into {node!r}: only {', '.join(WORKERS)} may be faulted")


# --------------------------------------------------------------------------------------
# Cluster state: what the watcher records (all values are strings, so the CSV round-trips)
# --------------------------------------------------------------------------------------


def empty_state() -> dict:
    return {kind: {} for kind in KINDS}


def node_fields(node: dict) -> dict[str, str]:
    conditions = node.get("status", {}).get("conditions") or []
    ready = next((c for c in conditions if c.get("type") == "Ready"), {})
    taints = sorted(f"{t['key']}:{t['effect']}" for t in node.get("spec", {}).get("taints") or [])
    return {
        "ready": ready.get("status", "Unknown"),
        "ready_reason": ready.get("reason", ""),
        "ready_since": ready.get("lastTransitionTime", ""),
        "taints": ",".join(taints),
    }


def _container_state(state: dict | None) -> str:
    if not state:
        return ""
    if "running" in state:
        return f"running since {state['running'].get('startedAt', '')}"
    if "waiting" in state:
        return f"waiting: {state['waiting'].get('reason', '')}"
    if "terminated" in state:
        t = state["terminated"]
        return f"terminated: {t.get('reason', '')} exit {t.get('exitCode')} at {t.get('finishedAt', '')}"
    return ""


def pod_fields(pod: dict) -> dict[str, str]:
    meta, spec, status = pod["metadata"], pod.get("spec", {}), pod.get("status", {})
    statuses = status.get("containerStatuses") or []
    cs = next((c for c in statuses if c.get("name") == CONTAINER), statuses[0] if statuses else {})
    ready = next((c for c in status.get("conditions") or [] if c.get("type") == "Ready"), {})
    return {
        "node": spec.get("nodeName") or "",
        "phase": status.get("phase", ""),
        "ready": "True" if pod_is_ready(pod) else "False",
        "ready_since": ready.get("lastTransitionTime", ""),
        "restarts": str(cs.get("restartCount", 0)),
        "container": _container_state(cs.get("state")),
        "last_terminated": _container_state(cs.get("lastState")),
        # Host-observed time of this field's first change is the eviction time; the value
        # itself is (deletion request + grace period).
        "deleting": meta.get("deletionTimestamp") or "",
        "created": meta.get("creationTimestamp", ""),
    }


def endpoint_states(slices: list[dict]) -> dict[str, str]:
    """Pod name -> 'ready' | 'terminating' | 'not-ready', from the Service's EndpointSlices.

    API semantics: a nil `ready` means ready, a nil `terminating` means not terminating.
    """
    rank = {"not-ready": 1, "terminating": 2, "ready": 3}
    out: dict[str, str] = {}
    for s in slices:
        for ep in s.get("endpoints") or []:
            ref = ep.get("targetRef") or {}
            name = ref.get("name") or ",".join(ep.get("addresses") or [])
            cond = ep.get("conditions") or {}
            if cond.get("ready", True) is not False:
                state = "ready"
            elif cond.get("terminating"):
                state = "terminating"
            else:
                state = "not-ready"
            if rank[state] > rank.get(out.get(name, ""), 0):
                out[name] = state
    return out


def observe(nodes: list[dict], pods: list[dict], slices: list[dict]) -> dict:
    eps = endpoint_states(slices)
    ready = sorted(name for name, s in eps.items() if s == "ready")
    return {
        "node": {n["metadata"]["name"]: {"exists": "yes", **node_fields(n)} for n in nodes},
        "pod": {p["metadata"]["name"]: {"exists": "yes", **pod_fields(p)} for p in pods},
        "endpoint": {name: {"state": s} for name, s in eps.items()},
        "service": {SERVICE: {"ready_count": str(len(ready)), "ready_pods": ";".join(ready)}},
    }


_GONE = {"node": ("exists", "gone"), "pod": ("exists", "gone"), "endpoint": ("state", "absent")}


def _field_key(field: str) -> tuple[int, str]:
    return (FIELD_ORDER.index(field) if field in FIELD_ORDER else len(FIELD_ORDER), field)


def diff_states(old: dict, new: dict) -> tuple[dict, list[tuple[str, str, str, str, str]]]:
    """Merge a fresh observation into the previous state; return it and the changes.

    Entities that disappear stay in the state, marked gone (pods, nodes) or absent
    (endpoints), so the history can be replayed from the transitions alone.
    """
    merged = empty_state()
    changes = []
    for kind in KINDS:
        for name in sorted(set(old[kind]) | set(new[kind])):
            before = old[kind].get(name, {})
            if name in new[kind]:
                after = new[kind][name]
            elif kind in _GONE:
                key, value = _GONE[kind]
                after = {**before, key: value}
            else:
                after = before
            for field in sorted(set(before) | set(after), key=_field_key):
                a, b = before.get(field, ""), after.get(field, "")
                if a != b:
                    changes.append((kind, name, field, a, b))
            merged[kind][name] = after
    return merged, changes


def apply_transition(state: dict, tr: dict) -> None:
    state[tr["kind"]].setdefault(tr["name"], {})[tr["field"]] = tr["new"]


def ready_endpoints(state: dict) -> int:
    return sum(e.get("state") == "ready" for e in state["endpoint"].values())


def endpoint_state(state: dict, pod: str) -> str:
    return state["endpoint"].get(pod, {}).get("state", "absent")


def recovery_context(run: dict) -> dict:
    return {
        "scenario": run["scenario"],
        "replicas": int(run["replicas"]),
        "victim_pod": run["victim"]["pod"],
        "victim_node": run["victim"]["node"],
        "pre_fault_pods": set(run.get("pre_fault_pods") or []),
        "victim_restarts_before": int(run.get("victim_restarts_before") or 0),
    }


def is_recovered(state: dict, ctx: dict) -> bool:
    """The RECOVERED criterion, used both live (to end the wait) and in the offline analysis."""
    if ready_endpoints(state) < ctx["replicas"]:
        return False
    if ctx["scenario"] == "node-kill":
        return any(
            p.get("exists") == "yes"
            and p.get("ready") == "True"
            and not p.get("deleting")
            and p.get("node") not in ("", ctx["victim_node"])
            and endpoint_state(state, name) == "ready"
            for name, p in state["pod"].items()
            if name not in ctx["pre_fault_pods"]
        )
    p = state["pod"].get(ctx["victim_pod"], {})
    return (
        p.get("exists") == "yes"
        and int(p.get("restarts") or 0) > ctx["victim_restarts_before"]
        and p.get("ready") == "True"
        and not p.get("deleting")
        and endpoint_state(state, ctx["victim_pod"]) == "ready"
    )


def exactly_ready(state: dict, replicas: int) -> bool:
    pods = [p for p in state["pod"].values() if p.get("exists") == "yes"]
    return (
        len(pods) == replicas
        and all(p.get("ready") == "True" and not p.get("deleting") for p in pods)
        and ready_endpoints(state) == replicas
    )


# --------------------------------------------------------------------------------------
# Watcher
# --------------------------------------------------------------------------------------


def utc_iso(t: float) -> str:
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def hms(t: float) -> str:
    return time.strftime("%H:%M:%S", time.localtime(t))


class ClusterWatcher:
    """Polls nodes, vLLM pods and the Service's EndpointSlices every POLL_S seconds.

    Every change is recorded as a transition with the host time at which the poll that
    first saw it returned (`t`, an upper bound) and the start of the previous poll that
    still saw the old value (`t_prev_poll`, a lower bound). Each vLLM container's log is
    tailed to pod_logs/<pod>.log (<pod>.restart<N>.log after a container restart).
    """

    def __init__(self, log_dir: Path):
        self.log_dir = log_dir
        # Only this run's lines from pods that already existed.
        self.since = utc_iso(time.time())
        self.state = empty_state()
        self.transitions: list[dict] = []
        self.errors: list[dict] = []
        self.last_t: float | None = None
        self._prev_poll_start: float | None = None
        self._tails: list[tuple[subprocess.Popen, object]] = []
        self._tail_restarts: dict[str, int] = {}
        self._stop = asyncio.Event()
        self._tick_event = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def fetch(self) -> dict:
        nodes, pods, slices = await asyncio.gather(
            kubectl_json("get", "nodes"),
            list_pods(SELECTOR),
            kubectl_json("get", "endpointslices", "-n", NAMESPACE, "-l", f"kubernetes.io/service-name={SERVICE}"),
        )
        return observe(nodes["items"], pods, slices["items"])

    async def tick(self) -> None:
        t0 = time.time()
        try:
            new = await self.fetch()
        except Exception as exc:  # a failed poll is recorded, not fatal
            self.errors.append({"t": t0, "error": f"{type(exc).__name__}: {exc}"[:300]})
            print(f"  {hms(t0)}  watcher: poll failed ({type(exc).__name__}: {str(exc)[:120]})")
            return
        t = time.time()
        merged, changes = diff_states(self.state, new)
        for kind, name, field, old, value in changes:
            self.transitions.append(
                {"t": t, "t_prev_poll": self._prev_poll_start, "kind": kind, "name": name, "field": field, "old": old, "new": value}
            )
            if field not in QUIET_FIELDS:
                print(f"  {hms(t)}  {kind:<8} {name}  {field}: {old or '-'} -> {value}")
        self.state = merged
        self.last_t = t
        self._prev_poll_start = t0
        self._update_tails()
        event, self._tick_event = self._tick_event, asyncio.Event()
        event.set()

    async def run(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            await self.tick()
            try:
                await asyncio.wait_for(self._stop.wait(), max(0.0, POLL_S - (time.monotonic() - started)))
            except asyncio.TimeoutError:
                pass

    async def start(self) -> None:
        await self.tick()
        self._task = asyncio.create_task(self.run())

    async def wait_for(self, predicate: Callable[[dict], bool], timeout: float, abort: asyncio.Future | None = None) -> float | None:
        """Time of the first poll whose state satisfies `predicate`; None on timeout/abort."""
        deadline = time.monotonic() + timeout
        while True:
            if predicate(self.state):
                return self.last_t
            remaining = deadline - time.monotonic()
            if remaining <= 0 or (abort is not None and abort.done()):
                return None
            tick = asyncio.ensure_future(self._tick_event.wait())
            waiters = {tick} | ({abort} if abort is not None else set())
            await asyncio.wait(waiters, timeout=remaining, return_when=asyncio.FIRST_COMPLETED)
            tick.cancel()

    def tail_command(self, pod: str) -> list[str]:
        return ["kubectl", "logs", "-f", "--timestamps", f"--since-time={self.since}", "-n", NAMESPACE, "-c", CONTAINER, pod]

    def _update_tails(self) -> None:
        for name, p in self.state["pod"].items():
            if p.get("exists") != "yes" or not p.get("container", "").startswith("running"):
                continue
            restarts = int(p.get("restarts") or 0)
            if name in self._tail_restarts and restarts <= self._tail_restarts[name]:
                continue
            path = self.log_dir / (f"{name}.log" if name not in self._tail_restarts else f"{name}.restart{restarts}.log")
            self._tail_restarts[name] = restarts
            self.log_dir.mkdir(parents=True, exist_ok=True)
            log_file = open(path, "w")
            # A killed node's stream may hang instead of ending; stop() terminates it.
            proc = subprocess.Popen(self.tail_command(name), stdout=log_file, stderr=subprocess.STDOUT)
            self._tails.append((proc, log_file))

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
        await asyncio.sleep(2)  # let `kubectl logs -f` flush the last lines
        for proc, _ in self._tails:
            if proc.poll() is None:
                proc.terminate()
        for proc, log_file in self._tails:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            log_file.close()


# --------------------------------------------------------------------------------------
# Client with one retry
# --------------------------------------------------------------------------------------


@dataclass
class RetryRecord(RequestRecord):
    """One logical request: start of the first attempt -> end of the last; outcome = last attempt."""

    attempts: int
    first_attempt_ok: bool
    first_attempt_error: str | None
    outcomes: str  # per attempt, e.g. "ReadTimeout 60.0s | ok 13.2s"


@dataclass
class Attempt:
    request_index: int
    attempt: int
    start: float
    end: float
    latency_s: float
    ok: bool
    status: int | None
    error: str | None
    prompt_tokens: int
    completion_tokens: int
    retryable: bool

    @classmethod
    def from_record(cls, r: RequestRecord, attempt: int = 1) -> Attempt:
        return cls(
            request_index=r.index, attempt=attempt, start=r.start, end=r.end, latency_s=r.latency_s, ok=r.ok,
            status=r.status, error=r.error, prompt_tokens=r.prompt_tokens, completion_tokens=r.completion_tokens,
            retryable=is_retryable(r),
        )


def is_retryable(r: RequestRecord) -> bool:
    """Transport-level failures (status None: connect/read errors, timeouts, protocol errors) and 5xx."""
    return not r.ok and (r.status is None or r.status >= 500)


def error_category(status: int | None, error: str | None) -> str:
    if status is not None and status != 200:
        return f"HTTP {status}"
    if status == 200 and not error:
        return "ok"
    return (error or "unknown").split(":", 1)[0].strip() or "unknown"


async def run_load_with_retry(
    base_url: str,
    *,
    concurrency: int,
    stop_event: asyncio.Event,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    timeout_s: float = 60.0,
    on_record: Callable[[RetryRecord], None] | None = None,
    on_attempt: Callable[[Attempt], None] | None = None,
) -> list[RetryRecord]:
    """bench.loadgen.run_load (closed loop, no keep-alive, same payloads and error strings,
    same host-sleep check) plus one immediate retry of a retryable failure. Without
    keep-alive every attempt, the retry included, opens a new TCP connection."""
    wall0, awake0 = time.time(), time.clock_gettime(_AWAKE_CLOCK)
    records: list[RetryRecord] = []
    next_index = 0
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=0)
    async with httpx.AsyncClient(base_url=base_url, timeout=timeout_s, limits=limits) as client:

        async def worker() -> None:
            nonlocal next_index
            while not stop_event.is_set():
                index = next_index
                next_index += 1
                tries = [await _send(client, index, max_tokens)]
                if is_retryable(tries[0]):
                    tries.append(await _send(client, index, max_tokens))
                for k, t in enumerate(tries, 1):
                    if on_attempt is not None:
                        on_attempt(Attempt.from_record(t, k))
                last = tries[-1]
                record = RetryRecord(
                    index=index, start=tries[0].start, end=last.end, latency_s=last.end - tries[0].start, ok=last.ok,
                    status=last.status, error=last.error, prompt_tokens=last.prompt_tokens,
                    completion_tokens=last.completion_tokens, attempts=len(tries), first_attempt_ok=tries[0].ok,
                    first_attempt_error=tries[0].error,
                    outcomes=" | ".join(f"{error_category(t.status, t.error)} {t.latency_s:.1f}s" for t in tries),
                )
                records.append(record)
                if on_record is not None:
                    on_record(record)

        async def sleep_watchdog() -> None:
            while _slept_since(wall0, awake0) <= MAX_SLEEP_S:
                await asyncio.sleep(1)

        workers = asyncio.gather(*(worker() for _ in range(concurrency)))
        watchdog = asyncio.ensure_future(sleep_watchdog())
        await asyncio.wait({workers, watchdog}, return_when=asyncio.FIRST_COMPLETED)
        for task in (workers, watchdog):
            task.cancel()
        await asyncio.gather(workers, watchdog, return_exceptions=True)

    slept = _slept_since(wall0, awake0)
    if slept > MAX_SLEEP_S:
        raise HostSleptError(f"the host was asleep for ~{slept:.0f}s during this run, so its numbers are invalid")
    return records


# --------------------------------------------------------------------------------------
# Scenario steps (these touch the cluster)
# --------------------------------------------------------------------------------------


def precondition_problem(nodes: list[dict], pods: list[dict], slices: list[dict], scenario: str, replicas: int) -> str | None:
    """None if the cluster is ready for the test, else what is still missing."""
    node_ready = {n["metadata"]["name"]: node_fields(n)["ready"] == "True" for n in nodes}
    missing = [w for w in WORKERS if w not in node_ready]
    if missing:
        return f"worker node(s) not found: {', '.join(missing)}"
    not_ready_nodes = [w for w in WORKERS if not node_ready[w]]
    if not_ready_nodes:
        return f"worker node(s) not Ready: {', '.join(not_ready_nodes)}"
    terminating = [p["metadata"]["name"] for p in pods if p["metadata"].get("deletionTimestamp")]
    if terminating:
        return f"vLLM pod(s) still terminating: {', '.join(terminating)}"
    if len(pods) != replicas:
        return f"{len(pods)} vLLM pods, want {replicas}"
    not_ready = [p["metadata"]["name"] for p in pods if not pod_is_ready(p)]
    if not_ready:
        return f"vLLM pod(s) not Ready yet: {', '.join(not_ready)}"
    pod_nodes = [p.get("spec", {}).get("nodeName") or "" for p in pods]
    if len(set(pod_nodes)) != len(pod_nodes):
        return f"vLLM pods share a node: {pod_nodes}"
    off_worker = [n for n in pod_nodes if n not in WORKERS]
    if off_worker:
        return f"vLLM pod(s) on unexpected node(s): {off_worker}"
    ready_eps = sum(s == "ready" for s in endpoint_states(slices).values())
    if ready_eps != replicas:
        return f"{ready_eps} ready endpoints in Service {SERVICE}, want {replicas}"
    if scenario == "node-kill" and not [w for w in WORKERS if w not in pod_nodes]:
        return "no spare worker without a vLLM pod (node-kill needs one for the replacement)"
    return None


def choose_victim(pods: list[dict]) -> dict:
    """Deterministic: the replica on the lowest-named worker."""
    pod = min(pods, key=lambda p: p["spec"]["nodeName"])
    return {"pod": pod["metadata"]["name"], "node": pod["spec"]["nodeName"]}


async def establish_precondition(scenario: str, replicas: int) -> dict:
    context = (await kubectl("config", "current-context")).strip()
    if context != KUBE_CONTEXT:
        raise SystemExit(f"kubectl context is {context!r}, expected {KUBE_CONTEXT!r}; refusing to run")
    if not 1 <= replicas <= len(WORKERS):
        raise SystemExit(f"--replicas must be 1..{len(WORKERS)} (one vLLM pod per worker)")
    if scenario == "node-kill" and replicas >= len(WORKERS):
        raise SystemExit(f"node-kill needs a spare worker for the replacement pod: use --replicas <= {len(WORKERS) - 1}")
    hpas = (await kubectl_json("get", "hpa", "-n", NAMESPACE))["items"]
    owners = [h["metadata"]["name"] for h in hpas if h.get("spec", {}).get("scaleTargetRef", {}).get("name") == DEPLOYMENT]
    if owners:
        raise SystemExit(f"HPA {owners} owns deployment/{DEPLOYMENT}'s replica count; delete it for this test")
    print(f"precondition: scaling deployment/{DEPLOYMENT} to {replicas} and waiting for {replicas} Ready pods on distinct workers")
    await scale(DEPLOYMENT, replicas)
    deadline = time.monotonic() + SETUP_TIMEOUT_S
    last = None
    while True:
        nodes, pods, slices = await asyncio.gather(
            kubectl_json("get", "nodes"),
            list_pods(SELECTOR),
            kubectl_json("get", "endpointslices", "-n", NAMESPACE, "-l", f"kubernetes.io/service-name={SERVICE}"),
        )
        problem = precondition_problem(nodes["items"], pods, slices["items"], scenario, replicas)
        if problem is None:
            break
        if problem != last:
            print(f"  waiting: {problem}")
            last = problem
        if time.monotonic() > deadline:
            raise SystemExit(f"precondition not reached after {SETUP_TIMEOUT_S}s: {problem}")
        await asyncio.sleep(2)
    victim = choose_victim(pods)
    pod_nodes = {p["metadata"]["name"]: p["spec"]["nodeName"] for p in pods}
    spare = [w for w in WORKERS if w not in pod_nodes.values()]
    print(f"  ok: pods {pod_nodes}; victim {victim['pod']} on {victim['node']}; spare {spare or 'none'}")
    return {"victim": victim, "spare_nodes": spare, "pods_before": pod_nodes}


async def crictl_target(node: str, pod: str) -> tuple[str, int]:
    """Container id and PID (in the node's PID namespace) of the pod's vLLM container."""
    assert_faultable(node)
    rc, out, err = await docker(
        "exec", node, "crictl", "ps", "-q", "--name", f"^{CONTAINER}$",
        "--label", f"io.kubernetes.pod.name={pod}", "--label", f"io.kubernetes.pod.namespace={NAMESPACE}",
    )
    ids = out.split()
    if rc != 0 or len(ids) != 1:
        raise RuntimeError(f"crictl ps on {node} for {pod}: rc={rc} ids={ids} {err.strip()}")
    rc, out, err = await docker("exec", node, "crictl", "inspect", ids[0])
    if rc != 0:
        raise RuntimeError(f"crictl inspect {ids[0]} on {node}: {err.strip()}")
    return ids[0], int(json.loads(out)["info"]["pid"])


async def crash_check(victim: dict, method: str) -> dict:
    """Evidence (and a pre-flight check) of exactly what the crash will kill."""
    node = victim["node"]
    assert_faultable(node)
    if method == "pkill":
        rc, out, err = await docker("exec", node, "pgrep", "-af", CRASH_PATTERN)
        lines = [line for line in out.splitlines() if line.strip()]
        if rc != 0 or not lines:
            raise SystemExit(
                f"`pgrep -af '{CRASH_PATTERN}'` on {node} matched nothing (rc={rc}, {err.strip()!r}); the crash would be a "
                "no-op. Check `docker exec <node> ps -eo pid,args`, or use --crash-method crictl"
            )
        return {"method": "pkill", "matched_processes": lines}
    cid, pid = await crictl_target(node, victim["pod"])
    return {"method": "crictl", "container_id": cid, "pid": pid}


async def inject_fault(scenario: str, victim: dict, crash_method: str) -> dict:
    node = victim["node"]
    assert_faultable(node)
    evidence = None
    if scenario == "node-kill":
        cmd = ["docker", "kill", node]
    elif crash_method == "pkill":
        evidence = await crash_check(victim, "pkill")
        cmd = ["docker", "exec", node, "pkill", "-9", "-f", CRASH_PATTERN]
    else:
        evidence = await crash_check(victim, "crictl")
        cmd = ["docker", "exec", node, "sh", "-c", f"kill -9 {int(evidence['pid'])}"]
    t = time.time()
    rc, out, err = await run_cmd(*cmd, timeout=60)
    result = {
        "t_attempted": t,
        "command": shlex.join(cmd),
        "returncode": rc,
        "output": (out + err).strip()[:2000],
        "cmd_duration_s": round(time.time() - t, 3),
        "ok": rc == 0,
    }
    if evidence is not None:
        result["evidence"] = evidence
    return result


async def node_container_state(node: str) -> str:
    rc, out, err = await docker("inspect", "-f", "{{.State.Status}} restarts={{.RestartCount}}", node)
    return out.strip() if rc == 0 else f"inspect failed: {err.strip()}"


async def smoke_request(url: str, timeout_s: float) -> None:
    async with httpx.AsyncClient(base_url=url, timeout=timeout_s) as client:
        r = await _send(client, 0, 8)
    if not r.ok:
        raise SystemExit(f"smoke request to {url} failed ({r.status}: {r.error}); is the Service reachable?")


async def sleep_unless_done(task: asyncio.Future, seconds: float) -> bool:
    """Sleep up to `seconds`; True if `task` finished meanwhile."""
    done, _ = await asyncio.wait({task}, timeout=seconds)
    return bool(done)


async def restore(run: dict, watcher: ClusterWatcher, node_killed: bool) -> dict:
    res: dict = {}
    node = run["victim"]["node"]
    if node_killed:
        res["node_container_before_restore"] = await node_container_state(node)
        print(f"restore: docker start {node}")
        res["t_docker_start"] = time.time()
        rc, out, err = await docker("start", node)
        res["docker_start"] = {"returncode": rc, "output": (out + err).strip()}
        if PIN_SCRIPT.exists():
            rc, out, err = await run_cmd(str(PIN_SCRIPT), timeout=180)
            res["pin_node_cpus"] = {"returncode": rc, "output": (out + err).strip()}
        else:
            res["pin_node_cpus"] = {"returncode": None, "output": f"{PIN_SCRIPT} not found"}
        print(f"restore: waiting for {node} to be Ready")
        res["t_node_ready_again"] = await watcher.wait_for(
            lambda s: s["node"].get(node, {}).get("ready") == "True", NODE_READY_TIMEOUT_S
        )
    replicas = int(run["replicas"])
    print(f"restore: waiting for exactly {replicas} Ready vLLM pods")
    res["t_exactly_R_ready"] = await watcher.wait_for(lambda s: exactly_ready(s, replicas), RESTORE_TIMEOUT_S)
    res["restored"] = res["t_exactly_R_ready"] is not None and (not node_killed or res["t_node_ready_again"] is not None)
    if not res["restored"]:
        print("restore: NOT complete -- check the cluster by hand"
              + (f" (docker start {node}; {PIN_SCRIPT.relative_to(ROOT)})" if node_killed else ""))
    return res


async def collect_events(path: Path) -> None:
    commands = [
        ("get", "events", "-n", NAMESPACE, "--sort-by=.lastTimestamp"),
        ("get", "events", "-A", "--field-selector", "involvedObject.kind=Node", "--sort-by=.lastTimestamp"),
        ("get", "nodes", "-o", "wide"),
        ("get", "pods", "-n", NAMESPACE, "-o", "wide"),
    ]
    parts = []
    for args in commands:
        try:
            text = await kubectl(*args)
        except RuntimeError as exc:
            text = f"(failed: {exc})\n"
        parts.append(f"$ kubectl {shlex.join(args)}\n{text}")
    path.write_text("\n".join(parts))


def default_out(scenario: str, retry: bool) -> Path:
    return ROOT / "multinode" / "results" / "failure" / (scenario + ("-retry" if retry else ""))


async def run_test(args: argparse.Namespace) -> Path:
    out = Path(args.out) if args.out else default_out(args.scenario, args.retry)
    out.mkdir(parents=True, exist_ok=True)
    log_dir = out / "pod_logs"
    if log_dir.exists():
        for old in log_dir.iterdir():
            old.unlink()
    for name in ("summary.json", "summary.md", "requests.csv", "attempts.csv", "timeline.csv", "events.txt", "failure_timeline.png"):
        (out / name).unlink(missing_ok=True)

    run: dict = {
        "scenario": args.scenario,
        "retry": bool(args.retry),
        "replicas": args.replicas,
        "concurrency": args.concurrency,
        "timeout_s": args.timeout,
        "max_tokens": args.max_tokens,
        "url": args.url,
        "pre_s": args.pre_s,
        "post_s": args.post_s,
        "recovery_cap_s": args.recovery_cap_s,
        "crash_method": args.crash_method if args.scenario == "process-crash" else None,
        "started_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    run.update(await establish_precondition(args.scenario, args.replicas))
    if args.scenario == "process-crash":
        run["crash_check"] = await crash_check(run["victim"], args.crash_method)
        print(f"  crash target ({args.crash_method}): {run['crash_check']}")
    await smoke_request(args.url, args.timeout)
    # Keep the smoke request's access-log line out of the --since-time window (seconds
    # resolution, and the node clock may be slightly ahead of the host's).
    await asyncio.sleep(5)

    watcher = ClusterWatcher(log_dir)
    await watcher.start()
    run["t_watch_start"] = time.time()
    await asyncio.sleep(2)

    records: list = []
    attempts: list[Attempt] = []
    stop_load = asyncio.Event()
    host_before = hostguard.snapshot()
    run["host_before"] = host_before.to_dict()
    run["load_start"] = time.time()
    if args.retry:
        load = run_load_with_retry(
            args.url, concurrency=args.concurrency, stop_event=stop_load, max_tokens=args.max_tokens,
            timeout_s=args.timeout, on_record=records.append, on_attempt=attempts.append,
        )
    else:
        load = run_load(
            args.url, concurrency=args.concurrency, stop_event=stop_load, max_tokens=args.max_tokens,
            keepalive=False, timeout_s=args.timeout, on_record=records.append,
        )
    load_task = asyncio.ensure_future(load)
    print(f"load: {args.concurrency} closed-loop clients, timeout {args.timeout:.0f}s, retry={'once' if args.retry else 'no'}")
    run["t_fault"] = None
    node_killed = False
    try:
        if await sleep_unless_done(load_task, args.pre_s):
            run["aborted"] = "the load ended before the fault was injected"
        else:
            run["pre_fault_pods"] = sorted(n for n, p in watcher.state["pod"].items() if p.get("exists") == "yes")
            victim_now = watcher.state["pod"].get(run["victim"]["pod"], {})
            run["victim_restarts_before"] = int(victim_now.get("restarts") or 0)
            print(f"fault: {args.scenario} on {run['victim']['node']} (pod {run['victim']['pod']})")
            node_killed = args.scenario == "node-kill"
            fault = await inject_fault(args.scenario, run["victim"], args.crash_method)
            run["fault"] = fault
            if not fault["ok"]:
                run["aborted"] = f"fault command failed (rc={fault['returncode']}): {fault['output'][:300]}"
                print(f"  {run['aborted']}")
            else:
                run["t_fault"] = fault["t_attempted"]
                if node_killed:
                    await asyncio.sleep(5)
                    run["fault"]["node_container_5s_after"] = await node_container_state(run["victim"]["node"])
                ctx = recovery_context(run)
                print(f"waiting for recovery (cap {args.recovery_cap_s:.0f}s)")
                t_rec = await watcher.wait_for(lambda s: is_recovered(s, ctx), args.recovery_cap_s, abort=load_task)
                run["t_recovered_live"] = t_rec
                if t_rec is None:  # cap reached (or the load died): end the run, the summary says so
                    print(f"  NOT recovered within {args.recovery_cap_s:.0f}s; ending the run")
                else:
                    print(f"  recovered after {t_rec - run['t_fault']:.1f}s; {args.post_s:.0f}s more of load")
                    await sleep_unless_done(load_task, args.post_s)
    finally:
        stop_load.set()
        run["t_stop_load"] = time.time()
        print("load: stopping, waiting for in-flight requests")
        try:
            await load_task
        except HostSleptError as exc:
            run["host_slept"] = str(exc)
            print(f"INVALID: {exc}")
        except Exception as exc:
            run["load_error"] = f"{type(exc).__name__}: {exc}"
        run["load_end"] = max((r.end for r in records), default=time.time())
        host_after = hostguard.snapshot()
        run["host_after"] = host_after.to_dict()
        run["host_compare"] = hostguard.compare(host_before, host_after)
        try:
            restored = await restore(run, watcher, node_killed)
        except Exception as exc:
            restored = {"restored": False, "error": f"{type(exc).__name__}: {exc}"}
            print(f"restore failed: {exc}")
        run["restore"] = restored
        if args.scenario == "process-crash" and run.get("t_fault"):
            try:
                text = await kubectl("logs", "--previous", "--timestamps", "-n", NAMESPACE, "-c", CONTAINER, run["victim"]["pod"])
            except RuntimeError as exc:
                text = f"(failed: {exc})\n"
            (log_dir / f"{run['victim']['pod']}.previous.txt").write_text(text)
        await watcher.stop()
        run["watcher_errors"] = len(watcher.errors)
        run["watcher_error_samples"] = watcher.errors[:20]
        await collect_events(out / "events.txt")
        save_raw(out, run, records, attempts if args.retry else None, watcher.transitions)
    return out


# --------------------------------------------------------------------------------------
# Saved files
# --------------------------------------------------------------------------------------


TIMELINE_FIELDS = ["t", "t_rel_fault_s", "t_prev_poll", "kind", "name", "field", "old", "new"]


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def save_raw(out: Path, run: dict, records: list, attempts: list[Attempt] | None, transitions: list[dict]) -> None:
    record_cls = RetryRecord if run["retry"] else RequestRecord
    _write_csv(out / "requests.csv", records_to_dicts(records), [f.name for f in fields(record_cls)])
    if attempts is not None:
        _write_csv(out / "attempts.csv", [asdict(a) for a in attempts], [f.name for f in fields(Attempt)])
    tf = run.get("t_fault")
    rows = [{**tr, "t_rel_fault_s": round(tr["t"] - tf, 2) if tf else ""} for tr in transitions]
    _write_csv(out / "timeline.csv", rows, TIMELINE_FIELDS)
    (out / "summary.json").write_text(json.dumps({"run": run}, indent=2))
    reanalyse(out)


@dataclass
class Saved:
    run: dict
    requests: list
    attempts: list[Attempt]
    transitions: list[dict]
    log_dir: Path


def _opt_int(s: str | None) -> int | None:
    return int(s) if s not in (None, "", "None") else None


def _opt_float(s: str | None) -> float | None:
    return float(s) if s not in (None, "", "None") else None


def _opt_str(s: str | None) -> str | None:
    return s if s not in (None, "") else None


def _record_kwargs(row: dict) -> dict:
    return dict(
        index=int(row["index"]), start=float(row["start"]), end=float(row["end"]), latency_s=float(row["latency_s"]),
        ok=row["ok"] == "True", status=_opt_int(row["status"]), error=_opt_str(row["error"]),
        prompt_tokens=int(row["prompt_tokens"]), completion_tokens=int(row["completion_tokens"]),
    )


def load_saved(out: Path) -> Saved:
    run = json.loads((out / "summary.json").read_text())["run"]
    with open(out / "requests.csv") as f:
        rows = list(csv.DictReader(f))
    if run["retry"]:
        requests = [
            RetryRecord(
                **_record_kwargs(r), attempts=int(r["attempts"]), first_attempt_ok=r["first_attempt_ok"] == "True",
                first_attempt_error=_opt_str(r["first_attempt_error"]), outcomes=r["outcomes"],
            )
            for r in rows
        ]
        with open(out / "attempts.csv") as f:
            attempts = [
                Attempt(
                    request_index=int(a["request_index"]), attempt=int(a["attempt"]), start=float(a["start"]),
                    end=float(a["end"]), latency_s=float(a["latency_s"]), ok=a["ok"] == "True",
                    status=_opt_int(a["status"]), error=_opt_str(a["error"]), prompt_tokens=int(a["prompt_tokens"]),
                    completion_tokens=int(a["completion_tokens"]), retryable=a["retryable"] == "True",
                )
                for a in csv.DictReader(f)
            ]
    else:
        requests = [RequestRecord(**_record_kwargs(r)) for r in rows]
        attempts = [Attempt.from_record(r) for r in requests]
    with open(out / "timeline.csv") as f:
        transitions = [
            {"t": float(r["t"]), "t_prev_poll": _opt_float(r["t_prev_poll"]), "kind": r["kind"], "name": r["name"],
             "field": r["field"], "old": r["old"], "new": r["new"]}
            for r in csv.DictReader(f)
        ]
    return Saved(run, requests, attempts, transitions, out / "pod_logs")


# --------------------------------------------------------------------------------------
# Analysis (only from saved files)
# --------------------------------------------------------------------------------------


class Timeline:
    """Replays recorded transitions; the state between two polls is the last one seen."""

    def __init__(self, transitions: list[dict]):
        self.transitions = sorted(transitions, key=lambda tr: tr["t"])

    def snapshots(self):
        """Yield (t, state) after applying each poll's changes (the same dict, mutated)."""
        state = empty_state()
        trs, i = self.transitions, 0
        while i < len(trs):
            t = trs[i]["t"]
            while i < len(trs) and trs[i]["t"] == t:
                apply_transition(state, trs[i])
                i += 1
            yield t, state

    def first(self, predicate: Callable[[dict], bool], after: float | None) -> float | None:
        if after is None:
            return None
        return next((t for t, s in self.snapshots() if t >= after and predicate(s)), None)

    def pod_nodes(self) -> dict[str, str]:
        nodes = {}
        for tr in self.transitions:
            if tr["kind"] == "pod" and tr["field"] == "node" and tr["new"]:
                nodes[tr["name"]] = tr["new"]
        return nodes


def key_times(tl: Timeline, run: dict) -> tuple[dict[str, float | None], dict]:
    """Absolute host times of the key events after the fault, and replacement-pod details."""
    tf = run["t_fault"]
    ctx = recovery_context(run)
    vp, vn, replicas = ctx["victim_pod"], ctx["victim_node"], ctx["replicas"]
    victim = lambda s: s["pod"].get(vp, {})  # noqa: E731
    k: dict[str, float | None] = {"fault": tf}
    if ctx["scenario"] == "node-kill":
        k["node_not_ready"] = tl.first(lambda s: s["node"].get(vn, {}).get("ready", "True") != "True", tf)
        k["node_unreachable_taint"] = tl.first(lambda s: "node.kubernetes.io/unreachable" in s["node"].get(vn, {}).get("taints", ""), tf)
    k["victim_pod_not_ready"] = tl.first(lambda s: victim(s).get("ready") != "True", tf)
    k["victim_endpoint_removed"] = tl.first(lambda s: endpoint_state(s, vp) != "ready", tf)
    k["ready_endpoints_below_R"] = tl.first(lambda s: ready_endpoints(s) < replicas, tf)
    k["victim_pod_deletion"] = tl.first(lambda s: bool(victim(s).get("deleting")), tf)
    if ctx["scenario"] == "process-crash":
        k["container_restart_observed"] = tl.first(lambda s: int(victim(s).get("restarts") or 0) > ctx["victim_restarts_before"], tf)
        k["victim_ready_again"] = tl.first(lambda s: victim(s).get("ready") == "True", k["victim_pod_not_ready"])
    replacement: dict = {}
    created = next(
        (tr for tr in tl.transitions
         if tr["kind"] == "pod" and tr["field"] == "exists" and tr["new"] == "yes" and tr["t"] >= tf and tr["name"] not in ctx["pre_fault_pods"]),
        None,
    )
    if created is not None:
        name = created["name"]
        pod = lambda s: s["pod"].get(name, {})  # noqa: E731
        k["replacement_created"] = created["t"]
        k["replacement_scheduled"] = tl.first(lambda s: bool(pod(s).get("node")), created["t"])
        k["replacement_running"] = tl.first(lambda s: pod(s).get("container", "").startswith("running"), created["t"])
        k["replacement_ready"] = tl.first(lambda s: pod(s).get("ready") == "True", created["t"])
        replacement = {"pod": name, "node": tl.pod_nodes().get(name, "")}
    k["ready_endpoints_back_to_R"] = tl.first(lambda s: ready_endpoints(s) >= replicas, k["ready_endpoints_below_R"])
    k["recovered"] = tl.first(lambda s: is_recovered(s, ctx), tf)
    k["load_stopped"] = run.get("t_stop_load")
    docker_start = (run.get("restore") or {}).get("t_docker_start")
    if docker_start:
        k["docker_start"] = docker_start
        k["node_ready_again"] = tl.first(lambda s: s["node"].get(vn, {}).get("ready") == "True", docker_start)
    k["victim_pod_gone"] = tl.first(lambda s: victim(s).get("exists") == "gone", tf)
    return k, replacement


def classify(r, tf: float | None) -> str:
    if r.ok:
        return "ok"
    if tf is None or r.end <= tf:
        return "failed_before_fault"
    if r.start < tf:
        return "failed_in_flight"
    return "failed_after_fault"


def phase_windows(k: dict, run: dict) -> list[tuple[str, float, float]]:
    start, stop = run["load_start"], run["t_stop_load"]
    tf = k.get("fault")
    if tf is None:
        return [("whole_run", start, stop)]
    te, tr = k.get("victim_endpoint_removed"), k.get("recovered")
    clip = lambda t: min(max(t, tf), stop)  # noqa: E731
    detection_end = clip(te if te is not None else (tr if tr is not None else stop))
    degraded_end = clip(tr if tr is not None else stop)
    phases = [("pre_fault", start, tf), ("detection", tf, detection_end)]
    if te is not None:
        phases.append(("degraded", detection_end, degraded_end))
    if tr is not None:
        phases.append(("post_recovery", degraded_end, stop))
    return phases


def _r(x: float | None, digits: int = 1) -> float | None:
    return None if x is None else round(x, digits)


def served_per_pod(log_dir: Path) -> dict[str, dict[str, int]]:
    """HTTP status counts of /v1/chat/completions in each pod's access log (all its containers)."""
    counts: dict[str, dict[str, int]] = {}
    for log in sorted(log_dir.glob("*.log")):
        pod = log.name.split(".")[0]  # <pod>.log, <pod>.restart<N>.log
        codes = counts.setdefault(pod, {})
        for match in ACCESS_LINE.finditer(log.read_text(errors="replace")):
            codes[match.group(1)] = codes.get(match.group(1), 0) + 1
    return counts


def analyse(saved: Saved) -> dict:
    run, reqs, atts = saved.run, saved.requests, saved.attempts
    tl = Timeline(saved.transitions)
    tf = run.get("t_fault")
    t0 = tf if tf is not None else run["load_start"]
    rel = lambda t: None if t is None else round(t - t0, 1)  # noqa: E731
    k, replacement = key_times(tl, run) if tf is not None else ({"load_stopped": run.get("t_stop_load")}, {})

    classes = {name: 0 for name in CLASS_LABELS}
    categories: dict[str, dict[str, int]] = {}
    failed = []
    for r in reqs:
        c = classify(r, tf)
        classes[c] += 1
        if c != "ok":
            cat = error_category(r.status, r.error)
            categories.setdefault(c, {})
            categories[c][cat] = categories[c].get(cat, 0) + 1
            failed.append({
                "index": r.index, "class": c, "category": cat, "start_s": rel(r.start), "end_s": rel(r.end),
                "latency_s": round(r.latency_s, 2), "error": (r.error or "")[:200],
                **({"outcomes": r.outcomes} if run["retry"] else {}),
            })
    latency_by_category: dict[str, dict] = {}
    for a in atts:
        if not a.ok:
            latency_by_category.setdefault(error_category(a.status, a.error), []).append(a.latency_s)
    latency_by_category = {
        cat: {"attempts": len(v), "median_s": _r(percentile(v, 50), 2), "max_s": _r(max(v), 2)}
        for cat, v in sorted(latency_by_category.items())
    }

    phases = []
    windows_def = phase_windows(k, run)
    for i, (name, a, b) in enumerate(windows_def):
        last = i == len(windows_def) - 1
        started = [r for r in reqs if a <= r.start and (last or r.start < b)]
        ok = [r for r in started if r.ok]
        tps = tokens_per_s(atts, a, b)
        lat = [r.latency_s for r in ok]
        phases.append({
            "phase": name, "from_s": rel(a), "to_s": rel(b), "duration_s": round(b - a, 1),
            "started": len(started), "succeeded": len(ok), "failed": len(started) - len(ok),
            "failed_in_flight_at_fault": sum(classify(r, tf) == "failed_in_flight" for r in started),
            "success_rate": _r(len(ok) / len(started), 4) if started else None,
            "output_tokens_per_s": _r(tps), "latency_p50_s": _r(percentile(lat, 50), 2),
            "latency_p95_s": _r(percentile(lat, 95), 2),
        })

    windows = []
    stop = run["t_stop_load"]
    w = t0 + math.ceil((run["load_start"] - t0) / WINDOW_S) * WINDOW_S
    while w + WINDOW_S <= stop + 1e-6:
        finished = [r for r in reqs if w <= r.end < w + WINDOW_S]
        firsts = [a for a in atts if a.attempt == 1 and w <= a.end < w + WINDOW_S]
        windows.append({
            "from_s": round(w - t0, 1), "finished": len(finished), "succeeded": sum(r.ok for r in finished),
            "first_attempts_finished": len(firsts), "first_attempts_succeeded": sum(a.ok for a in firsts),
            "tokens_per_s": _r(tokens_per_s(atts, w, w + WINDOW_S)),
        })
        w += WINDOW_S

    served = served_per_pod(saved.log_dir) if saved.log_dir.exists() else {}
    pod_nodes = tl.pod_nodes()
    per_node: dict[str, int] = {}
    for pod, codes in served.items():
        node = pod_nodes.get(pod, "?")
        per_node[node] = per_node.get(node, 0) + codes.get("200", 0)
    logged_200 = sum(c.get("200", 0) for c in served.values())
    client_ok_attempts = sum(a.ok for a in atts)

    retry = None
    if run["retry"]:
        retried = [r for r in reqs if r.attempts > 1]
        masked = [r for r in retried if r.ok]
        succeeded_attempt = {a.request_index: a for a in atts if a.ok}
        extra = [r.latency_s - succeeded_attempt[r.index].latency_s for r in masked if r.index in succeeded_attempt]
        attempt_classes = {name: 0 for name in CLASS_LABELS}
        for a in atts:
            attempt_classes[classify(a, tf)] += 1
        retry_cats: dict[str, int] = {}
        for a in atts:
            if a.attempt == 2 and not a.ok:
                cat = error_category(a.status, a.error)
                retry_cats[cat] = retry_cats.get(cat, 0) + 1
        retry = {
            "total_attempts": len(atts),
            "first_attempt_failures": sum(not r.first_attempt_ok for r in reqs),
            "requests_retried": len(retried),
            "succeeded_on_retry": len(masked),
            "failed_after_retry": len(retried) - len(masked),
            "failed_not_retryable": sum((not r.ok) and r.attempts == 1 for r in reqs),
            "attempts_by_class": attempt_classes,
            "retry_attempt_failure_categories": retry_cats,
            "time_lost_by_masked_requests_s": {
                "median": _r(percentile(extra, 50), 2), "max": _r(max(extra), 2) if extra else None,
            },
        }

    invalid, warnings = [], []
    if run.get("host_slept"):
        invalid.append(f"host slept during the load: {run['host_slept']}")
    if run.get("load_error"):
        invalid.append(f"load generator error: {run['load_error']}")
    if tf is None:
        invalid.append("no fault was injected" + (f" ({run['aborted']})" if run.get("aborted") else ""))
    swapped = (run.get("host_compare") or {}).get("swapped_out_mb")
    if swapped is not None and swapped > hostguard.SWAP_LIMIT_MB:
        warnings.append(f"the host swapped out {swapped:.0f} MiB during the load (limit {hostguard.SWAP_LIMIT_MB:.0f} MiB)")
    if tf is not None and k.get("recovered") is None:
        warnings.append(f"not recovered within the {run['recovery_cap_s']:.0f}s cap")
    if run.get("watcher_errors"):
        warnings.append(f"{run['watcher_errors']} watcher polls failed (see run.watcher_error_samples)")
    restore_info = run.get("restore") or {}
    if restore_info and not restore_info.get("restored"):
        warnings.append("the cluster was not fully restored after the test")
    after5 = (run.get("fault") or {}).get("node_container_5s_after", "")
    if after5 and not after5.startswith("exited"):
        warnings.append(f"victim node container 5 s after docker kill: {after5!r} (expected exited)")
    if served and logged_200 != client_ok_attempts:
        warnings.append(
            f"access logs show {logged_200} HTTP 200s but the client saw {client_ok_attempts} successful attempts "
            "(lines lost with the killed node's log stream, or responses lost in transit)"
        )
    lanes = max(client_lanes([{"start": a.start, "end": a.end} for a in atts]), default=-1) + 1
    if lanes > run["concurrency"]:
        warnings.append(f"the chart's lane reconstruction needed {lanes} lanes for {run['concurrency']} clients")
    matched = ((run.get("fault") or {}).get("evidence") or {}).get("matched_processes") or []
    if len(matched) > 1:
        warnings.append(f"pkill pattern matched {len(matched)} processes: {matched}")

    failed_n = len(reqs) - classes["ok"]
    return {
        "valid": not invalid,
        "invalid_reasons": invalid,
        "warnings": warnings,
        "headline": headline(run, len(reqs), failed_n, retry),
        "recovered": k.get("recovered") is not None,
        "recovered_after_s": rel(k.get("recovered")) if tf is not None else None,
        "key_times_s_after_fault": {key: rel(k.get(key)) for key, _ in KEY_LABELS if key in k},
        "replacement_pod": replacement,
        "victim_last_terminated": tl_last_value(tl, "pod", run["victim"]["pod"], "last_terminated") if run.get("victim") else None,
        "requests": {
            "total": len(reqs),
            "succeeded": classes["ok"],
            "failed": failed_n,
            "by_class": classes,
            "error_categories_by_class": categories,
            "failed_attempt_latency_by_category": latency_by_category,
            "failures_first_end_s": min((f["end_s"] for f in failed), default=None),
            "failures_last_end_s": max((f["end_s"] for f in failed), default=None),
            "overall": summarize(reqs),
            "failed_requests": failed,
        },
        "retry": retry,
        "phases": phases,
        "windows": windows,
        "served_per_pod_from_access_logs": served,
        "served_per_node_from_access_logs": per_node,
        "pod_nodes": pod_nodes,
        "access_log_200_total": logged_200,
        "client_successful_attempts": client_ok_attempts,
        "host": run.get("host_compare"),
    }


def tl_last_value(tl: Timeline, kind: str, name: str, field: str) -> str | None:
    values = [tr["new"] for tr in tl.transitions if tr["kind"] == kind and tr["name"] == name and tr["field"] == field and tr["new"]]
    return values[-1] if values else None


def headline(run: dict, total: int, failed: int, retry: dict | None) -> str:
    text = f"{failed} of {total} requests failed"
    if retry is not None:
        text += f" after one client retry ({retry['requests_retried']} retried, {retry['succeeded_on_retry']} of them succeeded)"
    return text


def reanalyse(out: Path) -> dict:
    saved = load_saved(out)
    an = analyse(saved)
    (out / "summary.json").write_text(json.dumps({"run": saved.run, **an}, indent=2))
    (out / "summary.md").write_text(summary_markdown(saved.run, an))
    chart(saved, an, out / "failure_timeline.png")
    return an


# --------------------------------------------------------------------------------------
# Human-readable outputs
# --------------------------------------------------------------------------------------


def _fmt(x, digits: int = 1, unit: str = "") -> str:
    if x is None:
        return "–"
    if isinstance(x, float):
        return f"{x:.{digits}f}{unit}"
    return f"{x}{unit}"


def _pct(x: float | None) -> str:
    return "–" if x is None else f"{100 * x:.1f}%"


def scenario_title(run: dict) -> str:
    node = run["victim"]["node"]
    if run["scenario"] == "node-kill":
        return f"Worker node killed under load (docker kill {node})"
    return f"vLLM process killed under load (SIGKILL in one pod on {node.replace(CLUSTER + '-', '')})"


def summary_markdown(run: dict, an: dict) -> str:
    req = an["requests"]
    lines = [f"# Failure test: {run['scenario']}{' with one client retry' if run['retry'] else ' (no client retry)'}", ""]
    lines.append(
        f"Run started {run.get('started_at', '?')}. {run['replicas']} vLLM replicas on {len(WORKERS)} worker nodes, "
        f"{run['concurrency']} closed-loop clients (new connection per request), {run['timeout_s']:.0f} s client timeout, "
        f"{run['max_tokens']} output tokens per request."
    )
    fault = run.get("fault") or {}
    if fault:
        lines.append(f"Fault: `{fault.get('command')}` (victim pod `{run['victim']['pod']}` on `{run['victim']['node']}`), "
                     f"{run['pre_s']:.0f} s after the load started; t = 0 below.")
    lines.append("")
    rec = f"back to {run['replicas']} ready endpoints {an['recovered_after_s']:.1f} s after the fault" if an["recovered"] else (
        f"NOT recovered within {run['recovery_cap_s']:.0f} s" if run.get("t_fault") else "no fault injected")
    lines.append(f"**Result: {an['headline']}; {rec}.**")
    lines.append("")
    lines.append(f"Valid: {'yes' if an['valid'] else 'NO -- ' + '; '.join(an['invalid_reasons'])}. "
                 f"Host swap during the load: {_fmt((an.get('host') or {}).get('swapped_out_mb'))} MiB "
                 f"(limit {hostguard.SWAP_LIMIT_MB:.0f}).")
    for w in an["warnings"]:
        lines.append(f"- Warning: {w}")
    lines += ["", "## Key times", "",
              "Seconds after the fault, as first seen by the 1 s watcher (an upper bound: the change happened "
              "after the previous poll; `t_prev_poll` in timeline.csv).", "", "| Event | s |", "|---|---:|"]
    for key, label in KEY_LABELS:
        if key in an["key_times_s_after_fault"]:
            lines.append(f"| {label} | {_fmt(an['key_times_s_after_fault'][key])} |")
    if an["replacement_pod"]:
        lines.append(f"\nReplacement pod: `{an['replacement_pod']['pod']}` on `{an['replacement_pod']['node']}`.")
    if an.get("victim_last_terminated") and run["scenario"] == "process-crash":
        lines.append(f"\nVictim container's last termination (kubelet): `{an['victim_last_terminated']}`.")
    lines += ["", "## Requests", "", "| Outcome | Requests |", "|---|---:|"]
    for c, label in CLASS_LABELS.items():
        lines.append(f"| {label} | {req['by_class'][c]} |")
    lines.append(f"| **total** | **{req['total']}** |")
    if req["error_categories_by_class"]:
        lines += ["", "| Failure class | Error | Requests |", "|---|---|---:|"]
        for c, cats in req["error_categories_by_class"].items():
            for cat, n in sorted(cats.items(), key=lambda kv: -kv[1]):
                lines.append(f"| {CLASS_LABELS[c]} | {cat} | {n} |")
        lines += ["", "Failed attempts by error (time from send to error):", "", "| Error | Attempts | Median s | Max s |", "|---|---:|---:|---:|"]
        for cat, v in req["failed_attempt_latency_by_category"].items():
            lines.append(f"| {cat} | {v['attempts']} | {_fmt(v['median_s'], 2)} | {_fmt(v['max_s'], 2)} |")
        first, last = _fmt(req["failures_first_end_s"]), _fmt(req["failures_last_end_s"])
        when = f"{first} s" if first == last else f"between {first} s and {last} s"
        lines.append(f"\nThe failed requests' errors reached the client {when} after the fault.")
    if an["retry"]:
        r = an["retry"]
        lines += ["", "## Client retry", "", "| | |", "|---|---:|",
                  f"| attempts sent | {r['total_attempts']} |",
                  f"| requests whose first attempt failed | {r['first_attempt_failures']} |",
                  f"| retried | {r['requests_retried']} |",
                  f"| succeeded on the retry (masked) | {r['succeeded_on_retry']} |",
                  f"| failed again on the retry | {r['failed_after_retry']} |",
                  f"| failed, not retryable | {r['failed_not_retryable']} |",
                  f"| time lost by masked requests, median / max (s) | {_fmt(r['time_lost_by_masked_requests_s']['median'], 2)} / "
                  f"{_fmt(r['time_lost_by_masked_requests_s']['max'], 2)} |"]
    lines += ["", "## Phases", "", "Requests are assigned to the phase in which they were sent, so a request sent before "
              "the fault that was still running when it hit counts as a pre-fault failure (marked \"in flight\"). Output "
              "tokens/s spreads each successful attempt's tokens evenly over its lifetime.", "",
              "| Phase | s after fault | Sent | Succeeded | Failed | Success rate | Output tok/s | p50 / p95 latency of successes (s) |",
              "|---|---|---:|---:|---:|---:|---:|---:|"]
    for p in an["phases"]:
        lines.append(
            f"| {PHASE_LABELS[p['phase']]} | {_fmt(p['from_s'])} → {_fmt(p['to_s'])} | {p['started']} | {p['succeeded']} | "
            f"{p['failed']}{f' ({n} in flight)' if (n := p['failed_in_flight_at_fault']) else ''} | "
            f"{_pct(p['success_rate'])} | {_fmt(p['output_tokens_per_s'])} | "
            f"{_fmt(p['latency_p50_s'], 2)} / {_fmt(p['latency_p95_s'], 2)} |"
        )
    lines += ["", "## Served per pod (access logs)", "", "| Pod | Node | HTTP 200 | other |", "|---|---|---:|---:|"]
    for pod, codes in an["served_per_pod_from_access_logs"].items():
        other = ", ".join(f"{c}: {n}" for c, n in codes.items() if c != "200") or "–"
        lines.append(f"| `{pod}` | {an['pod_nodes'].get(pod, '?')} | {codes.get('200', 0)} | {other} |")
    lines.append("")
    lines.append(" · ".join(f"`{n}`: {c}" for n, c in sorted(an["served_per_node_from_access_logs"].items())) or "(no access logs)")
    lines.append(f"\nHTTP 200 lines in the access logs: {an['access_log_200_total']}; successful attempts seen by the client: "
                 f"{an['client_successful_attempts']}.")
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------------------
# Chart
# --------------------------------------------------------------------------------------


def _pod_status(p: dict, was_ready: bool) -> str | None:
    if p.get("exists") != "yes" or not p.get("node"):
        return None
    if p.get("deleting"):
        return "terminating"
    if p.get("ready") == "True":
        return "ready"
    return "not-ready" if was_ready else "starting"


def segments(tl: Timeline, t_end: float) -> tuple[dict[str, list], dict[str, list]]:
    """Per pod and per node: [(t_from, t_to, status)] from the replayed timeline."""
    pod_segs: dict[str, list] = {}
    node_segs: dict[str, list] = {}
    open_pod: dict[str, tuple[float, str | None]] = {}
    open_node: dict[str, tuple[float, str]] = {}
    was_ready: set[str] = set()
    for t, s in tl.snapshots():
        for name, p in s["pod"].items():
            status = _pod_status(p, name in was_ready)
            if status == "ready":
                was_ready.add(name)
            prev = open_pod.get(name)
            if prev is None or prev[1] != status:
                if prev is not None and prev[1] is not None:
                    pod_segs.setdefault(name, []).append((prev[0], t, prev[1]))
                open_pod[name] = (t, status)
        for name, n in s["node"].items():
            status = "Ready" if n.get("ready") == "True" else "NotReady"
            prev = open_node.get(name)
            if prev is None or prev[1] != status:
                if prev is not None:
                    node_segs.setdefault(name, []).append((prev[0], t, prev[1]))
                open_node[name] = (t, status)
    for name, (t, status) in open_pod.items():
        if status is not None:
            pod_segs.setdefault(name, []).append((t, max(t, t_end), status))
    for name, (t, status) in open_node.items():
        node_segs.setdefault(name, []).append((t, max(t, t_end), status))
    return pod_segs, node_segs


HATCH = {"not-ready": "////", "terminating": "xxxx"}


def _state_bar(ax, y, t0, t1, color, status, height=0.46) -> None:
    if t1 <= t0:
        return
    if status == "ready":
        ax.barh(y, t1 - t0, left=t0, height=height, color=color, linewidth=0)
    else:
        ax.barh(y, t1 - t0, left=t0, height=height, color="none", edgecolor=color, linewidth=1.3, hatch=HATCH.get(status))


def _markers(run: dict, an: dict) -> list[tuple[str, float]]:
    k = an["key_times_s_after_fault"]
    if run["scenario"] == "node-kill":
        wanted = [("fault", "docker kill"), ("node_not_ready", "node NotReady"), ("victim_endpoint_removed", "endpoint removed"),
                  ("victim_pod_deletion", "pod evicted"), ("replacement_ready", "replacement Ready")]
    else:
        wanted = [("fault", "SIGKILL vLLM"), ("victim_endpoint_removed", "endpoint removed"),
                  ("container_restart_observed", "container restarted"), ("victim_ready_again", "pod Ready again")]
    out = []
    for key, label in wanted:
        t = k.get(key)
        if t is not None:
            out.append((label if key == "fault" else f"{label} +{t:.0f}s", t))
    return out


def chart(saved: Saved, an: dict, path: Path) -> None:
    run, reqs, atts = saved.run, saved.requests, saved.attempts
    tf = run.get("t_fault")
    t0 = tf if tf is not None else run["load_start"]
    tl = Timeline(saved.transitions)
    load_end = max([run["t_stop_load"], *(a.end for a in atts)])
    x0, x1 = run["load_start"] - t0 - 4, load_end - t0 + 4

    fig, (ax1, ax2, ax3, ax4) = plt.subplots(
        4, 1, figsize=(11, 11.5), sharex=True, gridspec_kw={"height_ratios": [1.35, 2.3, 0.85, 0.85]}
    )

    # --- 1. pods per worker node ------------------------------------------------------
    pod_segs, node_segs = segments(tl, load_end)
    pod_nodes = tl.pod_nodes()
    workers = sorted({*WORKERS, *(n for n in pod_nodes.values() if n != CONTROL_PLANE)})
    victim = run.get("victim") or {}
    spare = set(run.get("spare_nodes") or [])
    pre = set(run.get("pre_fault_pods") or run.get("pods_before", {}).keys())
    n_lanes = len(workers)
    for i, node in enumerate(workers):
        y = n_lanes - 1 - i
        for a, b, status in node_segs.get(node, []):
            color = charts.TEXT_SECONDARY if status == "NotReady" else charts.GRID
            ax1.barh(y - 0.36, b - a, left=a - t0, height=0.1, color=color, linewidth=0)
        for pod, segs in pod_segs.items():
            if pod_nodes.get(pod) != node:
                continue
            color = charts.VLLM if pod in pre else charts.AQUA
            for a, b, status in segs:
                _state_bar(ax1, y, a - t0, b - t0, color, status)
            first = max(segs[0][0] - t0, x0)
            tag = "replacement " if pod not in pre else ("victim " if pod == victim.get("pod") else "")
            ax1.annotate(f"{tag}pod …{pod[-5:]}", (first, y + 0.25), xytext=(2, 3), textcoords="offset points",
                         fontsize=8.5, color=charts.TEXT_PRIMARY, va="bottom")
    labels = []
    for node in workers:
        short = node.replace(f"{CLUSTER}-", "")
        role = " (victim)" if node == victim.get("node") else (" (spare)" if node in spare else "")
        labels.append(short + role)
    ax1.set_yticks(range(n_lanes), list(reversed(labels)))
    ax1.grid(False)
    ax1.set_title("vLLM pods per worker node, as Kubernetes reported them (1 s polling)", pad=24)
    from matplotlib.patches import Patch

    handles = [
        Patch(facecolor="none", edgecolor=charts.VLLM, linewidth=1.3, label="starting"),
        Patch(facecolor=charts.VLLM, label="Ready"),
        Patch(facecolor="none", edgecolor=charts.VLLM, hatch="////", linewidth=1.3, label="NotReady"),
        Patch(facecolor="none", edgecolor=charts.VLLM, hatch="xxxx", linewidth=1.3, label="terminating"),
        Patch(facecolor=charts.AQUA, label="replacement pod"),
        Patch(facecolor=charts.TEXT_SECONDARY, label="node NotReady"),
    ]
    ax1.legend(handles=handles, loc="lower left", bbox_to_anchor=(0, 1.0), ncol=6, fontsize=8.5, handlelength=1.6,
               borderaxespad=0.1, columnspacing=1.2)

    # --- 2. every request on its client lane ------------------------------------------
    lanes = client_lanes([{"start": a.start, "end": a.end} for a in atts])
    for a, lane in zip(atts, lanes):
        color = (charts.VLLM if a.attempt == 1 else charts.AQUA) if a.ok else charts.CRITICAL
        ax2.barh(lane, a.end - a.start, left=a.start - t0, height=0.62, color=color, edgecolor=charts.SURFACE, linewidth=1.2)
        if not a.ok:  # fast failures would otherwise be invisible slivers
            ax2.plot(a.end - t0, lane, marker="x", color=charts.CRITICAL, markersize=6, markeredgewidth=1.8, zorder=4)
    n_client = max(lanes, default=0) + 1
    ax2.set_yticks(range(n_client), [f"client {k + 1}" for k in range(n_client)])
    ax2.set_ylim(-0.6, n_client - 0.4)
    ax2.invert_yaxis()
    ax2.grid(False)
    req = an["requests"]
    if run["retry"]:
        r = an["retry"]
        ax2.set_title(
            f"Every attempt, send → response: {req['total']} requests, {req['failed']} failed after one retry; "
            f"{r['requests_retried']} retried, {r['succeeded_on_retry']} succeeded on the retry (aqua)"
        )
    else:
        ax2.set_title(f"Every request, send → response: {req['succeeded']} succeeded, {req['failed']} failed "
                      f"(red; × = when the client got the error)")

    # --- 3/4. 10 s windows ------------------------------------------------------------
    win = an["windows"]
    xs = [w["from_s"] + WINDOW_S / 2 for w in win]
    rate = [100 * w["succeeded"] / w["finished"] if w["finished"] else float("nan") for w in win]
    charts.line_with_markers(ax3, xs, rate, charts.VLLM, label="requests" + (" (after retry)" if run["retry"] else ""))
    if run["retry"]:
        first = [100 * w["first_attempts_succeeded"] / w["first_attempts_finished"] if w["first_attempts_finished"] else float("nan")
                 for w in win]
        ax3.plot(xs, first, color=charts.TEXT_SECONDARY, linewidth=1.5, marker="o", markersize=5,
                 markeredgecolor=charts.SURFACE, markeredgewidth=1.5, label="first attempts only", zorder=2)
        ax3.legend(loc="lower right", fontsize=8.5)
    ax3.set_ylim(-5, 108)
    ax3.set_yticks([0, 50, 100], ["0%", "50%", "100%"])
    ax3.set_title(f"Success rate of the requests that finished in each {WINDOW_S} s window (gap = none finished)")

    tps = [w["tokens_per_s"] if w["tokens_per_s"] is not None else float("nan") for w in win]
    charts.line_with_markers(ax4, xs, tps, charts.VLLM)
    pre_phase = next((p for p in an["phases"] if p["phase"] == "pre_fault"), None)
    if pre_phase and pre_phase["output_tokens_per_s"]:
        ax4.axhline(pre_phase["output_tokens_per_s"], color=charts.TEXT_SECONDARY, linewidth=1, linestyle="--", zorder=1)
        ax4.annotate(f"pre-fault mean {pre_phase['output_tokens_per_s']:.0f} tok/s", (x0, pre_phase["output_tokens_per_s"]),
                     xytext=(4, -5), textcoords="offset points", fontsize=8.5, color=charts.TEXT_SECONDARY, va="top")
    ax4.set_ylim(bottom=0, top=max([v for v in tps if v == v] + [1]) * 1.25)
    ax4.set_title(f"Output tokens/s ({WINDOW_S} s windows; each request's tokens spread over its lifetime)")
    ax4.set_xlim(x0, x1)
    if tf is not None:
        what = "docker kill" if run["scenario"] == "node-kill" else "SIGKILL of the vLLM process"
        ax4.set_xlabel(f"Seconds relative to the fault (0 = {what} on {victim['node'].replace(CLUSTER + '-', '')})")
    else:
        ax4.set_xlabel("Seconds since the load started (no fault was injected)")

    # --- vertical markers, labels staggered so close events stay readable --------------
    marks = _markers(run, an) if tf is not None else []
    for label, t in marks:
        for ax in (ax1, ax2, ax3, ax4):
            ax.axvline(t, color=charts.TEXT_PRIMARY if t == 0 else charts.TEXT_SECONDARY, linewidth=1, zorder=0)
    char_w = (x1 - x0) * 4.9 / 760  # ~data units per character at 8.5 pt on a ~10.5 in axis
    row_end: list[float] = []
    placed = []
    for label, t in sorted(marks, key=lambda m: m[1]):
        width = (len(label) + 1) * char_w
        row = next((i for i, end in enumerate(row_end) if end < t), len(row_end))
        if row == len(row_end):
            row_end.append(0.0)
        row_end[row] = t + width
        placed.append((label, t, row))
    top = n_lanes - 0.45 + 0.42 * max(len(row_end), 1)
    ax1.set_ylim(-0.6, top)
    for label, t, row in placed:
        ax1.annotate(label, (t, n_lanes - 0.4 + 0.42 * row), xytext=(3, 0), textcoords="offset points", fontsize=8.5,
                     color=charts.TEXT_SECONDARY, va="bottom")

    short = f"{req['failed']} of {req['total']} requests failed" + (" after one client retry" if run["retry"] else "")
    title = f"{scenario_title(run)}: {short}" if victim else short
    if not an["valid"]:
        title = "INVALID RUN -- " + title
    fig.suptitle(title, x=0.0, y=0.995, ha="left", va="top", fontsize=12, fontweight="bold")
    rec = (f"back to {run['replicas']} ready endpoints {an['recovered_after_s']:.0f} s after the fault" if an["recovered"]
           else ("not recovered within the cap" if tf is not None else "no fault injected"))
    sub = (f"{run['replicas']} vLLM replicas on {len(WORKERS)} worker nodes (kind, one laptop) · {run['concurrency']} closed-loop clients · "
           f"{run['timeout_s']:.0f} s client timeout · {'one client retry' if run['retry'] else 'no client retry'} · {rec}")
    fig.text(0.0, 0.973, sub, ha="left", va="top", fontsize=9.5, color=charts.TEXT_SECONDARY, transform=fig.transFigure)
    fig.tight_layout(rect=(0, 0, 1, 0.955))
    charts.save(fig, path)


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--scenario", required=True, choices=["node-kill", "process-crash"])
    p.add_argument("--retry", action="store_true", help="client retries a failed request once (separate run)")
    p.add_argument("--replicas", type=int, default=2)
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--timeout", type=float, default=60.0, help="client timeout per attempt, seconds")
    p.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    p.add_argument("--out", help="output directory (default multinode/results/failure/<scenario>[-retry])")
    p.add_argument("--chart-only", action="store_true", help="re-run the analysis and chart from the saved files")
    p.add_argument("--crash-method", choices=["pkill", "crictl"], default="pkill",
                   help="process-crash: pkill -9 -f 'vllm serve' in the node (default), or kill -9 the PID crictl reports "
                        "for the pod's vllm container")
    p.add_argument("--url", default=URL)
    p.add_argument("--pre-s", type=float, default=PRE_S, help=argparse.SUPPRESS)
    p.add_argument("--post-s", type=float, default=POST_S, help=argparse.SUPPRESS)
    p.add_argument("--recovery-cap-s", type=float, default=RECOVERY_CAP_S, help=argparse.SUPPRESS)
    return p.parse_args(argv)


def print_result(out: Path) -> None:
    summary = json.loads((out / "summary.json").read_text())
    print(f"\n{summary['headline']}; recovered: {summary['recovered']} ({summary['recovered_after_s']} s)")
    if not summary["valid"]:
        print("INVALID:", "; ".join(summary["invalid_reasons"]))
    for w in summary["warnings"]:
        print("warning:", w)
    try:
        print(f"wrote {out.relative_to(ROOT)}/")
    except ValueError:
        print(f"wrote {out}/")


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.chart_only:
        out = Path(args.out) if args.out else default_out(args.scenario, args.retry)
        reanalyse(out)
        print_result(out)
        return
    if platform.system() == "Darwin" and shutil.which("caffeinate"):
        # A closed lid sleeps the Mac and invalidates the run; keep it awake while we live.
        subprocess.Popen(["caffeinate", "-is", "-w", str(os.getpid())])
    out = asyncio.run(run_test(args))
    print_result(out)


if __name__ == "__main__":
    main()
