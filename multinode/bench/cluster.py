"""kubectl-backed helpers for the multi-node scaling benchmark.

A scaling number is only meaningful if the topology behind it is known, so these
helpers assert the cluster state instead of assuming it: exactly R vLLM pods, all
Ready, none terminating, on R distinct worker nodes, and the Service's endpoints
listing exactly those pods, before anything is measured. A violation raises
ClusterStateError, which aborts the sweep (retrying would not fix a wrong topology).

Reads are retried: one sweep issues thousands of kubectl calls (the CPU sampler
alone execs into every pod every 2 s), and one transient API-server error should not
kill an hour-long run. The only write, `kubectl scale`, is idempotent, so it is
retried the same way.

Node CPU pinning is done with `docker update --cpuset-cpus` on the kind node
containers, which Kubernetes cannot see (a node still reports the whole VM's CPUs as
capacity), so it is read from two places: `docker inspect` of each node (best
effort) and, authoritatively, each pod's own cgroup (cpuset.cpus.effective).
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass, field

from bench import kube
from bench.kube import NAMESPACE, pod_is_ready
from bench.podstats import PodStatsSampler
from bench.run_benchmark import _cmd
from multinode.bench import accesslog

READ_ATTEMPTS = 4
READ_RETRY_S = 2.0  # backoff: 2, 4, 6 s
REQUEST_TIMEOUT = "60s"  # kubectl's own per-request timeout, so a hung API server cannot stall the sweep
POLL_S = 3.0
CONTROL_PLANE_ROLE = "control-plane"
CPUSET_FILE = "/sys/fs/cgroup/cpuset.cpus.effective"
FINISHED_PHASES = ("Failed", "Succeeded")


class ClusterStateError(RuntimeError):
    """The cluster is not in the state the benchmark requires; abort, don't retry."""


async def kubectl_retry(*args: str, attempts: int = READ_ATTEMPTS) -> str:
    for attempt in range(1, attempts + 1):
        try:
            return await kube.kubectl(f"--request-timeout={REQUEST_TIMEOUT}", *args)
        except RuntimeError as exc:
            if attempt == attempts or "NotFound" in str(exc):
                raise
            print(f"    kubectl {' '.join(args[:2])} failed (attempt {attempt}/{attempts}), retrying: {exc}", flush=True)
            await asyncio.sleep(READ_RETRY_S * attempt)
    raise AssertionError("unreachable")


async def kubectl_json(*args: str) -> dict:
    return json.loads(await kubectl_retry(*args, "-o", "json"))


async def current_context() -> str:
    return (await kubectl_retry("config", "current-context")).strip()


async def scale(deployment: str, replicas: int) -> None:
    await kubectl_retry("scale", "-n", NAMESPACE, f"deployment/{deployment}", f"--replicas={replicas}")


async def get_pods(selector: str) -> list[dict]:
    return (await kubectl_json("get", "pods", "-n", NAMESPACE, "-l", selector))["items"]


def parse_cpuset(cpuset: str) -> set[int]:
    """cgroup cpuset list syntax ("1-3,7") -> {1, 2, 3, 7}."""
    cpus: set[int] = set()
    for part in cpuset.strip().split(","):
        if "-" in part:
            lo, hi = part.split("-")
            cpus.update(range(int(lo), int(hi) + 1))
        elif part:
            cpus.add(int(part))
    return cpus


def _restarts(pod: dict) -> int:
    return sum(s.get("restartCount", 0) for s in pod.get("status", {}).get("containerStatuses", []))


def _pod_state(pod: dict) -> str:
    """One-line pod status for error messages (why is it not Ready yet?)."""
    name = pod["metadata"]["name"]
    if pod["metadata"].get("deletionTimestamp"):
        return f"{name}: terminating"
    phase = pod.get("status", {}).get("phase", "?")
    node = pod.get("spec", {}).get("nodeName") or "unscheduled"
    why = [
        f"{c['type']}={c['status']}" + (f" ({c.get('reason')}: {c.get('message')})" if c.get("message") else "")
        for c in pod.get("status", {}).get("conditions", [])
        if c["status"] != "True"
    ]
    return f"{name}: {phase} on {node}" + (f", {'; '.join(why)}" if why else "")


@dataclass
class Placement:
    """Which pod runs where, captured once the Deployment has settled at R replicas."""

    nodes: dict[str, str]  # pod -> node
    containers: dict[str, str]  # pod -> container name (for kubectl logs / exec)
    restarts: dict[str, int]
    qos_class: dict[str, str | None]
    cpusets: dict[str, str | None] = field(default_factory=dict)  # pod -> cpuset.cpus.effective

    @property
    def replicas(self) -> int:
        return len(self.nodes)


async def wait_for_replicas(selector: str, replicas: int, timeout_s: float) -> list[dict]:
    """Poll until exactly `replicas` pods exist, none terminating, all Running and Ready,
    all from one ReplicaSet (no rollout in progress)."""
    deadline = time.monotonic() + timeout_s
    next_report = time.monotonic() + 30
    while True:
        # Evicted/completed pods linger as API objects but serve nothing: ignore them.
        pods = [p for p in await get_pods(selector) if p.get("status", {}).get("phase") not in FINISHED_PHASES]
        live = [p for p in pods if not p["metadata"].get("deletionTimestamp")]
        ready = [p for p in live if p.get("status", {}).get("phase") == "Running" and pod_is_ready(p)]
        hashes = {p["metadata"].get("labels", {}).get("pod-template-hash") for p in live}
        if len(pods) == len(live) == len(ready) == replicas and len(hashes) == 1:
            return live
        now = time.monotonic()
        if now >= deadline:
            states = "\n  ".join(_pod_state(p) for p in pods) or "(no pods)"
            raise ClusterStateError(
                f"after {timeout_s:.0f}s the deployment did not settle at exactly {replicas} Ready pod(s) "
                f"(have {len(pods)} pods, {len(ready)} ready, {len(pods) - len(live)} terminating, "
                f"{len(hashes)} pod template(s)):\n  {states}"
            )
        if now >= next_report:
            print(f"    waiting: {len(ready)}/{replicas} ready, {len(pods) - len(live)} terminating, {len(pods)} pods", flush=True)
            next_report = now + 30
        await asyncio.sleep(POLL_S)


def placement_of(pods: list[dict], control_plane_nodes: set[str]) -> Placement:
    """Assert one pod per distinct worker node and record where each pod runs."""
    nodes = {p["metadata"]["name"]: p["spec"]["nodeName"] for p in pods}
    if len(set(nodes.values())) != len(nodes):
        raise ClusterStateError(
            f"{len(nodes)} vLLM pods share {len(set(nodes.values()))} node(s): {nodes}; the required "
            "podAntiAffinity (one replica per node) is missing or not enforced"
        )
    on_control_plane = {pod: node for pod, node in nodes.items() if node in control_plane_nodes}
    if on_control_plane:
        raise ClusterStateError(f"vLLM pod(s) scheduled on the control-plane (the ingress node must run no workload): {on_control_plane}")
    return Placement(
        nodes=dict(sorted(nodes.items(), key=lambda kv: kv[1])),
        containers={p["metadata"]["name"]: p["spec"]["containers"][0]["name"] for p in pods},
        restarts={p["metadata"]["name"]: _restarts(p) for p in pods},
        qos_class={p["metadata"]["name"]: p.get("status", {}).get("qosClass") for p in pods},
    )


async def pod_cpuset(pod: str, container: str) -> str | None:
    try:
        out = await kubectl_retry("exec", "-n", NAMESPACE, pod, "-c", container, "--", "cat", CPUSET_FILE)
    except RuntimeError as exc:
        print(f"    could not read {pod}'s cpuset: {exc}", flush=True)
        return None
    return out.strip() or None


async def read_cpusets(placement: Placement) -> None:
    values = await asyncio.gather(*(pod_cpuset(pod, placement.containers[pod]) for pod in placement.nodes))
    placement.cpusets = dict(zip(placement.nodes, values))


def cpusets_disjoint(placement: Placement) -> bool | None:
    """True if no two pods can run on the same CPU; None if a cpuset could not be read."""
    if any(v is None for v in placement.cpusets.values()):
        return None
    seen: set[int] = set()
    for value in placement.cpusets.values():
        cpus = parse_cpuset(value)  # type: ignore[arg-type]
        if cpus & seen:
            return False
        seen |= cpus
    return True


async def wait_for_endpoints(service: str, pods: set[str], timeout_s: float = 120) -> None:
    """Wait until the Service's ready endpoints are exactly `pods`, so kube-proxy can
    route the first measured connection to any of them (and to nothing else)."""
    deadline = time.monotonic() + timeout_s
    while True:
        slices = (await kubectl_json("get", "endpointslices", "-n", NAMESPACE, "-l", f"kubernetes.io/service-name={service}"))["items"]
        ready = {
            ep["targetRef"]["name"]
            for s in slices
            for ep in s.get("endpoints") or []
            if ep.get("targetRef", {}).get("kind") == "Pod" and ep.get("conditions", {}).get("ready") is not False
        }
        if ready == pods:
            return
        if time.monotonic() >= deadline:
            raise ClusterStateError(f"service {service}: ready endpoints {sorted(ready)} != pods {sorted(pods)} after {timeout_s:.0f}s")
        await asyncio.sleep(1)


async def access_counts(placement: Placement) -> dict[str, dict[str, int]]:
    """Lifetime chat-completion access lines per pod, by status code."""
    logs = await asyncio.gather(*(kubectl_retry("logs", "-n", NAMESPACE, pod, "-c", c) for pod, c in placement.containers.items()))
    return {pod: accesslog.status_counts(text) for pod, text in zip(placement.containers, logs)}


async def pod_changes(placement: Placement, selector: str) -> tuple[list[str], list[str]]:
    """Compare the pods now with the placement: (fatal problems, warnings).

    Fatal: a pod vanished, is terminating, restarted or a new one appeared, i.e. the
    per-pod/per-node attribution of the level just measured is wrong. A pod that is
    merely not Ready at this instant (a probe timed out under load) is a warning.
    """
    current = {p["metadata"]["name"]: p for p in await get_pods(selector)}
    fatal, warnings = [], []
    for name in placement.nodes:
        pod = current.get(name)
        if pod is None:
            fatal.append(f"{name} is gone")
        elif pod["metadata"].get("deletionTimestamp"):
            fatal.append(f"{name} is terminating")
        elif pod.get("status", {}).get("phase") != "Running":
            fatal.append(f"{name} is {pod.get('status', {}).get('phase')}")
        elif _restarts(pod) != placement.restarts[name]:
            fatal.append(f"{name} restarted ({placement.restarts[name]} -> {_restarts(pod)} restarts)")
        elif not pod_is_ready(pod):
            warnings.append(f"{name} was not Ready right after the level")
    fatal.extend(
        f"unexpected pod {name} appeared"
        for name in sorted(current.keys() - placement.nodes.keys())
        if current[name].get("status", {}).get("phase") not in FINISHED_PHASES
    )
    return fatal, warnings


async def hpas_targeting(deployment: str) -> list[str]:
    items = (await kubectl_json("get", "hpa", "-n", NAMESPACE))["items"]
    return [
        h["metadata"]["name"]
        for h in items
        if h["spec"]["scaleTargetRef"].get("kind") == "Deployment" and h["spec"]["scaleTargetRef"].get("name") == deployment
    ]


def docker_node_info(node: str) -> dict | None:
    """The node container's CPU/memory limits as Docker applied them (best effort)."""
    out = _cmd("docker", "inspect", "-f", "{{.HostConfig.CpusetCpus}}|{{.HostConfig.NanoCpus}}|{{.HostConfig.Memory}}", node)
    if out.count("|") != 2:
        return None
    cpuset, nano_cpus, memory = out.split("|")
    return {
        "cpuset_cpus": cpuset or None,
        "nano_cpus": int(nano_cpus) if nano_cpus.isdigit() else None,
        "memory_bytes": int(memory) if memory.isdigit() else None,
    }


def docker_vm_info() -> dict | None:
    """CPUs and memory of the Linux VM that runs Docker (and so every kind node)."""
    out = _cmd("docker", "info", "-f", "{{.NCPU}}|{{.MemTotal}}")
    if out.count("|") != 1 or not all(x.isdigit() for x in out.split("|")):
        return None
    ncpu, mem = out.split("|")
    return {"cpus": int(ncpu), "memory_gib": round(int(mem) / 2**30, 2)}


async def describe_nodes() -> list[dict]:
    out = []
    for n in (await kubectl_json("get", "nodes"))["items"]:
        name = n["metadata"]["name"]
        labels = n["metadata"].get("labels", {})
        info = n["status"].get("nodeInfo", {})
        out.append(
            {
                "name": name,
                "roles": sorted(k.split("/", 1)[1] for k in labels if k.startswith("node-role.kubernetes.io/")) or ["worker"],
                "kubelet_version": info.get("kubeletVersion"),
                "container_runtime": info.get("containerRuntimeVersion"),
                "kernel": info.get("kernelVersion"),
                "os_image": info.get("osImage"),
                # What the kubelet believes (the whole VM); the docker cpuset is what the node gets.
                "capacity": {k: n["status"].get("capacity", {}).get(k) for k in ("cpu", "memory")},
                "allocatable": {k: n["status"].get("allocatable", {}).get(k) for k in ("cpu", "memory")},
                "labels": labels,
                "docker": docker_node_info(name),
            }
        )
    return sorted(out, key=lambda n: n["name"])


async def describe_deployment(name: str) -> dict:
    d = await kubectl_json("get", "deployment", name, "-n", NAMESPACE)
    spec = d["spec"]["template"]["spec"]
    c = spec["containers"][0]
    return {
        "name": name,
        "image": c["image"],
        "args": c.get("args", []),
        "env": c.get("env", []),
        "resources": c.get("resources", {}),
        "node_selector": spec.get("nodeSelector"),
        "affinity": spec.get("affinity"),
        "strategy": d["spec"].get("strategy"),
    }


async def describe_service(name: str) -> dict:
    s = await kubectl_json("get", "service", name, "-n", NAMESPACE)
    spec = s["spec"]
    return {
        "name": name,
        "type": spec.get("type"),
        "external_traffic_policy": spec.get("externalTrafficPolicy"),
        "internal_traffic_policy": spec.get("internalTrafficPolicy"),
        "session_affinity": spec.get("sessionAffinity", "None"),
        "selector": spec.get("selector"),
        "ports": [{k: p.get(k) for k in ("port", "targetPort", "nodePort")} for p in spec.get("ports", [])],
    }


async def kube_proxy_mode() -> str | None:
    """iptables/nftables pick a random endpoint per new connection; ipvs round-robins."""
    try:
        cm = await kubectl_json("get", "configmap", "kube-proxy", "-n", "kube-system")
    except RuntimeError:
        return None
    match = re.search(r'^mode:\s*"?([\w-]*)"?\s*$', cm.get("data", {}).get("config.conf", ""), re.MULTILINE)
    if match is None:
        return None
    return match.group(1) or "default (iptables)"


class RobustPodStatsSampler(PodStatsSampler):
    """PodStatsSampler whose ticks survive transient kubectl failures.

    The parent lists pods without retries, and an exception there would end its
    sampling task and resurface in stop(). CPU is read from cumulative counters, so a
    skipped tick costs one interval of peak resolution, never accuracy of the average.
    """

    tick_attempts = 3

    def __init__(self, selector: str, interval_s: float = 2.0, namespace: str = NAMESPACE):
        super().__init__(selector, interval_s, namespace)
        self.skipped_ticks = 0

    async def _tick(self) -> None:
        for attempt in range(1, self.tick_attempts + 1):
            try:
                return await super()._tick()
            except (RuntimeError, ValueError, KeyError) as exc:
                if attempt == self.tick_attempts:
                    self.skipped_ticks += 1
                    print(f"    pod stats sample skipped: {exc}", flush=True)
                    return
                await asyncio.sleep(1)
