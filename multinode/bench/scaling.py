"""Multi-node scaling benchmark: the same vLLM workload at R = 1, 2, 3 replicas, one per node.

Each replica count R is its own experiment. The Deployment is scaled to R and the
topology is asserted before anything is measured: exactly R pods, all Ready, none
terminating, on R distinct worker nodes, with non-overlapping cpusets, and the
Service's endpoints are exactly those pods. Every pod then gets warm-up traffic.

Each concurrency level c is measured with the single-node methodology of
bench.run_benchmark, unchanged: closed loop with c workers, N = max(16, 4c)
requests, exactly 128 output tokens (ignore_eos, greedy), a new TCP connection per
request, up to 5 attempts if the host slept, JSON saved after every level, 5 s idle
between levels. Two checks are added because every "node" is a container on one
laptop:
  * host swap (multinode.bench.hostguard): the nodes share the Mac's RAM, so a level
    during which macOS swapped is rejected and re-run, exactly like a host sleep;
  * who served what: kube-proxy spreads connections over pods and the client cannot
    see which pod answered, so each pod's access log is counted before and after the
    level (multinode.bench.accesslog). The per-pod counts must sum to the client's
    successful requests; a mismatch is recorded (served_check), not fatal.

One invocation is one repeat; run repeats as separate, interleaved invocations:

    uv run python -m multinode.bench.scaling --replicas 1,2,3 \\
        --concurrency 1,4,8,16,32,48 --out multinode/results/raw/scaling-run1.json

Output JSON. Keys shared with results/raw/vllm-run*.json keep their exact meaning
(summary and resources come from the same functions):
  target, url, repeat   "vllm", the base URL, the repeat label (--repeat, or N from
                        "runN" in --out)
  status, error         "running" while in progress, then "complete" or "aborted"
                        (with the reason in error); started_utc, finished_utc
  workload              the original's keys (max_tokens, ignore_eos, temperature,
                        concurrency_levels, requests_per_level, repeats, pattern)
                        + replica_counts, warmup, keepalive, level_pause_s,
                        max_attempts_per_level, host_max_sleep_s, host_swap_limit_mib
  environment           timestamp_utc;
                        host {machine, os, logical_cpus, memory_gib, docker_vm {cpus, memory_gib}};
                        cluster {kube_context, nodes (count), workers, kubelet_version,
                                 kube_proxy_mode, node_details [{name, roles,
                                 kubelet_version, container_runtime, kernel, os_image,
                                 capacity, allocatable, labels,
                                 docker {cpuset_cpus, nano_cpus, memory_bytes}}]};
                        deployment {name, image, args, env, resources, node_selector,
                                    affinity, strategy};
                        service {name, type, external_traffic_policy,
                                 internal_traffic_policy, session_affinity, selector, ports}
  replica_setups        one per replica count: {replicas, placement {pod: node},
                        pod_cpusets {pod: cpuset}, cpusets_disjoint, qos_class {pod: qos},
                        settle_s (scale -> all Ready + endpoints), served_models,
                        warmup {requests, failed, top_up_rounds, served_lifetime_per_pod, cold_pods}}
  runs                  one per measured level: {repeat, replicas, concurrency, attempt,
                        placement, pod_cpusets,
                        summary        bench.loadgen.summarize (same keys as the original),
                        resources      PodStatsSampler.summary (+ "node" per pod, skipped_ticks),
                        served_per_pod {pod: n}, served_per_node {node: n}  (status-200 access lines),
                        served_check   {served_total, succeeded, match, non_200_per_pod},
                        pod_warnings   [str] (normally empty),
                        host           hostguard.compare + loadavg_1_5_15_after,
                        requests       [bench.loadgen.RequestRecord as dict]}
  rejected_attempts     [{repeat, replicas, concurrency, attempt, kind (host_slept |
                        host_swapped | pods_changed), reason, host, t_utc}]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import httpx

from bench.loadgen import DEFAULT_MAX_TOKENS, MAX_SLEEP_S, MODEL, HostSleptError, records_to_dicts, run_load, summarize
from bench.run_benchmark import MAX_ATTEMPTS, _cmd, requests_for
from multinode.bench import accesslog, cluster, hostguard
from multinode.bench.cluster import ClusterStateError, Placement
from multinode.bench.hostguard import HostSwappedError

URL = "http://localhost:30080"
EXPECTED_CONTEXT = "kind-kubeserve-mn"
DEPLOYMENT, SELECTOR, SERVICE = "vllm", "app=vllm", "vllm"
LEVEL_PAUSE_S = 5.0  # let the pods go idle between levels (as the original)
REJECT_PAUSE_S = 5.0
WARMUP_MIN_PER_POD = 2  # the original warmed its single pod with 2 requests
WARMUP_MAX_TOP_UPS = 5
SERVED_SETTLE_READS = 5  # re-read the access logs for up to ~5 s if a line lags behind the response
QUIET_READS = 10  # baseline: wait up to ~10 s for the access logs to stop changing
PATTERN = "closed loop: c workers, each sends its next request as soon as the previous returns"


class LevelFailedError(RuntimeError):
    pass


@dataclass
class Config:
    replicas: list[int]
    levels: list[int]
    out: Path
    repeat: int
    url: str = URL
    max_tokens: int = DEFAULT_MAX_TOKENS
    ready_timeout_s: float = 900.0
    expect_context: str = EXPECTED_CONTEXT
    allow_shared_cpus: bool = False


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def save(result: dict, out: Path) -> None:
    """Write via a temp file + rename, so an interrupt never leaves half a JSON file."""
    tmp = out.with_name(out.name + ".tmp")
    tmp.write_text(json.dumps(result, indent=2))
    tmp.replace(out)


def short_node(node: str) -> str:
    """kubeserve-mn-worker2 -> worker2 (progress lines only; the JSON keeps full names)."""
    match = re.search(r"(worker\d*|control-plane)$", node)
    return match.group(1) if match else node


def _fmt(x: float | None, spec: str) -> str:
    return "n/a" if x is None else format(x, spec)


def _host_memory_gib() -> float | None:
    out = _cmd("sysctl", "-n", "hw.memsize")
    if out.isdigit():
        return round(int(out) / 2**30, 1)
    try:
        return round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1)
    except (ValueError, OSError, AttributeError):
        return None


async def preflight(cfg: Config) -> dict:
    """Refuse to run against the wrong cluster or a setup that would distort the curve."""
    context = await cluster.current_context()
    if cfg.expect_context and context != cfg.expect_context:
        raise ClusterStateError(f"kubectl context is {context!r}, expected {cfg.expect_context!r} (pass --expect-context to override)")
    hpas = await cluster.hpas_targeting(DEPLOYMENT)
    if hpas:
        raise ClusterStateError(f"HPA {hpas} targets deployment/{DEPLOYMENT} and would override `kubectl scale`; delete it first")
    service = await cluster.describe_service(SERVICE)
    if service["session_affinity"] == "ClientIP":
        raise ClusterStateError(f"service {SERVICE} has sessionAffinity=ClientIP: every request would go to one pod")
    if service["external_traffic_policy"] != "Cluster":
        print(f"warning: service {SERVICE} externalTrafficPolicy={service['external_traffic_policy']} (expected Cluster)", flush=True)
    nodes = await cluster.describe_nodes()
    workers = [n for n in nodes if cluster.CONTROL_PLANE_ROLE not in n["roles"]]
    too_many = [r for r in cfg.replicas if r > len(workers)]
    if too_many:
        raise ClusterStateError(f"replica count(s) {too_many} exceed the {len(workers)} worker nodes (at most one vLLM pod per node)")
    return {
        "timestamp_utc": _now(),
        "host": {
            "machine": _cmd("sysctl", "-n", "machdep.cpu.brand_string") or platform.processor(),
            "os": f"{platform.system()} {platform.release()}",
            "logical_cpus": os.cpu_count(),
            "memory_gib": _host_memory_gib(),
            "docker_vm": cluster.docker_vm_info(),
        },
        "cluster": {
            "kube_context": context,
            "nodes": len(nodes),
            "workers": len(workers),
            "kubelet_version": nodes[0]["kubelet_version"] if nodes else None,
            "kube_proxy_mode": await cluster.kube_proxy_mode(),
            "node_details": nodes,
        },
        "deployment": await cluster.describe_deployment(DEPLOYMENT),
        "service": service,
    }


async def served_models(url: str) -> list[str]:
    """GET /v1/models through the NodePort (retried: kube-proxy may lag the endpoints)."""
    last = ""
    for _ in range(10):
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"{url}/v1/models")
            resp.raise_for_status()
            ids = [m["id"] for m in resp.json()["data"]]
            if MODEL not in ids:
                raise ClusterStateError(f"{url} serves {ids}, not {MODEL!r}")
            return ids
        except (httpx.HTTPError, ValueError, KeyError) as exc:
            last = f"{type(exc).__name__}: {exc}"
            await asyncio.sleep(3)
    raise ClusterStateError(f"{url}/v1/models is not answering: {last}")


async def warm_up(cfg: Config, placement: Placement) -> dict:
    """2R requests at concurrency R; topped up until every pod has served WARMUP_MIN_PER_POD.

    kube-proxy picks a pod at random per connection, so 2R requests can miss a freshly
    started pod (p ~ 0.25 at R=3); its first measured requests would then pay the
    cold-start cost. Not recorded as a measurement.
    """
    r = placement.replicas
    sent, failed, top_ups, batch = 0, 0, 0, 2 * r
    while True:
        try:
            records = await run_load(cfg.url, concurrency=r, num_requests=batch, max_tokens=cfg.max_tokens)
        except HostSleptError as exc:
            print(f"    warm-up: {exc} (warm-up is not measured, continuing)", flush=True)
            records = []
        sent += batch
        failed += sum(not rec.ok for rec in records)
        if records and all(not rec.ok for rec in records):
            raise ClusterStateError(f"warm-up: all {len(records)} requests failed, e.g. {records[0].error or records[0].status}")
        counts = await _quiet_access_counts(placement)
        served = {pod: counts[pod].get(accesslog.OK, 0) for pod in placement.nodes}
        cold = [pod for pod, n in served.items() if n < WARMUP_MIN_PER_POD]
        if not cold or top_ups >= WARMUP_MAX_TOP_UPS:
            break
        top_ups += 1
        batch = r
    if cold:
        print(f"    warning: after warm-up, pods {cold} have served < {WARMUP_MIN_PER_POD} requests", flush=True)
    return {"requests": sent, "failed": failed, "top_up_rounds": top_ups, "served_lifetime_per_pod": served, "cold_pods": cold}


async def set_replicas(cfg: Config, replicas: int, control_plane: set[str]) -> tuple[Placement, dict]:
    print(f"[R={replicas}] scaling deployment/{DEPLOYMENT} to {replicas} ...", flush=True)
    t0 = time.monotonic()
    await cluster.scale(DEPLOYMENT, replicas)
    pods = await cluster.wait_for_replicas(SELECTOR, replicas, cfg.ready_timeout_s)
    placement = cluster.placement_of(pods, control_plane)
    await cluster.read_cpusets(placement)
    disjoint = cluster.cpusets_disjoint(placement)
    if disjoint is False and not cfg.allow_shared_cpus:
        raise ClusterStateError(
            f"pods can share CPUs (cpusets {placement.cpusets}); the per-node CPU pinning is missing "
            "(pass --allow-shared-cpus to measure anyway)"
        )
    await cluster.wait_for_endpoints(SERVICE, set(placement.nodes))
    settle_s = round(time.monotonic() - t0, 1)
    models = await served_models(cfg.url)
    where = ", ".join(f"{pod} on {short_node(node)} (cpus {placement.cpusets.get(pod)})" for pod, node in placement.nodes.items())
    print(f"    settled in {settle_s:.0f}s: {where}" + ("" if disjoint else f"  [cpusets_disjoint={disjoint}]"), flush=True)
    warmup = await warm_up(cfg, placement)
    print(f"    warm-up: {warmup['requests']} requests, {warmup['failed']} failed, {warmup['top_up_rounds']} top-up round(s)", flush=True)
    setup = {
        "replicas": replicas,
        "placement": placement.nodes,
        "pod_cpusets": placement.cpusets,
        "cpusets_disjoint": disjoint,
        "qos_class": placement.qos_class,
        "settle_s": settle_s,
        "served_models": models,
        "warmup": warmup,
    }
    return placement, setup


async def _quiet_access_counts(placement: Placement) -> dict[str, dict[str, int]]:
    """Access counts once two reads 1 s apart agree.

    Lines from earlier traffic (the warm-up, a rejected attempt) must have landed
    before a level's baseline is read, or they would be counted into that level.
    """
    counts = await cluster.access_counts(placement)
    for _ in range(QUIET_READS):
        await asyncio.sleep(1)
        again = await cluster.access_counts(placement)
        if again == counts:
            return counts
        counts = again
    print("    warning: access logs are still changing; is something else sending requests to vLLM?", flush=True)
    return counts


async def _served_during(placement: Placement, before: dict[str, dict[str, int]], succeeded: int) -> dict[str, dict[str, int]]:
    """Access-log delta since `before`, re-read briefly if lines lag behind the responses."""
    for read in range(SERVED_SETTLE_READS):
        served = accesslog.delta(before, await cluster.access_counts(placement))
        if sum(c.get(accesslog.OK, 0) for c in served.values()) >= succeeded or read == SERVED_SETTLE_READS - 1:
            return served
        await asyncio.sleep(1)
    raise AssertionError("unreachable")


def _reject(cfg: Config, result: dict, replicas: int, concurrency: int, attempt: int, kind: str, reason: str, host: dict | None) -> None:
    print(f"    attempt {attempt} rejected ({kind}): {reason}", flush=True)
    result["rejected_attempts"].append(
        {
            "repeat": cfg.repeat,
            "replicas": replicas,
            "concurrency": concurrency,
            "attempt": attempt,
            "kind": kind,
            "reason": reason,
            "host": host,
            "t_utc": _now(),
        }
    )
    save(result, cfg.out)


async def measure_level(cfg: Config, result: dict, placement: Placement, concurrency: int) -> dict:
    replicas, n = placement.replicas, requests_for(concurrency)
    print(f"[R={replicas} repeat {cfg.repeat}] concurrency={concurrency}  requests={n} ...", flush=True)
    for attempt in range(1, MAX_ATTEMPTS + 1):
        host_before = hostguard.snapshot()
        counts_before = await _quiet_access_counts(placement)
        sampler = cluster.RobustPodStatsSampler(SELECTOR)
        await sampler.start()
        try:
            records = await run_load(cfg.url, concurrency=concurrency, num_requests=n, max_tokens=cfg.max_tokens)
        except HostSleptError as exc:
            await sampler.stop()
            _reject(cfg, result, replicas, concurrency, attempt, "host_slept", str(exc), None)
            await asyncio.sleep(REJECT_PAUSE_S)
            continue
        await sampler.stop()
        host_after = hostguard.snapshot()
        summary = summarize(records)
        served = await _served_during(placement, counts_before, summary["succeeded"])
        try:
            host = hostguard.check(host_before, host_after)
        except HostSwappedError as exc:
            _reject(cfg, result, replicas, concurrency, attempt, "host_swapped", str(exc), hostguard.compare(host_before, host_after))
            await asyncio.sleep(REJECT_PAUSE_S)
            continue
        fatal, warnings = await cluster.pod_changes(placement, SELECTOR)
        if fatal:
            _reject(cfg, result, replicas, concurrency, attempt, "pods_changed", "; ".join(fatal), None)
            raise ClusterStateError(f"R={replicas} concurrency={concurrency}: vLLM pods changed during the level ({'; '.join(fatal)})")
        for warning in warnings:
            print(f"    warning: {warning}", flush=True)

        served_per_pod = {pod: served.get(pod, {}).get(accesslog.OK, 0) for pod in placement.nodes}
        served_per_node: dict[str, int] = {}
        for pod, node in placement.nodes.items():
            served_per_node[node] = served_per_node.get(node, 0) + served_per_pod[pod]
        non_200 = {pod: {k: v for k, v in codes.items() if k != accesslog.OK} for pod, codes in served.items()}
        served_check = {
            "served_total": sum(served_per_pod.values()),
            "succeeded": summary["succeeded"],
            "match": sum(served_per_pod.values()) == summary["succeeded"],
            "non_200_per_pod": {pod: codes for pod, codes in non_200.items() if codes},
        }
        resources = sampler.summary()
        for pod, stats in resources["pods"].items():
            stats["node"] = placement.nodes.get(pod)
        resources["skipped_ticks"] = sampler.skipped_ticks
        host["loadavg_1_5_15_after"] = [round(x, 2) for x in os.getloadavg()]

        split = " ".join(f"{short_node(node)}:{count}" for node, count in served_per_node.items())
        mismatch = "" if served_check["match"] else f"  [access logs: {served_check['served_total']} served != {summary['succeeded']} succeeded]"
        print(
            f"    {summary['throughput_req_per_s']:.3f} req/s  {summary['output_tokens_per_s']:.1f} tok/s  "
            f"p50={_fmt(summary['latency_p50_s'], '.2f')}s  p95={_fmt(summary['latency_p95_s'], '.2f')}s  "
            f"failed={summary['failed']}  cpu={resources['total_avg_cores_used']:.2f} cores  {split}{mismatch}",
            flush=True,
        )
        return {
            "repeat": cfg.repeat,
            "replicas": replicas,
            "concurrency": concurrency,
            "attempt": attempt,
            "placement": placement.nodes,
            "pod_cpusets": placement.cpusets,
            "summary": summary,
            "resources": resources,
            "served_per_pod": served_per_pod,
            "served_per_node": served_per_node,
            "served_check": served_check,
            "pod_warnings": warnings,
            "host": host,
            "requests": records_to_dicts(records),
        }
    raise LevelFailedError(f"R={replicas} concurrency={concurrency}: all {MAX_ATTEMPTS} attempts were rejected (see rejected_attempts in {cfg.out})")


def new_result(cfg: Config) -> dict:
    return {
        "target": "vllm",
        "url": cfg.url,
        "repeat": cfg.repeat,
        "status": "running",
        "error": None,
        "started_utc": _now(),
        "finished_utc": None,
        "workload": {
            "max_tokens": cfg.max_tokens,
            "ignore_eos": True,
            "temperature": 0.0,
            "concurrency_levels": cfg.levels,
            "requests_per_level": {c: requests_for(c) for c in cfg.levels},
            "repeats": 1,  # one repeat per file; the repeat label is top-level "repeat"
            "pattern": PATTERN,
            "replica_counts": cfg.replicas,
            "warmup": f"after each scale: 2R requests at concurrency R, topped up until every pod has served >= {WARMUP_MIN_PER_POD}; not recorded",
            "keepalive": False,
            "level_pause_s": LEVEL_PAUSE_S,
            "max_attempts_per_level": MAX_ATTEMPTS,
            "host_max_sleep_s": MAX_SLEEP_S,
            "host_swap_limit_mib": hostguard.SWAP_LIMIT_MB,
        },
        "environment": None,
        "replica_setups": [],
        "runs": [],
        "rejected_attempts": [],
    }


async def benchmark(cfg: Config) -> dict:
    cfg.out.parent.mkdir(parents=True, exist_ok=True)
    result = new_result(cfg)
    save(result, cfg.out)
    try:
        result["environment"] = await preflight(cfg)
        save(result, cfg.out)
        control_plane = {n["name"] for n in result["environment"]["cluster"]["node_details"] if cluster.CONTROL_PLANE_ROLE in n["roles"]}
        for replicas in cfg.replicas:
            placement, setup = await set_replicas(cfg, replicas, control_plane)
            result["replica_setups"].append(setup)
            save(result, cfg.out)
            await asyncio.sleep(LEVEL_PAUSE_S)  # idle after the warm-up, like between levels
            for concurrency in cfg.levels:
                result["runs"].append(await measure_level(cfg, result, placement, concurrency))
                save(result, cfg.out)  # save after every level
                await asyncio.sleep(LEVEL_PAUSE_S)
        result["status"] = "complete"
    except BaseException as exc:  # incl. Ctrl-C: keep what was measured, marked as aborted
        result["status"] = "aborted"
        result["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        result["finished_utc"] = _now()
        save(result, cfg.out)
    print(f"wrote {cfg.out}")
    return result


def _int_list(text: str) -> list[int]:
    values = [int(x) for x in text.split(",") if x.strip()]
    if not values or any(v < 1 for v in values):
        raise argparse.ArgumentTypeError(f"expected comma-separated positive integers, got {text!r}")
    return values


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--replicas", type=_int_list, default=[1, 2, 3], help="comma-separated replica counts, run in this order")
    parser.add_argument("--concurrency", type=_int_list, default=[1, 4, 8, 16, 32, 48], help="comma-separated concurrency levels")
    parser.add_argument("--out", type=Path, required=True, help="result JSON, one file per repeat")
    parser.add_argument("--repeat", type=int, help="repeat label stored in the JSON (default: N from 'runN' in --out, else 1)")
    parser.add_argument("--url", default=URL)
    parser.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    parser.add_argument("--ready-timeout", type=float, default=900.0, help="seconds to wait for R Ready pods after scaling")
    parser.add_argument("--expect-context", default=EXPECTED_CONTEXT, help="abort unless kubectl's current context is this ('' to skip)")
    parser.add_argument("--allow-shared-cpus", action="store_true", help="measure even if the pods' cpusets overlap")
    parser.add_argument("--force", action="store_true", help="overwrite --out if it already exists")
    args = parser.parse_args()

    if args.out.exists() and not args.force:
        raise SystemExit(f"{args.out} exists; pass --force to overwrite it")
    match = re.search(r"run(\d+)", args.out.stem)
    cfg = Config(
        replicas=args.replicas,
        levels=args.concurrency,
        out=args.out,
        repeat=args.repeat if args.repeat is not None else int(match.group(1)) if match else 1,
        url=args.url,
        max_tokens=args.max_tokens,
        ready_timeout_s=args.ready_timeout,
        expect_context=args.expect_context,
        allow_shared_cpus=args.allow_shared_cpus,
    )
    try:
        asyncio.run(benchmark(cfg))
    except RuntimeError as exc:  # ClusterStateError, LevelFailedError, kubectl failing after retries
        raise SystemExit(f"aborted: {exc}\n(partial results kept in {cfg.out})")


if __name__ == "__main__":
    main()
