"""Benchmark one deployment (vLLM or baseline) across a sweep of concurrency levels.

For each concurrency level c, sends N = max(16, 4*c) identical, deterministic
requests (128 output tokens each) with c requests in flight at all times, while
sampling the pod's cgroup CPU and memory. Writes everything (environment, per-level
summaries and every individual request) to results/raw/<target>.json.

    uv run python -m bench.run_benchmark --target vllm
    uv run python -m bench.run_benchmark --target baseline
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

from bench.kube import NAMESPACE, kubectl, kubectl_json, list_pods, pod_is_ready
from bench.loadgen import DEFAULT_MAX_TOKENS, HostSleptError, records_to_dicts, run_load, summarize
from bench.podstats import PodStatsSampler

TARGETS = {
    "vllm": {"url": "http://localhost:30080", "selector": "app=vllm"},
    "baseline": {"url": "http://localhost:30081", "selector": "app=baseline"},
}
ROOT = Path(__file__).resolve().parent.parent
MAX_ATTEMPTS = 5  # per level; an attempt is discarded if the host slept during it


def requests_for(concurrency: int) -> int:
    return max(16, 4 * concurrency)


def _cmd(*args: str) -> str:
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=30).stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        return ""


async def describe_environment(target: str) -> dict:
    pods = [p for p in await list_pods(TARGETS[target]["selector"]) if pod_is_ready(p)]
    nodes = (await kubectl_json("get", "nodes"))["items"]
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "host": {
            "machine": _cmd("sysctl", "-n", "machdep.cpu.brand_string") or platform.processor(),
            "os": f"{platform.system()} {platform.release()}",
        },
        "cluster": {
            "nodes": len(nodes),
            "node_cpu_capacity": nodes[0]["status"]["capacity"]["cpu"],
            "node_memory_capacity": nodes[0]["status"]["capacity"]["memory"],
            "kubelet_version": nodes[0]["status"]["nodeInfo"]["kubeletVersion"],
        },
        "pods": [
            {
                "name": p["metadata"]["name"],
                "image": p["spec"]["containers"][0]["image"],
                "args": p["spec"]["containers"][0].get("args", []),
                "resources": p["spec"]["containers"][0]["resources"],
                "qos_class": p["status"].get("qosClass"),
            }
            for p in pods
        ],
    }


async def pin_vllm_to_one_replica() -> None:
    """Engine-vs-engine comparison: exactly one vLLM pod for the whole sweep.

    Under benchmark load the HPA would otherwise add a second vLLM replica, which
    would compare two vLLM pods (6 cores) against one baseline pod (3 cores).
    `make hpa-demo` / `kubectl apply -k k8s/vllm` restore the autoscaling bounds.
    """
    await kubectl("patch", "hpa", "vllm", "-n", NAMESPACE, "--type", "merge", "-p", '{"spec":{"minReplicas":1,"maxReplicas":1}}')
    while True:
        pods = await list_pods(TARGETS["vllm"]["selector"])
        if len(pods) == 1 and pod_is_ready(pods[0]):
            return
        await asyncio.sleep(5)


async def benchmark(target: str, levels: list[int], repeats: int, max_tokens: int, out: Path) -> None:
    url, selector = TARGETS[target]["url"], TARGETS[target]["selector"]
    await pin_vllm_to_one_replica()
    async with httpx.AsyncClient(timeout=10) as client:
        (await client.get(f"{url}/v1/models")).raise_for_status()

    result = {
        "target": target,
        "url": url,
        "workload": {
            "max_tokens": max_tokens,
            "ignore_eos": True,
            "temperature": 0.0,
            "concurrency_levels": levels,
            "requests_per_level": {c: requests_for(c) for c in levels},
            "repeats": repeats,
            "pattern": "closed loop: c workers, each sends its next request as soon as the previous returns",
        },
        "environment": await describe_environment(target),
        "runs": [],
        "rejected_attempts": [],
    }
    out.parent.mkdir(parents=True, exist_ok=True)

    print(f"[{target}] warm-up")
    await run_load(url, concurrency=2, num_requests=2, max_tokens=max_tokens)

    for repeat in range(1, repeats + 1):
        for concurrency in levels:
            n = requests_for(concurrency)
            print(f"[{target}] repeat {repeat}/{repeats}  concurrency={concurrency}  requests={n} ...", flush=True)
            for attempt in range(1, MAX_ATTEMPTS + 1):
                sampler = PodStatsSampler(selector)
                await sampler.start()
                try:
                    records = await run_load(url, concurrency=concurrency, num_requests=n, max_tokens=max_tokens)
                    await sampler.stop()
                    break
                except HostSleptError as exc:
                    await sampler.stop()
                    print(f"    attempt {attempt} rejected: {exc}", flush=True)
                    result["rejected_attempts"].append(
                        {"repeat": repeat, "concurrency": concurrency, "attempt": attempt, "reason": str(exc)}
                    )
                    time.sleep(5)
            else:
                raise SystemExit(f"concurrency={concurrency}: host slept during all {MAX_ATTEMPTS} attempts")
            summary = summarize(records)
            resources = sampler.summary()
            print(
                f"    {summary['throughput_req_per_s']:.3f} req/s  {summary['output_tokens_per_s']:.1f} tok/s  "
                f"p50={summary['latency_p50_s']:.2f}s  p95={summary['latency_p95_s']:.2f}s  "
                f"failed={summary['failed']}  cpu={resources['total_avg_cores_used']:.2f} cores",
                flush=True,
            )
            result["runs"].append(
                {
                    "repeat": repeat,
                    "concurrency": concurrency,
                    "summary": summary,
                    "resources": resources,
                    "requests": records_to_dicts(records),
                }
            )
            out.write_text(json.dumps(result, indent=2))  # save after every level
            time.sleep(5)  # let the pod go idle between levels
    print(f"[{target}] wrote {out}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--target", choices=TARGETS, required=True)
    parser.add_argument("--concurrency", default="1,4,8,16", help="comma-separated levels")
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    parser.add_argument("--out", type=Path, help="default: results/raw/<target>.json")
    args = parser.parse_args()
    levels = [int(c) for c in args.concurrency.split(",")]
    out = args.out or ROOT / "results" / "raw" / f"{args.target}.json"
    asyncio.run(benchmark(args.target, levels, args.repeats, args.max_tokens, out))


if __name__ == "__main__":
    main()
