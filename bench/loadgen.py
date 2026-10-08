"""Closed-loop async load generator for OpenAI-compatible chat-completion servers.

`concurrency` workers each keep exactly one request in flight: as soon as a
response comes back, that worker sends the next request. Every request uses a
deterministic prompt (request i -> prompts[i % len(prompts)]), greedy decoding
and `ignore_eos`, so each request generates exactly `max_tokens` tokens on both
servers. The workload is therefore identical for vLLM and the baseline.

Usage (one-off run):
    uv run python -m bench.loadgen --url http://localhost:30080 --concurrency 4 --requests 16
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable

import httpx

PROMPTS: list[str] = json.loads((Path(__file__).parent / "prompts.json").read_text())
MODEL = "qwen2.5-0.5b-instruct"
DEFAULT_MAX_TOKENS = 128
# Advances only while the machine is awake (unlike the wall clock): used to reject
# measurements taken across a laptop sleep (e.g. the lid being closed).
_AWAKE_CLOCK = getattr(time, "CLOCK_UPTIME_RAW", time.CLOCK_MONOTONIC)
MAX_SLEEP_S = 5.0


class HostSleptError(RuntimeError):
    pass


def _slept_since(wall0: float, awake0: float) -> float:
    """Seconds the host spent asleep since (wall0, awake0) were sampled."""
    return (time.time() - wall0) - (time.clock_gettime(_AWAKE_CLOCK) - awake0)


@dataclass
class RequestRecord:
    index: int
    start: float  # unix timestamp when the request was sent
    end: float  # unix timestamp when the response (or error) arrived
    latency_s: float
    ok: bool
    status: int | None
    error: str | None
    prompt_tokens: int
    completion_tokens: int


def make_payload(index: int, max_tokens: int) -> dict:
    return {
        "model": MODEL,
        "messages": [{"role": "user", "content": PROMPTS[index % len(PROMPTS)]}],
        "max_tokens": max_tokens,
        "temperature": 0.0,
        "ignore_eos": True,
    }


async def _send(client: httpx.AsyncClient, index: int, max_tokens: int) -> RequestRecord:
    start = time.time()
    t0 = time.perf_counter()
    status, error, prompt_tokens, completion_tokens = None, None, 0, 0
    try:
        resp = await client.post("/v1/chat/completions", json=make_payload(index, max_tokens))
        status = resp.status_code
        if status == 200:
            usage = resp.json()["usage"]
            prompt_tokens = usage["prompt_tokens"]
            completion_tokens = usage["completion_tokens"]
        else:
            error = resp.text[:300]
    except httpx.HTTPError as exc:
        error = f"{type(exc).__name__}: {exc}"
    latency = time.perf_counter() - t0
    return RequestRecord(
        index=index,
        start=start,
        end=start + latency,
        latency_s=latency,
        ok=status == 200,
        status=status,
        error=error,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
    )


async def run_load(
    base_url: str,
    *,
    concurrency: int,
    num_requests: int | None = None,
    duration_s: float | None = None,
    stop_event: asyncio.Event | None = None,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    first_index: int = 0,
    keepalive: bool = False,
    timeout_s: float = 900.0,
    on_record: Callable[[RequestRecord], None] | None = None,
) -> list[RequestRecord]:
    """Keep `concurrency` requests in flight until a stop condition is met.

    Stop conditions: `num_requests` sent, `duration_s` elapsed, or `stop_event`
    set (in-flight requests always run to completion and are recorded).

    keepalive=False opens a fresh TCP connection per request. A Kubernetes Service
    balances *connections*, not requests, so pooled keep-alive connections would
    stay pinned to whichever pods existed when they were opened, and new replicas
    (after an HPA scale-up or a rollout) would get no traffic. Requests here take
    seconds, so connection setup cost is negligible.
    """
    if num_requests is None and duration_s is None and stop_event is None:
        raise ValueError("need num_requests, duration_s or stop_event")
    wall0, awake0 = time.time(), time.clock_gettime(_AWAKE_CLOCK)
    deadline = time.monotonic() + duration_s if duration_s is not None else None
    records: list[RequestRecord] = []
    next_index = first_index
    last_index = first_index + num_requests if num_requests is not None else None

    limits = httpx.Limits(
        max_connections=concurrency,
        max_keepalive_connections=concurrency if keepalive else 0,
    )
    async with httpx.AsyncClient(base_url=base_url, timeout=timeout_s, limits=limits) as client:

        async def worker() -> None:
            nonlocal next_index
            while True:
                if last_index is not None and next_index >= last_index:
                    return
                if deadline is not None and time.monotonic() >= deadline:
                    return
                if stop_event is not None and stop_event.is_set():
                    return
                index = next_index
                next_index += 1
                record = await _send(client, index, max_tokens)
                records.append(record)
                if on_record is not None:
                    on_record(record)

        async def sleep_watchdog() -> None:
            while _slept_since(wall0, awake0) <= MAX_SLEEP_S:
                await asyncio.sleep(1)

        # Abort as soon as a host sleep is noticed instead of finishing a run that
        # will be thrown away anyway.
        workers = asyncio.gather(*(worker() for _ in range(concurrency)))
        watchdog = asyncio.ensure_future(sleep_watchdog())
        await asyncio.wait({workers, watchdog}, return_when=asyncio.FIRST_COMPLETED)
        for task in (workers, watchdog):
            task.cancel()
        await asyncio.gather(workers, watchdog, return_exceptions=True)

    slept = _slept_since(wall0, awake0)
    if slept > MAX_SLEEP_S:
        raise HostSleptError(
            f"the host was asleep for ~{slept:.0f}s during this run, so its numbers are invalid; "
            "keep the machine awake (lid open, `caffeinate -is`) and re-run"
        )
    return records


def percentile(values: list[float], q: float) -> float | None:
    """Linear-interpolated percentile (same definition as numpy's default)."""
    if not values:
        return None
    ordered = sorted(values)
    pos = (len(ordered) - 1) * q / 100
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo)


def summarize(records: list[RequestRecord]) -> dict:
    """Throughput and latency over the window from the first send to the last response."""
    if not records:
        return {"requests": 0}
    ok = [r for r in records if r.ok]
    wall = max(r.end for r in records) - min(r.start for r in records)
    latencies = [r.latency_s for r in ok]
    completion = sum(r.completion_tokens for r in ok)
    return {
        "requests": len(records),
        "succeeded": len(ok),
        "failed": len(records) - len(ok),
        "wall_time_s": round(wall, 2),
        "throughput_req_per_s": round(len(ok) / wall, 4),
        "output_tokens_per_s": round(completion / wall, 2),
        "latency_p50_s": _round(percentile(latencies, 50)),
        "latency_p95_s": _round(percentile(latencies, 95)),
        "latency_p99_s": _round(percentile(latencies, 99)),
        "latency_mean_s": _round(sum(latencies) / len(latencies) if latencies else None),
        "latency_max_s": _round(max(latencies) if latencies else None),
        "mean_prompt_tokens": _round(sum(r.prompt_tokens for r in ok) / len(ok) if ok else None),
        "mean_completion_tokens": _round(completion / len(ok) if ok else None),
    }


def _round(x: float | None, digits: int = 3) -> float | None:
    return None if x is None else round(x, digits)


def records_to_dicts(records: list[RequestRecord]) -> list[dict]:
    return [asdict(r) for r in records]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", required=True, help="server base URL, e.g. http://localhost:30080")
    parser.add_argument("--concurrency", type=int, default=1)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--requests", type=int, help="number of requests to send")
    group.add_argument("--duration", type=float, help="seconds to keep sending")
    parser.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    parser.add_argument("--keepalive", action="store_true", help="reuse connections")
    args = parser.parse_args()

    records = asyncio.run(
        run_load(
            args.url,
            concurrency=args.concurrency,
            num_requests=args.requests,
            duration_s=args.duration,
            max_tokens=args.max_tokens,
            keepalive=args.keepalive,
        )
    )
    print(json.dumps(summarize(records), indent=2))


if __name__ == "__main__":
    main()
