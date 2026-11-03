"""Count the chat completions each vLLM pod served, from its uvicorn access log.

With several replicas behind one Service the client only ever talks to the Service:
it cannot tell which pod answered a request. vLLM's API server (uvicorn) writes one
access line per request, e.g.

    INFO:     10.244.0.1:37538 - "POST /v1/chat/completions HTTP/1.1" 200 OK

so counting those lines in every pod's log before and after a benchmark level gives
how many requests each pod (and therefore each node) served during that level. The
per-pod deltas must add up to the client's successful requests, which doubles as a
check that no other traffic reached the pods.

Only real access lines may count. vLLM also prints its route table at startup
("Route: /v1/chat/completions, Methods: POST"), and the kubelet's probes log
"GET /health" lines; bench.rolling_update_demo.ACCESS_LINE matches neither.
"""

from __future__ import annotations

from bench.rolling_update_demo import ACCESS_LINE

OK = "200"


def status_counts(log_text: str) -> dict[str, int]:
    """Chat-completion access lines in a log, by HTTP status code ("200" -> 31)."""
    counts: dict[str, int] = {}
    for match in ACCESS_LINE.finditer(log_text):
        counts[match.group(1)] = counts.get(match.group(1), 0) + 1
    return counts


def delta(before: dict[str, dict[str, int]], after: dict[str, dict[str, int]]) -> dict[str, dict[str, int]]:
    """Per-pod, per-status counts that appeared between two `status_counts` readings.

    A negative value means the log shrank (container restart or log rotation): the
    caller's sum check then fails, which is the intended signal.
    """
    out: dict[str, dict[str, int]] = {}
    for pod in sorted(before.keys() | after.keys()):
        b, a = before.get(pod, {}), after.get(pod, {})
        out[pod] = {code: a.get(code, 0) - b.get(code, 0) for code in sorted(a.keys() | b.keys()) if a.get(code, 0) != b.get(code, 0)}
    return out
