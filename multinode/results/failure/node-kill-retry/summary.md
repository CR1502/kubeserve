# Failure test: node-kill with one client retry

Run started 2026-11-02T19:06:19-05:00. 2 vLLM replicas on 3 worker nodes, 8 closed-loop clients (new connection per request), 60 s client timeout, 128 output tokens per request.
Fault: `docker kill kubeserve-mn-worker2` (victim pod `vllm-5b6f494778-47h62` on `kubeserve-mn-worker2`), 60 s after the load started; t = 0 below.

**Result: 5 of 141 requests failed after one client retry (15 retried, 10 of them succeeded); back to 2 ready endpoints 106.3 s after the fault.**

Valid: yes. Host swap during the load: 0.0 MiB (limit 64).

## Key times

Seconds after the fault, as first seen by the 1 s watcher (an upper bound: the change happened after the previous poll; `t_prev_poll` in timeline.csv).

| Event | s |
|---|---:|
| fault injected | 0.0 |
| victim node Ready≠True | 45.2 |
| node.kubernetes.io/unreachable taint on the victim node | 45.2 |
| victim pod Ready=False | 45.2 |
| victim pod no longer a ready endpoint of the Service | 45.2 |
| ready endpoints < R | 45.2 |
| victim pod deletionTimestamp set (evicted) | 75.2 |
| replacement pod created | 75.2 |
| replacement pod scheduled | 75.2 |
| replacement container running | 76.2 |
| replacement pod Ready | 106.3 |
| ready endpoints back to R | 106.3 |
| recovered (criterion met) | 106.3 |
| load stopped | 196.3 |
| restore: docker start of the victim node | 206.7 |
| restore: victim node Ready again | 209.4 |
| victim pod object deleted | 211.4 |

Replacement pod: `vllm-5b6f494778-4n8zl` on `kubeserve-mn-worker`.

## Requests

| Outcome | Requests |
|---|---:|
| succeeded | 136 |
| failed, in flight when the fault hit | 0 |
| failed, sent after the fault | 5 |
| failed before the fault | 0 |
| **total** | **141** |

| Failure class | Error | Requests |
|---|---|---:|
| failed, sent after the fault | RemoteProtocolError | 5 |

Failed attempts by error (time from send to error):

| Error | Attempts | Median s | Max s |
|---|---:|---:|---:|
| RemoteProtocolError | 20 | 3.07 | 39.07 |

The failed requests' errors reached the client between 34.2 s and 40.4 s after the fault.

## Client retry

| | |
|---|---:|
| attempts sent | 156 |
| requests whose first attempt failed | 15 |
| retried | 15 |
| succeeded on the retry (masked) | 10 |
| failed again on the retry | 5 |
| failed, not retryable | 0 |
| time lost by masked requests, median / max (s) | 3.07 / 39.07 |

## Phases

Requests are assigned to the phase in which they were sent, so a request sent before the fault that was still running when it hit counts as a pre-fault failure (marked "in flight"). Output tokens/s spreads each successful attempt's tokens evenly over its lifetime.

| Phase | s after fault | Sent | Succeeded | Failed | Success rate | Output tok/s | p50 / p95 latency of successes (s) |
|---|---|---:|---:|---:|---:|---:|---:|
| pre-fault | -60.0 → 0.0 | 36 | 36 | 0 | 100.0% | 66.6 | 14.85 / 18.70 |
| detection (fault → victim out of the ready endpoints) | 0.0 → 45.2 | 22 | 17 | 5 | 77.3% | 46.9 | 13.34 / 51.03 |
| degraded (→ recovered) | 45.2 → 106.3 | 31 | 31 | 0 | 100.0% | 68.1 | 15.18 / 15.95 |
| post-recovery | 106.3 → 196.3 | 52 | 52 | 0 | 100.0% | 73.2 | 13.88 / 14.72 |

## Served per pod (access logs)

| Pod | Node | HTTP 200 | other |
|---|---|---:|---:|
| `vllm-5b6f494778-47h62` | kubeserve-mn-worker2 | 12 | – |
| `vllm-5b6f494778-4n8zl` | kubeserve-mn-worker | 25 | – |
| `vllm-5b6f494778-7l7rx` | kubeserve-mn-worker3 | 99 | – |

`kubeserve-mn-worker`: 25 · `kubeserve-mn-worker2`: 12 · `kubeserve-mn-worker3`: 99

HTTP 200 lines in the access logs: 136; successful attempts seen by the client: 136.
