# Failure test: node-kill (no client retry)

Run started 2026-11-02T18:54:55-05:00. 2 vLLM replicas on 3 worker nodes, 8 closed-loop clients (new connection per request), 60 s client timeout, 128 output tokens per request.
Fault: `docker kill kubeserve-mn-worker` (victim pod `vllm-5b6f494778-vchkf` on `kubeserve-mn-worker`), 60 s after the load started; t = 0 below.

**Result: 22 of 156 requests failed; back to 2 ready endpoints 113.4 s after the fault.**

Valid: yes. Host swap during the load: 0.0 MiB (limit 64).

## Key times

Seconds after the fault, as first seen by the 1 s watcher (an upper bound: the change happened after the previous poll; `t_prev_poll` in timeline.csv).

| Event | s |
|---|---:|
| fault injected | 0.0 |
| victim node Ready≠True | 48.2 |
| node.kubernetes.io/unreachable taint on the victim node | 48.2 |
| victim pod Ready=False | 48.2 |
| victim pod no longer a ready endpoint of the Service | 48.2 |
| ready endpoints < R | 48.2 |
| victim pod deletionTimestamp set (evicted) | 78.3 |
| replacement pod created | 78.3 |
| replacement pod scheduled | 78.3 |
| replacement container running | 78.3 |
| replacement pod Ready | 113.4 |
| ready endpoints back to R | 113.4 |
| recovered (criterion met) | 113.4 |
| load stopped | 203.4 |
| restore: docker start of the victim node | 212.7 |
| restore: victim node Ready again | 215.5 |
| victim pod object deleted | 217.5 |

Replacement pod: `vllm-5b6f494778-47h62` on `kubeserve-mn-worker2`.

## Requests

| Outcome | Requests |
|---|---:|
| succeeded | 134 |
| failed, in flight when the fault hit | 2 |
| failed, sent after the fault | 20 |
| failed before the fault | 0 |
| **total** | **156** |

| Failure class | Error | Requests |
|---|---|---:|
| failed, in flight when the fault hit | RemoteProtocolError | 2 |
| failed, sent after the fault | RemoteProtocolError | 19 |
| failed, sent after the fault | ReadTimeout | 1 |

Failed attempts by error (time from send to error):

| Error | Attempts | Median s | Max s |
|---|---:|---:|---:|
| ReadTimeout | 1 | 60.00 | 60.00 |
| RemoteProtocolError | 21 | 6.15 | 36.65 |

The failed requests' errors reached the client between 0.2 s and 69.6 s after the fault.

## Phases

Requests are assigned to the phase in which they were sent, so a request sent before the fault that was still running when it hit counts as a pre-fault failure (marked "in flight"). Output tokens/s spreads each successful attempt's tokens evenly over its lifetime.

| Phase | s after fault | Sent | Succeeded | Failed | Success rate | Output tok/s | p50 / p95 latency of successes (s) |
|---|---|---:|---:|---:|---:|---:|---:|
| pre-fault | -60.0 → 0.0 | 40 | 38 | 2 (2 in flight) | 95.0% | 73.3 | 13.86 / 15.12 |
| detection (fault → victim out of the ready endpoints) | 0.0 → 48.2 | 32 | 12 | 20 | 37.5% | 34.9 | 12.31 / 13.15 |
| degraded (→ recovered) | 48.2 → 113.4 | 35 | 35 | 0 | 100.0% | 62.6 | 14.71 / 19.83 |
| post-recovery | 113.4 → 203.4 | 49 | 49 | 0 | 100.0% | 72.6 | 13.79 / 15.19 |

## Served per pod (access logs)

| Pod | Node | HTTP 200 | other |
|---|---|---:|---:|
| `vllm-5b6f494778-47h62` | kubeserve-mn-worker2 | 24 | – |
| `vllm-5b6f494778-7l7rx` | kubeserve-mn-worker3 | 94 | – |
| `vllm-5b6f494778-vchkf` | kubeserve-mn-worker | 16 | – |

`kubeserve-mn-worker`: 16 · `kubeserve-mn-worker2`: 24 · `kubeserve-mn-worker3`: 94

HTTP 200 lines in the access logs: 134; successful attempts seen by the client: 134.
