# Failure test: process-crash (no client retry)

Run started 2026-11-02T19:00:37-05:00. 2 vLLM replicas on 3 worker nodes, 8 closed-loop clients (new connection per request), 60 s client timeout, 128 output tokens per request.
Fault: `docker exec kubeserve-mn-worker2 pkill -9 -f 'vllm serve'` (victim pod `vllm-5b6f494778-47h62` on `kubeserve-mn-worker2`), 60 s after the load started; t = 0 below.

**Result: 7 of 115 requests failed; back to 2 ready endpoints 33.1 s after the fault.**

Valid: yes. Host swap during the load: 0.0 MiB (limit 64).

## Key times

Seconds after the fault, as first seen by the 1 s watcher (an upper bound: the change happened after the previous poll; `t_prev_poll` in timeline.csv).

| Event | s |
|---|---:|
| fault injected | 0.0 |
| victim pod Ready=False | 2.1 |
| victim pod no longer a ready endpoint of the Service | 2.1 |
| ready endpoints < R | 2.1 |
| victim pod deletionTimestamp set (evicted) | – |
| victim container restarted (restartCount +1) | 2.1 |
| victim pod Ready again | 33.1 |
| ready endpoints back to R | 33.1 |
| recovered (criterion met) | 33.1 |
| load stopped | 123.1 |
| victim pod object deleted | – |

Victim container's last termination (kubelet): `terminated: Error exit 137 at 2026-11-03T00:01:46Z`.

## Requests

| Outcome | Requests |
|---|---:|
| succeeded | 108 |
| failed, in flight when the fault hit | 5 |
| failed, sent after the fault | 2 |
| failed before the fault | 0 |
| **total** | **115** |

| Failure class | Error | Requests |
|---|---|---:|
| failed, in flight when the fault hit | RemoteProtocolError | 5 |
| failed, sent after the fault | RemoteProtocolError | 2 |

Failed attempts by error (time from send to error):

| Error | Attempts | Median s | Max s |
|---|---:|---:|---:|
| RemoteProtocolError | 7 | 3.90 | 7.05 |

The failed requests' errors reached the client 0.1 s after the fault.

## Phases

Requests are assigned to the phase in which they were sent, so a request sent before the fault that was still running when it hit counts as a pre-fault failure (marked "in flight"). Output tokens/s spreads each successful attempt's tokens evenly over its lifetime.

| Phase | s after fault | Sent | Succeeded | Failed | Success rate | Output tok/s | p50 / p95 latency of successes (s) |
|---|---|---:|---:|---:|---:|---:|---:|
| pre-fault | -60.1 → 0.0 | 40 | 35 | 5 (5 in flight) | 87.5% | 70.1 | 13.75 / 15.28 |
| detection (fault → victim out of the ready endpoints) | 0.0 → 2.1 | 7 | 5 | 2 | 71.4% | 67.6 | 14.67 / 14.67 |
| degraded (→ recovered) | 2.1 → 33.1 | 16 | 16 | 0 | 100.0% | 67.5 | 15.30 / 15.93 |
| post-recovery | 33.1 → 123.1 | 52 | 52 | 0 | 100.0% | 75.5 | 13.50 / 14.36 |

## Served per pod (access logs)

| Pod | Node | HTTP 200 | other |
|---|---|---:|---:|
| `vllm-5b6f494778-47h62` | kubeserve-mn-worker2 | 51 | – |
| `vllm-5b6f494778-7l7rx` | kubeserve-mn-worker3 | 57 | – |

`kubeserve-mn-worker2`: 51 · `kubeserve-mn-worker3`: 57

HTTP 200 lines in the access logs: 108; successful attempts seen by the client: 108.
