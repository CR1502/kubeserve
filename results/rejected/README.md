# Rejected runs (excluded from all results)

The laptop went to sleep during these runs, so their numbers are wrong. They are kept
only as a record. `bench/report.py` reads `results/raw/` only and never this folder.

| File | What happened |
|---|---|
| `benchmark.log`, `vllm-run1.json` | First sweep. The lid was closed on battery ("Clamshell Sleep" in `pmset -g log`) during the vLLM concurrency-16 level. That level shows 18.4 tok/s with the pod averaging 0.43 busy cores, against 68.7 tok/s at concurrency 8 in the same sweep, because no requests completed for ~5.5 minutes. This sweep ran before the host-sleep guard existed. |
| `baseline-run1-partial.json`, `benchmark-run1-attempt.log` | The host slept for ~58 s during the baseline concurrency-8 level. The load generator's sleep guard (wall clock vs. awake-only clock) raised `HostSleptError` and the level was not recorded. The whole baseline sweep was re-run. |
| `benchmark-run1-attempt2.log` | The baseline sweep was restarted, and the host slept again ("Clamshell Sleep", 27 s, on AC power) during concurrency 1. The guard rejected it. After this the benchmark retries just the affected level instead of aborting, and logs each rejected attempt in the run's JSON under `rejected_attempts`. |
| `../raw/benchmark-run2.log` | Second baseline sweep, on battery with the lid closed: macOS cycled through "Maintenance Sleep", and all 5 attempts at concurrency 1 were rejected (~118, 93, 338, 125 and 67 s asleep). The sweep was abandoned with no data written, so the baseline has one complete sweep. (vLLM's second sweep, earlier in the same log, ran without a sleep and is used.) |
