#!/usr/bin/env bash
# Pin every node container of the kubeserve-mn kind cluster to its own CPUs, so each
# worker behaves like a separate 3-core machine:
#   control-plane 0 | worker 1-3 | worker2 4-6 | worker3 7-9   (needs a 10-CPU Docker VM)
# Idempotent: safe to re-run at any time, e.g. after `docker start` of a node.
# `docker update` stores the cpuset in the container's HostConfig, so it also applies to
# a stopped node the next time it starts. Running nodes are verified from inside
# (cgroup cpuset.cpus.effective). Exits non-zero if a running node does not match.
set -euo pipefail
cd "$(dirname "$0")/../.."

CLUSTER=kubeserve-mn
PINS="$CLUSTER-control-plane:0 $CLUSTER-worker:1-3 $CLUSTER-worker2:4-6 $CLUSTER-worker3:7-9"
NEED_CPUS=10

ncpu="$(docker info --format '{{.NCPU}}')"
if [ "$ncpu" -lt "$NEED_CPUS" ]; then
  echo "Docker VM has $ncpu CPUs; the pinning layout needs $NEED_CPUS (colima start --cpu $NEED_CPUS ...)." >&2
  exit 1
fi

rc=0
for pair in $PINS; do
  node="${pair%%:*}"
  cpus="${pair#*:}"
  if ! docker inspect "$node" >/dev/null 2>&1; then
    echo "node container $node not found (is cluster $CLUSTER up?)" >&2
    exit 1
  fi
  before="$(docker inspect -f '{{.HostConfig.CpusetCpus}}' "$node")"
  # Always re-apply: cheap, and it also repairs a running container whose cgroup drifted.
  docker update --cpuset-cpus "$cpus" "$node" >/dev/null
  running="$(docker inspect -f '{{.State.Running}}' "$node")"
  if [ "$running" = true ]; then
    effective="$(docker exec "$node" cat /sys/fs/cgroup/cpuset.cpus.effective)"
  else
    effective="(stopped)"
  fi
  status=ok
  if [ "$running" = true ] && [ "$effective" != "$cpus" ]; then
    status=MISMATCH
    rc=1
  fi
  printf '%-28s want=%-4s config_before=%-6s effective=%-9s %s\n' \
    "$node" "$cpus" "${before:-(none)}" "$effective" "$status"
done
exit "$rc"
