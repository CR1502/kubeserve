#!/usr/bin/env bash
# Bring up the multi-node KubeServe cluster and deploy vLLM (1 replica). Idempotent:
# re-running skips what exists (but re-applying the vLLM manifests resets it to 1 replica).
#   1. kind cluster kubeserve-mn: 1 control-plane (ingress, host :30080) + 3 workers
#   2. pin each node container to its own CPUs (pin-node-cpus.sh)
#   3. check worker labels, keep cluster add-ons (CoreDNS, metrics-server) on the control-plane
#   4. namespace, load the vLLM image into the workers, apply multinode/k8s/vllm, wait
#   5. print node -> cpuset -> pods
# Needs a Docker VM with >= 10 CPUs, e.g.
#   colima start --cpu 10 --memory 13 --disk 100 --vm-type vz --mount-type virtiofs
set -euo pipefail
cd "$(dirname "$0")/../.."

CLUSTER=kubeserve-mn
CTX="kind-$CLUSTER"
NS=kubeserve
IMAGE=kubeserve-vllm:v1
WORKERS="$CLUSTER-worker $CLUSTER-worker2 $CLUSTER-worker3"
NODES="$CLUSTER-control-plane $WORKERS"
MODELS_DIR="$(pwd)/models"
RENDERED=multinode/cluster/kind-multinode.yaml

k() { kubectl --context "$CTX" "$@"; }

if [ ! -d "$MODELS_DIR/Qwen2.5-0.5B-Instruct" ]; then
  echo "Model not found in $MODELS_DIR -- run 'make model' first." >&2
  exit 1
fi

# --- 1. cluster -------------------------------------------------------------------------
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "kind cluster '$CLUSTER' already exists"
else
  if kind get clusters 2>/dev/null | grep -qx kubeserve; then
    echo "The single-node cluster 'kubeserve' exists and holds host port 30080." >&2
    echo "Delete it first ('make down'; it is reproducible with 'make cluster')." >&2
    exit 1
  fi
  sed "s|\${MODELS_DIR}|$MODELS_DIR|g" multinode/cluster/kind-multinode.yaml.tpl > "$RENDERED"
  kind create cluster --config "$RENDERED" --wait 180s
fi

# --- 2. per-node CPU pinning -------------------------------------------------------------
multinode/scripts/pin-node-cpus.sh
k wait --for=condition=Ready node --all --timeout=180s

# --- 3. labels and add-on placement ------------------------------------------------------
for n in $WORKERS; do
  if [ "$(k get node "$n" -o jsonpath='{.metadata.labels.kubeserve\.io/role}')" != inference ]; then
    echo "label kubeserve.io/role=inference missing on $n; adding it"
    k label node "$n" kubeserve.io/role=inference --overwrite
  fi
done
# CoreDNS already tolerates the control-plane taint; pin it there so the workers run only
# vLLM (plus the kube-proxy/kindnet DaemonSets) and killing a worker never takes DNS down.
# A merge patch keeps the existing kubernetes.io/os selector; it is a no-op when re-run.
k -n kube-system patch deployment coredns --type merge \
  -p '{"spec":{"template":{"spec":{"nodeSelector":{"node-role.kubernetes.io/control-plane":""}}}}}'
k -n kube-system rollout status deployment/coredns --timeout=180s

k apply -k multinode/k8s/metrics-server
k -n kube-system rollout status deployment/metrics-server --timeout=180s

# --- 4. vLLM -----------------------------------------------------------------------------
k apply -f k8s/namespace.yaml
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  docker build -t "$IMAGE" --build-arg KUBESERVE_VERSION=v1 serving/vllm
fi
# Only the workers run vLLM. kind's own "already present" check never matches here (Docker's
# containerd image store reports the OCI index digest, crictl in the node the config digest),
# so it would re-copy the 4.5 GB image into every worker on every run. Compare the index
# digest via ctr instead and load only into workers that lack it.
want_id="$(docker image inspect -f '{{.Id}}' "$IMAGE")"
missing=""
for n in $WORKERS; do
  have="$(docker exec "$n" ctr -n k8s.io images ls "name==docker.io/library/$IMAGE" 2>/dev/null | awk 'NR>1 {print $3}')"
  [ "$have" = "$want_id" ] || missing="$missing,$n"
done
if [ -n "$missing" ]; then
  kind load docker-image --name "$CLUSTER" --nodes "${missing#,}" "$IMAGE"
else
  echo "$IMAGE ($want_id) already on all workers"
fi
k apply -k multinode/k8s/vllm
k -n "$NS" rollout status deployment/vllm --timeout=600s

# --- 5. summary ----------------------------------------------------------------------------
echo
printf '%-28s %-6s %-8s %-11s %-6s %s\n' NODE READY CPUSET EFFECTIVE ROLE VLLM_PODS
for node in $NODES; do
  ready="$(k get node "$node" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')"
  cpuset="$(docker inspect -f '{{.HostConfig.CpusetCpus}}' "$node")"
  effective="$(docker exec "$node" cat /sys/fs/cgroup/cpuset.cpus.effective)"
  role="$(k get node "$node" -o jsonpath='{.metadata.labels.kubeserve\.io/role}')"
  pods="$(k -n "$NS" get pods -l app=vllm --field-selector "spec.nodeName=$node" -o go-template \
    --template '{{range .items}}{{.metadata.name}}({{if .metadata.deletionTimestamp}}Terminating{{else}}{{.status.phase}}{{end}}) {{end}}')"
  printf '%-28s %-6s %-8s %-11s %-6s %s\n' "$node" "$ready" "$cpuset" "$effective" "${role:--}" "${pods:--}"
done
echo
echo "vLLM: http://localhost:30080/v1  (namespace $NS, deployment vllm, kubectl context $CTX)"
