#!/usr/bin/env bash
# Create the single-node kind cluster and install metrics-server.
set -euo pipefail
cd "$(dirname "$0")/.."

MODELS_DIR="$(pwd)/models"
if [ ! -d "$MODELS_DIR/Qwen2.5-0.5B-Instruct" ]; then
  echo "Model not found in $MODELS_DIR -- run 'make model' first." >&2
  exit 1
fi

if kind get clusters 2>/dev/null | grep -qx kubeserve; then
  echo "kind cluster 'kubeserve' already exists"
else
  sed "s|\${MODELS_DIR}|$MODELS_DIR|" cluster/kind-config.yaml.tpl > cluster/kind-config.yaml
  kind create cluster --config cluster/kind-config.yaml --wait 120s
fi

kubectl apply -k k8s/metrics-server
kubectl -n kube-system rollout status deployment/metrics-server --timeout=180s
kubectl apply -f k8s/namespace.yaml
