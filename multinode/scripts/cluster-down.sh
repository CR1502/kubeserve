#!/usr/bin/env bash
# Delete the multi-node kind cluster (the single-node `kubeserve` cluster is untouched).
set -euo pipefail
cd "$(dirname "$0")/../.."

if kind get clusters 2>/dev/null | grep -qx kubeserve-mn; then
  kind delete cluster --name kubeserve-mn
else
  echo "kind cluster 'kubeserve-mn' does not exist"
fi
