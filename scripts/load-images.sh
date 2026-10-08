#!/usr/bin/env bash
# Build the serving images and side-load them into the kind node (no registry needed).
set -euo pipefail
cd "$(dirname "$0")/.."

docker build -t kubeserve-baseline:v1 serving/baseline
# v1 and v2 are the same server; v2 exists so the rolling-update demo has a real
# image change to roll out.
docker build -t kubeserve-vllm:v1 --build-arg KUBESERVE_VERSION=v1 serving/vllm
docker build -t kubeserve-vllm:v2 --build-arg KUBESERVE_VERSION=v2 serving/vllm

kind load docker-image --name kubeserve kubeserve-baseline:v1 kubeserve-vllm:v1 kubeserve-vllm:v2
