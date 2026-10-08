# vLLM OpenAI-compatible server, CPU backend (arm64).
# The upstream image already contains vLLM + a CPU build of PyTorch; this layer only
# pins the version, bakes in the serving defaults and stamps a version so that a
# rolling update has a real image change to roll out.
FROM vllm/vllm-openai-cpu:v0.31.0-arm64

ARG KUBESERVE_VERSION=v1
LABEL org.opencontainers.image.title="kubeserve-vllm" \
      org.opencontainers.image.version="${KUBESERVE_VERSION}"

# Run the engine core inside the API-server process instead of a separate process.
# Measured on this machine: ~3.4 GiB vs ~3.9 GiB RSS per replica with equal throughput,
# which is what lets two replicas fit on the single 9 GiB kind node.
ENV KUBESERVE_VERSION=${KUBESERVE_VERSION} \
    VLLM_ENABLE_V1_MULTIPROCESSING=0

# Make vLLM's KV-cache memory check ignore reclaimable page cache inside the pod's
# cgroup (see the script for the failure it fixes).
COPY patch_cgroup_memory.py /tmp/patch_cgroup_memory.py
RUN python3 /tmp/patch_cgroup_memory.py && rm /tmp/patch_cgroup_memory.py

ENTRYPOINT ["vllm", "serve"]
