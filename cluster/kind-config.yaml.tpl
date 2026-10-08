# Single-node kind cluster for KubeServe (rendered by scripts/cluster-up.sh).
# One node acts as both control plane and worker -- this is a local dev cluster,
# not a multi-node production topology.
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: kubeserve
nodes:
  - role: control-plane
    # Model weights are downloaded once on the host and mounted read-only into the
    # node, then into pods via hostPath (production would use a PVC / object store).
    extraMounts:
      - hostPath: ${MODELS_DIR}
        containerPath: /models
        readOnly: true
    # NodePorts exposed on the host: 30080 -> vLLM, 30081 -> baseline.
    extraPortMappings:
      - containerPort: 30080
        hostPort: 30080
      - containerPort: 30081
        hostPort: 30081
    kubeadmConfigPatches:
      # Static CPU manager: Guaranteed pods with integer CPU requests get exclusive
      # cores (a dedicated cpuset), so the inference pods never share cores with
      # each other and vLLM can pin its OpenMP threads inside that cpuset.
      - |
        kind: KubeletConfiguration
        cpuManagerPolicy: static
        reservedSystemCPUs: "0"
