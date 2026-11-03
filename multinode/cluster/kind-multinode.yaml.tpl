# Multi-node kind cluster for the KubeServe multi-node extension
# (rendered by multinode/scripts/cluster-up.sh; ${MODELS_DIR} -> absolute ./models path).
#
# 1 control-plane + 3 workers, all of them containers inside ONE Colima VM on ONE laptop.
# After creation multinode/scripts/pin-node-cpus.sh pins every node container to its own
# CPUs (control-plane 0, workers 1-3 / 4-6 / 7-9), so each worker behaves like a separate
# 3-core machine. Memory, disk and the physical laptop CPU are still shared.
#
# No kubeadmConfigPatches: the kubelet CPU manager stays at its default policy (`none`).
# The static policy would hand out exclusive CPUs from the kubelet's view of the machine
# (all VM CPUs), which lie outside a node's pinned cpuset.
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: kubeserve-mn
nodes:
  # Keeps kind's default node-role.kubernetes.io/control-plane:NoSchedule taint, so no
  # inference pod lands here. It is the ingress: the host reaches the vLLM NodePort
  # through this node, and kube-proxy forwards to a pod on any worker.
  - role: control-plane
    extraPortMappings:
      - containerPort: 30080
        hostPort: 30080
  # Each worker mounts the host's model weights read-only (pods use them via hostPath)
  # and carries the label the vLLM Deployment selects on.
  - role: worker
    labels:
      kubeserve.io/role: inference
    extraMounts:
      - hostPath: ${MODELS_DIR}
        containerPath: /models
        readOnly: true
  - role: worker
    labels:
      kubeserve.io/role: inference
    extraMounts:
      - hostPath: ${MODELS_DIR}
        containerPath: /models
        readOnly: true
  - role: worker
    labels:
      kubeserve.io/role: inference
    extraMounts:
      - hostPath: ${MODELS_DIR}
        containerPath: /models
        readOnly: true
