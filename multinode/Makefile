# KubeServe multi-node extension: vLLM on a kind cluster with 1 control-plane + 3 workers,
# all of them containers in one Colima VM on one laptop (see multinode/README.md).
#
# Run from the repo root:   make -C multinode <target>     (or `make <target>` in multinode/)
# Every recipe first cds to the repo root, because the Python modules (multinode.bench.*,
# bench.*) and uv's pyproject.toml live there.
#
# Typical order:   make -C multinode vm cluster scaling failure report

ROOT     := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
RAW      := multinode/results/raw
# The worker cpusets (control-plane 0, workers 1-3 / 4-6 / 7-9) need a 10-CPU Docker VM.
VM_CPUS   ?= 10
VM_MEMORY ?= 13
# Keep a Mac awake during long runs (a closed lid still sleeps it on battery; the harness
# detects a sleep and rejects the affected attempt).
AWAKE    := $(shell command -v caffeinate >/dev/null && echo caffeinate -is)
PY       := $(AWAKE) uv run python -m
CTX      := kind-kubeserve-mn

SHELL       := /bin/bash
.SHELLFLAGS := -o pipefail -c

.PHONY: help vm cluster status scaling scaling-run1 scaling-run2 failure failure-node-kill \
        failure-process-crash failure-node-kill-retry report down

help:  ## Show this help
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-26s %s\n", $$1, $$2}'

vm:  ## Start the Colima VM sized for this layout (10 vCPU, 13 GiB); macOS only
	colima start --cpu $(VM_CPUS) --memory $(VM_MEMORY) --disk 100 --vm-type vz --mount-type virtiofs

cluster:  ## Create kind cluster kubeserve-mn, pin node CPUs, deploy vLLM (1 replica)
	cd $(ROOT) && ./multinode/scripts/cluster-up.sh

status:  ## Show nodes and vLLM pods (with the node each pod runs on)
	kubectl --context $(CTX) get nodes -o wide
	kubectl --context $(CTX) -n kubeserve get pods,svc,endpointslices -o wide

scaling: scaling-run1 scaling-run2 report  ## Both scaling sweeps (node order 1,2,3 then 3,2,1) + report

# scaling.py refuses to overwrite its JSON; refuse here too, before `tee` would truncate the log.
define no_overwrite
	@if [ -e $(ROOT)/$(RAW)/$(1).json ]; then \
	  echo "$(RAW)/$(1).json exists; move it away to re-run this sweep (the report reads every scaling-run*.json)" >&2; exit 1; fi
endef

scaling-run1:  ## Scaling sweep 1: 1, 2, 3 replicas x default concurrency levels
	$(call no_overwrite,scaling-run1)
	cd $(ROOT) && mkdir -p $(RAW) && $(PY) multinode.bench.scaling --replicas 1,2,3 --out $(RAW)/scaling-run1.json \
	  2>&1 | tee $(RAW)/scaling-run1.log

scaling-run2:  ## Scaling sweep 2: replica order reversed (3, 2, 1)
	$(call no_overwrite,scaling-run2)
	cd $(ROOT) && mkdir -p $(RAW) && $(PY) multinode.bench.scaling --replicas 3,2,1 --out $(RAW)/scaling-run2.json \
	  2>&1 | tee $(RAW)/scaling-run2.log

failure: failure-node-kill failure-process-crash failure-node-kill-retry report  ## All failure tests + report

failure-node-kill:  ## docker kill a worker node under load (no client retry)
	cd $(ROOT) && $(PY) multinode.bench.failure_test --scenario node-kill

failure-process-crash:  ## SIGKILL the vLLM process in one pod under load (no client retry)
	cd $(ROOT) && $(PY) multinode.bench.failure_test --scenario process-crash

failure-node-kill-retry:  ## node-kill again, with the client retrying a failed request once
	cd $(ROOT) && $(PY) multinode.bench.failure_test --scenario node-kill --retry

report:  ## Rebuild multinode/results/results.{json,md}, charts and README blocks from the data
	cd $(ROOT) && uv run python -m multinode.bench.report

down:  ## Delete the kind cluster kubeserve-mn (the single-node cluster is untouched)
	cd $(ROOT) && ./multinode/scripts/cluster-down.sh
