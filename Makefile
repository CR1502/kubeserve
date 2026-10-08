# KubeServe: vLLM on a local single-node Kubernetes (kind) cluster vs. a FastAPI baseline.
# Run `make help` for the targets. Typical end-to-end run:
#   make vm model cluster images deploy bench hpa-demo rolling-update-demo

MODEL       ?= Qwen/Qwen2.5-0.5B-Instruct
MODEL_DIR   ?= models/Qwen2.5-0.5B-Instruct
VM_CPUS     ?= 9
VM_MEMORY   ?= 9
REPEATS     ?= 2
# Keep a Mac awake during long runs (a closed lid still sleeps it without an external display;
# the load generator detects that and the benchmark re-runs the affected level).
AWAKE       := $(shell command -v caffeinate >/dev/null && echo caffeinate -is)
NS          := kubeserve

.PHONY: help vm model cluster images deploy status bench report hpa-demo rolling-update-demo down vm-stop

help:  ## Show this help
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-22s %s\n", $$1, $$2}'

vm:  ## Start the Colima Linux VM that hosts Docker + kind (macOS)
	colima start --cpu $(VM_CPUS) --memory $(VM_MEMORY) --disk 100 --vm-type vz --mount-type virtiofs

model:  ## Download the model weights once into ./models
	uvx --from huggingface_hub hf download $(MODEL) --local-dir $(MODEL_DIR)

cluster:  ## Create the single-node kind cluster (+ metrics-server)
	./scripts/cluster-up.sh

images:  ## Build the vLLM (v1, v2) and baseline images and load them into kind
	./scripts/load-images.sh

deploy:  ## Deploy vLLM (Deployment + Service + HPA) and the baseline
	kubectl apply -k k8s/vllm
	kubectl apply -k k8s/baseline
	kubectl -n $(NS) rollout status deployment/baseline --timeout=300s
	kubectl -n $(NS) rollout status deployment/vllm --timeout=600s

status:  ## Show pods, HPA and per-pod CPU/memory
	kubectl -n $(NS) get pods,svc,hpa -o wide
	-kubectl -n $(NS) top pods

bench:  ## Benchmark vLLM and baseline (REPEATS interleaved runs each, default 2) + report
	uv sync -q
	for r in $$(seq 1 $(REPEATS)); do \
	  for t in vllm baseline; do \
	    $(AWAKE) uv run python -m bench.run_benchmark --target $$t --out results/raw/$$t-run$$r.json || exit 1; \
	  done; \
	done
	uv run python -m bench.report

report:  ## Rebuild results/results.{json,md} and the chart from results/raw/
	uv run python -m bench.report

hpa-demo:  ## Drive load at vLLM and record the HPA scaling 1 -> 2 -> 1 replicas
	$(AWAKE) uv run python -m bench.hpa_demo

rolling-update-demo:  ## Roll vLLM v1 -> v2 -> v1 under load and count dropped requests
	$(AWAKE) uv run python -m bench.rolling_update_demo

down:  ## Delete the kind cluster
	kind delete cluster --name kubeserve

vm-stop:  ## Stop the Colima VM (frees its memory on the host)
	colima stop
