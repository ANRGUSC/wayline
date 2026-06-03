## Wayline — a data-aware DAG scheduling framework for Kubernetes.
## All commands are run from the repo root.
##
## Registry: 192.168.1.163:5000 (local k3s registry on the master node).
## Override with REGISTRY=... on any image/push target.

REGISTRY   ?= 192.168.1.163:5000
NAMESPACE  ?= wl-system
GOFLAGS    ?=

.PHONY: all build ui-build test test-v \
        image-odag-controller image-data-agent image-ui-server image-examples \
        push-all push-controllers \
        install deploy rollout \
        example-odag clean-examples clean-deploy clean-all repro-figures help

# ─── default ──────────────────────────────────────────────────────────────────

all: build ui-build

# ─── Go build ─────────────────────────────────────────────────────────────────

## Build all Go binaries into bin/ (wayline CLI + controllers + data-agent)
build:
	mkdir -p bin
	go build $(GOFLAGS) -o bin/odag-controller ./cmd/odag-controller
	go build $(GOFLAGS) -o bin/data-agent      ./cmd/data-agent
	go build $(GOFLAGS) -o bin/ui-server       ./cmd/ui-server
	go build $(GOFLAGS) -o bin/wayline         ./cmd/cli

## Build the React UI into ui/dist/
ui-build:
	cd ui && npm ci && npm run build

## Run Go unit tests
test:
	go test $(GOFLAGS) ./...

## Run Go tests with verbose output
test-v:
	go test -v $(GOFLAGS) ./...

# ─── Docker images ────────────────────────────────────────────────────────────

## Build the odag-controller image
image-odag-controller:
	docker build -f cmd/odag-controller/Dockerfile \
		-t $(REGISTRY)/wl-odag-controller:latest .

## Build the data-agent image (per-node DaemonSet)
image-data-agent:
	docker build -f cmd/data-agent/Dockerfile \
		-t $(REGISTRY)/wl-data-agent:latest .

## Build the ui-server image (includes the compiled React frontend)
image-ui-server:
	docker build -f cmd/ui-server/Dockerfile \
		-t $(REGISTRY)/wl-ui-server:latest .

## Build the dag-pipeline example task images
image-examples:
	docker build -f examples/dag-pipeline/tasks/generate/Dockerfile \
		-t $(REGISTRY)/dag-pipeline-generate:latest .
	docker build -f examples/dag-pipeline/tasks/transform/Dockerfile \
		-t $(REGISTRY)/dag-pipeline-transform:latest .
	docker build -f examples/dag-pipeline/tasks/output/Dockerfile \
		-t $(REGISTRY)/dag-pipeline-output:latest .

## Build and push the control plane + example images to the registry
push-all: push-controllers image-examples
	docker push $(REGISTRY)/dag-pipeline-generate:latest
	docker push $(REGISTRY)/dag-pipeline-transform:latest
	docker push $(REGISTRY)/dag-pipeline-output:latest

## Build and push only the control-plane images (controllers + agent + UI)
push-controllers: image-odag-controller image-data-agent image-ui-server
	docker push $(REGISTRY)/wl-odag-controller:latest
	docker push $(REGISTRY)/wl-data-agent:latest
	docker push $(REGISTRY)/wl-ui-server:latest

# ─── Cluster install / deploy ─────────────────────────────────────────────────

## Install CRDs, namespace, and RBAC into the cluster
install:
	kubectl apply -f api/v1/odag-crd.yml
	kubectl apply -f api/v1/odagtemplate-crd.yml
	kubectl apply -f deployments/namespace.yml
	kubectl apply -f deployments/odag-controller/rbac.yml
	kubectl apply -f deployments/ui-server/rbac.yml

## Deploy the control plane (data-agent DaemonSet, odag-controller, ui-server)
deploy:
	kubectl apply -f deployments/odag-controller/network-profile-configmap.yml
	kubectl apply -f deployments/data-agent/daemonset.yml
	kubectl apply -f deployments/odag-controller/deployment.yml
	kubectl apply -f deployments/ui-server/deployment.yml
	kubectl apply -f deployments/ui-server/service.yml

## Force-restart the control-plane deployments (picks up new :latest images)
rollout:
	kubectl rollout restart daemonset/data-agent      -n $(NAMESPACE)
	kubectl rollout restart deployment/odag-controller -n $(NAMESPACE)
	kubectl rollout restart deployment/ui-server       -n $(NAMESPACE)
	kubectl rollout status  deployment/odag-controller -n $(NAMESPACE)
	kubectl rollout status  deployment/ui-server       -n $(NAMESPACE)

# ─── Examples ─────────────────────────────────────────────────────────────────

## Submit the one-shot dag-pipeline example
example-odag:
	kubectl apply -f examples/dag-pipeline/odag.yml

## Delete the example from the cluster
clean-examples:
	-kubectl delete -f examples/dag-pipeline/odag.yml --ignore-not-found

# ─── Cleanup ──────────────────────────────────────────────────────────────────

## Delete all Wayline control-plane resources (keeps CRDs and namespace)
clean-deploy:
	-kubectl delete -f deployments/ui-server/service.yml         --ignore-not-found
	-kubectl delete -f deployments/ui-server/deployment.yml      --ignore-not-found
	-kubectl delete -f deployments/odag-controller/deployment.yml --ignore-not-found
	-kubectl delete -f deployments/data-agent/daemonset.yml      --ignore-not-found

## Delete everything including CRDs and namespace (destructive!)
clean-all: clean-examples clean-deploy
	-kubectl delete -f api/v1/odag-crd.yml         --ignore-not-found
	-kubectl delete -f api/v1/odagtemplate-crd.yml --ignore-not-found
	-kubectl delete -f deployments/namespace.yml   --ignore-not-found

# ─── Reproducibility ────────────────────────────────────────────────────────

## Regenerate the paper figures from committed result data (no cluster needed)
repro-figures:
	cd eval/e0-microbench            && python3 plot.py
	cd eval/synthetic-dags/e1        && python3 plot.py
	cd eval/synthetic-dags/e2        && python3 plot.py
	cd eval/synthetic-dags/scheduler && python3 plot-results.py
	cd eval/mcmt                     && python3 scripts/plot-fair.py
	@echo "figures regenerated; see eval/README.md for the claim->figure map"

# ─── Help ─────────────────────────────────────────────────────────────────────

## Show this help message
help:
	@echo "Wayline Makefile targets:"
	@echo ""
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /'
	@echo ""
	@echo "Variables:"
	@echo "  REGISTRY=$(REGISTRY)   (override with REGISTRY=... make push-all)"
	@echo "  NAMESPACE=$(NAMESPACE)"
