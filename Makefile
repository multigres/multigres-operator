###----------------------------------------
##   Variables
#------------------------------------------

# Version from git tags (for root module - operator binary)
# Root module uses tags like v0.1.0 (without prefix)
VERSION ?= $(shell git describe --tags --match "v*" --always --dirty 2>/dev/null || echo "v0.0.1-dev")
VERSION_SHORT ?= $(shell echo $(VERSION) | sed 's/^v//')

# Image configuration
# IMG is pinned to the last-built tag (written by the container target) when available.
# This prevents tag drift between docker build and kind load caused by git state
# changes (staging files, new commits) between make invocations.
IMG_PREFIX ?= ghcr.io/multigres
IMG_REPO ?= multigres-operator
IMG_TAG_FILE ?= .last-built-tag
IMG ?= $(if $(wildcard $(IMG_TAG_FILE)),$(IMG_PREFIX)/$(IMG_REPO):$(shell cat $(IMG_TAG_FILE)),$(IMG_PREFIX)/$(IMG_REPO):$(VERSION_SHORT))

# Observer tool configuration
OBSERVER_IMG ?= $(if $(wildcard $(IMG_TAG_FILE)),$(IMG_PREFIX)/multigres-observer:$(shell cat $(IMG_TAG_FILE)),$(IMG_PREFIX)/multigres-observer:$(VERSION_SHORT))

.PHONY: print-img
print-img: ## Print the full operator container image reference
	@echo $(IMG)

# Images required by MultigresCluster pods (must match pkg/testutil/e2e.go MultigresImages)
E2E_IMAGES ?= ghcr.io/multigres/multigres:main ghcr.io/multigres/pgctld:main ghcr.io/multigres/multiadmin-web:main gcr.io/etcd-development/etcd:v3.6.7

.PHONY: pull-e2e-images
pull-e2e-images: ## Pull container images needed by e2e tests
	@for img in $(E2E_IMAGES); do \
		echo "Pulling $$img..."; \
		docker pull $$img; \
	done

# Build metadata
BUILD_DATE ?= $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
GIT_COMMIT ?= $(shell git rev-parse HEAD 2>/dev/null || echo "unknown")

# LDFLAGS for version info
LDFLAGS := -X main.version=$(VERSION) \
           -X main.buildDate=$(BUILD_DATE) \
           -X main.gitCommit=$(GIT_COMMIT)

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
# Defaults to docker when unset
CONTAINER_TOOL ?= docker
CONTAINER_TOOL := $(if $(strip $(CONTAINER_TOOL)),$(CONTAINER_TOOL),docker)

# Kind cluster name for local development
KIND_CLUSTER ?= multigres-operator-dev

# TODO(user): To use a different vendor for e2e tests, modify the setup under 'tests/e2e'.
# The default setup assumes Kind is pre-installed and builds/loads the Manager Docker image locally.
# CertManager is installed by default; skip with:
# - CERT_MANAGER_INSTALL_SKIP=true

# Local kubeconfig for kind cluster (doesn't modify user's ~/.kube/config)
KIND_KUBECONFIG ?= $(shell pwd)/kubeconfig.yaml

# Upstream images used by MultigresCluster data plane components.
# Pre-pulled to local Docker cache and loaded into kind to avoid slow pulls
# inside the cluster on every kind-deploy cycle.
MULTIGRES_IMAGES ?= $(shell sed -n 's/.*= "\(.*\)"$$/\1/p' api/v1alpha1/image_defaults.go | sort -u)

# Observability stack images, pre-loaded only for kind-deploy-observability.
OBSERVABILITY_IMAGES ?= \
	grafana/grafana:11.4.0 \
	grafana/tempo:2.7.2 \
	otel/opentelemetry-collector-contrib:0.120.0

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

# Remove target files when a recipe fails, preventing stale artifacts.
.DELETE_ON_ERROR:

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
# renovate: datasource=github-releases depName=kubernetes-sigs/kustomize
KUSTOMIZE_VERSION ?= v5.6.0
# renovate: datasource=github-releases depName=kubernetes-sigs/controller-tools
CONTROLLER_TOOLS_VERSION ?= v0.18.0
# renovate: datasource=github-releases depName=golangci/golangci-lint
GOLANGCI_LINT_VERSION ?= v2.12.2

CERT_MANAGER_VERSION ?= v1.19.2

## Envtest
#ENVTEST_VERSION is the version of controller-runtime release branch to fetch the envtest setup script (i.e. release-0.20)
ENVTEST_VERSION ?= $(shell go list -m -f "{{ .Version }}" sigs.k8s.io/controller-runtime | awk -F'[v.]' '{printf "release-%d.%d", $$2, $$3}')
#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
# NOTE: This version should match the version defined in nix/devshell.nix
ENVTEST_K8S_VERSION ?= 1.35

###----------------------------------------
##   Commands
#------------------------------------------

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook paths="./api/...;./pkg/webhook/..." output:crd:artifacts:config=config/crd/bases output:rbac:artifacts:config=config/rbac output:webhook:artifacts:config=config/webhook
.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object paths="./api/...;./pkg/webhook/..."
# $(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code
	go vet ./...

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	$(GOLANGCI_LINT) config verify

##@ Code Quality

.PHONY: check
check: lint test ## Run all checks before committing (lint + test)
	@echo "==> All checks passed!"

.PHONY: verify
verify: manifests generate ## Verify generated files are up to date
	@echo "==> Verifying generated files are committed"
	@git diff --exit-code config/crd api/ || { \
		echo "ERROR: Generated files are out of date."; \
		echo "Run 'make manifests generate' and commit the changes."; \
		exit 1; \
	}
	@echo "==> Verification passed!"

.PHONY: verify-warn
verify-warn: manifests generate ## Verify generated files are up to date (non-blocking, warns only)
	@echo "==> Verifying generated files are committed"
	@if ! git diff --exit-code config/crd api/ >/dev/null 2>&1; then \
		echo ""; \
		echo "========================================"; \
		echo "WARNING: Generated files are out of date"; \
		echo "========================================"; \
		echo ""; \
		echo "The following files have changes:"; \
		git diff --stat config/crd api/; \
		echo ""; \
		echo "This is not blocking the build, but you should run"; \
		echo "'make manifests generate' and commit the changes"; \
		echo "before merging to ensure CRDs are synchronized."; \
		echo ""; \
		echo "========================================"; \
		exit 0; \
	else \
		echo "==> Verification passed!"; \
	fi

.PHONY: pre-commit
pre-commit: fmt vet lint test ## Run full pre-commit checks (fmt, vet, lint, test)
	@echo "==> Pre-commit checks passed!"


##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary with version metadata
	@echo "==> Building operator binary (version: $(VERSION))"
	go build -ldflags="$(LDFLAGS)" -o bin/multigres-operator cmd/multigres-operator/main.go

.PHONY: build-multigres-gc
build-multigres-gc: fmt vet ## Build the multigres garbage-collector binary
	@echo "==> Building multigres-gc binary (version: $(VERSION))"
	go build -ldflags="$(LDFLAGS)" -o bin/multigres-gc cmd/multigres-gc/main.go

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/multigres-operator/main.go

# Cross-platform builds are handled natively via --platform=$BUILDPLATFORM in the Dockerfile.
# Use docker-buildx target or pass --platform to docker build for multi-arch images.
.PHONY: container
container: ## Build container image
	$(CONTAINER_TOOL) build \
		--build-arg VERSION=$$(git rev-parse --short HEAD) \
		--build-arg GIT_COMMIT=$$(git rev-parse HEAD) \
		--build-arg BUILD_DATE=$$(date -u +%Y-%m-%dT%H:%M:%SZ) \
		-t $(IMG_PREFIX)/$(IMG_REPO):$(VERSION_SHORT) .
	@echo $(VERSION_SHORT) > $(IMG_TAG_FILE)

.PHONY: minikube-load
minikube-load:
	minikube image load ${IMG}

.PHONY: container-push
container-push: ## Push container image
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image.
PLATFORMS ?= linux/arm64,linux/amd64
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for cross-platform support
	- $(CONTAINER_TOOL) buildx create --name multigres-operator-builder
	$(CONTAINER_TOOL) buildx use multigres-operator-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) \
		--build-arg VERSION=$$(git rev-parse --short HEAD) \
		--build-arg GIT_COMMIT=$$(git rev-parse HEAD) \
		--build-arg BUILD_DATE=$$(date -u +%Y-%m-%dT%H:%M:%SZ) \
		--tag ${IMG} .
	- $(CONTAINER_TOOL) buildx rm multigres-operator-builder

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate consolidated install YAMLs under dist/.
	mkdir -p dist
	# kustomize has no build-time image override, so we mutate then restore.
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default > dist/install.yaml
	$(KUSTOMIZE) build config/installer-crds > dist/install-crds.yaml
	$(KUSTOMIZE) build config/installer-rbac > dist/install-rbac.yaml
	$(KUSTOMIZE) build config/deploy-certmanager > dist/install-certmanager.yaml
	$(KUSTOMIZE) build config/deploy-observability > dist/install-observability.yaml
	@git checkout -- config/manager/kustomization.yaml 2>/dev/null || true

##@ Test

.PHONY: test
test: manifests generate fmt vet ## Run tests (no integration testing)
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" \
		go test -p 1 $$(go list ./... | grep -v /e2e) -coverprofile=cover.out

.PHONY: test-integration
test-integration: manifests generate fmt vet setup-envtest ## Run integration tests
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" \
		go test -p 1 -tags=integration,verbose $$(go list ./... | grep -v /e2e) -coverprofile=cover.out

.PHONY: test-coverage
test-coverage: manifests generate fmt vet setup-envtest ## Generate coverage report with HTML
	@mkdir -p coverage
	@echo "==> Generating coverage..."
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" \
		go test -p 1 -tags=integration,verbose ./... -coverprofile=coverage/combined.out -covermode=atomic
	@echo "==> Generating HTML report..."
	@go tool cover -html=coverage/combined.out -o=coverage/combined.html
	@echo "Generated: coverage/combined.html"
	@echo "==> Calculating total coverage..."
	@go tool cover -func=coverage/combined.out | tail -1
	@echo ""
	@echo "Coverage reports in coverage/"
	@echo "  - Data: coverage/combined.out (for CI/codecov)"
	@echo "  - HTML: coverage/combined.html"

##@ Test End-to-End
# Each e2e test creates its own ephemeral Kind cluster via e2e-framework.
# The Makefile only needs to build the operator image — the Go tests handle
# cluster lifecycle, image loading, CRD installation, and operator deployment.
#
# Set E2E_KEEP_CLUSTERS to control cluster cleanup:
#   never      (default) always destroy clusters
#   on-failure keep clusters from failed tests for debugging
#   always     never destroy clusters

# E2E_TEST_SPEC: comma-separated list of test names to run (default: all).
# Example: make test-e2e E2E_TEST_SPEC="minimal,deletion"
E2E_TEST_SPEC ?=
E2E_PACKAGES_SHARED = $(if $(E2E_TEST_SPEC),$(foreach t,$(subst $(comma),$(space),$(E2E_TEST_SPEC)),./test/e2e/shared/$(t)/),./test/e2e/shared/...)
E2E_PACKAGES_DEDICATED = $(if $(E2E_TEST_SPEC),$(foreach t,$(subst $(comma),$(space),$(E2E_TEST_SPEC)),./test/e2e/dedicated/$(t)/),./test/e2e/dedicated/...)
comma := ,
space := $(empty) $(empty)

.PHONY: test-e2e
test-e2e: manifests generate fmt vet container ## Run e2e tests (shared cluster, fast)
	OPERATOR_IMG=$(IMG) \
	REPO_ROOT=$(shell pwd) \
	go test -tags=e2e $(E2E_PACKAGES_SHARED) -p 3 -v -count=1 -timeout=20m

.PHONY: test-e2e-keep
test-e2e-keep: manifests generate fmt vet container ## Run e2e tests; keep cluster on failure
	OPERATOR_IMG=$(IMG) \
	REPO_ROOT=$(shell pwd) \
	E2E_KEEP_CLUSTERS=on-failure \
	go test -tags=e2e $(E2E_PACKAGES_SHARED) -p 3 -v -count=1 -timeout=20m

.PHONY: test-e2e-full
test-e2e-full: manifests generate fmt vet container ## Run e2e tests (dedicated cluster per test, full isolation)
	OPERATOR_IMG=$(IMG) \
	REPO_ROOT=$(shell pwd) \
	go test -tags=e2e $(E2E_PACKAGES_DEDICATED) -v -count=1 -timeout=30m -parallel 2

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) apply --server-side -f -

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build config/default | $(KUBECTL) apply --server-side -f -
	@git checkout -- config/manager/kustomization.yaml 2>/dev/null || true

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/default | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

## Kind deployment helpers
# Install CRDs into the kind cluster
define kind-install-crds
	@echo "==> Installing CRDs..."
	KUBECONFIG=$(KIND_KUBECONFIG) $(KUSTOMIZE) build config/crd | \
		KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) apply --server-side -f -
endef

# Load a single image into every node of the kind cluster.
define kind-load-image
	@echo "  Loading $(1)..."
	@for node in $$($(KIND) get nodes --name $(KIND_CLUSTER)); do \
		$(CONTAINER_TOOL) save $(1) | $(CONTAINER_TOOL) exec -i $$node ctr -n k8s.io images import -; \
	done
endef

##@ Kind Cluster (Local Development)

.PHONY: kind-up
kind-up: ## Create a kind cluster for local development
	@command -v $(KIND) >/dev/null 2>&1 || { \
		echo "ERROR: kind is not installed."; \
		echo "Install it from: https://kind.sigs.k8s.io/docs/user/quick-start/"; \
		exit 1; \
	}
	@if $(KIND) get clusters | grep -q "^$(KIND_CLUSTER)$$"; then \
		echo "Kind cluster '$(KIND_CLUSTER)' already exists."; \
		echo "==> Exporting kubeconfig to $(KIND_KUBECONFIG)"; \
		$(KIND) get kubeconfig --name $(KIND_CLUSTER) > $(KIND_KUBECONFIG); \
	else \
		echo "Creating kind cluster '$(KIND_CLUSTER)'..."; \
		$(KIND) create cluster --name $(KIND_CLUSTER) --kubeconfig $(KIND_KUBECONFIG); \
	fi
	$(MAKE) kind-label-nodes
	@echo "==> Cluster ready. Use: export KUBECONFIG=$(KIND_KUBECONFIG)"

# Single-zone topology labels for the dev cluster.
.PHONY: kind-label-nodes
kind-label-nodes: ## Label kind nodes with the single-zone topology labels samples select on
	@echo "==> Labeling nodes with zone us-central1-a / region us-central1..."
	@KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) label nodes --all --overwrite \
		topology.k8s.aws/zone-id=us-central1-a \
		topology.kubernetes.io/region=us-central1

.PHONY: kind-up-topology
kind-up-topology: ## Create a multi-node kind cluster with topology zone ID labels
	@command -v $(KIND) >/dev/null 2>&1 || { \
		echo "ERROR: kind is not installed."; \
		echo "Install it from: https://kind.sigs.k8s.io/docs/user/quick-start/"; \
		exit 1; \
	}
	@if $(KIND) get clusters | grep -q "^$(KIND_CLUSTER)$$"; then \
		echo "Kind cluster '$(KIND_CLUSTER)' already exists."; \
		echo "==> Exporting kubeconfig to $(KIND_KUBECONFIG)"; \
		$(KIND) get kubeconfig --name $(KIND_CLUSTER) > $(KIND_KUBECONFIG); \
	else \
		echo "Creating kind cluster '$(KIND_CLUSTER)' with topology zones..."; \
		$(KIND) create cluster --name $(KIND_CLUSTER) --kubeconfig $(KIND_KUBECONFIG) \
			--config config/kind/kind-config-topology.yaml; \
	fi
	@echo "==> Cluster ready (3 workers with zone ID labels). Use: export KUBECONFIG=$(KIND_KUBECONFIG)"

.PHONY: kind-deploy-topology
kind-deploy-topology: kind-up-topology manifests kustomize kind-load kind-load-images ## Deploy operator to multi-node kind cluster with topology zones
	$(call kind-install-crds)
	@echo "==> Deploying operator..."
	cd config/manager && $(KUSTOMIZE) edit set image controller=$(IMG)
	KUBECONFIG=$(KIND_KUBECONFIG) $(KUSTOMIZE) build config/default | KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) apply --server-side -f -
	@git checkout -- config/manager/kustomization.yaml 2>/dev/null || true
	@echo "==> Deployment complete!"
	@echo "Check status: KUBECONFIG=$(KIND_KUBECONFIG) kubectl get pods -n multigres-operator"
	$(MAKE) kind-deploy-observer

.PHONY: kind-load
kind-load: container ## Build and load image into kind cluster
	@echo "==> Loading image $(IMG) into kind cluster..."
	$(call kind-load-image,$(IMG))

.PHONY: kind-load-images
kind-load-images: ## Pull latest upstream images and load into kind (incremental, skips unchanged)
	@echo "==> Pulling latest upstream images (only downloads changes)..."
	@for img in $(MULTIGRES_IMAGES); do \
		echo "  Pulling $$img..."; \
		$(CONTAINER_TOOL) pull $$img; \
	done
	@echo "==> Loading upstream images into kind cluster..."
	@for img in $(MULTIGRES_IMAGES); do \
		echo "  Loading $$img..."; \
		for node in $$($(KIND) get nodes --name $(KIND_CLUSTER)); do \
			$(CONTAINER_TOOL) save $$img | $(CONTAINER_TOOL) exec -i $$node ctr -n k8s.io images import -; \
		done; \
	done
	@echo "==> All upstream images loaded"

.PHONY: kind-load-observability-images
kind-load-observability-images: ## Pull and load observability stack images into kind
	@echo "==> Pulling observability images..."
	@for img in $(OBSERVABILITY_IMAGES); do \
		echo "  Pulling $$img..."; \
		$(CONTAINER_TOOL) pull $$img; \
	done
	@echo "==> Loading observability images into kind cluster..."
	@for img in $(OBSERVABILITY_IMAGES); do \
		echo "  Loading $$img..."; \
		for node in $$($(KIND) get nodes --name $(KIND_CLUSTER)); do \
			$(CONTAINER_TOOL) save $$img | $(CONTAINER_TOOL) exec -i $$node ctr -n k8s.io images import -; \
		done; \
	done
	@echo "==> All observability images loaded"

.PHONY: kind-deploy
kind-deploy: kind-up manifests kustomize kind-load kind-load-images ## Deploy operator to kind cluster using webhook with self-signed certificates
	$(call kind-install-crds)
	@echo "==> Deploying operator..."
	cd config/manager && $(KUSTOMIZE) edit set image controller=$(IMG)
	KUBECONFIG=$(KIND_KUBECONFIG) $(KUSTOMIZE) build config/default | KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) apply --server-side -f -
	@git checkout -- config/manager/kustomization.yaml 2>/dev/null || true
	@echo "==> Deployment complete!"
	@echo "Check status: KUBECONFIG=$(KIND_KUBECONFIG) kubectl get pods -n multigres-operator"
	$(MAKE) kind-deploy-observer

.PHONY: kind-deploy-certmanager
kind-deploy-certmanager: kind-up install-certmanager manifests kustomize kind-load kind-load-images ## Deploy operator to kind cluster using cert manager
	$(call kind-install-crds)
	@echo "==> Deploying operator (Cert-Manager Mode)..."
	cd config/manager && $(KUSTOMIZE) edit set image controller=$(IMG)
	KUBECONFIG=$(KIND_KUBECONFIG) $(KUSTOMIZE) build config/deploy-certmanager | \
		KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) apply --server-side -f -
	@git checkout -- config/manager/kustomization.yaml 2>/dev/null || true
	@echo "==> Deployment complete!"
	@echo "Check status: KUBECONFIG=$(KIND_KUBECONFIG) kubectl get pods -n multigres-operator"
	$(MAKE) kind-deploy-observer

.PHONY: kind-deploy-no-webhook
kind-deploy-no-webhook: kind-up manifests kustomize kind-load kind-load-images ## Deploy controller to Kind without the webhook enabled.
	$(call kind-install-crds)
	@echo "==> Deploying operator..."
	cd config/manager && $(KUSTOMIZE) edit set image controller=$(IMG)
	KUBECONFIG=$(KIND_KUBECONFIG) $(KUSTOMIZE) build config/no-webhook | KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) apply --server-side -f -
	@git checkout -- config/manager/kustomization.yaml 2>/dev/null || true
	@echo "==> Deployment complete!"
	@echo "Check status: KUBECONFIG=$(KIND_KUBECONFIG) kubectl get pods -n multigres-operator"
	$(MAKE) kind-deploy-observer

.PHONY: kind-deploy-observability
kind-deploy-observability: kind-up manifests kustomize kind-load kind-load-images kind-load-observability-images ## Deploy operator with full observability stack (Prometheus Operator, OTel Collector, Tempo, Grafana)
	$(call kind-install-crds)
	@echo "==> Installing Prometheus Operator..."
	KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) apply --server-side -f \
		https://github.com/prometheus-operator/prometheus-operator/releases/download/v0.80.0/bundle.yaml
	KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) wait --for=condition=Available \
		deployment/prometheus-operator -n default --timeout=120s
	@echo "==> Deploying operator with observability stack..."
	cd config/manager && $(KUSTOMIZE) edit set image controller=$(IMG)
	KUBECONFIG=$(KIND_KUBECONFIG) $(KUSTOMIZE) build config/deploy-observability | \
		KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) apply --server-side -f -
	@git checkout -- config/manager/kustomization.yaml 2>/dev/null || true
	@echo "==> Waiting for observability stack..."
	KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) wait --for=condition=Available \
		deployment/otel-collector deployment/tempo deployment/grafana \
		-n multigres-operator --timeout=180s
	KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) rollout status \
		statefulset/prometheus-multigres -n multigres-operator --timeout=180s
	@echo "==> Deployment complete!"
	@echo "Run 'make kind-portforward' to port-forward Grafana, Prometheus, and Tempo"
	$(MAKE) kind-deploy-observer

.PHONY: kind-portforward
kind-portforward: ## Port-forward Grafana (3000), Prometheus (9090), Tempo (3200). Re-run if connection drops.
	@echo "==> Starting port-forwards (Ctrl+C to stop)..."
	@echo "    Grafana:    http://localhost:3000"
	@echo "    Prometheus: http://localhost:9090"
	@echo "    Tempo:      http://localhost:3200"
	@KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) port-forward svc/grafana 3000:3000 -n multigres-operator & \
	 KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) port-forward svc/prometheus 9090:9090 -n multigres-operator & \
	 KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) port-forward svc/tempo 3200:3200 -n multigres-operator & \
	 wait


.PHONY: kind-redeploy
kind-redeploy: container manifests kustomize ## Rebuild image, reload to kind, and redeploy
	@echo "==> Clearing cached image from kind node..."
	$(CONTAINER_TOOL) exec $(KIND_CLUSTER)-control-plane crictl rmi $(IMG) 2>/dev/null || true
	$(call kind-load-image,$(IMG))
	$(call kind-install-crds)
	@echo "==> Deploying operator..."
	cd config/manager && $(KUSTOMIZE) edit set image controller=$(IMG)
	KUBECONFIG=$(KIND_KUBECONFIG) $(KUSTOMIZE) build config/default | KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) apply --server-side --force-conflicts -f -
	@git checkout -- config/manager/kustomization.yaml 2>/dev/null || true
	@echo "==> Redeploy complete!"
	$(MAKE) kind-deploy-observer

.PHONY: kind-down
kind-down: ## Delete the kind cluster
	@echo "==> Deleting kind cluster '$(KIND_CLUSTER)'..."
	$(KIND) delete cluster --name $(KIND_CLUSTER)
	@rm -f $(KIND_KUBECONFIG) $(IMG_TAG_FILE)
	@echo "==> Cluster and kubeconfig deleted"

##@ Observer

.PHONY: observer-build
observer-build: ## Build the observer container image
	$(CONTAINER_TOOL) build -t $(OBSERVER_IMG) -f tools/observer/Dockerfile tools/observer/

.PHONY: kind-load-observer
kind-load-observer: observer-build ## Build and load observer image into kind
	$(call kind-load-image,$(OBSERVER_IMG))

.PHONY: kind-deploy-observer
kind-deploy-observer: kind-load-observer ## Deploy observer alongside the operator
	@echo "==> Deploying multigres observer..."
	cd tools/observer/deploy/base && $(KUSTOMIZE) edit set image observer=$(OBSERVER_IMG)
	KUBECONFIG=$(KIND_KUBECONFIG) $(KUSTOMIZE) build tools/observer/deploy/base | \
		KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) apply --server-side -f -
	@git checkout -- tools/observer/deploy/base/kustomization.yaml 2>/dev/null || true

.PHONY: kind-undeploy-observer
kind-undeploy-observer: ## Remove observer
	KUBECONFIG=$(KIND_KUBECONFIG) $(KUSTOMIZE) build tools/observer/deploy/base | \
		KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) delete --ignore-not-found -f -

##@ Dependencies

.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@$(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
# golangci-lint's own go.mod selects an older toolchain than this module
# targets, and a linter built with a lower Go version refuses to run. Pin the
# build toolchain to the one resolved by this module's go.mod.
$(GOLANGCI_LINT): export GOTOOLCHAIN = $(shell go env GOVERSION)
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

.PHONY: install-certmanager
install-certmanager: ## Install Cert-Manager into the cluster
	@echo "==> Installing Cert-Manager $(CERT_MANAGER_VERSION)..."
	KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) apply -f https://github.com/cert-manager/cert-manager/releases/download/$(CERT_MANAGER_VERSION)/cert-manager.yaml
	@echo "==> Waiting for Cert-Manager to be ready..."
	KUBECONFIG=$(KIND_KUBECONFIG) $(KUBECTL) wait --for=condition=Available deployment --all -n cert-manager --timeout=300s

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f $(1) ;\
GOBIN=$(LOCALBIN) go install $${package} ;\
mv $(1) $(1)-$(3) ;\
} ;\
ln -sf $$(realpath $(1)-$(3)) $(1)
endef

##@ Backward Compatibility Aliases

.PHONY: docker-build
docker-build: container ## Alias for container (backward compatibility)

.PHONY: docker-push
docker-push: container-push ## Alias for container-push (backward compatibility)
