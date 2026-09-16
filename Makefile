# Fallow -- an idle-workload reclamation controller.
#
# Generated files (the CRD, deepcopy, RBAC) are produced by controller-gen,
# which is pinned in go.mod as a tool dependency -- so `go tool controller-gen`
# works on a clean checkout with no separate install step and no version drift
# between contributors.

SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

KUBECTL ?= kubectl
NAMESPACE ?= fallow-system
CRD := config/crd/fallow.dev_reclaimpolicies.yaml
GENERATED := $(CRD) api/v1alpha1/zz_generated.deepcopy.go config/rbac/role.yaml

##@ General

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} \
		/^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Development

.PHONY: build
build: ## Build the manager binary into bin/
	go build -o bin/manager ./cmd/manager

.PHONY: test
test: ## Run all tests
	go test ./... -count=1

.PHONY: cover
cover: ## Run tests and open a coverage report
	go test ./... -count=1 -coverprofile=cover.out
	go tool cover -html=cover.out

.PHONY: fmt
fmt: ## Format the code
	go fmt ./...

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: tidy
tidy: ## Tidy go.mod and go.sum
	go mod tidy

.PHONY: check
check: fmt vet test ## Format, vet and test -- what CI should run

.PHONY: demo
demo: ## Walk the escalation ladder against an in-memory cluster
	go run ./cmd/demo

.PHONY: run
run: manifests generate ## Run the controller locally against the current kubecontext
	go run ./cmd/manager --leader-elect=false

##@ Code generation

.PHONY: generate
generate: ## Regenerate deepcopy functions
	go tool controller-gen object:headerFile=/dev/null paths=./api/...

.PHONY: manifests
manifests: ## Regenerate the CRD and RBAC from source markers
	go tool controller-gen crd rbac:roleName=fallow-manager-role paths=./... \
		output:crd:artifacts:config=config/crd \
		output:rbac:artifacts:config=config/rbac

.PHONY: verify
verify: ## Fail if the generated files are stale
	@tmp=$$(mktemp -d); \
	for f in $(GENERATED); do cp $$f $$tmp/$$(echo $$f | tr / _); done; \
	$(MAKE) --no-print-directory generate manifests; \
	status=0; \
	for f in $(GENERATED); do \
		diff -u $$tmp/$$(echo $$f | tr / _) $$f || status=1; \
	done; \
	rm -rf $$tmp; \
	if [ $$status -ne 0 ]; then \
		echo "generated files are stale -- run 'make generate manifests'"; exit 1; \
	fi; \
	echo "generated files are up to date"

##@ Cluster

.PHONY: install
install: manifests ## Install the ReclaimPolicy CRD into the current cluster
	$(KUBECTL) apply -f $(CRD)

.PHONY: uninstall
uninstall: ## Remove the CRD (and with it every ReclaimPolicy)
	$(KUBECTL) delete --ignore-not-found -f $(CRD)

.PHONY: install-rbac
install-rbac: manifests ## Create the controller namespace, service account and roles
	$(KUBECTL) create namespace $(NAMESPACE) --dry-run=client -o yaml | $(KUBECTL) apply -f -
	$(KUBECTL) apply -k config/rbac

.PHONY: uninstall-rbac
uninstall-rbac: ## Remove the controller's RBAC and namespace
	$(KUBECTL) delete --ignore-not-found -k config/rbac
	$(KUBECTL) delete --ignore-not-found namespace $(NAMESPACE)

.PHONY: samples
samples: ## Apply the sample policies (dry-run starter, notify-only, ephemeral)
	$(KUBECTL) apply -k config/samples

.PHONY: status
status: ## Show what the installed policies are doing
	$(KUBECTL) get reclaimpolicies

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf bin cover.out
