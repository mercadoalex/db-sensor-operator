# CDC Observability Benchmark harness. Local only: Docker + kind + kubectl.
# See bench/README.md.

CLUSTER ?= db-sensor-bench
NS      ?= bench
CTX      = kind-$(CLUSTER)
KUBECTL  = kubectl --context $(CTX)

.PHONY: help
help: ## Show targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

.PHONY: setup
setup: ## Create the kind cluster, deploy PostgreSQL, apply the schema
	@command -v kind >/dev/null || { echo "kind not found (brew install kind)"; exit 1; }
	@command -v kubectl >/dev/null || { echo "kubectl not found (brew install kubectl)"; exit 1; }
	@if kind get clusters 2>/dev/null | grep -qx "$(CLUSTER)"; then \
	  echo "==> cluster '$(CLUSTER)' already exists, reusing it"; \
	else \
	  echo "==> creating kind cluster '$(CLUSTER)'"; \
	  kind create cluster --name $(CLUSTER) --config cluster/kind-config.yaml; \
	fi
	@echo "==> deploying PostgreSQL (wal_level=logical)"
	$(KUBECTL) apply -f cluster/postgres.yaml
	$(KUBECTL) -n $(NS) rollout status statefulset/bench-postgres --timeout=180s
	@echo "==> applying schema"
	$(KUBECTL) -n $(NS) exec -i bench-postgres-0 -- psql -U bench -d bench -v ON_ERROR_STOP=1 -q < cluster/schema.sql
	@echo "==> setup complete"

.PHONY: run
run: ## Run the deterministic workload (SEED, RATE, DURATION, *_PCT overridable)
	CLUSTER=$(CLUSTER) NS=$(NS) ./workload/run.sh

.PHONY: cdc-up
cdc-up: ## Deploy the CDC Approach (Debezium Operator + Lactiflluus + sensor)
	CLUSTER=$(CLUSTER) NS=$(NS) ./collectors/cdc/up.sh

.PHONY: cdc-down
cdc-down: ## Remove the CDC Approach and its replication slot
	CLUSTER=$(CLUSTER) NS=$(NS) ./collectors/cdc/down.sh

.PHONY: collect
collect: ## Collect results artifacts (pending: tasks 8-10)
	@echo "collect: not implemented yet (measurement harness is tasks 8-10)"

.PHONY: report
report: ## Render the report from results (pending: task 11)
	@echo "report: not implemented yet (report body is task 11)"

.PHONY: teardown
teardown: ## Delete everything setup created (the kind cluster)
	@if kind get clusters 2>/dev/null | grep -qx "$(CLUSTER)"; then \
	  echo "==> deleting kind cluster '$(CLUSTER)'"; \
	  kind delete cluster --name $(CLUSTER); \
	else \
	  echo "cluster '$(CLUSTER)' not found, nothing to delete"; \
	fi
