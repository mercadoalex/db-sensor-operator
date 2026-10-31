#!/usr/bin/env bash
#
# bench/collectors/cdc/up.sh — deploy the CDC-derived Approach into the benchmark
# cluster: official Debezium Operator + Lactifluus + the bench DatabaseSensor.
#
# Requires `make -C bench setup` to have created the cluster and PostgreSQL.
#
# Standing costs this Approach adds (disclosed in the report, never hidden):
#   - the Debezium Operator and a running Debezium Server (JVM) pod
#   - a PostgreSQL replication slot, which retains WAL until it is consumed
#   - an initial snapshot of every monitored table on first start
#
# Env: CLUSTER (db-sensor-bench) NS (bench) IMG (db-sensor-operator:bench)
#      DEBEZIUM_OPERATOR_VERSION (3.6.3) PROM_OPERATOR_VERSION (v0.76.0)
#
set -euo pipefail

CLUSTER="${CLUSTER:-db-sensor-bench}"
NS="${NS:-bench}"
IMG="${IMG:-db-sensor-operator:bench}"
DEBEZIUM_OPERATOR_VERSION="${DEBEZIUM_OPERATOR_VERSION:-3.6.3}"
PROM_OPERATOR_VERSION="${PROM_OPERATOR_VERSION:-v0.76.0}"
OP_NS="db-sensor"
CTX="kind-${CLUSTER}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../../.." && pwd)"
K="kubectl --context ${CTX}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "ERROR: '$1' not found on PATH." >&2; exit 1; }; }
need docker; need kind; need kubectl; need helm

$K -n "$NS" get statefulset bench-postgres >/dev/null 2>&1 || {
  echo "ERROR: benchmark PostgreSQL not found. Run 'make -C bench setup' first." >&2; exit 1; }

echo "==> [1/5] Prometheus Operator CRDs (the operator watches ServiceMonitor/PrometheusRule)"
$K apply --server-side \
  -f "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/${PROM_OPERATOR_VERSION}/example/prometheus-operator-crd/monitoring.coreos.com_servicemonitors.yaml" \
  -f "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/${PROM_OPERATOR_VERSION}/example/prometheus-operator-crd/monitoring.coreos.com_prometheusrules.yaml" >/dev/null

echo "==> [2/5] official Debezium Operator ${DEBEZIUM_OPERATOR_VERSION}"
helm repo add debezium https://charts.debezium.io >/dev/null 2>&1 || true
helm repo update debezium >/dev/null
helm upgrade --install debezium-operator debezium/debezium-operator \
  --version "$DEBEZIUM_OPERATOR_VERSION" --kube-context "$CTX" \
  -n debezium --create-namespace --wait --timeout 300s >/dev/null

echo "==> [3/5] build the Lactifluus image and load it into kind"
(cd "$REPO_ROOT" && make docker-build IMG="$IMG")
kind load docker-image "$IMG" --name "$CLUSTER"

echo "==> [4/5] install Lactifluus (operator HTTP server on :8080)"
helm upgrade --install db-sensor "${REPO_ROOT}/charts/db-sensor-operator" \
  --kube-context "$CTX" -n "$OP_NS" --create-namespace \
  --set operatorHttp.enabled=true \
  --set-string manager.image.repository="${IMG%:*}" \
  --set-string manager.image.tag="${IMG##*:}" \
  --set-string manager.image.pullPolicy=Never \
  --wait --timeout 180s >/dev/null

echo "==> [5/5] apply the bench DatabaseSensor"
$K apply -f "${HERE}/sensor.yaml"
$K -n "$NS" wait --for=condition=Available deployment/bench-cdc --timeout=240s 2>/dev/null || \
  echo "    (DebeziumServer deployment not Available yet; check: kubectl -n ${NS} get pods)"

echo "==> CDC Approach deployed. Read counts with: bench/collectors/cdc/read.sh"
