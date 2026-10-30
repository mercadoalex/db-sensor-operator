#!/usr/bin/env bash
#
# bench/collectors/cdc/read.sh — read the CDC-derived metrics from the operator.
#
# Goes through the Kubernetes API service proxy, so no port-forward is needed.
# Prints the raw debezium_* series for the benchmark database (db="bench").
#
# Usage:
#   ./bench/collectors/cdc/read.sh            # all debezium_* series
#   ./bench/collectors/cdc/read.sh events     # debezium_events_total only
#
set -euo pipefail

CLUSTER="${CLUSTER:-db-sensor-bench}"
OP_NS="${OP_NS:-db-sensor}"
SVC="${SVC:-db-sensor-db-sensor-operator-operator-http}"
FILTER="${1:-}"

PATTERN='^debezium_'
case "$FILTER" in
  events) PATTERN='^debezium_events_total' ;;
  lag)    PATTERN='^debezium_lag_seconds' ;;
  ddl)    PATTERN='^debezium_schema_changes_total' ;;
  "") ;;
  *) echo "usage: $0 [events|lag|ddl]" >&2; exit 1 ;;
esac

kubectl --context "kind-${CLUSTER}" get --raw \
  "/api/v1/namespaces/${OP_NS}/services/${SVC}:8080/proxy/metrics" | grep -E "$PATTERN" || true
