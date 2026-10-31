#!/usr/bin/env bash
#
# bench/collectors/cdc/down.sh — remove the CDC Approach, including the PostgreSQL
# replication slot (otherwise it keeps retaining WAL after Debezium is gone).
#
set -euo pipefail

CLUSTER="${CLUSTER:-db-sensor-bench}"
NS="${NS:-bench}"
CTX="kind-${CLUSTER}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K="kubectl --context ${CTX}"

echo "==> deleting the bench DatabaseSensor (the DebeziumServer is garbage collected)"
$K delete -f "${HERE}/sensor.yaml" --ignore-not-found --wait=true

echo "==> uninstalling Lactiflluus and the Debezium Operator"
helm uninstall db-sensor -n db-sensor --kube-context "$CTX" 2>/dev/null || true
helm uninstall debezium-operator -n debezium --kube-context "$CTX" 2>/dev/null || true

echo "==> dropping leftover replication slots"
$K -n "$NS" exec bench-postgres-0 -- psql -U bench -d bench -tAc \
  "select pg_drop_replication_slot(slot_name) from pg_replication_slots where not active" 2>/dev/null || true

echo "CDC Approach removed."
