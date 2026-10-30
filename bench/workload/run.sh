#!/usr/bin/env bash
#
# bench/workload/run.sh — run the deterministic Workload_Generator against the
# benchmark PostgreSQL.
#
# Env (all optional; defaults are the documented benchmark parameters):
#   SEED       fixed seed                      (default 42)
#   INSERT_PCT insert share of the mix         (default 50)
#   UPDATE_PCT update share of the mix         (default 40)
#   DELETE_PCT delete share of the mix         (default 10)
#   RATE       operations per second           (default 50)
#   DURATION   seconds                         (default 60)
#   CLUSTER    kind cluster name               (default db-sensor-bench)
#   NS         benchmark namespace             (default bench)
#
# Usage:
#   ./bench/workload/run.sh
#   RATE=200 DURATION=120 ./bench/workload/run.sh
#
set -euo pipefail

SEED="${SEED:-42}"
INSERT_PCT="${INSERT_PCT:-50}"
UPDATE_PCT="${UPDATE_PCT:-40}"
DELETE_PCT="${DELETE_PCT:-10}"
RATE="${RATE:-50}"
DURATION="${DURATION:-60}"
CLUSTER="${CLUSTER:-db-sensor-bench}"
NS="${NS:-bench}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CTX="kind-${CLUSTER}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "ERROR: '$1' not found on PATH." >&2; exit 1; }; }
need kubectl

psql_exec() {
  kubectl --context "$CTX" -n "$NS" exec -i bench-postgres-0 -- \
    psql -U bench -d bench -v ON_ERROR_STOP=1 "$@"
}

echo "==> installing workload procedure"
psql_exec -q < "${HERE}/workload.sql"

echo "==> running workload: seed=${SEED} mix=${INSERT_PCT}/${UPDATE_PCT}/${DELETE_PCT} rate=${RATE}/s duration=${DURATION}s"
# CALL must run outside an explicit transaction block so the procedure can COMMIT.
psql_exec -c "CALL bench.run_workload(${SEED}, ${INSERT_PCT}, ${UPDATE_PCT}, ${DELETE_PCT}, ${RATE}, ${DURATION});"
