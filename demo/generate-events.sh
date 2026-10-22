#!/usr/bin/env bash
#
# demo/generate-events.sh — POST synthetic Debezium CDC events to the operator's
# HTTP ingestion endpoint (POST /cdc), driving the whole observability pipeline
# (metrics, lag, events/min, dashboard, status) WITHOUT a real database, Debezium
# Server, or Kafka.
#
# The payload shape matches internal/events/parser.go, which reads:
#   op            -> "c" (insert) | "u" (update) | "d" (delete) | "t" (truncate)   ("ddl" for DDL)
#   source.db     -> metric label "db"
#   source.table  -> metric label "table"
#   ts_ms         -> event time in Unix ms (used to derive lag)
#
# Modes:
#   (default)        steady stream of c/u/d events with fresh timestamps -> Running
#   --lag SECONDS    emit events whose ts_ms is SECONDS in the past -> raises
#                    debezium_lag_seconds (cross 30s to show Degraded / HighLag)
#   --ddl            emit a schema-change (DDL) event -> debezium_schema_changes_total
#   --burst N        send N events as fast as possible, then exit
#   --stop           send nothing (just a hint) — to show the NoEvents path, simply
#                    stop running this script and watch lag climb / status change
#
# Env:
#   URL      ingestion base (default http://localhost:8080) — port-forward first
#   DB       source db label   (default demo-postgres)
#   TABLES   space-separated    (default "orders users payments")
#   RATE     events per second  (default 5)
#
# Usage:
#   ./demo/generate-events.sh                 # steady healthy stream (Ctrl-C to stop)
#   ./demo/generate-events.sh --lag 60        # stale events -> HighLag / Degraded
#   ./demo/generate-events.sh --ddl           # one DDL event
#   ./demo/generate-events.sh --burst 200     # 200 events then exit
#
set -euo pipefail

URL="${URL:-http://localhost:8080}"
DB="${DB:-demo-postgres}"
TABLES=(${TABLES:-orders users payments})
RATE="${RATE:-5}"

LAG_SECONDS=0
DDL_ONLY=false
BURST=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --lag)   LAG_SECONDS="${2:?--lag needs SECONDS}"; shift 2 ;;
    --ddl)   DDL_ONLY=true; shift ;;
    --burst) BURST="${2:?--burst needs N}"; shift 2 ;;
    --stop)  echo "To demo NoEvents: just don't run this script and watch lag climb."; exit 0 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

command -v curl >/dev/null 2>&1 || { echo "ERROR: curl not found" >&2; exit 1; }

now_ms() { echo $(( $(date +%s) * 1000 )); }
rand_choice() { local arr=("$@"); echo "${arr[$((RANDOM % ${#arr[@]}))]}"; }

# post_event OP TABLE TS_MS
post_event() {
  local op="$1" table="$2" ts="$3"
  local body
  body=$(cat <<JSON
{"payload":{"op":"${op}","ts_ms":${ts},"source":{"db":"${DB}","table":"${table}","ts_ms":${ts}}}}
JSON
)
  # 204 No Content on success; 400 on malformed. -s silent, -o /dev/null, print code.
  local code
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    -H 'Content-Type: application/json' --data "$body" "${URL}/cdc" || echo "000")
  echo "  op=${op} table=${table} ts_ms=${ts} -> HTTP ${code}"
}

# post_ddl TABLE
post_ddl() {
  local table="$1" ts; ts=$(now_ms)
  local body
  body=$(cat <<JSON
{"payload":{"ddl":"ALTER TABLE ${table} ADD COLUMN demo_col text","ts_ms":${ts},"source":{"db":"${DB}","table":"${table}","ts_ms":${ts}}}}
JSON
)
  local code
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    -H 'Content-Type: application/json' --data "$body" "${URL}/cdc" || echo "000")
  echo "  DDL table=${table} -> HTTP ${code}  (debezium_schema_changes_total++)"
}

echo "Posting synthetic CDC events to ${URL}/cdc"
echo "  db=${DB} tables=${TABLES[*]} rate=${RATE}/s lag=${LAG_SECONDS}s"
echo

if $DDL_ONLY; then
  post_ddl "$(rand_choice "${TABLES[@]}")"
  exit 0
fi

ops=(c u d)
count=0
sleep_s=$(awk "BEGIN { printf \"%.3f\", 1.0/${RATE} }")

while true; do
  op=$(rand_choice "${ops[@]}")
  table=$(rand_choice "${TABLES[@]}")
  ts=$(( $(now_ms) - LAG_SECONDS * 1000 ))
  post_event "$op" "$table" "$ts"
  count=$((count + 1))
  if [[ "$BURST" -gt 0 && "$count" -ge "$BURST" ]]; then
    echo
    echo "Sent ${count} events (burst)."
    break
  fi
  sleep "$sleep_s"
done
