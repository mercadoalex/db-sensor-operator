#!/usr/bin/env bash
#
# Attribution, approach 4 — in-band identity through the PostgreSQL server log.
#
# Question: if a workload declares who it is (application_name, and a SQL comment tag), and the
# server log prints the transaction id next to it, can a CDC event be joined to the workload
# EXACTLY by txId, with no time window?
#
# Two logging variants, because log_statement logs a statement BEFORE it runs (the transaction
# id may not exist yet) while log_min_duration_statement logs AFTER it ran:
#   A. log_statement = 'mod'
#   B. log_min_duration_statement = 0 (log every statement, with its duration)
# Each variant runs two sessions at the same time, so a time window could not tell them apart.
#
# Pass: for each session, the xid in a log line that carries its application_name equals the
#       txId of its CDC event.
# Fail: the xid is missing (0) or differs, so the join falls back to a time window.
#
# This changes server settings with ALTER SYSTEM and the sensor's sink URL. Both are restored on
# exit (also on error). Bench cluster only.
#
# Prerequisites: make -C bench setup && bench/collectors/cdc/up.sh
# Env: CLUSTER (db-sensor-bench) NS (bench)
set -uo pipefail
CLUSTER="${CLUSTER:-db-sensor-bench}"; NS="${NS:-bench}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CTX="kind-${CLUSTER}"
ORIG_URL="http://db-sensor-db-sensor-operator-operator-http.db-sensor.svc:8080/cdc"
OUT="$(mktemp -d)"
say() { printf '\n== %s\n' "$*"; }
k() { kubectl --context "$CTX" -n "$NS" "$@"; }
psql_pod() { k exec -i bench-postgres-0 -- "$@"; }
PSQL() { psql_pod psql -U bench -d bench -v ON_ERROR_STOP=1 "$@"; }

cleanup() {
  say "cleanup: restore server settings, sink URL, remove test rows and receiver"
  PSQL -c "alter system reset log_line_prefix" -c "alter system reset log_statement" \
       -c "alter system reset log_min_duration_statement" -c "select pg_reload_conf()" >/dev/null 2>&1
  k patch databasesensor bench-cdc --type=merge \
    -p "{\"spec\":{\"transport\":{\"http\":{\"url\":\"${ORIG_URL}\"}}}}" >/dev/null 2>&1
  PSQL -c "delete from public.orders where customer in (811,812,821,822)" >/dev/null 2>&1
  k delete pod/inband-receiver svc/inband-receiver configmap/inband-receiver-code --ignore-not-found >/dev/null 2>&1
  echo "outputs kept in: $OUT"
}
trap cleanup EXIT

k get statefulset bench-postgres >/dev/null 2>&1 || {
  echo "ERROR: run 'make -C bench setup' and 'bench/collectors/cdc/up.sh' first." >&2; exit 1; }

say "settings before (restored on exit)"
PSQL -At -F'|' -c "select name, setting from pg_settings where name in ('log_line_prefix','log_statement','log_min_duration_statement') order by 1"

say "point the sink at a receiver that prints txId and the customer"
k apply -f "${HERE}/receiver.yaml" >/dev/null
k wait --for=condition=ready pod/inband-receiver --timeout=90s >/dev/null 2>&1 || true
k patch databasesensor bench-cdc --type=merge \
  -p '{"spec":{"transport":{"type":"http","http":{"url":"http://inband-receiver.bench.svc:8080/"}}}}' >/dev/null
echo "waiting for Debezium to restart against the receiver..."
sleep 45

session() {  # $1 = application_name, $2 = customer, $3 = tag
  k exec -i bench-postgres-0 -- env "PGAPPNAME=$1" psql -U bench -d bench -v ON_ERROR_STOP=1 -At <<SQL
begin;
insert into public.orders (customer, amount) values ($2, 1.00) /* workload=$3 */;
select pg_sleep(1);
select 'GROUND_TRUTH app=' || current_setting('application_name') || ' customer=$2 xid=' || txid_current();
commit;
SQL
}

run_variant() {  # $1 = label, $2 = customerA, $3 = customerB
  local label="$1" ca="$2" cb="$3"
  say "variant ${label}: two sessions at the same time"
  local t0; t0="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  session "job/inband-a" "$ca" "job-a" >"$OUT/${label}-a.txt" 2>&1 &
  session "job/inband-b" "$cb" "job-b" >"$OUT/${label}-b.txt" 2>&1 &
  wait
  cat "$OUT/${label}-a.txt" "$OUT/${label}-b.txt" | grep GROUND_TRUTH
  sleep 12
  k logs bench-postgres-0 --since-time="$t0" 2>/dev/null | grep 'app=job/inband' >"$OUT/${label}-pglog.txt"
  k logs inband-receiver --since-time="$t0" 2>/dev/null | grep '^EVENT' | grep -E "customer=(${ca}|${cb})\b" >"$OUT/${label}-cdc.txt"
  echo "-- PostgreSQL log lines with the workload name:"; cat "$OUT/${label}-pglog.txt"
  echo "-- CDC events:"; cat "$OUT/${label}-cdc.txt"
}

PSQL -c "alter system set log_line_prefix = '%m [%p] app=%a xid=%x '" \
     -c "alter system set log_statement = 'mod'" \
     -c "alter system set log_min_duration_statement = -1" -c "select pg_reload_conf()" >/dev/null
sleep 2
run_variant A 811 812

PSQL -c "alter system set log_statement = 'none'" \
     -c "alter system set log_min_duration_statement = 0" -c "select pg_reload_conf()" >/dev/null
sleep 2
run_variant B 821 822

say "READ THE RESULT"
echo "  Compare, per session: GROUND_TRUTH xid, the xid in the log lines that carry the workload name,"
echo "  and the txId of the CDC event for the same customer."
