#!/usr/bin/env bash
#
# R11 follow-up — content-based policy: is CDC the only source that can evaluate a rule on
# the VALUE of a change, not its count?
#
# Scenario: a policy says "payments.status must never go from 'settled' back to 'pending'".
# A Job does exactly that on one row. Every source sees "1 update on payments". Only a source
# that carries the row's content can tell this particular update broke the rule.
#
# This runs a throwaway receiver that logs each event's before/after, pointed at by the
# Debezium sink, so we read the RAW CDC stream without touching the operator pipeline. It also
# shows the REPLICA IDENTITY effect: default identity carries no "before", FULL does.
#
# Prerequisites: make -C bench setup && bench/collectors/cdc/up.sh
# Env: CLUSTER (db-sensor-bench) NS (bench)
set -uo pipefail
CLUSTER="${CLUSTER:-db-sensor-bench}"; NS="${NS:-bench}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K="kubectl --context kind-${CLUSTER}"
PSQL() { $K -n "$NS" exec -i bench-postgres-0 -- psql -U bench -d bench -v ON_ERROR_STOP=1 "$@"; }
say() { printf '\n== %s\n' "$*"; }
rlog() { $K -n "$NS" logs rowcontent-receiver --since="${1:-60s}" 2>/dev/null | grep '^EVENT' || echo "(no events)"; }

$K -n "$NS" get statefulset bench-postgres >/dev/null 2>&1 || {
  echo "ERROR: run 'make -C bench setup' and 'bench/collectors/cdc/up.sh' first." >&2; exit 1; }

say "add a status column to payments and point the sensor's sink at the logging receiver"
PSQL -c "alter table public.payments add column if not exists status text not null default 'pending'" >/dev/null
PSQL -c "update public.payments set status='settled' where id <= 5" >/dev/null
# Repoint the DebeziumServer sink URL at our receiver by patching the sensor's transport.
$K -n "$NS" apply -f "${HERE}/receiver.yaml" >/dev/null
$K -n "$NS" wait --for=condition=ready pod/rowcontent-receiver --timeout=60s >/dev/null 2>&1 || true
$K -n "$NS" patch databasesensor bench-cdc --type=merge \
  -p '{"spec":{"transport":{"type":"http","http":{"url":"http://rowcontent-receiver.bench.svc:8080/"}}}}' >/dev/null
echo "waiting for Debezium to restart against the new sink..."
sleep 45

say "CASE A — default REPLICA IDENTITY (what the bench ships): the forbidden update"
PSQL -c "update public.payments set status='pending' where id=1" >/dev/null
sleep 8
echo "raw CDC event the receiver logged:"; rlog 30s | tail -3
echo
echo "pg_stat sees only a count:"
PSQL -At -F'|' -c "select relname, n_tup_upd from pg_stat_user_tables where relname='payments'"
echo "  -> 'payments had N updates'. It cannot tell this one set status back to 'pending'."

say "CASE B — REPLICA IDENTITY FULL: same forbidden update, now with the old values"
PSQL -c "alter table public.payments replica identity full" >/dev/null
sleep 5
PSQL -c "update public.payments set status='pending' where id=2" >/dev/null
sleep 8
echo "raw CDC event the receiver logged:"; rlog 20s | tail -3

say "READ THE RESULT"
cat <<'TXT'
Compare the two cases:
  - Default identity: the DELETE/UPDATE event carries "after" but "before" is null (only the key).
    A policy on "settled -> pending" needs the OLD value, so it cannot be evaluated. This is the
    inspector's replica-identity gap, made concrete.
  - FULL identity: "before" carries the old row, so the stream shows status went settled -> pending.
    Only CDC has this. pg_stat and the Kubernetes API never do.
Decision this informs: is a content policy worth building, and at what cost (FULL identity means
more WAL on every change to that table)?

Cleanup:
  kubectl -n bench patch databasesensor bench-cdc --type=merge -p '{"spec":{"transport":{"http":{"url":"http://db-sensor-db-sensor-operator-operator-http.db-sensor.svc:8080/cdc"}}}}'
  kubectl -n bench delete pod rowcontent-receiver ; kubectl -n bench delete svc rowcontent-receiver ; kubectl -n bench delete configmap rowcontent-receiver-code
  psql: alter table public.payments replica identity default; alter table public.payments drop column status;
TXT
