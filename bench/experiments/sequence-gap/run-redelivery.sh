#!/usr/bin/env bash
#
# C2, version 1 — the confirmed/processed frontier where it actually lives: in Debezium.
#
# In our architecture the sequence consumer receives HTTP POSTs; it does NOT confirm the slot.
# Debezium owns the offset. So the redelivery that would cause a false duplicate comes from
# restarting DEBEZIUM with an in-memory offset, not from restarting the consumer.
#
# Hypothesis (owner's, adapted to our architecture): with the default in-memory offset, killing
# Debezium mid-stream makes it re-deliver already-seen events, and a stateful sequence consumer
# then raises a FALSE duplicate for a number it already processed.
#
# Pass: a false duplicate (or false gap) appears after the Debezium restart for an event that
#       was already delivered before the kill.
# Fail: no re-delivery is observed (the offset mechanism made it invisible).
#
# Prerequisites: make -C bench setup && bench/collectors/cdc/up.sh
# Env: CLUSTER (db-sensor-bench) NS (bench)
set -uo pipefail
CLUSTER="${CLUSTER:-db-sensor-bench}"; NS="${NS:-bench}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K="kubectl --context kind-${CLUSTER}"
PSQL() { $K -n "$NS" exec -i bench-postgres-0 -- psql -U bench -d bench -v ON_ERROR_STOP=1 "$@"; }
state() { $K -n "$NS" get --raw "/api/v1/namespaces/${NS}/services/seqgap-consumer:8080/proxy/state" 2>/dev/null; }
say() { printf '\n== %s\n' "$*"; }

$K -n "$NS" get statefulset bench-postgres >/dev/null 2>&1 || {
  echo "ERROR: run setup and bench/collectors/cdc/up.sh first." >&2; exit 1; }

say "set up invoices, the consumer, and point the sensor at it (offset stays default = memory)"
PSQL -c "create table if not exists public.invoices (id bigserial primary key, invoice_number bigint not null, amount numeric(12,2) not null default 0)" >/dev/null
$K -n "$NS" apply -f "${HERE}/consumer.yaml" >/dev/null
$K -n "$NS" rollout status deploy/seqgap-consumer --timeout=90s >/dev/null 2>&1 || true
$K -n "$NS" patch databasesensor bench-cdc --type=merge \
  -p '{"spec":{"tables":["public.orders","public.users","public.payments","public.invoices"],"transport":{"type":"http","http":{"url":"http://seqgap-consumer.bench.svc:8080/"}}}}' >/dev/null
echo "waiting for Debezium to pick up invoices..."
sleep 50

say "deliver a clean run 1001..1010 (consumer sees them in order)"
for n in $(seq 1001 1010); do PSQL -c "insert into public.invoices (invoice_number, amount) values ($n, 10)" >/dev/null; done
sleep 8
echo "state before the kill (expect last=1010, no findings):"; state

say "KILL Debezium (in-memory offset) while the consumer stays alive"
$K -n "$NS" delete pod -l app.kubernetes.io/instance=bench-cdc --force --grace-period=0 >/dev/null 2>&1 || \
  $K -n "$NS" delete pod -l strimzi.io/cluster=bench-cdc --force --grace-period=0 >/dev/null 2>&1 || \
  $K -n "$NS" delete pod -l app=bench-cdc --force --grace-period=0 >/dev/null 2>&1 || true
# fall back: delete by name match
$K -n "$NS" get pods -o name | grep bench-cdc | while read p; do $K -n "$NS" delete "$p" --force --grace-period=0 >/dev/null 2>&1; done
echo "waiting for Debezium to come back..."
for i in $(seq 1 24); do
  a=$($K -n "$NS" get databasesensor bench-cdc -o jsonpath='{range .status.conditions[?(@.type=="DebeziumAvailable")]}{.status}{end}' 2>/dev/null)
  [ "$a" = "True" ] && break; sleep 5
done
sleep 20

say "deliver the next correct number, 1011, after the restart"
PSQL -c "insert into public.invoices (invoice_number, amount) values (1011, 10)" >/dev/null
sleep 10

say "RESULT — did Debezium re-deliver, and did the consumer raise a false finding?"
echo "consumer state:"; state
cat <<'TXT'

Read it:
  - If count_seen jumped well past 11 and findings contain a 'duplicate' for a number in
    1001..1010, Debezium re-delivered on restart (in-memory offset) and the consumer raised a
    FALSE duplicate -> hypothesis holds; a memory offset makes sequence detection unreliable.
  - If count_seen is ~11 and findings are clean, no re-delivery happened here (snapshot.mode
    no_data means a restart resumes from the live position, so it may NOT replay committed rows).
    That would mean the frontier did not produce a false positive in this configuration -> record
    that honestly; the risk would then be about the gap during downtime, not duplicates.

Cleanup:
  kubectl -n bench patch databasesensor bench-cdc --type=merge -p '{"spec":{"tables":["public.orders","public.users","public.payments"],"transport":{"http":{"url":"http://db-sensor-db-sensor-operator-operator-http.db-sensor.svc:8080/cdc"}}}}'
  kubectl -n bench delete deploy seqgap-consumer ; kubectl -n bench delete svc seqgap-consumer ; kubectl -n bench delete configmap seqgap-consumer-code
  psql: drop table public.invoices;
TXT
