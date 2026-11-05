#!/usr/bin/env bash
#
# C2 — skipped / duplicate sequence detection on the CDC stream.
#
# Hypothesis: a stateful stream consumer can detect a gap (invoice_number jumps 1003 -> 1007)
# or a duplicate, from the ordered CDC stream — something a counter or a GROUP BY cannot do,
# because it depends on order and history, not totals.
#
# Also tested, as the honest contras:
#   - ORDER: does the consumer see events in commit order?
#   - RESTART: what does the consumer lose when it restarts mid-stream? (state is in memory)
#
# Pass: gaps and duplicates are detected from the stream. Restart behavior is characterized.
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
  echo "ERROR: run 'make -C bench setup' and 'bench/collectors/cdc/up.sh' first." >&2; exit 1; }

say "create an invoices table with a sequence column, and capture it via the sensor"
PSQL -c "create table if not exists public.invoices (id bigserial primary key, invoice_number bigint not null, amount numeric(12,2) not null default 0)" >/dev/null
# The sensor must watch invoices: patch spec.tables, and repoint the sink at our consumer.
$K -n "$NS" apply -f "${HERE}/consumer.yaml" >/dev/null
$K -n "$NS" rollout status deploy/seqgap-consumer --timeout=90s >/dev/null 2>&1 || true
$K -n "$NS" patch databasesensor bench-cdc --type=merge \
  -p '{"spec":{"tables":["public.orders","public.users","public.payments","public.invoices"],"transport":{"type":"http","http":{"url":"http://seqgap-consumer.bench.svc:8080/"}}}}' >/dev/null
echo "waiting for Debezium to restart against the consumer and pick up invoices..."
sleep 50

say "PART 1 — a clean run then a gap: insert 1001..1005, then 1009 (skips 1006..1008)"
for n in 1001 1002 1003 1004 1005; do PSQL -c "insert into public.invoices (invoice_number, amount) values ($n, 10)" >/dev/null; done
sleep 6
PSQL -c "insert into public.invoices (invoice_number, amount) values (1009, 10)" >/dev/null
sleep 6
echo "consumer state:"; state

say "PART 2 — a duplicate: insert 1009 again"
PSQL -c "insert into public.invoices (invoice_number, amount) values (1009, 10)" >/dev/null
sleep 6
echo "consumer state:"; state

say "PART 3 — RESTART: kill the consumer, then insert 1010 (the correct next number)"
$K -n "$NS" rollout restart deploy/seqgap-consumer >/dev/null
$K -n "$NS" rollout status deploy/seqgap-consumer --timeout=90s >/dev/null 2>&1 || true
sleep 5
PSQL -c "insert into public.invoices (invoice_number, amount) values (1010, 10)" >/dev/null
sleep 6
echo "consumer state after restart (note: 'last' is empty-started; 1010 looks like a first value, so a real gap right after a restart would be MISSED):"; state

say "READ THE RESULT — fill RESULTS.md"
cat <<'TXT'
Expect in PART 1/2: findings list a 'gap' at 1009 (prev=1005) and a 'duplicate' at 1009.
PART 3 shows the restart contra: in-memory state is lost, so the first number after a restart
sets a new baseline and a gap straddling the restart is not caught. That is the honest limit of
a memory-only consumer; persisting 'last' per sequence would fix it (more work).

Cleanup:
  kubectl -n bench patch databasesensor bench-cdc --type=merge -p '{"spec":{"tables":["public.orders","public.users","public.payments"],"transport":{"http":{"url":"http://db-sensor-db-sensor-operator-operator-http.db-sensor.svc:8080/cdc"}}}}'
  kubectl -n bench delete deploy seqgap-consumer ; kubectl -n bench delete svc seqgap-consumer ; kubectl -n bench delete configmap seqgap-consumer-code
  psql: drop table public.invoices;
TXT
