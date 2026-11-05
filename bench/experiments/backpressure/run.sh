#!/usr/bin/env bash
#
# Backpressure — a consumer slower than the database retains WAL on the slot.
#
# Risk: the slot holds WAL until the consumer confirms. A slow/stuck consumer makes WAL grow
# without bound; the disk fills and PostgreSQL goes read-only. This matters whether or not
# content policies are ever built — it is the slot-fills-the-disk risk the product keeps citing.
#
# Hypothesis: with a sink much slower than the write rate, the slot's retained WAL grows steadily
# and does not recover while the lag persists; the operator's inspector sees retainedBytes climb,
# so it could detect its own backlog before the disk is at risk.
#
# Pass: retained WAL grows monotonically under load with the slow sink, and the inspected
#       slot facts reflect it. Fail: retained WAL stays flat (the slot is not the pressure point).
#
# Note: the bench disk is large, so this measures the TREND, not an actual disk-full event.
# Prerequisites: make -C bench setup && bench/collectors/cdc/up.sh
# Env: CLUSTER (db-sensor-bench) NS (bench) ROWS_PER_TICK (2000) TICKS (6)
set -uo pipefail
CLUSTER="${CLUSTER:-db-sensor-bench}"; NS="${NS:-bench}"
ROWS_PER_TICK="${ROWS_PER_TICK:-2000}"; TICKS="${TICKS:-6}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K="kubectl --context kind-${CLUSTER}"
PSQL() { $K -n "$NS" exec -i bench-postgres-0 -- psql -U bench -d bench -v ON_ERROR_STOP=1 "$@"; }
retained() { PSQL -At -c "select pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) from pg_replication_slots where slot_name='bench_bench_cdc'"; }
retained_h() { PSQL -At -c "select pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) from pg_replication_slots where slot_name='bench_bench_cdc'"; }
say() { printf '\n== %s\n' "$*"; }

$K -n "$NS" get statefulset bench-postgres >/dev/null 2>&1 || { echo "ERROR: run setup + cdc/up.sh first." >&2; exit 1; }

say "point the sensor's sink at the deliberately slow consumer (2s per event)"
$K -n "$NS" apply -f "${HERE}/slow-sink.yaml" >/dev/null
$K -n "$NS" rollout status deploy/slow-sink --timeout=90s >/dev/null 2>&1 || true
$K -n "$NS" patch databasesensor bench-cdc --type=merge \
  -p '{"spec":{"transport":{"type":"http","http":{"url":"http://slow-sink.bench.svc:8080/"}}}}' >/dev/null
echo "waiting for Debezium to attach to the slow sink..."
sleep 45

say "baseline retained WAL"; echo "retained: $(retained_h) ($(retained) bytes)"

say "drive writes faster than a 2s/event sink can drain, and watch retained WAL per tick"
printf "%-6s %-16s %-12s\n" "tick" "retained_bytes" "human"
for t in $(seq 1 "$TICKS"); do
  PSQL -c "insert into public.orders (customer, amount) select (g%100)+1, (g%500)+1 from generate_series(1,${ROWS_PER_TICK}) g" >/dev/null
  sleep 10
  printf "%-6s %-16s %-12s\n" "$t" "$(retained)" "$(retained_h)"
done

say "what the operator's inspector reports about the slot (its own view of the backlog)"
# trigger a fresh inspection by bumping the sensor generation, then read status
$K -n "$NS" patch databasesensor bench-cdc --type=merge -p '{"spec":{"snapshot":{"inspection":{"enabled":true,"intervalSeconds":0}}}}' >/dev/null 2>&1 || true
sleep 20
$K -n "$NS" get databasesensor bench-cdc -o jsonpath='{.status.snapshot.inspection.postgres.replicationSlot}{"\n"}' 2>/dev/null || echo "(inspection not enabled / no slot facts)"
$K -n "$NS" get databasesensor bench-cdc -o jsonpath='{range .status.conditions[?(@.type=="SlotRetentionHealthy")]}SlotRetentionHealthy={.status} {.reason}{"\n"}{end}' 2>/dev/null

say "RELIEF CHECK — restore a fast sink and see if retained WAL recovers"
$K -n "$NS" patch databasesensor bench-cdc --type=merge \
  -p '{"spec":{"transport":{"http":{"url":"http://db-sensor-db-sensor-operator-operator-http.db-sensor.svc:8080/cdc"}}}}' >/dev/null
sleep 40
echo "retained after relief: $(retained_h) ($(retained) bytes)"

say "READ THE RESULT — fill RESULTS.md"
cat <<'TXT'
Expect: retained WAL climbs each tick while the slow sink is attached (the slot cannot advance
faster than the consumer acks), then falls once a fast sink drains the backlog. If the inspector
shows retainedBytes rising / SlotRetentionHealthy degrading, the operator can see its own backlog
— the hook for a backpressure reaction. If retained stays flat, the slot was not the bottleneck
here and the hypothesis needs rework.

Cleanup:
  kubectl -n bench delete deploy slow-sink ; kubectl -n bench delete svc slow-sink ; kubectl -n bench delete configmap slow-sink-code
  (sink already restored to the operator above)
TXT
