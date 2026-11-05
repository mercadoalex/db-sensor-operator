#!/usr/bin/env bash
#
# R11 — Scheduler catch-up burst: where does CDC add value over the database's own
# counters and the Kubernetes API?
#
# Scenario (simulated): a time-based scheduler was down and comes back, firing every
# missed run at once. We model the moment of the burst by creating N Kubernetes Jobs at
# the same time, each writing to the same table, one of them deleting rows it should not.
#
# Hypothesis: for a Kubernetes reaction to the data, CDC shows something the other sources
# do not — the content of each change (which rows), the exact commit order, and a link to
# the Job that caused it — not just an aggregate count.
#
# Pass:   we observe at least one thing CDC shows that counters + the Kubernetes API do not,
#         and that a reaction could key on.
# Fail:   counters + the Kubernetes API show the same, as soon; CDC adds nothing here.
# Either outcome is a valid result and is recorded in RESULTS.md.
#
# This does NOT prove real teams hit this; only user conversations (R8) can.
#
# Prerequisites (this script builds and installs nothing):
#   make -C bench setup           # kind cluster + PostgreSQL (wal_level=logical)
#   bench/collectors/cdc/up.sh    # Debezium Operator + Lactifluus + the bench DatabaseSensor
#
# Env: CLUSTER (db-sensor-bench) NS (bench) JOBS (6) DELETE_ROWS (300)
set -uo pipefail
CLUSTER="${CLUSTER:-db-sensor-bench}"; NS="${NS:-bench}"
JOBS="${JOBS:-6}"; DELETE_ROWS="${DELETE_ROWS:-300}"
K="kubectl --context kind-${CLUSTER}"
PSQL="$K -n $NS exec -i bench-postgres-0 -- psql -U bench -d bench -At"

say() { printf '\n== %s\n' "$*"; }
counters() { # one line of pg_stat counters for public.orders
  $K -n "$NS" exec -i bench-postgres-0 -- psql -U bench -d bench -At -F'|' -c \
    "select n_tup_ins, n_tup_upd, n_tup_del, n_live_tup from pg_stat_user_tables where relname='orders'" 2>/dev/null
}
# The exporter labels the table by its bare name ("orders"), not "public.orders".
cdc_orders() {
  $K -n db-sensor get --raw \
    "/api/v1/namespaces/db-sensor/services/db-sensor-db-sensor-operator-operator-http:8080/proxy/metrics" 2>/dev/null \
    | grep 'debezium_events_total' | grep 'table="orders"' || echo "(no debezium_events_total for orders yet)"
}

command -v "$(echo "$K" | awk '{print $1}')" >/dev/null || { echo "kubectl not found" >&2; exit 1; }
$K -n "$NS" get statefulset bench-postgres >/dev/null 2>&1 || {
  echo "ERROR: run 'make -C bench setup' and 'bench/collectors/cdc/up.sh' first." >&2; exit 1; }

say "baseline: pg_stat counters for public.orders, and current CDC delete count"
BEFORE_COUNTERS="$(counters)"; echo "pg_stat (ins|upd|del|live): $BEFORE_COUNTERS"
echo "CDC series before:"; cdc_orders

say "fire $JOBS Jobs at once (the catch-up burst); one deletes $DELETE_ROWS rows it should not"
BURST_START="$(date +%s)"
for i in $(seq 1 "$JOBS"); do
  if [ "$i" = "1" ]; then
    SQL="delete from public.orders where id in (select id from public.orders order by id limit ${DELETE_ROWS});"
    LABEL="delete"
  else
    SQL="insert into public.orders (customer, amount) select (g%100)+1, (g%500)+1 from generate_series(1,50) g;"
    LABEL="insert"
  fi
  cat <<EOF | $K apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: catchup-${i}-${LABEL}
  namespace: ${NS}
  labels: { experiment: scheduler-catchup }
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 600
  template:
    metadata:
      labels: { experiment: scheduler-catchup }
    spec:
      restartPolicy: Never
      containers:
        - name: writer
          image: postgres:16-alpine
          command: ["psql"]
          args: ["-v","ON_ERROR_STOP=1","-c","${SQL}"]
          env:
            - { name: PGHOST,     value: bench-postgres }
            - { name: PGUSER,     value: bench }
            - { name: PGDATABASE, value: bench }
            - name: PGPASSWORD
              valueFrom: { secretKeyRef: { name: bench-pg-creds, key: password } }
EOF
done

say "what the Kubernetes API shows about the burst (immediately)"
$K -n "$NS" get jobs -l experiment=scheduler-catchup \
  -o custom-columns=NAME:.metadata.name,CREATED:.metadata.creationTimestamp --sort-by=.metadata.creationTimestamp
echo "The API shows $JOBS Jobs created at ~the same time. It does NOT show what they wrote."

say "wait for the Jobs to finish"
$K -n "$NS" wait --for=condition=complete job -l experiment=scheduler-catchup --timeout=120s 2>&1 | tail -3 || true
sleep 8   # let CDC and pg_stat settle

say "AFTER — three sources side by side"
AFTER_COUNTERS="$(counters)"
echo "1) pg_stat counters (ins|upd|del|live): $AFTER_COUNTERS"
echo "   -> aggregate totals only: it says deletes happened, not which rows or by which Job."
echo
echo "2) Kubernetes API: $JOBS Jobs ran in the burst window starting ${BURST_START}."
echo "   -> it knows who and when, not what changed in the data."
echo
echo "3) CDC (debezium_events_total for public.orders):"
cdc_orders
echo "   -> a per-operation stream, delivered as each change commits."

say "READ THE RESULT"
cat <<'TXT'
Fill in RESULTS.md from the three blocks above. The question is narrow and honest:
  - Did CDC show anything the counters + the Kubernetes API did not, that a reaction could use?
  - As delivered here (counts by op/table), CDC overlaps a lot with pg_stat. The row content
    and the attribution to a Job are NOT exposed by our current pipeline; note that as a gap,
    not as a win. Whether to expose them is the open design question.
This run cleans nothing up; inspect the Jobs, then: kubectl delete jobs -l experiment=scheduler-catchup -n bench
TXT
