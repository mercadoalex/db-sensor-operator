#!/usr/bin/env bash
#
# R11 variant — duplicate rows from a catch-up burst.
#
# Scenario (simulated): a daily job writes one summary row per run. The scheduler was down,
# comes back, and reruns the same job RUNS times, inserting the same logical row RUNS times.
# This is the data-corruption flavour of a catch-up burst, not the load flavour.
#
# Question: does any source show that these are DUPLICATES (the same logical content written
# again), rather than RUNS legitimate new rows?
#
# Pass: CDC (as exported, or in the raw stream) distinguishes the duplicate content in a way
#       pg_stat counters and the Kubernetes API cannot.
# Fail: no source distinguishes it without reading row content that we do not expose.
#
# Prerequisites: make -C bench setup && bench/collectors/cdc/up.sh
# Env: CLUSTER (db-sensor-bench) NS (bench) RUNS (5)
set -uo pipefail
CLUSTER="${CLUSTER:-db-sensor-bench}"; NS="${NS:-bench}"; RUNS="${RUNS:-5}"
K="kubectl --context kind-${CLUSTER}"
PSQL_AT() { $K -n "$NS" exec -i bench-postgres-0 -- psql -U bench -d bench -At "$@"; }
cdc_orders() {
  $K -n db-sensor get --raw \
    "/api/v1/namespaces/db-sensor/services/db-sensor-db-sensor-operator-operator-http:8080/proxy/metrics" 2>/dev/null \
    | grep 'debezium_events_total' | grep 'table="orders"' || echo "(none)"
}
say() { printf '\n== %s\n' "$*"; }

$K -n "$NS" get statefulset bench-postgres >/dev/null 2>&1 || {
  echo "ERROR: run 'make -C bench setup' and 'bench/collectors/cdc/up.sh' first." >&2; exit 1; }

say "baseline"
echo "orders rows: $(PSQL_AT -c 'select count(*) from public.orders')"
echo "CDC inserts before:"; cdc_orders | grep 'op="c"' || true

say "the scheduler reruns the SAME daily summary job $RUNS times"
# Each run inserts the identical logical row: same customer, same amount. A correct run would
# insert it once. The duplicates are what a catch-up storm produces.
for i in $(seq 1 "$RUNS"); do
  cat <<EOF | $K apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: daily-summary-rerun-${i}
  namespace: ${NS}
  labels: { experiment: scheduler-catchup-dup }
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 600
  template:
    metadata:
      labels: { experiment: scheduler-catchup-dup }
    spec:
      restartPolicy: Never
      containers:
        - name: writer
          image: postgres:16-alpine
          command: ["psql"]
          args: ["-v","ON_ERROR_STOP=1","-c","insert into public.orders (customer, amount) values (999, 42.00);"]
          env:
            - { name: PGHOST, value: bench-postgres }
            - { name: PGUSER, value: bench }
            - { name: PGDATABASE, value: bench }
            - name: PGPASSWORD
              valueFrom: { secretKeyRef: { name: bench-pg-creds, key: password } }
EOF
done

say "wait for the reruns to finish"
$K -n "$NS" wait --for=condition=complete job -l experiment=scheduler-catchup-dup --timeout=120s 2>&1 | tail -2 || true
sleep 8

say "AFTER — can any source tell these $RUNS rows are duplicates?"
echo "1) pg_stat counters:"
PSQL_AT -F'|' -c "select n_tup_ins, n_live_tup from pg_stat_user_tables where relname='orders'"
echo "   -> 'ins went up by $RUNS'. It cannot say they are the same logical row."
echo
echo "2) Kubernetes API:"
$K -n "$NS" get jobs -l experiment=scheduler-catchup-dup -o custom-columns=NAME:.metadata.name --no-headers | sed 's/^/   /'
echo "   -> $RUNS reruns of the same job name. This DOES hint at duplication by job name,"
echo "      but says nothing about the data written."
echo
echo "3) CDC (exported counts):"; cdc_orders | grep 'op="c"' || true
echo "   -> op=c went up by $RUNS. As exported, still a count: no row content, so no dedup signal."
echo
echo "4) Ground truth in the DB (what CDC's raw stream *contains* but we do not expose):"
PSQL_AT -F'|' -c "select customer, amount, count(*) from public.orders where customer=999 group by 1,2 having count(*)>1"
echo "   -> $RUNS identical (customer=999, amount=42.00) rows. CDC carried each INSERT's full row,"
echo "      so the raw stream could show the repeat; our pipeline keeps only counts."

say "READ THE RESULT — fill RESULTS.md"
echo "Clean up: kubectl delete jobs -l experiment=scheduler-catchup-dup -n bench"
echo "          psql ... delete from public.orders where customer=999"
