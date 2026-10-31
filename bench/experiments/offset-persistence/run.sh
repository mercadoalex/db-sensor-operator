#!/usr/bin/env bash
#
# bench/experiments/offset-persistence/run.sh — does a persistent offset store stop
# a Debezium restart from re-delivering events? (task A4/A6 of the snapshot-policy
# spec; follows bench/experiments/snapshot-modes/RESULTS.md.)
#
# Variants, all with snapshot.mode=no_data on public.orders:
#   memory        the Debezium Operator default (MemoryOffsetBackingStore)
#   file          persistent data volume + file offset store, default flush (60 s)
#   file-flush1s  same, with offset.flush.interval.ms=1000
#
# Per variant: start, make ONE live INSERT, wait 6 s, restart the Debezium pod,
# wait, and count the events that arrive again. Then a second INSERT proves the
# connector still delivers. A correct result is 0 re-delivered after the restart.
#
# Requires `make -C bench setup`. Reuses the logging receiver of the
# snapshot-modes experiment.
#
set -euo pipefail

CLUSTER="${CLUSTER:-db-sensor-bench}"
NS="${NS:-bench}"
DEBEZIUM_OPERATOR_VERSION="${DEBEZIUM_OPERATOR_VERSION:-3.6.3}"
CTX="kind-${CLUSTER}"
K="kubectl --context ${CTX}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PSQL="$K -n $NS exec bench-postgres-0 -- psql -U bench -d bench -tAq"
VARIANTS=("$@"); [[ ${#VARIANTS[@]} -eq 0 ]] && VARIANTS=(memory file file-flush1s)

helm repo add debezium https://charts.debezium.io >/dev/null 2>&1 || true
helm repo update debezium >/dev/null
helm upgrade --install debezium-operator debezium/debezium-operator \
  --version "$DEBEZIUM_OPERATOR_VERSION" --kube-context "$CTX" \
  -n debezium --create-namespace --wait --timeout 300s >/dev/null

$K -n "$NS" delete pod snap-receiver --ignore-not-found --wait=true >/dev/null 2>&1 || true
$K apply -f "${HERE}/../snapshot-modes/receiver.yaml" >/dev/null
$K -n "$NS" wait --for=condition=Ready pod/snap-receiver --timeout=180s >/dev/null

events_for() { $K -n "$NS" logs snap-receiver 2>/dev/null | grep "^EVENT $1 " || true; }
count() { grep -c . <<<"$1" || true; }

apply_server() { # variant name
  local runtime_storage="" offset="" flush=""
  if [[ "$1" != "memory" ]]; then
    # The Debezium Operator rejects type=persistent without a PVC template
    # ("Missing PVC template"), so runtime.templates.volumeClaim is required.
    runtime_storage="    storage:
      data:
        type: persistent
    templates:
      volumeClaim:
        accessModes: [ReadWriteOnce]
        resources:
          requests:
            storage: 100Mi"
    offset="    offset:
      file:
        fileName: /debezium/data/offsets.dat"
  fi
  [[ "$1" == "file-flush1s" ]] && flush="      flushMs: 1000"
  cat <<EOF | $K apply -f - >/dev/null
apiVersion: debezium.io/v1alpha1
kind: DebeziumServer
metadata: { name: $2, namespace: $NS }
spec:
  runtime:
    environment:
      from:
        - secretRef: { name: bench-pg-creds }
          prefix: DATABASE_
${runtime_storage}
  source:
    class: io.debezium.connector.postgresql.PostgresConnector
${offset}
${flush}
    config:
      database.hostname: bench-postgres
      database.port: "5432"
      database.user: bench
      database.password: \${DATABASE_password}
      database.dbname: bench
      topic.prefix: $2
      slot.name: ${2//-/_}
      plugin.name: pgoutput
      table.include.list: public.orders
      snapshot.mode: no_data
  sink:
    type: http
    config:
      url: http://snap-receiver.${NS}.svc:8080/cdc
EOF
}

wait_pod() {
  for _ in $(seq 1 60); do
    [[ "$($K -n "$NS" get deploy "$1" -o jsonpath='{.status.readyReplicas}' 2>/dev/null)" == "1" ]] && return 0
    sleep 3
  done
  return 1
}

cleanup() {
  $K -n "$NS" delete debeziumserver "$1" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  $K -n "$NS" delete pvc -l "debezium.io/instance=$1" --ignore-not-found >/dev/null 2>&1 || true
  sleep 3
  $PSQL -c "select pg_drop_replication_slot('${1//-/_}') where exists (select 1 from pg_replication_slots where slot_name='${1//-/_}')" >/dev/null 2>&1 || true
}

RESULTS=()
for v in "${VARIANTS[@]}"; do
  name="off-${v//_/-}"
  echo; echo "================ variant=${v} ================"
  cleanup "$name"
  if ! apply_server "$v" "$name"; then echo "!! apply rejected"; RESULTS+=("$v|REJECTED|-|-"); continue; fi
  if ! wait_pod "$name"; then
    echo "!! pod not ready"
    $K -n "$NS" get pods,pvc 2>&1 | grep "$name" || true
    RESULTS+=("$v|NOT-READY|-|-"); cleanup "$name"; continue
  fi
  sleep 20
  echo "-- volume: $($K -n "$NS" get pvc 2>/dev/null | grep -c "$name" || true) PVC(s) for ${name}"

  $PSQL -c "insert into public.orders (customer, amount) values (111, 1.11)" >/dev/null
  sleep 6
  before="$(events_for "$name")"
  echo "-- delivered before restart: $(count "$before") event(s)"

  $K -n "$NS" delete pod -l "debezium.io/instance=${name}" --wait=true >/dev/null 2>&1 || true
  wait_pod "$name" || true
  sleep 25
  after="$(events_for "$name")"
  redelivered=$(( $(count "$after") - $(count "$before") ))
  echo "-- re-delivered after restart: ${redelivered}"

  $PSQL -c "insert into public.orders (customer, amount) values (222, 2.22)" >/dev/null
  sleep 6
  final="$(events_for "$name")"
  still_live=$(( $(count "$final") - $(count "$after") ))
  echo "-- delivered after a new INSERT: ${still_live}"
  RESULTS+=("$v|$(count "$before")|${redelivered}|${still_live}")
  cleanup "$name"
done

echo; echo "================ SUMMARY (Debezium Operator ${DEBEZIUM_OPERATOR_VERSION}) ================"
printf '%-14s | %-16s | %-26s | %s\n' variant "before restart" "re-delivered after restart" "new INSERT delivered"
for r in "${RESULTS[@]}"; do IFS='|' read -r a b c d <<<"$r"; printf '%-14s | %-16s | %-26s | %s\n' "$a" "$b" "$c" "$d"; done
