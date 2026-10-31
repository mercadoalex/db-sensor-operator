#!/usr/bin/env bash
#
# bench/experiments/snapshot-modes/run.sh — observe what each Debezium PostgreSQL
# snapshot.mode actually does on the pinned Debezium Server (task A0 of the
# snapshot-policy spec).
#
# For each mode it: starts a DebeziumServer on public.orders (1000+ seeded rows)
# with an in-memory offset store, then records
#   1. what arrives at startup (snapshot reads op=r vs nothing),
#   2. whether a live INSERT afterwards is delivered (and carries row data),
#   3. what happens after the Debezium pod restarts (re-snapshot or not).
#
# Requires `make -C bench setup` and the Debezium Operator (installed here if
# missing). Does NOT need Lactiflluus. Prints a summary table and the raw event
# lines it based it on.
#
# Usage: ./bench/experiments/snapshot-modes/run.sh [mode ...]
#        default modes: initial no_data never when_needed
#
set -euo pipefail

CLUSTER="${CLUSTER:-db-sensor-bench}"
NS="${NS:-bench}"
DEBEZIUM_OPERATOR_VERSION="${DEBEZIUM_OPERATOR_VERSION:-3.6.3}"
CTX="kind-${CLUSTER}"
K="kubectl --context ${CTX}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODES=("$@"); [[ ${#MODES[@]} -eq 0 ]] && MODES=(initial no_data never when_needed)
PSQL="$K -n $NS exec bench-postgres-0 -- psql -U bench -d bench -tAq"

echo "==> Debezium Operator ${DEBEZIUM_OPERATOR_VERSION}"
helm repo add debezium https://charts.debezium.io >/dev/null 2>&1 || true
helm repo update debezium >/dev/null
helm upgrade --install debezium-operator debezium/debezium-operator \
  --version "$DEBEZIUM_OPERATOR_VERSION" --kube-context "$CTX" \
  -n debezium --create-namespace --wait --timeout 300s >/dev/null

echo "==> receiver (fresh, so earlier runs do not leak into the log)"
$K -n "$NS" delete pod snap-receiver --ignore-not-found --wait=true >/dev/null 2>&1 || true
$K apply -f "${HERE}/receiver.yaml" >/dev/null
$K -n "$NS" wait --for=condition=Ready pod/snap-receiver --timeout=180s >/dev/null

events_for() { # name -> lines for that connector
  $K -n "$NS" logs snap-receiver 2>/dev/null | grep "^EVENT $1 " || true
}
summarize() { # lines -> "N snapshot-read, M live"
  local lines="$1" snap live
  snap=$(grep -cE 'op=r( |$)|snapshot=(true|first|first_in_data_collection|last_in_data_collection|last|incremental)' <<<"$lines" || true)
  live=$(grep -cE 'op=[cud]' <<<"$lines" | head -1 || true)
  echo "${snap:-0} snapshot reads, ${live:-0} c/u/d"
}

apply_server() { # mode name
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
  source:
    class: io.debezium.connector.postgresql.PostgresConnector
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
      snapshot.mode: $1
  sink:
    type: http
    config:
      url: http://snap-receiver.${NS}.svc:8080/cdc
EOF
}

wait_pod() { # name
  for _ in $(seq 1 60); do
    if $K -n "$NS" get deploy "$1" >/dev/null 2>&1 &&
       [[ "$($K -n "$NS" get deploy "$1" -o jsonpath='{.status.readyReplicas}' 2>/dev/null)" == "1" ]]; then return 0; fi
    sleep 3
  done
  return 1
}

cleanup() { # name
  $K -n "$NS" delete debeziumserver "$1" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  sleep 3
  $PSQL -c "select pg_drop_replication_slot('${1//-/_}') where exists (select 1 from pg_replication_slots where slot_name='${1//-/_}')" >/dev/null 2>&1 || true
}

RESULTS=()
for mode in "${MODES[@]}"; do
  name="snap-${mode//_/-}"
  echo
  echo "================ snapshot.mode=${mode} ================"
  cleanup "$name"
  apply_server "$mode" "$name"
  if ! wait_pod "$name"; then
    echo "!! pod did not become ready; connector rejected or crashed. Log tail:"
    $K -n "$NS" logs "deploy/$name" --tail=400 2>&1 | python3 -c "
import sys, json
for l in sys.stdin:
    try: d = json.loads(l)
    except Exception: continue
    if d.get('level') == 'ERROR':
        ex = d.get('exception') or {}
        while ex.get('causedBy'): ex = ex['causedBy']['exception']
        print('  ERROR', (ex.get('message') or d['message'])[:300]); break
"
    RESULTS+=("${mode}|REJECTED/CRASHED|-|-")
    cleanup "$name"
    continue
  fi
  sleep 25

  startup="$(events_for "$name")"
  echo "-- at startup ($(grep -c . <<<"$startup") events, first 5):"; { echo "${startup:-  (no events)}" | head -5; } || true
  s_start="$(summarize "$startup")"

  $PSQL -c "insert into public.orders (customer, amount) values (999, 9.99)" >/dev/null
  sleep 6
  after_insert="$(events_for "$name")"
  new_live="$(diff <(echo "$startup") <(echo "$after_insert") | grep '^>' || true)"
  echo "-- after a live INSERT:"; echo "${new_live:-  (nothing delivered)}"
  live_ok="no"; grep -q 'op=c.*hasAfter=True' <<<"$new_live" && live_ok="yes (with row data)"

  $K -n "$NS" delete pod -l "debezium.io/instance=${name}" --wait=true >/dev/null 2>&1 || true
  wait_pod "$name" || true
  sleep 25
  after_restart="$(events_for "$name")"
  restart_new="$(diff <(echo "$after_insert") <(echo "$after_restart") | grep '^>' || true)"
  echo "-- after restarting the Debezium pod, in-memory offset ($(grep -c . <<<"$restart_new") new events, first 5):"; { echo "${restart_new:-  (nothing re-delivered)}" | head -5; } || true
  s_restart="$(summarize "$restart_new")"

  RESULTS+=("${mode}|${s_start}|${live_ok}|${s_restart}")
  cleanup "$name"
done

echo
echo "================ SUMMARY (Debezium Operator ${DEBEZIUM_OPERATOR_VERSION}) ================"
printf '%-14s | %-34s | %-22s | %s\n' "snapshot.mode" "startup" "live INSERT delivered" "after pod restart"
for r in "${RESULTS[@]}"; do IFS='|' read -r a b c d <<<"$r"; printf '%-14s | %-34s | %-22s | %s\n' "$a" "$b" "$c" "$d"; done
