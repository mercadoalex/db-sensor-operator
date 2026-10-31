#!/usr/bin/env bash
#
# bench/experiments/blind-window/run.sh — how long is a sensor blind at start?
# (task A6 of the snapshot-policy spec.)
#
# Loads ROWS rows into public.orders, then for each snapshot.mode starts a
# DebeziumServer and inserts a marker row every second. The blind window is the
# time from applying the DebeziumServer until the receiver sees the first LIVE
# change (op=c with snapshot=false). A blocking snapshot has to finish first, so
# `initial` is blind for as long as it takes to read the table; `no_data` is not.
#
# Env: ROWS (default 100000)  TIMEOUT seconds per mode (default 900)
# Usage: ./bench/experiments/blind-window/run.sh [mode ...]   (default: initial no_data)
#
set -euo pipefail

CLUSTER="${CLUSTER:-db-sensor-bench}"
NS="${NS:-bench}"
ROWS="${ROWS:-100000}"
TIMEOUT="${TIMEOUT:-900}"
DEBEZIUM_OPERATOR_VERSION="${DEBEZIUM_OPERATOR_VERSION:-3.6.3}"
CTX="kind-${CLUSTER}"
K="kubectl --context ${CTX}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PSQL="$K -n $NS exec bench-postgres-0 -- psql -U bench -d bench -tAq"
MODES=("$@"); [[ ${#MODES[@]} -eq 0 ]] && MODES=(initial no_data)

helm repo add debezium https://charts.debezium.io >/dev/null 2>&1 || true
helm repo update debezium >/dev/null
helm upgrade --install debezium-operator debezium/debezium-operator \
  --version "$DEBEZIUM_OPERATOR_VERSION" --kube-context "$CTX" \
  -n debezium --create-namespace --wait --timeout 300s >/dev/null

echo "==> loading rows so public.orders has at least ${ROWS}"
have=$($PSQL -c "select count(*) from public.orders")
if (( have < ROWS )); then
  $PSQL -c "insert into public.orders (customer, amount) select (g % 100) + 1, (g % 500) + 1 from generate_series(1, $((ROWS - have))) g" >/dev/null
fi
$PSQL -c "analyze public.orders" >/dev/null
echo "    rows: $($PSQL -c 'select count(*) from public.orders')"

$K -n "$NS" delete pod snap-receiver --ignore-not-found --wait=true >/dev/null 2>&1 || true
$K apply -f "${HERE}/../snapshot-modes/receiver.yaml" >/dev/null
$K -n "$NS" wait --for=condition=Ready pod/snap-receiver --timeout=180s >/dev/null

rstat() { # name field -> counter from the receiver (log-rotation safe)
  $K -n "$NS" exec snap-receiver -- python -c "import json,urllib.request;d=json.load(urllib.request.urlopen('http://localhost:8080/stats'));print(d.get('$1',{}).get('$2',0))" 2>/dev/null || echo 0
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

cleanup() {
  $K -n "$NS" delete debeziumserver "$1" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  sleep 3
  $PSQL -c "select pg_drop_replication_slot('${1//-/_}') where exists (select 1 from pg_replication_slots where slot_name='${1//-/_}')" >/dev/null 2>&1 || true
}

RESULTS=()
for mode in "${MODES[@]}"; do
  name="blind-${mode//_/-}"
  echo; echo "================ snapshot.mode=${mode} ================"
  cleanup "$name"
  apply_server "$mode" "$name"
  SECONDS=0; found=""
  while (( SECONDS < TIMEOUT )); do
    $PSQL -c "insert into public.orders (customer, amount) values (777, 7.77)" >/dev/null 2>&1 || true
    if (( $(rstat "$name" live) > 0 )); then found="$SECONDS"; break; fi
    sleep 1
  done
  snaps=$(rstat "$name" snapshot)
  if [[ -n "$found" ]]; then
    echo "first live change seen after ${found}s (snapshot reads delivered by then: ${snaps})"
    RESULTS+=("${mode}|${found}s|${snaps}")
  else
    echo "no live change within ${TIMEOUT}s (snapshot reads delivered: ${snaps})"
    RESULTS+=("${mode}|>${TIMEOUT}s (still blind)|${snaps}")
  fi
  cleanup "$name"
done

echo; echo "================ SUMMARY: ${ROWS}+ rows, Debezium Operator ${DEBEZIUM_OPERATOR_VERSION} ================"
printf '%-12s | %-26s | %s\n' snapshot.mode "time to first live change" "snapshot reads delivered by then"
for r in "${RESULTS[@]}"; do IFS='|' read -r a b c <<<"$r"; printf '%-12s | %-26s | %s\n' "$a" "$b" "$c"; done
