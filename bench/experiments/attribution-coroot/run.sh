#!/usr/bin/env bash
#
# Attribution, approach 5 — statement-level client-side eBPF (coroot-node-agent).
#
# Question: if an eBPF agent on the client's node parses the PostgreSQL protocol, does a span say
# "this pod sent this statement at this time", and does the end of the COMMIT span line up with the
# CDC event's commit time closely enough to join them?
#
# Two client pods are started for the same wall-clock second; a third writes alone, later.
# Output: the Postgres spans the agent exported (statement, container id, start/end) and the CDC
# events (txId, customer, ts_ms). Plain PostgreSQL connections, no TLS, no pooler.
#
# Changes the bench: installs the agent and a collector in namespace "coroot", and repoints the
# sensor's sink to a receiver for the duration. Restored on exit.
# Prerequisites: make -C bench setup && bench/collectors/cdc/up.sh
set -uo pipefail
CLUSTER="${CLUSTER:-db-sensor-bench}"; NS=bench
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CTX="kind-${CLUSTER}"
ORIG_URL="http://db-sensor-db-sensor-operator-operator-http.db-sensor.svc:8080/cdc"
OUT="${OUT:-$(mktemp -d)}"
k() { kubectl --context "$CTX" "$@"; }
say() { printf '\n== %s\n' "$*"; }

cleanup() {
  say "cleanup"
  k -n "$NS" delete pod -l experiment=attribution-coroot --ignore-not-found >/dev/null 2>&1
  k delete namespace coroot --ignore-not-found >/dev/null 2>&1
  k -n "$NS" patch databasesensor bench-cdc --type=merge \
    -p "{\"spec\":{\"transport\":{\"http\":{\"url\":\"${ORIG_URL}\"}}}}" >/dev/null 2>&1
  k -n "$NS" exec -i bench-postgres-0 -- psql -U bench -d bench -c "delete from public.orders where customer in (831,832,833)" >/dev/null 2>&1
  k -n "$NS" delete pod/inband-receiver svc/inband-receiver configmap/inband-receiver-code --ignore-not-found >/dev/null 2>&1
  echo "outputs kept in: $OUT"
}
trap cleanup EXIT

say "agent + collector"
k apply -f "$HERE/agent.yaml" >/dev/null
k -n coroot rollout status deploy/otlp-sink --timeout=180s
k -n coroot rollout status ds/coroot-node-agent --timeout=240s || { k -n coroot describe pod -l app=coroot-node-agent | tail -20; exit 1; }
sleep 10
k -n coroot logs ds/coroot-node-agent --tail=15 2>&1 | cut -c1-220

say "receiver for CDC events (reused from attribution-inband)"
k apply -f "$HERE/../attribution-inband/receiver.yaml" >/dev/null
k -n "$NS" wait --for=condition=ready pod/inband-receiver --timeout=90s >/dev/null 2>&1 || true
k -n "$NS" patch databasesensor bench-cdc --type=merge \
  -p '{"spec":{"transport":{"type":"http","http":{"url":"http://inband-receiver.bench.svc:8080/"}}}}' >/dev/null
echo "waiting for Debezium to restart against the receiver..."
sleep 45

say "three clients: a and b for the same second, c alone 8 s later"
BASE=$(( $(date +%s) + 60 ))
for spec in "client-a 831 $BASE" "client-b 832 $BASE" "client-c 833 $((BASE + 8))"; do
  set -- $spec
  sed -e "s/@NAME@/$1/g" -e "s/@CUSTOMER@/$2/g" -e "s/@BOUNDARY@/$3/g" "$HERE/client.yaml.tpl" | k apply -f - >/dev/null
done
sleep $(( BASE + 8 + 20 - $(date +%s) ))

say "collecting"
k -n coroot logs deploy/otlp-sink --tail=-1 >"$OUT/spans.txt" 2>&1
k -n coroot logs ds/coroot-node-agent --tail=-1 >"$OUT/agent.log" 2>&1
k -n "$NS" logs inband-receiver --tail=-1 2>/dev/null | grep '^EVENT' | grep -E 'customer=(831|832|833)\b' >"$OUT/cdc.txt"
wc -l "$OUT/spans.txt" "$OUT/cdc.txt"
python3 "$HERE/parse.py" "$OUT/spans.txt" "$OUT/cdc.txt"
