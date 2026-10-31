#!/usr/bin/env bash
#
# Inspection end to end in kind: the real operator creates the inspector Job, the kubelet
# keeps the termination message, the operator copies it into status and then deletes the Job.
#
# Prerequisites (this script does not build or install anything):
#   make -C bench setup                     # kind cluster + PostgreSQL (wal_level=logical)
#   bench/collectors/cdc/up.sh              # Debezium Operator + Lactifluus (chart sets OPERATOR_IMAGE)
#
# Checks (prints PASS/FAIL, exits non-zero on any FAIL):
#   1 the first inspection reaches InspectionAvailable=True with real PostgreSQL facts
#   2 the plan is reported with applied=false
#   3 the Job is gone after its result is stored
#   4 exactly one Job was created for that run (intervalSeconds: 0 means once per generation)
#   5 a spec change (new generation) triggers a new inspection and the slot is then healthy
#   6 a wrong password gives an error-only result: no facts, no plan, no derived conditions
#   7 the Job is admitted under Pod Security "restricted"
#
# Env: CLUSTER (db-sensor-bench) NS (bench)
set -uo pipefail
CLUSTER="${CLUSTER:-db-sensor-bench}"; NS="${NS:-bench}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K="kubectl --context kind-${CLUSTER}"
FAILS=0
pass() { echo "PASS  $*"; }
fail() { echo "FAIL  $*"; FAILS=$((FAILS+1)); }
jp() { $K -n "$1" get databasesensor "$2" -o jsonpath="$3" 2>/dev/null; }
wait_for() { # seconds, shell expression (re-evaluated on every attempt)
  local t="$1"; shift
  for _ in $(seq 1 "$t"); do if eval "$*" >/dev/null 2>&1; then return 0; fi; sleep 1; done; return 1
}
jobs_created() { $K -n "$NS" get events --field-selector reason=SuccessfulCreate,involvedObject.kind=Job -o name 2>/dev/null | wc -l | tr -d ' '; }
cond() { jp "$1" "$2" "{.status.conditions[?(@.type==\"$3\")].$4}"; }

sensor_yaml() { # name ns host secret interval
cat <<EOF
apiVersion: sensor.io/v1
kind: DatabaseSensor
metadata: {name: $1, namespace: $2}
spec:
  database: {type: postgresql, host: $3, port: 5432, user: bench, name: bench, passwordSecret: $4}
  tables: [public.orders, public.users, public.payments]
  transport: {type: http, http: {url: "http://db-sensor-db-sensor-operator-operator-http.db-sensor.svc:8080/cdc"}}
  sink: {type: prometheus, prometheus: {serviceMonitor: false}}
  snapshot: {inspection: {enabled: true, intervalSeconds: $5, timeoutSeconds: 30}}
EOF
}

cleanup() {
  $K -n "$NS" delete databasesensor bench-cdc badpw-sensor --ignore-not-found --wait=false >/dev/null 2>&1
  $K -n "$NS" delete secret bad-creds --ignore-not-found >/dev/null 2>&1
  $K delete namespace bench-psa --ignore-not-found --wait=false >/dev/null 2>&1
}
trap cleanup EXIT

echo "== start from a clean sensor so the Job count is exact"
$K -n "$NS" delete databasesensor bench-cdc --ignore-not-found --wait=true >/dev/null 2>&1
sleep 3
BEFORE=$(jobs_created)

echo "== 1/2/3/4 first inspection"
sensor_yaml bench-cdc "$NS" bench-postgres bench-pg-creds 0 | $K apply -f - >/dev/null
if wait_for 90 'test "$(cond "$NS" bench-cdc InspectionAvailable status)" = "True"'; then
  pass "1 InspectionAvailable=True ($(cond "$NS" bench-cdc InspectionAvailable reason))"
else
  fail "1 inspection did not finish: $(cond "$NS" bench-cdc InspectionAvailable message)"
fi
WAL=$(jp "$NS" bench-cdc '{.status.snapshot.inspection.postgres.walLevel}')
FOUND=$(jp "$NS" bench-cdc '{.status.snapshot.inspection.tables[?(@.found==true)].name}' | wc -w | tr -d ' ')
[ "$WAL" = "logical" ] && [ "$FOUND" = "3" ] && pass "1 real facts: wal_level=$WAL, $FOUND tables found" || fail "1 facts: wal_level='$WAL' found='$FOUND'"
[ "$(jp "$NS" bench-cdc '{.status.snapshot.plan.applied}')" = "false" ] && pass "2 plan reported with applied=false" || fail "2 plan.applied is not false"
wait_for 20 'test -z "$($K -n "$NS" get jobs -l app.kubernetes.io/name=db-sensor-inspector -o name)"' \
  && pass "3 Job deleted after the result was stored" || fail "3 Job still present"
sleep 70   # two reconcile periods: a second Job would show up here
CREATED=$(( $(jobs_created) - BEFORE ))
[ "$(jp "$NS" bench-cdc '{.status.snapshot.inspection.observedGeneration}')" = "$(jp "$NS" bench-cdc '{.metadata.generation}')" ] \
  && [ "$CREATED" = "1" ] && pass "4 exactly one Job ran, none after 70 s" \
  || fail "4 Jobs created in this run: $CREATED (expected 1)"

echo "== 5 spec change -> new generation -> new inspection, slot healthy"
GEN1=$(jp "$NS" bench-cdc '{.metadata.generation}')
sensor_yaml bench-cdc "$NS" bench-postgres bench-pg-creds 60 | $K apply -f - >/dev/null
GEN2=$(jp "$NS" bench-cdc '{.metadata.generation}')
if [ "$GEN2" != "$GEN1" ] && wait_for 90 'test "$(jp "$NS" bench-cdc "{.status.snapshot.inspection.observedGeneration}")" = "$GEN2"'; then
  pass "5 re-inspected for generation $GEN2"
else fail "5 no new inspection for generation $GEN2"; fi
if wait_for 120 'test "$(cond "$NS" bench-cdc SlotRetentionHealthy reason)" = "SlotHealthy"'; then
  pass "5 slot diagnosis: SlotHealthy ($(jp "$NS" bench-cdc '{.status.snapshot.inspection.postgres.replicationSlot.walStatus}'))"
else fail "5 slot condition is '$(cond "$NS" bench-cdc SlotRetentionHealthy reason)'"; fi

echo "== 6 wrong password"
$K -n "$NS" create secret generic bad-creds --from-literal=password=definitely-wrong >/dev/null
sensor_yaml badpw-sensor "$NS" bench-postgres bad-creds 0 | $K apply -f - >/dev/null
wait_for 90 'test "$(cond "$NS" badpw-sensor InspectionAvailable reason)" = "AuthenticationFailed"' \
  && pass "6 reason AuthenticationFailed" || fail "6 reason is '$(cond "$NS" badpw-sensor InspectionAvailable reason)'"
[ -z "$(jp "$NS" badpw-sensor '{.status.snapshot.inspection.tables}')" ] && [ -z "$(jp "$NS" badpw-sensor '{.status.snapshot.plan}')" ] \
  && pass "6 no facts and no plan beside the error" || fail "6 stale facts or plan present"
[ -z "$(cond "$NS" badpw-sensor SlotRetentionHealthy status)" ] && pass "6 derived conditions absent" || fail "6 derived conditions present"
jp "$NS" badpw-sensor '{.status.snapshot.inspection.error}' | grep -q "definitely-wrong" && fail "6 the password leaked into status" || pass "6 password not in status"

echo "== 7 Pod Security restricted"
$K create namespace bench-psa >/dev/null
$K label namespace bench-psa pod-security.kubernetes.io/enforce=restricted pod-security.kubernetes.io/enforce-version=latest >/dev/null
$K -n bench-psa create secret generic bench-pg-creds --from-literal=password="$($K -n "$NS" get secret bench-pg-creds -o jsonpath='{.data.password}' | base64 -d)" >/dev/null
sensor_yaml psa-sensor bench-psa bench-postgres.bench.svc bench-pg-creds 0 | $K apply -f - >/dev/null
wait_for 90 'test "$(cond bench-psa psa-sensor InspectionAvailable status)" = "True"' \
  && pass "7 inspector Job admitted and finished under restricted" || fail "7 $(cond bench-psa psa-sensor InspectionAvailable message)"

echo
[ "$FAILS" -eq 0 ] && echo "ALL CHECKS PASSED" || { echo "$FAILS CHECK(S) FAILED"; exit 1; }
