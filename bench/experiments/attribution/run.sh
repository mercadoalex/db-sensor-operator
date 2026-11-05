#!/usr/bin/env bash
#
# Attribution, approach 1 — session identity.
#
# Question: can a change be tied to the Job that made it, honestly, without guessing by a
# time window? Approach: the Job sets application_name on its PostgreSQL connection to its own
# name, writes a change, and we check two things:
#   1. does the raw CDC event carry the session identity (application_name / user)?
#   2. if not, can we correlate the change with pg_stat_activity (who held the session)?
#
# Pass: the change can be linked to "job/<name>" from the stream, or reliably correlated.
# Fail: the only link left is "it happened while the Job ran", i.e. a time-window guess.
#
# Prerequisites: make -C bench setup && bench/collectors/cdc/up.sh
# Env: CLUSTER (db-sensor-bench) NS (bench)
set -uo pipefail
CLUSTER="${CLUSTER:-db-sensor-bench}"; NS="${NS:-bench}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K="kubectl --context kind-${CLUSTER}"
PSQL() { $K -n "$NS" exec -i bench-postgres-0 -- psql -U bench -d bench -v ON_ERROR_STOP=1 "$@"; }
say() { printf '\n== %s\n' "$*"; }

$K -n "$NS" get statefulset bench-postgres >/dev/null 2>&1 || {
  echo "ERROR: run 'make -C bench setup' and 'bench/collectors/cdc/up.sh' first." >&2; exit 1; }

say "point the sensor's sink at a receiver that logs the full source block"
$K -n "$NS" apply -f "${HERE}/receiver.yaml" >/dev/null
$K -n "$NS" wait --for=condition=ready pod/attr-receiver --timeout=60s >/dev/null 2>&1 || true
$K -n "$NS" patch databasesensor bench-cdc --type=merge \
  -p '{"spec":{"transport":{"type":"http","http":{"url":"http://attr-receiver.bench.svc:8080/"}}}}' >/dev/null
echo "waiting for Debezium to restart against the receiver..."
sleep 45

say "run a Job that labels its connection application_name=job/attrib-writer and makes a change"
cat <<'EOF' | $K apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: attrib-writer
  namespace: bench
  labels: { experiment: attribution }
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 600
  template:
    metadata: { labels: { experiment: attribution } }
    spec:
      restartPolicy: Never
      containers:
        - name: writer
          image: postgres:16-alpine
          # application_name is set via the connection; then hold the tx briefly so
          # pg_stat_activity can be sampled, and make a visible change.
          command: ["sh","-c"]
          args:
            - |
              export PGAPPNAME=job/attrib-writer
              psql -v ON_ERROR_STOP=1 -c "select pg_sleep(3); insert into public.orders (customer, amount) values (777, 7.77);"
          env:
            - { name: PGHOST, value: bench-postgres }
            - { name: PGUSER, value: bench }
            - { name: PGDATABASE, value: bench }
            - name: PGPASSWORD
              valueFrom: { secretKeyRef: { name: bench-pg-creds, key: password } }
EOF

say "while the Job holds its transaction, sample pg_stat_activity (approach 2)"
sleep 3
PSQL -At -F'|' -c "select application_name, state, query from pg_stat_activity where application_name like 'job/%'" || true

$K -n "$NS" wait --for=condition=complete job/attrib-writer --timeout=60s >/dev/null 2>&1 || true
sleep 8

say "approach 1: does the raw CDC event carry the session identity?"
$K -n "$NS" logs attr-receiver --since=60s 2>/dev/null | grep '^EVENT' | grep '777' | tail -2 || \
  $K -n "$NS" logs attr-receiver --since=60s 2>/dev/null | grep '^EVENT' | tail -3
echo "  -> look for application_name or the db user in the 'source' block above."

say "READ THE RESULT"
cat <<'TXT'
  - Approach 1 (in the stream): if 'source' has no application_name/user, CDC alone cannot
    attribute. Debezium's source block is about the WAL position and the server, not the client.
  - Approach 2 (pg_stat_activity): we saw application_name=job/attrib-writer WHILE the tx was
    open. But this is a live sample: by the time the CDC event arrives, the session may be gone.
    Correlating reliably needs the commit's session, which pg_stat_activity does not keep after
    the session closes. So session identity helps only if sampled in the moment, which is racy.
Conclusion feeds the attribution decision: session-based is weak on its own. Next: Cilium/Hubble
(who connected) or eBPF, or lean on the Kubernetes side (a Job that is the sole writer in its
window). Record honestly in RESULTS.md.

Cleanup:
  kubectl -n bench patch databasesensor bench-cdc --type=merge -p '{"spec":{"transport":{"http":{"url":"http://db-sensor-db-sensor-operator-operator-http.db-sensor.svc:8080/cdc"}}}}'
  kubectl -n bench delete job attrib-writer ; kubectl -n bench delete pod attr-receiver ; kubectl -n bench delete svc attr-receiver ; kubectl -n bench delete configmap attr-receiver-code
  psql: delete from public.orders where customer=777;
TXT
