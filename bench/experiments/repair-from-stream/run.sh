#!/usr/bin/env bash
#
# bench/experiments/repair-from-stream/run.sh — P3, first half: can an incident be repaired from the change stream?
#
# Question: after a bad job has updated, deleted and inserted rows, and legitimate writes have kept arriving,
# can the before and after images that Debezium emits (with REPLICA IDENTITY FULL) restore the rows the bad job
# changed, without touching the legitimate writes?
#
# Hypothesis: yes. Guarded compensating statements, built only from the events of the bad transactions and run in
# reverse order, bring the table back to exactly the state it would have had without the bad job. A legitimate
# later write to the same column is never overwritten: it is reported as a conflict.
#
# Ground truth: the legitimate writes are applied to a second table, rp_expected, that the bad job never touches.
# After the repair the two tables must be identical.
#
# Fails (hypothesis refuted) if: the table differs from the oracle after the repair; a legitimate write is lost;
# an event lacks what the repair needs (for example a deleted row that arrives without all its columns); or the
# flagged conflicts are not exactly the rows where a legitimate write changed a column the bad job also changed.
#
# NOT covered, and not claimed: the comparison with restoring a backup (PITR) — that is the second half of P3;
# a durable event store (here a pod with an emptyDir file); scale (ROWS defaults to 5000); schema changes; a
# repair run while the bad job is still writing; other column types than the ones in the table below.
#
# Prerequisites: make -C bench setup   (kind, PostgreSQL, the Debezium Operator; bench/collectors/cdc/up.sh installs it)
# Env: CLUSTER (db-sensor-bench)  NS (bench)  ROWS (5000, at least 3000)  OUT (results directory)
#
set -uo pipefail
CLUSTER="${CLUSTER:-db-sensor-bench}"; NS="${NS:-bench}"; ROWS="${ROWS:-5000}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${OUT:-${HERE}/../../results/repair-$(date +%Y%m%d-%H%M%S)}"
CTX="kind-${CLUSTER}"
PSQL_CMD="kubectl --context ${CTX} -n ${NS} exec -i bench-postgres-0 -- psql -X -U bench -d bench"
PSQL() { ${PSQL_CMD} "$@"; }
say() { printf '\n== %s\n' "$*"; }
PASS=0; FAIL=0
check() { if [ "$2" -eq 0 ]; then echo "PASS  $1"; PASS=$((PASS + 1)); else echo "FAIL  $1"; FAIL=$((FAIL + 1)); fi; }

[ "${ROWS}" -ge 3000 ] || { echo "ROWS must be at least 3000" >&2; exit 1; }
kubectl --context "$CTX" -n "$NS" get statefulset bench-postgres >/dev/null 2>&1 || { echo "ERROR: run 'make -C bench setup' first." >&2; exit 1; }
kubectl --context "$CTX" get crd debeziumservers.debezium.io >/dev/null 2>&1 || { echo "ERROR: the Debezium Operator is missing (bench/collectors/cdc/up.sh)." >&2; exit 1; }
mkdir -p "$OUT"; OUT="$(cd "$OUT" && pwd)"

cleanup() {
  say "cleanup"
  kubectl --context "$CTX" -n "$NS" delete debeziumserver rp-probe --ignore-not-found --wait=true >/dev/null 2>&1
  for _ in $(seq 1 30); do
    PSQL -tAq -c "select pg_drop_replication_slot('rp_probe') where exists (select 1 from pg_replication_slots where slot_name='rp_probe')" >/dev/null 2>&1 && break
    sleep 3
  done
  PSQL -q -c "drop publication if exists rp_probe_pub; drop table if exists public.rp_orders, public.rp_expected" >/dev/null 2>&1
  kubectl --context "$CTX" -n "$NS" delete pod/rp-receiver svc/rp-receiver configmap/rp-receiver-code --ignore-not-found >/dev/null 2>&1
  echo "  removed the probe DebeziumServer, its slot and publication, both tables and the receiver"
}
trap cleanup EXIT
cleanup >/dev/null 2>&1

both() { # run a statement on the real table and on the oracle; @T@ is the table name
  local t
  for t in rp_orders rp_expected; do
    PSQL -q -v ON_ERROR_STOP=1 -c "${1//@T@/public.$t}" >/dev/null || { echo "statement failed: $1" >&2; exit 1; }
  done
}
mismatch() { # rows that differ between the table and the oracle, both directions
  PSQL -tA -c "select (select count(*) from (select * from public.rp_orders except select * from public.rp_expected) a) + (select count(*) from (select * from public.rp_expected except select * from public.rp_orders) b)"
}

say "1. receiver (a durable log: one event per line)"
kubectl --context "$CTX" -n "$NS" apply -f "${HERE}/receiver.yaml" >/dev/null
kubectl --context "$CTX" -n "$NS" wait --for=condition=ready pod/rp-receiver --timeout=120s >/dev/null || { echo "receiver not ready" >&2; exit 1; }

say "2. tables: rp_orders (REPLICA IDENTITY FULL, ${ROWS} rows, a TOASTed body) and its oracle rp_expected"
PSQL -q -v ON_ERROR_STOP=1 <<SQL >/dev/null
create table public.rp_orders (
  id int primary key, status text not null, amount numeric(12,2) not null, qty int not null, note text,
  updated_at timestamptz not null, meta jsonb not null, active boolean not null, body text not null);
alter table public.rp_orders replica identity full;
create table public.rp_expected (like public.rp_orders including all);
insert into public.rp_orders
select g, 'pending', ((g % 500) + 1)::numeric + 0.25 * (g % 4), g % 20, 'n' || g,
       timestamptz '2026-01-01 00:00:00+00' + (g || ' minutes')::interval,
       jsonb_build_object('k', g, 'tags', jsonb_build_array('a', 'b')), (g % 2 = 0),
       (select string_agg(md5(g::text || i::text), '') from generate_series(1, 200) i)
from generate_series(1, ${ROWS}) g;
insert into public.rp_expected select * from public.rp_orders;
SQL
PSQL -tA -c "select 'rows=' || count(*) || '  body bytes (compressed, in the row)=' || min(pg_column_size(body)) || '  table+toast=' || pg_size_pretty(pg_total_relation_size('public.rp_orders')) from public.rp_orders"

say "3. Debezium Server probe (slot rp_probe, only rp_orders, decimals as strings, no schema in the events)"
cat <<EOF | kubectl --context "$CTX" -n "$NS" apply -f - >/dev/null
apiVersion: debezium.io/v1alpha1
kind: DebeziumServer
metadata:
  name: rp-probe
  namespace: ${NS}
spec:
  runtime:
    environment:
      from:
        - prefix: DATABASE_
          secretRef:
            name: bench-pg-creds
  sink:
    type: http
    config:
      url: http://rp-receiver.${NS}.svc:8080/
  format:
    value:
      type: json
      config:
        schemas.enable: "false"
  source:
    class: io.debezium.connector.postgresql.PostgresConnector
    config:
      connector.class: io.debezium.connector.postgresql.PostgresConnector
      database.hostname: bench-postgres
      database.port: "5432"
      database.user: bench
      database.password: \${DATABASE_password}
      database.dbname: bench
      topic.prefix: rp-probe
      plugin.name: pgoutput
      slot.name: rp_probe
      publication.name: rp_probe_pub
      publication.autocreate.mode: filtered
      snapshot.mode: no_data
      table.include.list: public.rp_orders
      decimal.handling.mode: string
      tombstones.on.delete: "false"
EOF
ok=0
for _ in $(seq 1 80); do
  [ "$(PSQL -tAq -c "select active from pg_replication_slots where slot_name='rp_probe'")" = "t" ] && { ok=1; break; }
  sleep 3
done
[ "$ok" = 1 ] || { echo "ERROR: the slot never became active" >&2; exit 1; }
sleep 12; echo "  slot active"

say "4. workload"
echo "-- good writes before the incident (note on ids 1 to 500)"
both "UPDATE @T@ SET note = 'pre' || id WHERE id BETWEEN 1 AND 500"

echo "-- a good writer keeps running during the incident: it updates note on rows 1 to 1000, the rows the bad job is changing"
PSQL -q -v ON_ERROR_STOP=1 <<'SQL' >/dev/null &
do $$
declare i int := 0; dl timestamptz := clock_timestamp() + interval '14 seconds';
begin
  while clock_timestamp() < dl loop
    i := i + 1;
    update public.rp_orders   set note = 'cw' || i where id = 1 + ((i * 37) % 1000);
    update public.rp_expected set note = 'cw' || i where id = 1 + ((i * 37) % 1000);
    commit;
    perform pg_sleep(0.05);
  end loop;
end $$;
SQL
WRITER=$!
sleep 2
N0=$(PSQL -tA -c "select count(*) from public.rp_orders where note like 'cw%'")

echo "-- the bad job: 10 batches of 100 updates (status, amount x10, meta) 0.3 s apart, a second pass on ids 1 to 100, 100 deletes, 100 junk inserts"
{
  echo "\\set ON_ERROR_STOP on"
  for b in 0 1 2 3 4 5 6 7 8 9; do
    lo=$((b * 100 + 1)); hi=$((b * 100 + 100))
    echo "begin; update public.rp_orders set status='cancelled', amount=amount*10, meta = meta || '{\"migrated\": true}'::jsonb where id between ${lo} and ${hi}; select txid_current() as tx \\gset"
    echo "\\echo BADTX :tx"; echo "commit;"; echo "select pg_sleep(0.3);"
  done
  echo "begin; update public.rp_orders set status='void' where id between 1 and 100; select txid_current() as tx \\gset"
  echo "\\echo BADTX :tx"; echo "commit;"
  echo "begin; delete from public.rp_orders where id between 1001 and 1100; select txid_current() as tx \\gset"
  echo "\\echo BADTX :tx"; echo "commit;"
  echo "begin; insert into public.rp_orders select 100000+g, 'junk', 1.00, 0, 'junk', now(), '{\"junk\": true}'::jsonb, false, 'x' from generate_series(1,100) g; select txid_current() as tx \\gset"
  echo "\\echo BADTX :tx"; echo "commit;"
} > "${OUT}/bad-job.sql"
T_BAD_START=$(python3 -c 'import time;print(time.time())')
BADTX=$(PSQL -q -f - < "${OUT}/bad-job.sql" 2>&1 | grep '^BADTX' | awk '{print $2}' | paste -sd, -)
T_BAD_END=$(python3 -c 'import time;print(time.time())')
echo "  bad transactions: ${BADTX}"
OTHERS=$(python3 -c "import sys;t=[int(x) for x in sys.argv[1].split(',')];print(max(t)-min(t)+1-len(t))" "${BADTX}")
echo "  transaction ids taken by other sessions between the first and the last bad transaction: ${OTHERS}"
wait "$WRITER" 2>/dev/null
NTOT=$(PSQL -tA -c "select count(*) from public.rp_orders where note like 'cw%'")
echo "  the good writer made ${NTOT} writes (${N0} before the bad job began)"

echo "-- good writes after the incident, before anyone notices"
both "UPDATE @T@ SET note = 'post' || id WHERE id BETWEEN 1 AND 300"
both "UPDATE @T@ SET status = 'shipped' WHERE id BETWEEN 301 AND 350"
both "DELETE FROM @T@ WHERE id BETWEEN 351 AND 360"
both "INSERT INTO @T@ SELECT 200000+g, 'pending', 10.00, 1, 'good', timestamptz '2026-06-01 00:00:00+00', '{\"good\": true}'::jsonb, true, 'x' FROM generate_series(1,200) g"

EXPECTED=$((500 + 1300 + NTOT + 560))
say "5. wait for the stream (expecting ${EXPECTED} events: 500 + 1300 bad + ${NTOT} concurrent + 560)"
GOT=0
for _ in $(seq 1 90); do
  GOT=$(kubectl --context "$CTX" -n "$NS" exec rp-receiver -- python -c "import urllib.request;print(urllib.request.urlopen('http://localhost:8080/').read().decode())" 2>/dev/null || echo 0)
  [ "${GOT}" = "${EXPECTED}" ] && break
  sleep 2
done
echo "  receiver holds ${GOT} events"
check "the stream is complete (${GOT} of ${EXPECTED} events)" $([ "${GOT}" = "${EXPECTED}" ]; echo $?)
check "good transactions landed between the bad ones, so picking by txId is a real test (${OTHERS} other ids in between)" $([ "${OTHERS}" -ge 20 ]; echo $?)
kubectl --context "$CTX" -n "$NS" cp "rp-receiver:/data/events.jsonl" "${OUT}/events.jsonl" >/dev/null 2>&1
echo "  events saved: $(wc -c < "${OUT}/events.jsonl" | tr -d ' ') bytes"

say "6. the incident, as the table shows it"
PRE=$(mismatch); echo "  rows that differ from the oracle before the repair: ${PRE}"
check "the incident is visible (the table differs from the oracle)" $([ "${PRE}" -gt 0 ]; echo $?)

say "7. repair from the events of the bad transactions only"
T_R0=$(python3 -c 'import time;print(time.time())')
python3 "${HERE}/repair.py" --events "${OUT}/events.jsonl" --bad-txids "${BADTX}" --psql "${PSQL_CMD}" --report "${OUT}/repair-report.json" > "${OUT}/repair-stdout.txt" 2> "${OUT}/repair-stderr.txt"
RC=$?
T_R1=$(python3 -c 'import time;print(time.time())')
cat "${OUT}/repair-stdout.txt"; cat "${OUT}/repair-stderr.txt"
check "the repair ran and committed" $RC

say "8. result"
POST=$(mismatch); echo "  rows that differ from the oracle after the repair: ${POST}"
check "the table equals the oracle: every legitimate write is kept and every bad change is undone" $([ "${POST}" = "0" ]; echo $?)
python3 - "${OUT}/repair-report.json" <<'PYEOF'
import json, sys
r = json.load(open(sys.argv[1])); ok = True
def chk(name, cond):
    global ok
    print(("PASS  " if cond else "FAIL  ") + name); ok &= bool(cond)
chk("the bad transactions hold 1100 updates, 100 deletes and 100 inserts", r["bad_ops"] == {"c": 100, "u": 1100, "d": 100})
chk("the conflicts are exactly ids 301 to 350, column status", r["column_conflict_ids"] == list(range(301, 351)) and r["column_conflict_columns"] == ["status"])
chk("the 10 rows a legitimate write deleted are reported as gone (ids 351 to 360)", r["rows_gone_ids"] == list(range(351, 361)))
chk("the 100 deleted rows are re-inserted and the 100 junk rows removed, without conflicts",
    r["rows_reinserted"] == 100 and r["rows_removed"] == 100 and r["reinsert_conflicts"] == 0 and r["remove_conflicts"] == 0)
sys.exit(0 if ok else 1)
PYEOF
check "the report matches the scenario" $?

say "9. numbers for the write-up"
python3 - "${OUT}/repair-report.json" "${T_BAD_START}" "${T_BAD_END}" "${T_R0}" "${T_R1}" "${N0}" "${NTOT}" "${ROWS}" <<'PYEOF'
import json, sys
r = json.load(open(sys.argv[1])); bs, be, r0, r1, n0, ntot, rows = sys.argv[2:]
print("bad job ran for %.1f s; repair took %.1f s end to end (build %.2f s, apply %.2f s) for %d statements"
      % (float(be) - float(bs), float(r1) - float(r0), r["seconds_build"], r["seconds_apply"], r["statements"]))
print("events file: %d bytes for %d events; the %d events of the bad job: %d bytes (about %d bytes per event)"
      % (r["file_bytes"], r["events_in_file"], r["bad_events"], r["bad_event_bytes"], r["bad_event_bytes"] // max(1, r["bad_events"])))
print("columns restored: %d; column conflicts left for a person: %d (%d rows); rows gone: %d; unchanged TOAST columns skipped: %d"
      % (r["columns_restored"], r["column_conflicts"], len(r["column_conflict_ids"]), r["rows_gone"], r["unchanged_toast_placeholders_skipped"]))
after_bad = 560 + (int(ntot) - int(n0))
print("arithmetic, not a measurement: restoring a whole backup to just before the bad job would also discard the "
      "%d legitimate writes made after it began (560 after the incident + %d by the concurrent writer from the moment it began)" % (after_bad, int(ntot) - int(n0)))
PYEOF

say "result: ${PASS} passed, ${FAIL} failed (raw data in ${OUT})"
[ "$FAIL" -eq 0 ]
