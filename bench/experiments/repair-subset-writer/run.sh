#!/usr/bin/env bash
#
# bench/experiments/repair-subset-writer/run.sh — Scenario 1, Bungie-shaped: a bad writer corrupts a SUBSET of rows with a
# plausible wrong value while good writers keep working; three repairs are compared on the SAME incident.
#
# This is a lab approximation of the SHAPE of a real incident (see RESULTS.md); it is not a reproduction of it.
# Hypotheses are pre-registered in RESULTS.md (committed before the first run). This script only measures.
#
# One throwaway PostgreSQL 16 pod (sc1-pg) with WAL archiving AND logical decoding hosts everything, so the stream
# repair and the PITR strategies see exactly the same incident:
#   - sc_inventory: 5000 rows on 10 "servers" (server_id), REPLICA IDENTITY FULL, a trigger that stamps writer_app with
#     current_setting('application_name'). That column is how the bad writer is identified in the event (txId is not).
#   - sc_expected: the oracle; receives only the legitimate writes. legit_ver counts legitimate writes per row.
#   - bad writer (application_name inventory-patch-v2): qty = 100 on rows of servers 3, 4, 5 whose qty > 100.
#   - good writer (application_name game-economy): other rows and the same rows, during and after the bad writer.
#   - restore point pre_patch just before the bad writer starts (what a pre-patch backup would be).
# Strategies, each scored against the oracle by analyze.py:
#   (a) stream repair: undo only the events tagged with the bad writer (repair.py), on a copy of the incident table
#   (b) PITR clone at pre_patch, rows of servers 3, 4, 5 copied from the clone into a copy of the incident table
#   (c) full restore: the whole dataset replaced by the PITR clone at pre_patch
#
# Prerequisites: a kind cluster with the Debezium Operator (make -C bench setup; bench/collectors/cdc/up.sh).
# Never touches bench-postgres or bench-cdc. Everything created here is named sc1-* and removed on exit.
# Env: CLUSTER (db-sensor-bench)  NS (bench)  RUN (label, default timestamp)  PAD_ROWS (0)  PAD_PASSES (0)
#
set -uo pipefail
CLUSTER="${CLUSTER:-db-sensor-bench}"; NS="${NS:-bench}"; ROWS=5000
PAD_ROWS="${PAD_ROWS:-0}"; PAD_PASSES="${PAD_PASSES:-0}"
RUN="${RUN:-$(date +%Y%m%d-%H%M%S)}"
BAD_APP="inventory-patch-v2"; GOOD_APP="game-economy"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${OUT:-${HERE}/raw/${RUN}}"
CTX="kind-${CLUSTER}"
KX() { kubectl --context "$CTX" -n "$NS" "$@"; }
PA() { local app="$1"; shift; KX exec -i sc1-pg -- env PGAPPNAME="$app" psql -X -q -v ON_ERROR_STOP=1 -h /tmp -p 5432 -U postgres -d postgres "$@"; }
P()  { PA seed "$@"; }
G()  { PA "$GOOD_APP" "$@"; }
C()  { KX exec -i sc1-pg -- psql -X -q -v ON_ERROR_STOP=1 -h /tmp -p 5434 -U postgres -d postgres "$@"; }
now() { python3 -c 'import time;print(time.time())'; }
secs() { python3 -c "print('%.2f' % ($2 - $1))"; }
say() { printf '\n== %s\n' "$*"; }
PASS=0; FAIL=0
check() { if [ "$2" -eq 0 ]; then echo "PASS  $1"; PASS=$((PASS + 1)); else echo "FAIL  $1"; FAIL=$((FAIL + 1)); fi; }
mkdir -p "$OUT"; OUT="$(cd "$OUT" && pwd)"

cleanup() {
  say "cleanup"
  KX delete debeziumserver sc1-probe --ignore-not-found --wait=true >/dev/null 2>&1
  KX delete pod/sc1-pg svc/sc1-pg pod/sc1-receiver svc/sc1-receiver configmap/sc1-receiver-code --ignore-not-found --wait=true >/dev/null 2>&1
  echo "  removed: DebeziumServer sc1-probe, pod and service sc1-pg (its slot and clone go with it), receiver pod, service and configmap"
}
trap cleanup EXIT
cleanup >/dev/null 2>&1

say "1. pods: sc1-pg (WAL archive + logical decoding) and sc1-receiver (event log)"
KX apply -f "${HERE}/pod.yaml" -f "${HERE}/receiver.yaml" >/dev/null
KX wait --for=condition=ready pod/sc1-pg pod/sc1-receiver --timeout=180s >/dev/null || { echo "pods not ready" >&2; exit 1; }
KX exec sc1-pg -- bash -c 'mkdir -p /work/archive && initdb -D /work/primary -U postgres --auth=trust >/dev/null' || { echo "initdb failed" >&2; exit 1; }
KX exec -i sc1-pg -- bash -c 'cat >> /work/primary/postgresql.conf' <<'EOF'
port = 5432
unix_socket_directories = '/tmp'
listen_addresses = '*'
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
archive_mode = on
archive_command = 'test ! -f /work/archive/%f && cp %p /work/archive/%f'
fsync = on
EOF
KX exec sc1-pg -- bash -c 'echo "host all all 0.0.0.0/0 trust" >> /work/primary/pg_hba.conf && echo "host replication all 0.0.0.0/0 trust" >> /work/primary/pg_hba.conf && pg_ctl -D /work/primary -l /work/primary.log -w start >/dev/null' || { echo "primary failed" >&2; exit 1; }
P -tA -c "select version()"

say "2. tables: sc_inventory (${ROWS} rows, 10 servers of 500, tag trigger), oracle sc_expected, ledger, pad"
P <<SQL >/dev/null
create table public.sc_inventory (
  id int primary key, server_id int not null, account int not null, item text not null, qty int not null,
  note text, legit_ver int not null default 0, writer_app text, body text not null);
insert into public.sc_inventory
select g, ((g - 1) / 500) + 1, g % 1000, 'item' || (g % 7), (g * 7) % 300, 'n' || g, 0, 'seed',
       (select string_agg(md5(g::text || i::text), '') from generate_series(1, 50) i)
from generate_series(1, ${ROWS}) g;
create table public.sc_expected (like public.sc_inventory including all);
insert into public.sc_expected select * from public.sc_inventory;
create table public.sc_bad_ledger (id int not null, tx bigint not null);
create table public.sc_pad (id int primary key, v int not null, filler text not null);
insert into public.sc_pad select g, 0, repeat('p', 100) from generate_series(1, ${PAD_ROWS}) g;
alter table public.sc_inventory replica identity full;
create function public.sc_stamp() returns trigger language plpgsql as \$\$
begin NEW.writer_app := current_setting('application_name'); return NEW; end \$\$;
create trigger sc_stamp before insert or update on public.sc_inventory for each row execute function public.sc_stamp();
SQL
P -tA -c "select 'rows=' || count(*) || '  bad-server rows with qty>100: ' || count(*) filter (where server_id in (3,4,5) and qty > 100) from public.sc_inventory"

say "3. base backup (before the workload, before the stream)"
T0=$(now)
KX exec sc1-pg -- pg_basebackup -h /tmp -p 5432 -U postgres -D /work/base -c fast --wal-method=none >/dev/null || { echo "basebackup failed" >&2; exit 1; }
T1=$(now); echo "  base backup took $(secs "$T0" "$T1") s"

say "4. Debezium Server probe (slot sc1_probe, only sc_inventory, decimals as strings, no schema in the events)"
cat <<EOF | KX apply -f - >/dev/null
apiVersion: debezium.io/v1alpha1
kind: DebeziumServer
metadata:
  name: sc1-probe
  namespace: ${NS}
spec:
  sink:
    type: http
    config:
      url: http://sc1-receiver.${NS}.svc:8080/
  format:
    value:
      type: json
      config:
        schemas.enable: "false"
  source:
    class: io.debezium.connector.postgresql.PostgresConnector
    config:
      connector.class: io.debezium.connector.postgresql.PostgresConnector
      database.hostname: sc1-pg
      database.port: "5432"
      database.user: postgres
      database.password: unused
      database.dbname: postgres
      topic.prefix: sc1-probe
      plugin.name: pgoutput
      slot.name: sc1_probe
      publication.name: sc1_probe_pub
      publication.autocreate.mode: filtered
      snapshot.mode: no_data
      table.include.list: public.sc_inventory
      decimal.handling.mode: string
      tombstones.on.delete: "false"
EOF
ok=0
for _ in $(seq 1 80); do
  [ "$(P -tAq -c "select active from pg_replication_slots where slot_name='sc1_probe'")" = "t" ] && { ok=1; break; }
  sleep 3
done
[ "$ok" = 1 ] || { echo "ERROR: the slot never became active" >&2; exit 1; }
sleep 12; echo "  slot active"
sleep 2
stat() { P -tA -c "select n_tup_ins + n_tup_upd + n_tup_del from pg_stat_user_tables where relname = 'sc_inventory'"; }
STAT0=$(stat)

both() { # run a statement on the real table and on the oracle as the good writer; @T@ is the table name
  local t
  for t in sc_inventory sc_expected; do
    G -c "${1//@T@/public.$t}" >/dev/null || { echo "statement failed: $1" >&2; exit 1; }
  done
}

say "5. workload"
if [ "${PAD_PASSES}" -gt 0 ] && [ "${PAD_ROWS}" -gt 0 ]; then
  echo "-- padding: ${PAD_PASSES} passes over ${PAD_ROWS} unrelated rows (WAL between the base backup and the restore point)"
  for _ in $(seq 1 "${PAD_PASSES}"); do P -c "update public.sc_pad set v = v + 1" >/dev/null; done
fi
LSN0=$(P -tA -c "select pg_current_wal_lsn()")
echo "-- legitimate writes before the patch"
both "UPDATE @T@ SET note = 'pre' || id, legit_ver = legit_ver + 1 WHERE id BETWEEN 1 AND 400 OR id BETWEEN 1001 AND 1200"
both "UPDATE @T@ SET qty = qty + 7, legit_ver = legit_ver + 1 WHERE id BETWEEN 1201 AND 1400"
P -tA -c "copy (select id,server_id,account,item,qty,note,legit_ver,md5(body) from public.sc_expected order by id) to stdout with csv" > "${OUT}/oracle-pre.csv"
P -tA -c "select pg_create_restore_point('pre_patch')" | sed 's/^/  restore point pre_patch at /'

echo "-- the good writer keeps running for 14 s: other servers, and also the bad servers (note, small qty deltas)"
G <<'SQL' >/dev/null &
do $$
declare i int := 0; dl timestamptz := clock_timestamp() + interval '14 seconds';
begin
  while clock_timestamp() < dl loop
    i := i + 1;
    update public.sc_inventory set qty = qty + 1, legit_ver = legit_ver + 1 where id = 1 + ((i * 37) % 1000);
    update public.sc_expected  set qty = qty + 1, legit_ver = legit_ver + 1 where id = 1 + ((i * 37) % 1000);
    if i % 3 = 0 then
      update public.sc_inventory set note = 'cw' || i, legit_ver = legit_ver + 1 where id = 1001 + ((i * 53) % 1500);
      update public.sc_expected  set note = 'cw' || i, legit_ver = legit_ver + 1 where id = 1001 + ((i * 53) % 1500);
    end if;
    if i % 7 = 0 then
      update public.sc_inventory set qty = qty + 2, legit_ver = legit_ver + 1 where id = 1001 + ((i * 91) % 1500);
      update public.sc_expected  set qty = qty + 2, legit_ver = legit_ver + 1 where id = 1001 + ((i * 91) % 1500);
    end if;
    if i % 5 = 0 then
      update public.sc_inventory set note = 'cw' || i, legit_ver = legit_ver + 1 where id = 2501 + ((i * 29) % 2500);
      update public.sc_expected  set note = 'cw' || i, legit_ver = legit_ver + 1 where id = 2501 + ((i * 29) % 2500);
    end if;
    commit;
    perform pg_sleep(0.05);
  end loop;
end $$;
SQL
WRITER=$!
sleep 2

echo "-- the bad writer (${BAD_APP}): qty = 100 where qty > 100, on servers 3, 4, 5, in 10 batches 0.3 s apart"
{
  echo "\\set ON_ERROR_STOP on"
  for b in 0 1 2 3 4 5 6 7 8 9; do
    lo=$((1001 + b * 150)); hi=$((1000 + (b + 1) * 150))
    echo "begin; select txid_current() as tx \\gset"
    echo "\\echo BADTX :tx"
    echo "with u as (update public.sc_inventory set qty = 100 where server_id in (3,4,5) and id between ${lo} and ${hi} and qty > 100 returning id) insert into public.sc_bad_ledger select id, :tx from u;"
    echo "commit;"; echo "select pg_sleep(0.3);"
  done
} > "${OUT}/bad-job.sql"
T_BAD0=$(now)
BADTX=$(PA "$BAD_APP" -f - < "${OUT}/bad-job.sql" 2>&1 | grep '^BADTX' | awk '{print $2}' | paste -sd, -)
T_BAD1=$(now)
echo "  bad job ran for $(secs "$T_BAD0" "$T_BAD1") s; transactions: ${BADTX}"
wait "$WRITER" 2>/dev/null
echo "-- legitimate writes after the bad writer stopped (same rows and others)"
both "UPDATE @T@ SET note = 'post' || id, legit_ver = legit_ver + 1 WHERE id BETWEEN 1001 AND 1300"
both "UPDATE @T@ SET qty = qty + 5, legit_ver = legit_ver + 1 WHERE id BETWEEN 1301 AND 1340"
both "UPDATE @T@ SET qty = qty + 1, legit_ver = legit_ver + 1 WHERE id BETWEEN 2501 AND 2600"
both "DELETE FROM @T@ WHERE id BETWEEN 1401 AND 1410"
both "INSERT INTO @T@ SELECT 300000+g, 1 + (g % 10), g, 'new', 50, 'new', 1, null, 'x' FROM generate_series(1,100) g"
sleep 3
EXPECTED=$(( $(stat) - STAT0 ))
LSN1=$(P -tA -c "select pg_current_wal_lsn()")
WAL_BYTES=$(P -tA -c "select pg_wal_lsn_diff('${LSN1}', '${LSN0}')::bigint")
P -tA -c "copy (select id, tx from public.sc_bad_ledger order by id) to stdout with csv" > "${OUT}/bad-ledger.csv"
AFFECTED=$(wc -l < "${OUT}/bad-ledger.csv" | tr -d ' ')
echo "  the bad writer changed ${AFFECTED} rows; ${EXPECTED} change events expected on sc_inventory since the stream started"
dump() { # $1 = P or C, $2 = table
  "$1" -tA -c "copy (select id,server_id,account,item,qty,note,legit_ver,md5(body) from public.$2 order by id) to stdout with csv"
}
dump P sc_expected > "${OUT}/oracle.csv"
dump P sc_inventory > "${OUT}/incident.csv"
P -c "create table public.sc_work_a (like public.sc_inventory including all); insert into public.sc_work_a select * from public.sc_inventory;
      create table public.sc_work_b (like public.sc_inventory including all); insert into public.sc_work_b select * from public.sc_inventory;" >/dev/null
P -tA -c "select pg_switch_wal()" >/dev/null
for _ in $(seq 1 90); do
  [ "$(P -tA -c "select (last_archived_wal >= pg_walfile_name('${LSN1}'))::int from pg_stat_archiver")" = "1" ] && break
  sleep 1
done
check "the archiver holds the WAL up to the end of the workload" $([ "$(P -tA -c "select (last_archived_wal >= pg_walfile_name('${LSN1}'))::int from pg_stat_archiver")" = "1" ]; echo $?)
ARCH_BYTES=$(KX exec sc1-pg -- du -sb /work/archive | awk '{print $1}')

say "6. wait for the stream (expecting ${EXPECTED} events)"
GOT=0
for _ in $(seq 1 120); do
  GOT=$(KX exec sc1-receiver -- python -c "import urllib.request;print(urllib.request.urlopen('http://localhost:8080/').read().decode())" 2>/dev/null || echo 0)
  [ "${GOT}" = "${EXPECTED}" ] && break
  sleep 2
done
echo "  receiver holds ${GOT} events"
check "the stream is complete (${GOT} of ${EXPECTED} events)" $([ "${GOT}" = "${EXPECTED}" ]; echo $?)

say "7. strategy (a): stream repair, events selected by the tag ${BAD_APP}"
T_F0=$(now)
KX cp "sc1-receiver:/data/events.jsonl" "${OUT}/events.jsonl" >/dev/null 2>&1
T_F1=$(now)
T_A0=$(now)
python3 "${HERE}/repair.py" --events "${OUT}/events.jsonl" --tag "${BAD_APP}" --table public.sc_work_a \
  --psql "kubectl --context ${CTX} -n ${NS} exec -i sc1-pg -- env PGAPPNAME=stream-repair psql -X -h /tmp -p 5432 -U postgres -d postgres" \
  --report "${OUT}/repair-report.json" > "${OUT}/repair-stdout.txt" 2> "${OUT}/repair-stderr.txt"
RC=$?
T_A1=$(now)
cat "${OUT}/repair-stdout.txt" "${OUT}/repair-stderr.txt"
check "the stream repair ran and committed" $RC
FETCH_S=$(secs "$T_F0" "$T_F1"); REPAIR_S=$(secs "$T_A0" "$T_A1"); STREAM_S=$(secs "$T_F0" "$T_A1")
echo "  fetch ${FETCH_S} s + repair ${REPAIR_S} s = ${STREAM_S} s"
dump P sc_work_a > "${OUT}/state-a.csv"
gzip -9 -f "${OUT}/events.jsonl"

say "8. PITR clone at pre_patch (shared by (b) and (c))"
T_R0=$(now)
KX exec sc1-pg -- bash -c "cp -r /work/base /work/clone && chmod 700 /work/clone && touch /work/clone/recovery.signal && cat >> /work/clone/postgresql.auto.conf <<'EOF'
port = 5434
listen_addresses = ''
archive_mode = off
restore_command = 'cp /work/archive/%f %p'
recovery_target_name = 'pre_patch'
recovery_target_action = promote
EOF
pg_ctl -D /work/clone -l /work/clone.log -w start >/dev/null" || { echo "clone failed to start" >&2; KX exec sc1-pg -- tail -30 /work/clone.log; exit 1; }
for _ in $(seq 1 1200); do [ "$(C -tA -c "select pg_is_in_recovery()::int" 2>/dev/null)" = "0" ] && break; sleep 0.25; done
T_R1=$(now)
RECOVERY_S=$(secs "$T_R0" "$T_R1")
echo "  recovery time (copy the backup, replay the archive, promote): ${RECOVERY_S} s"
KX exec sc1-pg -- grep -E "recovery stopping|redo done|archive recovery complete" /work/clone.log | sed 's/^/  log: /'
check "the clone was promoted" $([ "$(C -tA -c "select pg_is_in_recovery()::int")" = "0" ]; echo $?)
dump C sc_inventory > "${OUT}/state-c.csv"      # strategy (c): the whole dataset is the clone

say "9. strategy (b): rows of servers 3, 4, 5 copied from the clone into the incident table"
T_B0=$(now)
C -tA -c "copy (select * from public.sc_inventory where server_id in (3,4,5)) to stdout with csv" > "${OUT}/clone-bad-servers.csv"
{
  echo "begin;"
  echo "create temp table t (like public.sc_work_b);"
  echo "copy t from stdin with csv;"
  cat "${OUT}/clone-bad-servers.csv"
  echo "\\."
  echo "insert into public.sc_work_b select * from t on conflict (id) do update set server_id=excluded.server_id, account=excluded.account, item=excluded.item, qty=excluded.qty, note=excluded.note, legit_ver=excluded.legit_ver, writer_app=excluded.writer_app, body=excluded.body;"
  echo "commit;"
} | PA rowcopy >/dev/null
T_B1=$(now)
ROWCOPY_S=$(secs "$T_B0" "$T_B1")
echo "  copied $(wc -l < "${OUT}/clone-bad-servers.csv" | tr -d ' ') rows in ${ROWCOPY_S} s"
dump P sc_work_b > "${OUT}/state-b.csv"
gzip -9 -f "${OUT}/clone-bad-servers.csv"

say "10. numbers"
cat > "${OUT}/summary.json" <<EOF
{"run": "${RUN}", "rows": ${ROWS}, "pad_rows": ${PAD_ROWS}, "pad_passes": ${PAD_PASSES},
 "wal_bytes_since_pre_backup_point": ${WAL_BYTES}, "archive_bytes": ${ARCH_BYTES},
 "events_expected": ${EXPECTED}, "events_received": ${GOT}, "affected_rows": ${AFFECTED}, "bad_txids": "${BADTX}",
 "stream_fetch_s": ${FETCH_S}, "stream_repair_s": ${REPAIR_S}, "stream_total_s": ${STREAM_S},
 "pitr_recovery_s": ${RECOVERY_S}, "rowcopy_s": ${ROWCOPY_S}}
EOF
python3 "${HERE}/analyze.py" "${OUT}"
say "harness: ${PASS} passed, ${FAIL} failed (raw data in ${OUT}; hypotheses are judged by summarize.py across runs)"
[ "$FAIL" -eq 0 ]
