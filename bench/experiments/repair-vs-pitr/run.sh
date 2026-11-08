#!/usr/bin/env bash
#
# bench/experiments/repair-vs-pitr/run.sh — P3, second half: the same incident, repaired with point-in-time recovery.
#
# Question: for the incident that repair-from-stream repaired exactly, what does the usual alternative (a base backup
# plus archived WAL, recovered to just before the bad job) leave behind, and how long does it take?
#
# Same workload as repair-from-stream/run.sh (5000-row table, a bad job of 13 transactions: 1100 updates, 100 deletes,
# 100 junk inserts; a good writer running during the incident; 560 good writes after it; the oracle table rp_expected).
# Here the database is a throwaway PostgreSQL 16 with archive_mode on, and a base backup taken before the workload.
#
# Hypotheses (written before the first run):
#   H1  A recovered clone, stopped just before the first bad commit, differs from the oracle exactly by the legitimate
#       writes made after that point: it is correct for the past and wrong for the present. It cannot be "swapped in"
#       without losing them.
#   H2  The common middle road (copy only the rows the bad job touched from the clone back into the live table)
#       overwrites every legitimate write made to those rows after the incident began, so it leaves rows that differ
#       from the oracle; the stream repair left none.
#   H3  Recovery time grows with the WAL written between the backup and the target; the stream repair time does not
#       (it depends on the events of the bad transactions). Tested with PAD_ROWS x PAD_PASSES extra WAL before the
#       incident.
#
# Fails (a hypothesis is refuted) if: the clone equals the oracle; the row-copy result equals the oracle; the recovery
# time does not grow with the extra WAL.
#
# NOT covered, and not claimed: the stream repair in this same run (its numbers come from repair-from-stream, a
# different PostgreSQL, so times are only indicative); a real object store or a managed service PITR; the cost of
# taking the backups; network; a recovery target found by time instead of by transaction id (here the bad transaction
# id is GIVEN, as it is in the stream repair); logical differences caused by foreign keys or triggers.
#
# Prerequisites: a kind cluster (make -C bench setup). Does not touch bench-postgres.
# Env: CLUSTER (db-sensor-bench)  NS (bench)  ROWS (5000)  PAD_ROWS (0)  PAD_PASSES (0)  OUT (results directory)
#
set -uo pipefail
CLUSTER="${CLUSTER:-db-sensor-bench}"; NS="${NS:-bench}"; ROWS="${ROWS:-5000}"
PAD_ROWS="${PAD_ROWS:-0}"; PAD_PASSES="${PAD_PASSES:-0}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${OUT:-${HERE}/../../results/repair-pitr-$(date +%Y%m%d-%H%M%S)}"
CTX="kind-${CLUSTER}"
X() { kubectl --context "$CTX" -n "$NS" exec -i pitr-pg -- "$@"; }
P() { X psql -X -q -v ON_ERROR_STOP=1 -h /tmp -p 5433 -U postgres -d postgres "$@"; }   # primary
C() { X psql -X -q -v ON_ERROR_STOP=1 -h /tmp -p 5434 -U postgres -d postgres "$@"; }   # recovered clone
now() { python3 -c 'import time;print(time.time())'; }
say() { printf '\n== %s\n' "$*"; }
PASS=0; FAIL=0
check() { if [ "$2" -eq 0 ]; then echo "PASS  $1"; PASS=$((PASS + 1)); else echo "FAIL  $1"; FAIL=$((FAIL + 1)); fi; }
mkdir -p "$OUT"; OUT="$(cd "$OUT" && pwd)"
cleanup() { say "cleanup"; kubectl --context "$CTX" -n "$NS" delete pod pitr-pg --ignore-not-found --wait=true >/dev/null 2>&1; echo "  removed the pitr-pg pod"; }
trap cleanup EXIT
kubectl --context "$CTX" -n "$NS" delete pod pitr-pg --ignore-not-found --wait=true >/dev/null 2>&1
kubectl --context "$CTX" -n "$NS" apply -f "${HERE}/pod.yaml" >/dev/null
kubectl --context "$CTX" -n "$NS" wait --for=condition=ready pod/pitr-pg --timeout=180s >/dev/null || { echo "pod not ready" >&2; exit 1; }

say "1. primary with WAL archiving (archive_mode=on, archive_command copies each segment to /work/archive)"
X bash -c 'mkdir -p /work/archive && initdb -D /work/primary -U postgres --auth=trust >/dev/null' || exit 1
X bash -c "cat >> /work/primary/postgresql.conf <<'EOF'
port = 5433
unix_socket_directories = '/tmp'
listen_addresses = ''
wal_level = replica
archive_mode = on
archive_command = 'test ! -f /work/archive/%f && cp %p /work/archive/%f'
max_wal_senders = 4
fsync = on
EOF
pg_ctl -D /work/primary -l /work/primary.log -w start >/dev/null" || exit 1
P -c "select version()" -tA

say "2. tables (${ROWS} rows, TOASTed body) and optional WAL padding table (${PAD_ROWS} rows x ${PAD_PASSES} passes)"
P <<SQL >/dev/null
create table public.rp_orders (
  id int primary key, status text not null, amount numeric(12,2) not null, qty int not null, note text,
  updated_at timestamptz not null, meta jsonb not null, active boolean not null, body text not null);
create table public.rp_expected (like public.rp_orders including all);
insert into public.rp_orders
select g, 'pending', ((g % 500) + 1)::numeric + 0.25 * (g % 4), g % 20, 'n' || g,
       timestamptz '2026-01-01 00:00:00+00' + (g || ' minutes')::interval,
       jsonb_build_object('k', g, 'tags', jsonb_build_array('a', 'b')), (g % 2 = 0),
       (select string_agg(md5(g::text || i::text), '') from generate_series(1, 200) i)
from generate_series(1, ${ROWS}) g;
insert into public.rp_expected select * from public.rp_orders;
create table public.rp_pad (id int primary key, v int not null, filler text not null);
insert into public.rp_pad select g, 0, repeat('p', 100) from generate_series(1, ${PAD_ROWS}) g;
SQL

say "3. base backup (pg_basebackup, WAL from the archive), taken before the workload"
T0=$(now)
X pg_basebackup -h /tmp -p 5433 -U postgres -D /work/base -c fast --wal-method=none >/dev/null || { echo "basebackup failed" >&2; exit 1; }
T1=$(now)
BASE_BYTES=$(X du -sb /work/base | awk '{print $1}')
python3 -c "print('  base backup: %d bytes in %.1f s' % ($BASE_BYTES, $T1 - $T0))"

say "4. workload (same as repair-from-stream)"
LSN0=$(P -tA -c "select pg_current_wal_lsn()")
if [ "${PAD_PASSES}" -gt 0 ] && [ "${PAD_ROWS}" -gt 0 ]; then
  echo "-- padding: ${PAD_PASSES} full passes over ${PAD_ROWS} rows (legitimate WAL between the backup and the incident)"
  for _ in $(seq 1 "${PAD_PASSES}"); do P -c "update public.rp_pad set v = v + 1" >/dev/null; done
fi
both() { local t; for t in rp_orders rp_expected; do P -c "${1//@T@/public.$t}" >/dev/null || { echo "statement failed: $1" >&2; exit 1; }; done; }
echo "-- good writes before the incident"
both "UPDATE @T@ SET note = 'pre' || id WHERE id BETWEEN 1 AND 500"
echo "-- a good writer keeps running for 14 s on rows 1 to 1000"
P <<'SQL' >/dev/null &
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
N0=$(P -tA -c "select count(*) from public.rp_orders where note like 'cw%'")
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
BADTX=$(X psql -X -q -h /tmp -p 5433 -U postgres -d postgres -f - < "${OUT}/bad-job.sql" 2>&1 | grep '^BADTX' | awk '{print $2}' | paste -sd, -)
echo "  bad transactions: ${BADTX}"
FIRST=$(echo "${BADTX}" | cut -d, -f1)
wait "$WRITER" 2>/dev/null
NTOT=$(P -tA -c "select count(*) from public.rp_orders where note like 'cw%'")
echo "  the good writer made ${NTOT} writes (${N0} before the bad job began)"
echo "-- good writes after the incident"
both "UPDATE @T@ SET note = 'post' || id WHERE id BETWEEN 1 AND 300"
both "UPDATE @T@ SET status = 'shipped' WHERE id BETWEEN 301 AND 350"
both "DELETE FROM @T@ WHERE id BETWEEN 351 AND 360"
both "INSERT INTO @T@ SELECT 200000+g, 'pending', 10.00, 1, 'good', timestamptz '2026-06-01 00:00:00+00', '{\"good\": true}'::jsonb, true, 'x' FROM generate_series(1,200) g"
LSN1=$(P -tA -c "select pg_current_wal_lsn()")
WAL_BYTES=$(P -tA -c "select pg_wal_lsn_diff('${LSN1}', '${LSN0}')::bigint")
P -tA -c "select pg_switch_wal()" >/dev/null
for _ in $(seq 1 60); do
  [ "$(P -tA -c "select (last_archived_wal >= pg_walfile_name('${LSN1}'))::int from pg_stat_archiver")" = "1" ] && break
  sleep 1
done
ARCH_FILES=$(X bash -c 'ls /work/archive | grep -vc history')
ARCH_BYTES=$(X du -sb /work/archive | awk '{print $1}')
echo "  WAL written since the backup: ${WAL_BYTES} bytes; archive: ${ARCH_FILES} segments, ${ARCH_BYTES} bytes"
LEGIT_AFTER=$((560 + NTOT - N0))
check "the archiver has the WAL up to the end of the workload" $([ "$(P -tA -c "select (last_archived_wal >= pg_walfile_name('${LSN1}'))::int from pg_stat_archiver")" = "1" ]; echo $?)

say "5. point-in-time recovery: restore the base backup, replay the archive, stop just before the first bad commit (xid ${FIRST})"
T_R0=$(now)
X bash -c "cp -r /work/base /work/clone && chmod 700 /work/clone && touch /work/clone/recovery.signal && cat >> /work/clone/postgresql.auto.conf <<'EOF'
port = 5434
archive_mode = off
restore_command = 'cp /work/archive/%f %p'
recovery_target_xid = '${FIRST}'
recovery_target_inclusive = off
recovery_target_action = promote
EOF
pg_ctl -D /work/clone -l /work/clone.log -w start >/dev/null" || { echo "clone failed to start" >&2; X tail -30 /work/clone.log; exit 1; }
for _ in $(seq 1 600); do [ "$(C -tA -c "select pg_is_in_recovery()::int" 2>/dev/null)" = "0" ] && break; sleep 0.5; done
T_R1=$(now)
python3 -c "print('  recovery time (copy the backup, replay, promote): %.1f s' % ($T_R1 - $T_R0))"
X grep -E "last completed transaction|recovery stopping|redo done|archive recovery complete" /work/clone.log | sed 's/^/  log: /'
check "the clone was promoted" $([ "$(C -tA -c "select pg_is_in_recovery()::int")" = "0" ]; echo $?)
RECOVERY_S=$(python3 -c "print('%.1f' % ($T_R1 - $T_R0))")

say "6. the clone against the oracle (swap the clone in: what remains wrong)"
dump() { # $1 = P or C, $2 = table, $3 = file
  "$1" -tA -c "copy (select * from public.$2 order by id) to stdout with csv" > "$3"
}
dump P rp_expected "${OUT}/oracle.csv"; dump C rp_orders "${OUT}/clone.csv"
diffids() { sort "$1" > "$1.s"; sort "$2" > "$2.s"; comm -3 "$1.s" "$2.s" | cut -d, -f1 | sort -u | wc -l | tr -d ' '; }
SWAP_DIFF=$(diffids "${OUT}/oracle.csv" "${OUT}/clone.csv")
echo "  rows (distinct ids) that differ between the clone and the oracle: ${SWAP_DIFF}"
echo "  legitimate writes made after the first bad commit, which the clone cannot contain: ${LEGIT_AFTER}"
check "H1: the clone is not the oracle (it lacks the legitimate writes after the incident began)" $([ "${SWAP_DIFF}" -gt 0 ]; echo $?)

say "7. row-copy: copy the rows the bad job touched from the clone into the live table"
# the bad job touched ids 1 to 1100 and inserted 100001 to 100100
C -tA -c "copy (select * from public.rp_orders where id between 1 and 1100) to stdout with csv" > "${OUT}/clone-touched.csv"
T_C0=$(now)
{
  echo "begin;"
  echo "create temp table touched (like public.rp_orders);"
  echo "copy touched from stdin with csv;"
  cat "${OUT}/clone-touched.csv"
  echo "\\."
  echo "delete from public.rp_orders where id between 1 and 1100 or id between 100001 and 100100;"
  echo "insert into public.rp_orders select * from touched;"
  echo "commit;"
} | X psql -X -q -v ON_ERROR_STOP=1 -h /tmp -p 5433 -U postgres -d postgres >/dev/null
T_C1=$(now)
dump P rp_orders "${OUT}/rowcopy.csv"
COPY_DIFF=$(diffids "${OUT}/oracle.csv" "${OUT}/rowcopy.csv")
echo "  copy of $(wc -l < "${OUT}/clone-touched.csv" | tr -d ' ') rows took $(python3 -c "print('%.2f' % ($T_C1 - $T_C0))") s"
echo "  rows (distinct ids) that differ from the oracle after the row-copy: ${COPY_DIFF}"
echo "  of which a legitimate note/status write was overwritten: $(comm -3 "${OUT}/oracle.csv.s" "${OUT}/rowcopy.csv.s" | cut -d, -f1 | sort -u | awk -F, '$1<=1100' | wc -l | tr -d ' ') rows with id <= 1100"
check "H2: the row-copy leaves rows that differ from the oracle (the stream repair left 0)" $([ "${COPY_DIFF}" -gt 0 ]; echo $?)

say "8. numbers for the write-up"
cat > "${OUT}/summary.json" <<EOF
{"rows": ${ROWS}, "pad_rows": ${PAD_ROWS}, "pad_passes": ${PAD_PASSES}, "wal_bytes_since_backup": ${WAL_BYTES},
 "archive_segments": ${ARCH_FILES}, "archive_bytes": ${ARCH_BYTES}, "base_bytes": ${BASE_BYTES},
 "recovery_seconds": ${RECOVERY_S}, "swap_rows_differing": ${SWAP_DIFF}, "rowcopy_rows_differing": ${COPY_DIFF},
 "legit_writes_after_incident": ${LEGIT_AFTER}}
EOF
cat "${OUT}/summary.json"
say "result: ${PASS} passed, ${FAIL} failed (raw data in ${OUT}; H3 is judged by comparing two runs, see RESULTS.md)"
[ "$FAIL" -eq 0 ]
