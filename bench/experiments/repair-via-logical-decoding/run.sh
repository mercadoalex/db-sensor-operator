#!/usr/bin/env bash
#
# bench/experiments/repair-via-logical-decoding/run.sh — Scenario 2, Matrix-shaped: does logical decoding (Debezium)
# report the rows a DELETE REALLY removed when the index that selected them was wrong?
#
# This is a lab approximation of the SHAPE of a real incident (see RESULTS.md); it is not a reproduction of it.
# Hypotheses are pre-registered in RESULTS.md (committed before the first run). This script only measures.
#
# One throwaway PostgreSQL 16 pod (sc2-pg), logical decoding on. Five tables of 2000 rows (donor + 4 experiment tables):
#   sc2_full_bad / sc2_full_ok   REPLICA IDENTITY FULL, index on state_group corrupted / healthy
#   sc2_pk_bad   / sc2_pk_ok     default identity (primary key),  index on state_group corrupted / healthy
# The fault: with the server stopped cleanly, the donor's index file (whose layout is the clean one, group K at heap
# positions (K-1)*10+1..K*10) is copied over the state_group index file of the two *_bad tables, whose heap is shifted
# by SHIFT rows (default 5). The index is a valid btree that disagrees with the heap. Then the server is restarted,
# amcheck is run on all four indexes, the Debezium probe is started, and the "clean-up job"
#   DELETE FROM t WHERE state_group = ANY (ARRAY[10,60,110])      (enable_seqscan=off, enable_bitmapscan=off)
# is run on each table. Ground truth never uses the index: a seqscan JSON dump of each table before and after, and the
# DELETE's own RETURNING * (what the executor removed). analyze.py compares the Debezium events with it.
#
# Prerequisites: a kind cluster with the Debezium Operator (make -C bench setup; bench/collectors/cdc/up.sh).
# Never touches bench-postgres or bench-cdc. Everything created here is named sc2-* / sc2_* and removed on exit.
# Env: CLUSTER (db-sensor-bench)  NS (bench)  RUN (label, default timestamp)  SHIFT (5)
#
set -uo pipefail
CLUSTER="${CLUSTER:-db-sensor-bench}"; NS="${NS:-bench}"; ROWS=2000; SHIFT="${SHIFT:-5}"
TARGETS="10,60,110"
RUN="${RUN:-$(date +%Y%m%d-%H%M%S)}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${OUT:-${HERE}/raw/${RUN}}"
CTX="kind-${CLUSTER}"
TABLES="sc2_full_bad sc2_full_ok sc2_pk_bad sc2_pk_ok"
KX() { kubectl --context "$CTX" -n "$NS" "$@"; }
PSQL="psql -X -q -h /tmp -p 5432 -U postgres -d postgres"
P()  { KX exec -i sc2-pg -- env PGAPPNAME=seed ${PSQL} -v ON_ERROR_STOP=1 "$@"; }
# P_SEQ: a session that cannot use indexes to read (used for the ground-truth dumps of the heap)
P_SEQ() { KX exec -i sc2-pg -- env PGAPPNAME=seed PGOPTIONS="-c enable_indexscan=off -c enable_bitmapscan=off -c enable_indexonlyscan=off" ${PSQL} -v ON_ERROR_STOP=1 "$@"; }
say() { printf '\n== %s\n' "$*"; }
PASS=0; FAIL=0
check() { if [ "$2" -eq 0 ]; then echo "PASS  $1"; PASS=$((PASS + 1)); else echo "FAIL  $1"; FAIL=$((FAIL + 1)); fi; }
mkdir -p "$OUT"; OUT="$(cd "$OUT" && pwd)"
cleanup() {
  say "cleanup"
  KX delete debeziumserver sc2-probe --ignore-not-found --wait=true >/dev/null 2>&1
  KX delete pod/sc2-pg svc/sc2-pg pod/sc2-receiver svc/sc2-receiver configmap/sc2-receiver-code --ignore-not-found --wait=true >/dev/null 2>&1
  echo "  removed: DebeziumServer sc2-probe, pod and service sc2-pg (its data directory, slot and tables go with it), receiver pod, service and configmap"
}
trap cleanup EXIT
cleanup >/dev/null 2>&1
echo "{\"run\": \"${RUN}\", \"shift\": ${SHIFT}, \"rows\": ${ROWS}, \"targets\": [${TARGETS}]}" > "${OUT}/config.json"
say "1. pods: sc2-pg (logical decoding) and sc2-receiver (event log)"
KX apply -f "${HERE}/pod.yaml" -f "${HERE}/receiver.yaml" >/dev/null
KX wait --for=condition=ready pod/sc2-pg pod/sc2-receiver --timeout=180s >/dev/null || { echo "pods not ready" >&2; exit 1; }
KX exec sc2-pg -- bash -c 'initdb -D /work/primary -U postgres --auth=trust >/dev/null' || { echo "initdb failed" >&2; exit 1; }
KX exec -i sc2-pg -- bash -c 'cat >> /work/primary/postgresql.conf' <<'EOF'
port = 5432
unix_socket_directories = '/tmp'
listen_addresses = '*'
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
autovacuum = off
fsync = on
EOF
KX exec sc2-pg -- bash -c 'echo "host all all 0.0.0.0/0 trust" >> /work/primary/pg_hba.conf && echo "host replication all 0.0.0.0/0 trust" >> /work/primary/pg_hba.conf && pg_ctl -D /work/primary -l /work/primary.log -w start >/dev/null' || { echo "primary failed" >&2; exit 1; }
P -tA -c "select version()" | tee "${OUT}/version.txt"
say "2. tables (${ROWS} rows each, inserted in id order), donor index, SHIFT=${SHIFT}"
P > /dev/null <<SQL
create extension amcheck;
create table public.sc2_donor (id int primary key, state_group int not null, event_id text not null, depth int not null,
  amount numeric(12,2) not null, ts timestamptz not null, content text not null);
insert into public.sc2_donor
select g, (g - 1) / 10 + 1, 'ev' || g, g % 7, ((g * 1337) % 100000) / 100.0,
       timestamptz '2025-01-01 00:00:00+00' + g * interval '1 minute',
       (select string_agg(md5(g::text || i::text), '') from generate_series(1, 5) i)
from generate_series(1, ${ROWS}) g order by g;
create index sc2_donor_grp_idx on public.sc2_donor (state_group);
SQL
for t in ${TABLES}; do
P > /dev/null <<SQL
create table public.${t} (like public.sc2_donor including defaults);
alter table public.${t} add primary key (id);
insert into public.${t}
select g, ((g - 1 + ${SHIFT}) / 10) % 200 + 1, 'ev' || g, g % 7, ((g * 1337) % 100000) / 100.0,
       timestamptz '2025-01-01 00:00:00+00' + g * interval '1 minute',
       (select string_agg(md5(g::text || i::text), '') from generate_series(1, 5) i)
from generate_series(1, ${ROWS}) g order by g;
create index ${t}_grp_idx on public.${t} (state_group);
analyze public.${t};
SQL
done
P -c "alter table public.sc2_full_bad replica identity full; alter table public.sc2_full_ok replica identity full;"
P -tA -c "select relname, relreplident, (select count(*) from pg_index i where i.indrelid = c.oid) as indexes from pg_class c where relname like 'sc2\_%' and relkind = 'r' order by 1" | tee "${OUT}/tables.txt"
say "3. pre-swap sanity: all five indexes pass amcheck (heapallindexed)"
for t in sc2_donor ${TABLES}; do
  P -c "select bt_index_check('public.${t}_grp_idx'::regclass, true)" >/dev/null 2>"${OUT}/amcheck-preswap-${t}.err"
  RC=$?; check "amcheck passes before the swap on ${t}_grp_idx" $RC
done
P -tA -c "select relname, pg_relation_size(oid) from pg_class where relname like 'sc2\_%\_grp\_idx' order by 1" | tee "${OUT}/index-sizes.txt"
say "4. the fault: stop the server cleanly, copy the donor's index file over the *_bad indexes, restart"
DONOR_F=$(P -tA -c "select pg_relation_filepath('public.sc2_donor_grp_idx'::regclass)")
FULL_F=$(P -tA -c "select pg_relation_filepath('public.sc2_full_bad_grp_idx'::regclass)")
PK_F=$(P -tA -c "select pg_relation_filepath('public.sc2_pk_bad_grp_idx'::regclass)")
echo "  donor ${DONOR_F}; full_bad ${FULL_F}; pk_bad ${PK_F}" | tee "${OUT}/swap-files.txt"
P -c "checkpoint"
KX exec sc2-pg -- bash -c "pg_ctl -D /work/primary -m fast -w stop >/dev/null \
  && S1=\$(stat -c %s /work/primary/${DONOR_F}) && S2=\$(stat -c %s /work/primary/${FULL_F}) && S3=\$(stat -c %s /work/primary/${PK_F}) \
  && echo \"  file sizes donor=\$S1 full_bad=\$S2 pk_bad=\$S3\" && [ \"\$S1\" = \"\$S2\" ] && [ \"\$S1\" = \"\$S3\" ] \
  && cp /work/primary/${DONOR_F} /work/primary/${FULL_F} && cp /work/primary/${DONOR_F} /work/primary/${PK_F} \
  && pg_ctl -D /work/primary -l /work/primary.log -w start >/dev/null && echo '  swapped and restarted'" | tee "${OUT}/swap.txt"
check "the index files were the same size and were swapped" $([ "$(grep -c 'swapped and restarted' "${OUT}/swap.txt")" = "1" ]; echo $?)
[ "$(grep -c 'swapped and restarted' "${OUT}/swap.txt")" = "1" ] || { echo "swap failed; the server may be stopped" >&2; exit 1; }
say "5. amcheck on all four indexes (the corrupted ones must raise, the controls must not)"
for t in ${TABLES}; do
  P -c "select bt_index_check('public.${t}_grp_idx'::regclass, true)" >"${OUT}/amcheck-${t}.out" 2>"${OUT}/amcheck-${t}.err"
  RC=$?; echo "${RC}" > "${OUT}/amcheck-${t}.rc"
  echo "  ${t}: rc=${RC} $(head -c 300 "${OUT}/amcheck-${t}.err" | tr '\n' ' ')"
done
say "6. ground truth before: seqscan JSON dump of every table (index not used)"
for t in ${TABLES}; do
  P_SEQ -tA -c "select json_agg(t order by id) from public.${t} t" > "${OUT}/pre-${t}.json"
done
for t in ${TABLES}; do
  P_SEQ -tA -c "select '${t}', count(*) from public.${t} where state_group = any (array[${TARGETS}])" | tee -a "${OUT}/intended-counts.txt"
done
say "7. Debezium Server probe (slot sc2_probe, the four tables, decimals as strings, no schema in the events)"
cat <<EOF | KX apply -f - >/dev/null
apiVersion: debezium.io/v1alpha1
kind: DebeziumServer
metadata:
  name: sc2-probe
  namespace: ${NS}
spec:
  sink:
    type: http
    config:
      url: http://sc2-receiver.${NS}.svc:8080/
  format:
    value:
      type: json
      config:
        schemas.enable: "false"
  source:
    class: io.debezium.connector.postgresql.PostgresConnector
    config:
      connector.class: io.debezium.connector.postgresql.PostgresConnector
      database.hostname: sc2-pg
      database.port: "5432"
      database.user: postgres
      database.password: unused
      database.dbname: postgres
      topic.prefix: sc2-probe
      plugin.name: pgoutput
      slot.name: sc2_probe
      publication.name: sc2_probe_pub
      publication.autocreate.mode: filtered
      snapshot.mode: no_data
      table.include.list: public.sc2_full_bad,public.sc2_full_ok,public.sc2_pk_bad,public.sc2_pk_ok
      decimal.handling.mode: string
      tombstones.on.delete: "false"
EOF
ok=0
for _ in $(seq 1 80); do
  [ "$(P -tAq -c "select active from pg_replication_slots where slot_name='sc2_probe'")" = "t" ] && { ok=1; break; }
  sleep 3
done
[ "$ok" = 1 ] || { echo "ERROR: the slot never became active" >&2; exit 1; }
sleep 12; echo "  slot active"
say "8. the incident: the clean-up DELETE on each table, index-driven (enable_seqscan=off, enable_bitmapscan=off)"
stats() { # $1 = file to append to; prints, per table, seq_scan and the state_group index's idx_scan
  P -tA -c "select t.relname || ' seq_scan=' || t.seq_scan || ' idx_scan=' || coalesce(i.idx_scan, -1) from pg_stat_user_tables t left join pg_stat_user_indexes i on i.relid = t.relid and i.indexrelname = t.relname || '_grp_idx' where t.relname in ('sc2_full_bad','sc2_full_ok','sc2_pk_bad','sc2_pk_ok') order by 1" >> "$1"
}
sleep 2; : > "${OUT}/stats-before.txt"; stats "${OUT}/stats-before.txt"
for t in ${TABLES}; do
  cat > "${OUT}/incident-${t}.sql" <<SQL
set enable_seqscan = off;
set enable_bitmapscan = off;
begin;
select txid_current() as tx \gset
\echo TX :tx
explain (costs off) delete from public.${t} where state_group = any (array[${TARGETS}]);
\echo REMOVED_BEGIN
with d as (delete from public.${t} where state_group = any (array[${TARGETS}]) returning *)
select coalesce(json_agg(d order by id), '[]'::json) from d;
\echo REMOVED_END
commit;
SQL
  KX exec -i sc2-pg -- env PGAPPNAME=cleanup-job ${PSQL} -tA -v ON_ERROR_STOP=1 < "${OUT}/incident-${t}.sql" > "${OUT}/incident-${t}.txt" 2>&1
  check "the DELETE on ${t} ran and committed" $?
  sed -n '1,/REMOVED_BEGIN/p' "${OUT}/incident-${t}.txt" | sed 's/^/  /'
done
# the stats are read BEFORE the post-incident seqscan dumps, which would otherwise add a seq_scan to each table
sleep 2; : > "${OUT}/stats-after.txt"; stats "${OUT}/stats-after.txt"
for t in ${TABLES}; do
  P_SEQ -tA -c "select coalesce(json_agg(t order by id), '[]'::json) from public.${t} t" > "${OUT}/post-${t}.json"
done
cat "${OUT}/stats-before.txt" "${OUT}/stats-after.txt" | sed 's/^/  /'
say "9. wait for the stream"
REMOVED_TOTAL=$(python3 - "${OUT}" <<'PY'
import json, sys
d = sys.argv[1]; n = 0
for t in "sc2_full_bad sc2_full_ok sc2_pk_bad sc2_pk_ok".split():
    txt = open(f"{d}/incident-{t}.txt").read()
    body = txt.split("REMOVED_BEGIN")[1].split("REMOVED_END")[0].strip()
    n += len(json.loads(body))
print(n)
PY
)
echo "  removed rows over the four tables (RETURNING): ${REMOVED_TOTAL}"
GOT=0
for _ in $(seq 1 90); do
  GOT=$(KX exec sc2-receiver -- python -c "import urllib.request;print(urllib.request.urlopen('http://localhost:8080/').read().decode())" 2>/dev/null || echo 0)
  [ "${GOT}" = "${REMOVED_TOTAL}" ] && break
  sleep 2
done
sleep 5
GOT=$(KX exec sc2-receiver -- python -c "import urllib.request;print(urllib.request.urlopen('http://localhost:8080/').read().decode())" 2>/dev/null || echo 0)
echo "  receiver holds ${GOT} events"
check "the stream holds exactly the removed rows' events (${GOT} of ${REMOVED_TOTAL})" $([ "${GOT}" = "${REMOVED_TOTAL}" ]; echo $?)
KX cp "sc2-receiver:/data/events.jsonl" "${OUT}/events.jsonl" >/dev/null 2>&1
echo "{\"events_received\": ${GOT}, \"removed_total\": ${REMOVED_TOTAL}}" > "${OUT}/stream.json"
say "10. repair check (H5): before images re-inserted into a seqscan copy of the post-incident FULL tables"
for t in sc2_full_bad sc2_full_ok; do
  python3 "${HERE}/restore_sql.py" --events "${OUT}/events.jsonl" --table "${t}" > "${OUT}/restore-${t}.sql" || { echo "restore_sql failed" >&2; }
  P_SEQ > /dev/null < "${OUT}/restore-${t}.sql"
  check "the restore SQL for ${t} ran" $?
  P_SEQ -tA -c "select json_agg(t order by id) from public.${t}_fixed t" > "${OUT}/fixed-${t}.json"
done
say "11. analysis"
python3 "${HERE}/analyze.py" "${OUT}"
RCA=$?
gzip -9 -f "${OUT}/events.jsonl"
for f in "${OUT}"/pre-*.json "${OUT}"/post-*.json "${OUT}"/fixed-*.json "${OUT}"/restore-*.sql; do gzip -9 -f "$f"; done
say "harness: ${PASS} passed, ${FAIL} failed (raw data in ${OUT}; hypotheses are judged by analyze.py per run and summarize.py across runs)"
[ "$FAIL" -eq 0 ] && [ "$RCA" -eq 0 ]
