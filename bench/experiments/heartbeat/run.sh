#!/usr/bin/env bash
#
# bench/experiments/heartbeat/run.sh — ADR-018, assumptions A1 and E1.
#
# A1  Does Debezium Server's HTTP sink deliver heartbeat records to the endpoint, and what is the
#     body? Hypothesis: with heartbeat.interval.ms=10000 and NO writes at all, the endpoint gets
#     about one POST per interval, whose body carries ts_ms and none of source/op/before/after.
#     Falsified by: zero requests in the silent window (A1 false), or a different shape.
#
# E1  When every change is filtered by the connector, does the heartbeat keep the slot moving?
#     Hypothesis: with writes to a table outside table.include.list, the gap
#     pg_current_wal_lsn() - confirmed_flush_lsn grows with the WAL written when there is no
#     heartbeat, and stays bounded when there is one. Pass: the growth with heartbeat is at most
#     20% of the growth without. Inconclusive if the baseline does not stall (a pass would mean
#     nothing then).
#
# Isolation: this does NOT touch the bench-cdc sensor or the operator code. It starts its own
# DebeziumServer "hb-probe" (own slot, own publication) pointed at its own receiver, once without
# heartbeat ("nohb", also the control for A1) and once with it ("hb"), on the same PostgreSQL.
# It drops every object it created on exit. While a probe runs, its slot retains WAL; the volume
# is a few MiB by design (the "nohb" run is the one that lets it pile up).
#
# Prerequisites: make -C bench setup && bench/collectors/cdc/up.sh   (kind, PostgreSQL, Debezium Operator)
#
# Env: CLUSTER (db-sensor-bench)  NS (bench)
#      INTERVAL_MS (10000)  SILENT (90)  NOISE (240)  INTERLEAVE (30)  SAMPLE (10)   seconds
#      PUB_MODE (all_tables)  -- what the operator's publication is today; use "filtered" for E1b
#      OUT   results directory (default bench/results/heartbeat-<timestamp>)
#
set -uo pipefail
CLUSTER="${CLUSTER:-db-sensor-bench}"; NS="${NS:-bench}"
INTERVAL_MS="${INTERVAL_MS:-10000}"
SILENT="${SILENT:-90}"; NOISE="${NOISE:-240}"; INTERLEAVE="${INTERLEAVE:-30}"; SAMPLE="${SAMPLE:-10}"
PUB_MODE="${PUB_MODE:-all_tables}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${OUT:-${HERE}/../../results/heartbeat-$(date +%Y%m%d-%H%M%S)}"
K="kubectl --context kind-${CLUSTER}"
PSQL() { $K -n "$NS" exec -i bench-postgres-0 -- psql -U bench -d bench -tAq -v ON_ERROR_STOP=1 "$@"; }
say() { printf '\n== %s\n' "$*"; }
now_s() { PSQL -c "select extract(epoch from now())::bigint"; }

$K -n "$NS" get statefulset bench-postgres >/dev/null 2>&1 || {
  echo "ERROR: run 'make -C bench setup' and 'bench/collectors/cdc/up.sh' first." >&2; exit 1; }
$K get crd debeziumservers.debezium.io >/dev/null 2>&1 || {
  echo "ERROR: the Debezium Operator CRD is missing; run bench/collectors/cdc/up.sh." >&2; exit 1; }

mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"

cleanup() {
  say "cleanup"
  $K -n "$NS" delete debeziumserver hb-probe --ignore-not-found --wait=true >/dev/null 2>&1
  for s in hb_probe_nohb hb_probe_hb; do
    for _ in $(seq 1 30); do
      PSQL -c "select pg_drop_replication_slot('$s') where exists (select 1 from pg_replication_slots where slot_name='$s')" >/dev/null 2>&1 && break
      sleep 3
    done
  done
  PSQL -c "drop publication if exists hb_probe_pub_nohb; drop publication if exists hb_probe_pub_hb; drop table if exists public.hb_noise; delete from public.users where name like 'hb-%'" >/dev/null 2>&1
  $K -n "$NS" delete pod/hb-receiver svc/hb-receiver configmap/hb-receiver-code --ignore-not-found >/dev/null 2>&1
  echo "  removed the probe DebeziumServer, both slots, publications, noise table and receiver"
}
trap cleanup EXIT

# Start from a clean state in case an earlier run was interrupted.
$K -n "$NS" delete debeziumserver hb-probe --ignore-not-found --wait=true >/dev/null 2>&1
PSQL -c "drop table if exists public.hb_noise; create table public.hb_noise (id bigserial primary key, payload text not null)" >/dev/null || exit 1

cat > "${OUT}/params.env" <<EOF
INTERVAL_MS=${INTERVAL_MS}
SILENT=${SILENT}
NOISE=${NOISE}
INTERLEAVE=${INTERLEAVE}
SAMPLE=${SAMPLE}
PUB_MODE=${PUB_MODE}
EOF
echo "variant,epoch_s,current_lsn,confirmed_flush_lsn,restart_lsn,active,lag_bytes" > "${OUT}/samples.csv"
: > "${OUT}/phases.tsv"

say "receiver (logs every request untouched)"
$K -n "$NS" apply -f "${HERE}/receiver.yaml" >/dev/null
$K -n "$NS" wait --for=condition=ready pod/hb-receiver --timeout=120s >/dev/null || { echo "receiver not ready" >&2; exit 1; }

sample_for() { # sample_for <variant> <slot> <seconds>
  local variant="$1" slot="$2" secs="$3" end row
  end=$(( $(date +%s) + secs ))
  while (( $(date +%s) < end )); do
    row=$(PSQL -F, -c "select extract(epoch from now())::bigint, pg_current_wal_lsn(), coalesce(confirmed_flush_lsn::text,'0/0'), coalesce(restart_lsn::text,'0/0'), active, coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn),0)::bigint from pg_replication_slots where slot_name='${slot}'")
    [[ -n "$row" ]] && echo "${variant},${row}" >> "${OUT}/samples.csv"
    sleep "$SAMPLE"
  done
}

phase() { # phase <variant> <name> <start> <end>
  printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" >> "${OUT}/phases.tsv"
}

run_variant() { # run_variant <nohb|hb> <heartbeat_ms: 0 = unset>
  local v="$1" hb="$2" slot="hb_probe_$1" t0 t1 pid
  say "variant ${v} (heartbeat.interval.ms $([[ "$hb" == 0 ]] && echo 'not set' || echo "$hb"))"
  local hbline=""
  [[ "$hb" != 0 ]] && hbline="      heartbeat.interval.ms: \"${hb}\""
  cat <<EOF | $K -n "$NS" apply -f - >/dev/null
apiVersion: debezium.io/v1alpha1
kind: DebeziumServer
metadata:
  name: hb-probe
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
      url: http://hb-receiver.${NS}.svc:8080/${v}
  source:
    class: io.debezium.connector.postgresql.PostgresConnector
    config:
      connector.class: io.debezium.connector.postgresql.PostgresConnector
      database.hostname: bench-postgres
      database.port: "5432"
      database.user: bench
      database.password: \${DATABASE_password}
      database.dbname: bench
      topic.prefix: hb-probe-${v}
      plugin.name: pgoutput
      slot.name: ${slot}
      publication.name: hb_probe_pub_${v}
      publication.autocreate.mode: ${PUB_MODE}
      snapshot.mode: no_data
      table.include.list: public.users
${hbline}
EOF
  echo "  waiting for the slot ${slot} to become active..."
  local ok=0
  for _ in $(seq 1 80); do
    [[ "$(PSQL -c "select active from pg_replication_slots where slot_name='${slot}'")" == "t" ]] && { ok=1; break; }
    sleep 3
  done
  (( ok )) || { echo "ERROR: slot ${slot} never became active" >&2; return 1; }
  sleep 10   # let the streaming loop settle before the silent window

  say "[${v}] silent window: ${SILENT}s, no writes at all (A1)"
  t0=$(now_s); sample_for "$v" "$slot" "$SILENT"; t1=$(now_s); phase "$v" silent "$t0" "$t1"

  say "[${v}] noise window: ${NOISE}s of ~10 writes/s x 1 KiB to public.hb_noise, outside the include list (E1)"
  t0=$(now_s)
  PSQL <<SQL &
do \$\$
declare dl timestamptz := clock_timestamp() + interval '${NOISE} seconds';
begin
  while clock_timestamp() < dl loop
    insert into public.hb_noise (payload) values (repeat('x', 1024));
    commit;
    perform pg_sleep(0.1);
  end loop;
end \$\$;
SQL
  pid=$!
  sample_for "$v" "$slot" "$NOISE"; t1=$(now_s); phase "$v" noise "$t0" "$t1"
  wait "$pid" 2>/dev/null

  say "[${v}] interleave window: ${INTERLEAVE}s, 20 writes to the monitored table public.users (A1: events and heartbeats together)"
  t0=$(now_s)
  PSQL <<SQL &
do \$\$
begin
  for i in 1..20 loop
    insert into public.users (name, score) values ('hb-' || i, i);
    commit;
    perform pg_sleep(0.5);
  end loop;
end \$\$;
SQL
  pid=$!
  sample_for "$v" "$slot" "$INTERLEAVE"; t1=$(now_s); phase "$v" interleave "$t0" "$t1"
  wait "$pid" 2>/dev/null

  say "[${v}] stop the probe"
  $K -n "$NS" delete debeziumserver hb-probe --wait=true >/dev/null 2>&1
}

run_variant nohb 0 || exit 1
run_variant hb "$INTERVAL_MS" || exit 1

say "collect the raw receiver log and analyse"
$K -n "$NS" logs hb-receiver > "${OUT}/receiver.log" 2>&1
python3 "${HERE}/analyze.py" "$OUT"
echo
echo "raw data: ${OUT}  (receiver.log, samples.csv, phases.tsv, summary.txt)"
