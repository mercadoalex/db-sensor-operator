#!/usr/bin/env bash
#
# bench/experiments/replica-identity-full-cost/run.sh — P4: what does REPLICA IDENTITY FULL cost in WAL?
#
# Question: the content policies and the repair from the stream need the old row image in each change event, which
# on PostgreSQL means REPLICA IDENTITY FULL. How many more WAL bytes does one UPDATE or DELETE write with FULL than
# with the default identity, as a function of the row width?
#
# Hypotheses (written before the first run, with the thresholds that would refute them):
#   H1a With DEFAULT, the WAL bytes of an UPDATE that changes one small column do not depend on the width of the
#       row (the update is HOT, no index changes). Refuted if the largest steady UPDATE is more than 2x the smallest.
#   H1b With FULL they grow with the row width. Refuted if the FULL/DEFAULT ratio does not grow from the narrowest
#       to the widest case.
#   H2  For a narrow row (about 100 bytes) the overhead of FULL is below 2x per UPDATE. Refuted if it is 2x or more.
#   H3  For a 6.4 KB row whose width is in a TOASTed column the overhead is 5x or more per UPDATE. Refuted if below 5x.
#   H4  A DELETE costs more under FULL (the old row is logged) in every case. Refuted if any case is equal or lower.
#
# Method: a throwaway PostgreSQL 16 (wal_level=logical, fsync on, full_page_writes on, default wal_compression=off).
# For each width and each identity: N rows, fillfactor 70, a primary key only; CHECKPOINT; pass 1 updates every row
# (touching a column that is not indexed, so the update can be HOT), pass 2 updates every row again, then every row is
# deleted. WAL bytes are the difference of pg_current_wal_lsn() around each step. Pass 1 includes the full-page
# images after the checkpoint; pass 2 is the steady state and is the one reported as "per UPDATE".
# Each configuration runs REPS times on a fresh table and the three numbers are reported as min / max.
#
# NOT covered, and not claimed: INSERT (identity does not change it); updates that change an indexed column or the
# key; concurrent load; wal_compression=on; replication lag or network cost; storage of the downstream event store
# (see repair-from-stream: about 12 KB per event for a 6.4 KB row); production row shapes, which are not known.
#
# Prerequisites: a kind cluster (make -C bench setup). Does not touch bench-postgres.
# Env: CLUSTER (db-sensor-bench)  NS (bench)  N (20000)  REPS (3)  OUT (results directory)
#
set -uo pipefail
CLUSTER="${CLUSTER:-db-sensor-bench}"; NS="${NS:-bench}"; N="${N:-20000}"; REPS="${REPS:-3}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${OUT:-${HERE}/../../results/fullcost-$(date +%Y%m%d-%H%M%S)}"
CTX="kind-${CLUSTER}"
X() { kubectl --context "$CTX" -n "$NS" exec -i fullcost-pg -- "$@"; }
P() { X psql -X -q -v ON_ERROR_STOP=1 -h /tmp -p 5433 -U postgres -d postgres "$@"; }
say() { printf '\n== %s\n' "$*"; }
mkdir -p "$OUT"; OUT="$(cd "$OUT" && pwd)"
cleanup() { say "cleanup"; kubectl --context "$CTX" -n "$NS" delete pod fullcost-pg --ignore-not-found --wait=true >/dev/null 2>&1; echo "  removed the pod"; }
trap cleanup EXIT
kubectl --context "$CTX" -n "$NS" delete pod fullcost-pg --ignore-not-found --wait=true >/dev/null 2>&1
kubectl --context "$CTX" -n "$NS" apply -f "${HERE}/pod.yaml" >/dev/null
kubectl --context "$CTX" -n "$NS" wait --for=condition=ready pod/fullcost-pg --timeout=180s >/dev/null || { echo "pod not ready" >&2; exit 1; }
X bash -c "initdb -D /work/pg -U postgres --auth=trust >/dev/null && cat >> /work/pg/postgresql.conf <<'EOF'
port = 5433
unix_socket_directories = '/tmp'
listen_addresses = ''
wal_level = logical
max_wal_size = 8GB
checkpoint_timeout = 1h
EOF
pg_ctl -D /work/pg -l /work/pg.log -w start >/dev/null" || exit 1
P -tA -c "select version()"

# width cases: name, the expression that builds the payload column, what it means
NAMES="narrow 1kb 1800b toast6k"
expr() { # the payload expression for a width case (bash 3 compatible)
  case "$1" in
    narrow)  echo "repeat('a', 40)";;
    1kb)     echo "repeat('b', 1000)";;
    1800b)   echo "(select string_agg(md5(g::text || i::text), '') from generate_series(1, 56) i)";;
    toast6k) echo "(select string_agg(md5(g::text || i::text), '') from generate_series(1, 200) i)";;
  esac
}
echo "case,identity,rep,row_bytes,update_pass1,update_pass2,delete" > "${OUT}/raw.csv"
lsn() { P -tA -c "select pg_current_wal_lsn()"; }
diffb() { P -tA -c "select pg_wal_lsn_diff('$2', '$1')::bigint"; }
for rep in $(seq 1 "$REPS"); do
  for c in $NAMES; do
    for ident in default full; do
      T="w_${c}_${ident}_${rep}"
      P <<SQL >/dev/null
create table public.${T} (id int primary key, status text not null, amount numeric(12,2) not null, note text, payload text not null) with (fillfactor = 70);
alter table public.${T} replica identity ${ident};
insert into public.${T} select g, 'x', g % 500, 'n' || g, $(expr "$c") from generate_series(1, ${N}) g;
vacuum analyze public.${T};
SQL
      RB=$(P -tA -c "select round(avg(pg_column_size(t.*)))::int from public.${T} t")
      P -c "checkpoint" >/dev/null
      A=$(lsn); P -c "update public.${T} set status = 'a'" >/dev/null
      B=$(lsn); P -c "update public.${T} set status = 'b'" >/dev/null
      Cc=$(lsn); P -c "delete from public.${T}" >/dev/null
      D=$(lsn)
      echo "${c},${ident},${rep},${RB},$(diffb "$A" "$B"),$(diffb "$B" "$Cc"),$(diffb "$Cc" "$D")" >> "${OUT}/raw.csv"
      P -c "drop table public.${T}" >/dev/null
    done
  done
  echo "  repetition ${rep} of ${REPS} done"
done
say "results (WAL bytes per row; min / max over ${REPS} repetitions; N=${N})"
N="$N" python3 - "${OUT}/raw.csv" <<'PYEOF'
import csv, sys, collections
rows = list(csv.DictReader(open(sys.argv[1])))
agg = collections.defaultdict(lambda: collections.defaultdict(list))
N = None
for r in rows:
    k = (r["case"], r["identity"])
    agg[k]["row"].append(int(r["row_bytes"]))
    for f in ("update_pass1", "update_pass2", "delete"):
        agg[k][f].append(int(r[f]))
import os
N = int(os.environ.get("N", "20000"))
cases = []
for r in rows:
    if r["case"] not in cases: cases.append(r["case"])
def rng(v): return (min(v) / N, max(v) / N)
print("%-9s %-8s %9s | %14s | %14s | %14s" % ("case", "identity", "row bytes", "UPDATE steady", "UPDATE pass 1", "DELETE"))
res = {}
for c in cases:
    for i in ("default", "full"):
        a = agg[(c, i)]
        u2, u1, d = rng(a["update_pass2"]), rng(a["update_pass1"]), rng(a["delete"])
        res[(c, i)] = (u2, u1, d)
        print("%-9s %-8s %9d | %6.0f / %-6.0f | %6.0f / %-6.0f | %6.0f / %-6.0f" % (c, i, a["row"][0], u2[0], u2[1], u1[0], u1[1], d[0], d[1]))
print()
print("ratio FULL / DEFAULT (using the mid value of each range):")
ratios = {}
for c in cases:
    f = lambda k, j: sum(res[(c, k)][j]) / 2
    ratios[c] = (f("full", 0) / f("default", 0), f("full", 1) / f("default", 1), f("full", 2) / f("default", 2))
    print("  %-9s UPDATE steady %.2fx   UPDATE pass 1 %.2fx   DELETE %.2fx" % ((c,) + ratios[c]))
print()
ok = True
def chk(name, cond):
    global ok
    print(("PASS  " if cond else "FAIL  ") + name); ok &= bool(cond)
first, last = cases[0], cases[-1]
d_flat = max(res[(c, "default")][0][1] for c in cases) / max(1.0, min(res[(c, "default")][0][0] for c in cases))
chk("H1a: DEFAULT UPDATE does not depend on the row width (largest over smallest %.2fx, limit 2x)" % d_flat, d_flat <= 2)
chk("H1b: FULL/DEFAULT grows from the narrowest to the widest case (%.2fx -> %.2fx)" % (ratios[first][0], ratios[last][0]), ratios[last][0] > ratios[first][0])
chk("H2: the narrow row costs less than 2x (%.2fx)" % ratios[first][0], ratios[first][0] < 2)
chk("H3: the 6.4 KB TOASTed row costs 5x or more (%.2fx)" % ratios[last][0], ratios[last][0] >= 5)
chk("H4: DELETE costs more under FULL in every case", all(ratios[c][2] > 1 for c in cases))
sys.exit(0 if ok else 1)
PYEOF
RC=$?
echo; echo "raw data: ${OUT}/raw.csv"
exit $RC
