#!/usr/bin/env bash
#
# bench/experiments/trigger-bypass/run.sh — can a change get past a guard trigger, and does CDC still see it?
#
# Hypothesis: a BEFORE UPDATE trigger in its default mode (pg_trigger.tgenabled = 'O') does not fire in a
# session with session_replication_role = replica, so a change the trigger forbids goes through. The WAL still
# records the old and the new value (with REPLICA IDENTITY FULL) and the sensor counts the event. A trigger
# declared ENABLE ALWAYS ('A') does fire in that session.
#
# Part 1 needs only PostgreSQL: a table with a guard trigger, and a logical slot with test_decoding to read the WAL.
# Part 2 needs the CDC Approach (bench/collectors/cdc/up.sh): it makes a bypassing update on public.payments and
# reads the sensor's counter. It is skipped when the bench-cdc sensor does not exist.
#
# Pass: A blocked in a normal session; B passes under the replica role AND the WAL shows settled -> pending;
#       C blocked under the replica role once the trigger is ENABLE ALWAYS; D (part 2) the sensor counts the bypass
#       and does not count the blocked attempt.
# It changes nothing permanent: every object it creates is dropped, and the payments rows it touches are put back.
#
# Prerequisites: make -C bench setup   (and bench/collectors/cdc/up.sh for part 2)
# Env: CLUSTER (db-sensor-bench)  NS (bench)
#
set -uo pipefail
CLUSTER="${CLUSTER:-db-sensor-bench}"; NS="${NS:-bench}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/../../.." && pwd)"
CTX="kind-${CLUSTER}"
PSQL() { kubectl --context "$CTX" -n "$NS" exec -i bench-postgres-0 -- psql -X -U bench -d bench -v schema=public "$@"; }
say() { printf '\n== %s\n' "$*"; }
PASS=0; FAIL=0
check() { # check "<name>" <exit code of the condition>
  if [ "$2" -eq 0 ]; then echo "PASS  $1"; PASS=$((PASS + 1)); else echo "FAIL  $1"; FAIL=$((FAIL + 1)); fi
}

kubectl --context "$CTX" -n "$NS" get statefulset bench-postgres >/dev/null 2>&1 || {
  echo "ERROR: run 'make -C bench setup' first." >&2; exit 1; }
[ "$(PSQL -At -c "select rolsuper from pg_roles where rolname = current_user")" = "t" ] || {
  echo "ERROR: the bench user must be a superuser to set session_replication_role." >&2; exit 1; }

cleanup() {
  say "cleanup"
  PSQL -q -c "drop trigger if exists zz_ts_guard on public.payments" -c "drop function if exists public.zz_ts_guard()" \
       -c "drop table if exists public.zz_pay cascade" -c "drop function if exists public.zz_guard()" >/dev/null 2>&1
  PSQL -q -c "select pg_drop_replication_slot('zz_probe') from pg_replication_slots where slot_name = 'zz_probe'" >/dev/null 2>&1
  echo "  dropped the test table, triggers, functions and the zz_probe slot"
}
trap cleanup EXIT
cleanup >/dev/null 2>&1

# ----------------------------------------------------------------------------------------- part 1
say "part 1 setup: table zz_pay with a guard trigger (default mode) and REPLICA IDENTITY FULL"
PSQL -q <<'SQL' >/dev/null
create table public.zz_pay (id int primary key, status text not null check (status in ('pending','settled','refunded')));
alter table public.zz_pay replica identity full;
create function public.zz_guard() returns trigger language plpgsql as $$
begin
  if old.status = 'settled' and new.status = 'pending' then
    raise exception 'guard: a settled payment cannot go back to pending (id %)', old.id;
  end if;
  return new;
end $$;
create trigger zz_guard before update on public.zz_pay for each row execute function public.zz_guard();
insert into public.zz_pay values (1, 'settled'), (2, 'settled'), (3, 'settled');
select pg_create_logical_replication_slot('zz_probe', 'test_decoding');
SQL

say "guards script with the trigger in its default mode"
OUT=$(PSQL -f - < "${ROOT}/docs/guards/export-db-guards.sql" 2>&1); RC=$?
echo "$OUT" | grep -E 'zz_guard|zz_pay' | cut -c1-170
check "export-db-guards.sql runs without errors" $(( RC != 0 || $(echo "$OUT" | grep -c 'ERROR') > 0 ))
echo "$OUT" | grep 'zz_guard' | grep -q 'origin (default)'; check "the script reports the trigger as 'origin (default)'" $?
echo "$OUT" | grep 'zz_pay' | grep -q 'FULL'; check "the script reports REPLICA IDENTITY FULL" $?

say "A) normal session: the trigger must block"
OUT=$(PSQL -c "update public.zz_pay set status = 'pending' where id = 1" 2>&1); echo "$OUT" | head -2
echo "$OUT" | grep -q 'guard: a settled payment cannot go back to pending'; check "A: blocked in a normal session" $?
[ "$(PSQL -At -c "select count(*) from public.zz_pay where status = 'pending'")" = "0" ]; check "A: no row changed" $?

say "B) session_replication_role = replica, trigger in default mode"
OUT=$(PSQL 2>&1 <<'SQL'
set session_replication_role = replica;
update public.zz_pay set status = 'pending' where id in (1, 2);
SQL
); echo "$OUT"
echo "$OUT" | grep -q 'UPDATE 2'; check "B: the forbidden update went through (2 rows)" $?
echo "-- what the WAL recorded (logical decoding, test_decoding):"
WAL=$(PSQL -At -c "select data from pg_logical_slot_get_changes('zz_probe', null, null) where data like '%zz_pay%'")
echo "$WAL" | cut -c1-150
[ "$(echo "$WAL" | grep -c -E "old-key:.*'settled'.*new-tuple:.*'pending'")" = "2" ]; check "B: the WAL holds old=settled and new=pending for both rows" $?

say "C) the same after ALTER TABLE ... ENABLE ALWAYS TRIGGER"
PSQL -q -c "update public.zz_pay set status = 'settled'" -c "alter table public.zz_pay enable always trigger zz_guard" >/dev/null
OUT=$(PSQL -f - < "${ROOT}/docs/guards/export-db-guards.sql" 2>&1)
echo "$OUT" | grep 'zz_guard' | grep -q ' always '; check "the script reports the trigger as 'always'" $?
OUT=$(PSQL 2>&1 <<'SQL'
set session_replication_role = replica;
update public.zz_pay set status = 'pending' where id = 3;
SQL
); echo "$OUT" | head -3
echo "$OUT" | grep -q 'guard: a settled payment cannot go back to pending'; check "C: blocked under the replica role when the trigger is ENABLE ALWAYS" $?

# ----------------------------------------------------------------------------------------- part 2
if ! kubectl --context "$CTX" -n "$NS" get databasesensor bench-cdc >/dev/null 2>&1; then
  say "part 2 skipped: the bench-cdc sensor does not exist (run bench/collectors/cdc/up.sh)"
else
  count_updates() {
    "${ROOT}/bench/collectors/cdc/read.sh" events | grep -E 'table="payments"' | grep -E 'op="u"' | sed -E 's/.*[}] //' | head -1
  }
  say "part 2: does the sensor count a change that bypassed a trigger? (public.payments, rule: updated_at never goes back)"
  BASE=$(count_updates); BASE=${BASE:-0}; echo "baseline: payments op=u events counted by the sensor = ${BASE}"
  PSQL -q <<'SQL' >/dev/null
create function public.zz_ts_guard() returns trigger language plpgsql as $$
begin
  if new.updated_at < old.updated_at then
    raise exception 'guard: updated_at cannot go backwards (id %)', old.id;
  end if;
  return new;
end $$;
create trigger zz_ts_guard before update on public.payments for each row execute function public.zz_ts_guard();
SQL
  OUT=$(PSQL -c "update public.payments set updated_at = updated_at - interval '1 day' where id <= 5" 2>&1); echo "$OUT" | head -1
  sleep 8; MID=$(count_updates); MID=${MID:-0}
  [ "$MID" = "$BASE" ]; check "D: the blocked attempt is not counted (${BASE} -> ${MID})" $?
  OUT=$(PSQL 2>&1 <<'SQL'
set session_replication_role = replica;
update public.payments set updated_at = updated_at - interval '1 day' where id <= 5;
SQL
  ); echo "$OUT"
  sleep 10; AFTER=$(count_updates); AFTER=${AFTER:-0}
  [ "$AFTER" = "$((BASE + 5))" ]; check "D: the sensor counts the 5 bypassing updates (${BASE} -> ${AFTER})" $?
  echo "-- putting the 5 rows back (same bypass; the sensor counts these too)"
  PSQL -q <<'SQL' >/dev/null
set session_replication_role = replica;
update public.payments set updated_at = updated_at + interval '1 day' where id <= 5;
SQL
fi

say "result: ${PASS} passed, ${FAIL} failed"
[ "$FAIL" -eq 0 ]
