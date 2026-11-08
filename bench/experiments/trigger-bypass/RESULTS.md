# Trigger bypass — results

Run: 2026-11-08, kind `db-sensor-bench`, PostgreSQL 16.15, the bench user (a superuser), one run. Script:
`./run.sh`. Part 2 used the `bench-cdc` sensor with the operator built from `main` after the heartbeat
(Debezium Server 3.6.3).

## Question

Can a change get past a guard trigger, and does CDC still see it?

**Hypothesis:** a `BEFORE UPDATE` trigger in its default mode (`pg_trigger.tgenabled = 'O'`) does not fire in a
session with `session_replication_role = replica`, so a change the trigger forbids goes through. The WAL still
records the old and the new value (with `REPLICA IDENTITY FULL`), and the sensor counts the event. A trigger
declared `ENABLE ALWAYS` (`'A'`) does fire in that session.

## Result: 11 of 11 checks passed

| Check | Result |
|---|---|
| A. Normal session, update `settled` to `pending` | blocked by the trigger, no row changed |
| B. `session_replication_role = replica`, trigger in default mode | went through (`UPDATE 2`) |
| B. What the WAL recorded (`test_decoding`) | for each row: old `'settled'`, new `'pending'` |
| C. Same, after `ALTER TABLE ... ENABLE ALWAYS TRIGGER` | blocked again |
| D. Sensor, blocked attempt on `public.payments` | counter unchanged (5 to 5) |
| D. Sensor, same change under the replica role | counter rose by 5 (5 to 10) |
| `docs/guards/export-db-guards.sql` | ran without errors; reported `origin (default)` then `always`, and `FULL` |

The five `payments` rows were put back with the same bypass, so the sensor counted five more events; no
trigger, table or slot of the experiment is left behind.

## What this shows

- The bypass is real on this setup: a guard trigger in its default mode is skipped by a session that sets the
  replication role. `session_replication_role` is documented by PostgreSQL for logical replication systems and
  needs a superuser or the `SET` privilege.
- **CDC saw the change that the trigger let through**, because it reads the log, which is written either way.
  With `REPLICA IDENTITY FULL` the log carries the old value, so a rule such as "settled never goes back to
  pending" can be evaluated on it.
- **The database can close this bypass by itself**: `ENABLE ALWAYS` made the same trigger block the same
  session. Where a team can do that, CDC adds nothing for this case. Its use is where the trigger is in its
  default mode, is disabled, or the rule is not in the database at all.
- The export script found the difference (`origin (default)` against `always`), so the inventory in
  `docs/guards/` works for this.

## What this does not show

- **How often it happens.** Nothing here measures how many teams run bulk loads, restores or data-fix scripts
  with triggers off, nor how often that ends in an incident.
- Part 2 checked that the sensor **counted** the updates. On `payments` the replica identity is the default
  (primary key), so the event carries no old value; the old value was seen only in the `test_decoding` read.
  A sensor that compares old and new would need `REPLICA IDENTITY FULL` on the table. No such rule engine exists
  yet in the operator.
- Other ways around a trigger were not tried (`ALTER TABLE ... DISABLE TRIGGER`, `pg_restore --disable-triggers`).
- Managed services were not tried. AWS documents the same parameter for RDS (a secondary source here); whether a
  given provider lets your role set it was not checked.
- `test_decoding` was used to show the WAL; the sensor reads it through `pgoutput`. One run.
