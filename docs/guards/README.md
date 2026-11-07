# Export the guards your database already has

A first step, by hand, with no change to the operator. The idea: before writing any rule for
Lactifluus to watch, see what PostgreSQL already enforces, and in which mode.

```bash
psql -d YOUR_DB -v schema=public -f docs/guards/export-db-guards.sql
```

The script only reads the system catalogs and returns no row data. It does return names and
definitions of tables, triggers and constraints, so read the output before sharing it. It has not
been run against a database yet (see the header of the file).

## What it tells you

| Section | What to look for |
|---|---|
| 1. Triggers | The **mode**. `origin (default)` triggers are skipped in a session that sets `session_replication_role = replica`, which bulk loaders, restores and some data-fix scripts do. `always` triggers still fire. `DISABLED` ones enforce nothing. |
| 2. Constraints | `check`, `foreign key`, `unique` and `exclusion` rules. They describe a valid row, not how a row may change, so "a settled payment never goes back to pending" cannot be written as one. |
| 3. Enums | The states a column can hold. A starting list of values for a transition rule. |
| 4. Replica identity | A rule that compares the old and the new value needs `FULL`. `default` only carries the primary key in the old row. |

The trigger modes are documented in
[pg_trigger](https://www.postgresql.org/docs/current/catalog-pg-trigger.html) (`tgenabled`) and
[session_replication_role](https://www.postgresql.org/docs/current/runtime-config-client.html).

## What it does not do

It does not read the meaning of a trigger. The body of a PL/pgSQL function is arbitrary code, so the
script lists that the trigger exists and how it is set, not what it forbids. Rules that live in
application code do not appear at all. Turning this output into a rule for Lactifluus is still done by
a person; there is no rule format in the operator yet.

## Next step

Having the operator's inspector run the same queries and report them in `status` is noted in
[HANDOFF](../HANDOFF.md), item 7. Not built.
