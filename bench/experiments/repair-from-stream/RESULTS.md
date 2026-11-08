# Repair from the change stream (P3, first half): results

Reproduce with `make -C bench setup && bench/collectors/cdc/up.sh && bench/experiments/repair-from-stream/run.sh`.

- **Cluster:** kind, PostgreSQL 16.15, Debezium Operator 3.6.3 (a probe `DebeziumServer` with its own slot), Debezium Server 3.6.3, HTTP sink to a pod that appends every event to a file.
- **Run date:** 2026-11-08. Three runs; all passed (details below).
- **Question (P3):** after a bad job has updated, deleted and inserted rows, and legitimate writes have kept arriving, can the before and after images Debezium emits restore the rows the bad job changed, without touching the legitimate writes?
- **This is the first half of P3.** The comparison with restoring a backup (PITR) has not been run. Nothing here says the stream is better than a backup.

## Setup

- Table `rp_orders` with nine columns (`int`, `text`, `numeric(12,2)`, `int`, `text`, `timestamptz`, `jsonb`, `boolean`, and a 6.4 KB `text` that PostgreSQL keeps out of the row, checked by looking at the TOAST relation), 5,000 rows, `REPLICA IDENTITY FULL`.
- The connector: `decimal.handling.mode=string`, no schema in the events, tombstones off, a filtered publication, `snapshot.mode=no_data`.
- **Oracle:** a copy of the table, `rp_expected`, that receives every legitimate write and never the bad job. After the repair the two must be identical.
- Legitimate writes: 500 before the incident; a writer running during it that updates `note` on rows 1 to 1000 (the rows the bad job is changing); 560 after it (a column the bad job did not change, a column it did change, deletes, inserts).
- The bad job, in 13 transactions: ten batches of 100 rows (`status`, `amount` times 10, `meta`), 0.3 s apart; a second pass over rows 1 to 100 (`status` again, to test undoing in reverse order); 100 deletes; 100 junk inserts.
- The repair (`repair.py`) reads the events file, keeps the events whose `txId` belongs to the bad transactions, and walks them in reverse arrival order. Each compensating statement is guarded: it changes a column only if the row still holds the value the bad change wrote. The whole repair is one transaction.

## Result

| Run | Other transaction ids between the first and the last bad one | Rows differing from the oracle: before the repair, after | Conflicts, rows gone | Outcome |
|---|---|---|---|---|
| 1 | 1 | 2180, 0 | 50, 10 | passed |
| 2 | 1 | 2180, 0 | 50, 10 | passed |
| 3 | 59 | 2180, 0 | 50, 10 | passed |

Runs 1 and 2 had the bad job run in 0.1 s, so only one legitimate transaction landed between its batches: the test that picking events by `txId` excludes legitimate writes was thin. Run 3 added the pauses and the writer on the same rows; it is the one to read.

Run 3 in detail:
- The stream was complete: 2,627 events arrived, exactly the 2,627 expected, in order, with no duplicates.
- The bad transactions held 1,100 updates, 100 deletes and 100 inserts. The repair built 3,300 statements and ran them in 0.56 s (0.8 s end to end, counting the call to the cluster).
- **After the repair the table equals the oracle: 0 differing rows**, in every column, including the `numeric`, `timestamptz`, `jsonb` and the TOASTed text of the re-inserted rows.
- 3,020 columns were restored.
- **50 column conflicts, exactly rows 301 to 350, column `status`:** a legitimate write set `status` to a new value after the bad job did, so the repair did not overwrite it. It restored the other columns of those rows (`amount`, `meta`).
- **10 rows reported as gone, exactly rows 351 to 360:** a legitimate write deleted them after the bad job changed them.
- 100 deleted rows were re-inserted and 100 junk rows removed, without conflicts.
- A second bad pass over the same column was undone correctly because the statements ran in reverse order.

## What this supports

- With `REPLICA IDENTITY FULL` and a stream kept somewhere, an incident of this shape can be repaired exactly and automatically, in seconds, without discarding legitimate writes. For comparison, and as arithmetic from this workload and not as a measurement: restoring a whole backup to just before the bad job would also discard the 788 legitimate writes made after it began.
- A legitimate later write to the same column is detected, not overwritten. That is the part a plain "put the old values back" script gets wrong.
- Unchanged out-of-row (TOAST) columns did not arrive as a placeholder in the `after` image: 0 placeholders in 1,100 updates, with `REPLICA IDENTITY FULL`. Without `FULL` this was not tried.

## What it costs and what it needs (found by running it)

- **A durable store of the events.** The repair read a file written by the probe's receiver pod (an `emptyDir`). The Lactifluus operator today keeps nothing: its pipeline counts events and drops them. A repair feature would need Kafka with retention, a log, or something similar, sized for the time window you want to be able to go back.
- **Size.** Each event for this table was about 12 KB, because `FULL` makes the event carry the 6.4 KB row twice. The 1,300 events of the bad job were 15.8 MB. That is the P4 question in miniature: the cost scales with row width and with how many days you keep.
- **`REPLICA IDENTITY FULL`.** Without it the old value is not in the event (see `row-content`), and nothing can be restored.
- **Connector settings.** `decimal.handling.mode=string` and JSON without schema made the values easy to turn back into SQL. With the default decimal mode, `numeric` values arrive as base64 bytes (seen in `row-content`); the repair would have to decode them. Not tried here.
- **Knowing which transactions are bad.** The experiment was handed them (the job printed its `txid_current()`). In a product they would come from the rule that fired, or from attribution (which is expensive; see `attribution-inband`). This is the biggest assumption in the result.

## What it does not show

- Anything about PITR or any other backup route: not run.
- **Scale.** 5,000 rows and a bad job of 1,300 changes. Time and size were not measured as the bad job grows.
- **Side effects.** Triggers, cascades and foreign keys that the bad job fired also produce events and would need their own undo, and their order matters. The table has none.
- **Other column types:** arrays, enums, `bytea`, ranges, domains, geometry. Not tried.
- **Schema change** between the bad job and the repair. Not tried.
- **A repair while the bad job is still writing**, and a repair that is itself interrupted (it ran as one transaction here).
- **Whether anyone would trust an automatic repair.** The conflicts left for a person (50 here) are exactly where a business decision is needed.
- The oracle and the repair were written by the same person. The oracle is simple (the same legitimate statements on a copy), but the scenario was designed to be repairable.
- One cluster, one table shape; three runs of one scenario are not three independent tests.

## Next, for P3

1. **The second half: the same incident against a restore.** A separate PostgreSQL with WAL archiving, a base backup before the incident, a point-in-time restore to just before the bad job into a clone, and then (a) swapping the clone in, which discards the legitimate writes made since, and (b) copying only the affected rows from the clone, which needs to know which rows and columns the bad job touched, since a plain diff of the clone against production also contains the legitimate changes. Measure the time to bring the clone up and the fraction of legitimate writes kept.
2. A bad job of 10x and 100x the size, to see time and event bytes scale.
3. A table with a trigger and a foreign key the bad job fires.
