# Scenario 1, Bungie-shaped: a bad writer corrupts a subset of rows; three repairs compared

Script: `run.sh`. Reproduce with `make -C bench setup && bench/collectors/cdc/up.sh && bench/experiments/repair-subset-writer/run.sh`.

**Status of this file: hypotheses written, no run made yet.** The sections below the hypotheses are filled in after the runs. The hypotheses and their thresholds are not edited after the first run; a git diff of this file shows it.

## What this is, and what it is not

This is a lab approximation of the SHAPE of a real incident, not a reproduction of it. The shape, taken from the public postmortem of the Destiny 2 incident of January and February 2020 (`docs/research/p0-public-postmortems.md`, incident 7): a bug wrote a plausible, valid-looking but wrong value (a quantity cap) on a subset of servers; players noticed; the operator rolled back every account to a pre-patch backup instead of finding the affected accounts, because it judged that finding them risked missing some. We do not know Bungie's database engine, schema, volumes or tooling, and nothing here is a statement about their stack or their decision. The table, the numbers and the writers below are invented for the experiment.

## How the bad writer is identified (and why not by txId)

The attribution experiments found that the Debezium event carries `txId` but not the client: `attribution/RESULTS.md` shows the `source` block has no `application_name` or user, and `attribution-inband/RESULTS.md` shows that tying a `txId` to a workload needs the server log with specific settings, not the stream. So `txId` alone cannot say who wrote a change. Here the tag reaches the stream as data: the table has a column `writer_app`, and a `BEFORE INSERT OR UPDATE` trigger stamps it with `current_setting('application_name')` of the writing session. The bad writer connects with `application_name = inventory-patch-v2`, the good writer with `game-economy`. The tag therefore appears in the `after` image of every event. This is an in-band tag: it needs a trigger (a schema change on the watched table) and it trusts the writer to set its `application_name`. The `txId`s of the bad writer are recorded only as ground truth, to check the selection by tag; the repair does not use them.

## Pre-registered hypotheses (written before the first run)

Setup these refer to: one throwaway PostgreSQL 16 pod with WAL archiving and logical decoding, `sc_inventory` with 5,000 rows on 10 "servers" (`server_id`), `REPLICA IDENTITY FULL`, an oracle table `sc_expected` that receives only the legitimate writes, a column `legit_ver` that every legitimate write increments (instrumentation, to count lost legitimate writes), a bad writer that sets `qty = 100` on rows of servers 3, 4 and 5 whose `qty > 100`, a good writer that keeps writing to other rows and to the same rows, and a restore point `pre_patch` created just before the bad writer starts. Three strategies start from the same incident state: (a) stream repair from the events tagged with the bad writer, (b) PITR clone recovered to `pre_patch`, then the rows of servers 3, 4, 5 copied from the clone into the live table, (c) full restore: the whole dataset replaced by the clone at `pre_patch`. Four runs: three unpadded and one with extra unrelated WAL (`PAD_ROWS=100000 PAD_PASSES=6`) written between the base backup and the restore point.

Definitions used below. *Affected rows*: ids the bad writer actually changed (ground-truth ledger). *Rows wrong*: ids whose row differs from the oracle in any column except `writer_app` (rows present in one and absent in the other count). *Legitimate writes lost*: over the oracle's rows, the sum of `max(oracle.legit_ver - state.legit_ver, 0)`, plus the rows present in the state but deleted legitimately (deletes undone). *Post-point legitimate writes (W_post)*: the same quantity measured for the full restore's ideal, namely the sum over the final oracle rows of `(legit_ver - legit_ver at pre_patch)`, plus the legitimate deletes after `pre_patch`; it is a floor, because it does not see writes to rows that were later deleted.

- **H1, the tag reaches the stream and selects exactly the bad writer.** The `source` block of the events has no key whose name contains `application`, `client` or `user`; and the tagged events (`op = u`, `after.writer_app = inventory-patch-v2`) are exactly the bad writer's: the set of `txId`s of tagged events equals the set of `txId`s in the ground-truth ledger, and the number of tagged events equals the number of ledger rows. *Falsified in a run if* any of these differ.
- **H2, the stream repair is exact except where it flags a conflict.** After (a): 0 rows wrong outside the affected set; 0 legitimate writes lost; the affected rows still wrong are exactly the rows the repair reported as conflicts (set equality, not just equal counts); and there are at least 10 such conflict rows (a legitimate `qty` write landed on an affected row after the bad write), otherwise the test is vacuous. *Falsified in a run if* any of these fails.
- **H3, the full restore (c) is correct for the bad writer and discards the later good work.** The clone is at the right point: legitimate writes lost by (c) are between 95% and 100% of W_post. And (c) leaves at least 10 times as many rows wrong as (a). *Falsified in a run if* either fails.
- **H4, the surgical PITR copy-back (b) confines its damage to the copied servers but still hurts.** After (b): 0 rows wrong on servers other than 3, 4, 5; more than 0 legitimate writes lost; more rows wrong than (a); fewer rows wrong than (c). *Falsified in a run if* any of these fails.
- **H5, time at this small scale.** In the three unpadded runs every strategy finishes in under 10 s, and the full restore (c, the recovery of the clone) is faster than the stream repair (a, fetching the events plus building and applying the statements). The prediction that PITR is faster here is written down because the earlier experiments (`repair-vs-pitr`: 0.5 s; `repair-from-stream`: 0.8 s) pointed that way. *Falsified if* any strategy takes 10 s or more, or if (a) is faster than (c) in any unpadded run.
- **H6, time grows with the WAL for PITR, not for the stream repair.** In the padded run the clone recovery takes at least 2 times the mean of the unpadded runs, and the stream repair end to end takes at most 2 times the mean of the unpadded runs. *Falsified if* either fails.

A run whose harness fails (a pod does not start, the stream is incomplete) is reported as a failed run, not dropped. A hypothesis that fails in any run is reported as failed.

## Not covered, and not claimed

(Written before the runs; extended after them.)

- Anything about Bungie's real database, schema, volumes, or the reasons for its decision. This is a lab shape, not a replay.
- Detection of the incident. The tag and the restore point are GIVEN to the strategies, as the bad transaction ids were in the earlier experiments.
- Production-scale volumes: 5,000 rows, local disk, one table, a few seconds of bad writes.
- Managed-service PITR (RDS, Cloud SQL) with its provisioning time; an object store for the archive.
- A durable event store: events are kept in a file in a pod (`emptyDir`).
- Triggers, foreign keys, schema changes between the incident and the repair, other column types.
- Whether the in-band tag (a trigger-stamped column) would be acceptable on a real table, or survive a writer that sets no `application_name`.
