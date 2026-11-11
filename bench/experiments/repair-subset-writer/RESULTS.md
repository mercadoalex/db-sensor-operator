# Scenario 1, Bungie-shaped: a bad writer corrupts a subset of rows; three repairs compared

Script: `run.sh`. Reproduce with `make -C bench setup && bench/collectors/cdc/up.sh && bench/experiments/repair-subset-writer/run.sh`.

**Status of this file: four runs made (run-1 to run-4); results and verdicts are in the sections after the hypotheses.** The hypotheses were committed (`afbe7de`) before any run and their text and thresholds were not edited afterwards; `git diff afbe7de -- RESULTS.md` shows exactly what was added.

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

## What was run

- **Cluster:** kind (`db-sensor-bench`), PostgreSQL 16.15 in a throwaway pod `sc1-pg` (aarch64), Debezium Operator 3.6.3 with a probe `DebeziumServer` `sc1-probe` (slot `sc1_probe`, `pgoutput`, decimals as strings, no schema in events, `snapshot.mode=no_data`, only `sc_inventory`), an HTTP sink to a pod `sc1-receiver` that appends every event to a file. `bench-postgres` and `bench-cdc` were not written to. Everything created was named `sc1-*` and removed at the end of each run (checked after the last run: no `sc1` pod, service, configmap or `DebeziumServer` left).
- **Run date:** 2026-11-11 (the container clock). Four runs, harness 4 of 4 checks passed in each: run-1, run-2, run-3 unpadded; run-4 with `PAD_ROWS=100000 PAD_PASSES=6` (an unrelated table updated 6 times between the base backup and the restore point; the archive grew from 101 MB to 319 MB, about 218 MB more).
- **Per run:** 5,000 rows on 10 servers; the bad writer ran in 10 transactions over 3.2 s and changed 1,000 rows (rows of servers 3, 4, 5 whose `qty` was above 100 became 100); the stream carried 2,804 to 2,807 events, always exactly the number expected from the table's own counters; 1,000 of them were the bad writer's. Legitimate writes: before `pre_patch` (600 rows with notes, 200 with a `qty` change), a writer running for 14 s alongside the bad writer (other servers and the bad servers), and after the bad writer stopped: 300 `note` writes and 40 `qty` writes on bad-server rows, 100 `qty` writes elsewhere, 10 deletes, 100 inserts. W_post (legitimate writes after the restore point) was 1,002 to 1,005.
- **How the strategies were applied:** each one was applied to its own copy of the incident table (`sc_work_a`, `sc_work_b`), or, for (c), taken from the clone directly. The copy is not in the times. The same single incident, the same stream and the same clone feed all three, in one database, in one run (unlike the earlier two experiments, which used different PostgreSQL instances).
- **What (b) and (c) mean here.** The task names "PITR clone-swap" and a "full restore". In `repair-vs-pitr` the clone-swap meant swapping the whole clone in, which is what (c) does here. So (b) is the surgical variant of the same clone: only the rows of the servers known to have run the bad code (3, 4, 5) are copied back, as whole rows, with an upsert (rows the clone does not have are left alone, rows deleted since are resurrected). (c) is the whole dataset replaced by the clone at the restore point. This is my reading of the task; if "clone-swap" was meant as something else, (b) is the one to reconsider.

## Results

Rows are counted by id. "Fixed" = affected rows now equal to the oracle; "left wrong" = affected rows that still differ; "outside" = rows the bad writer never touched that now differ from the oracle (damage done by the repair); "lost" = legitimate writes lost, as defined above. Raw: `raw/run-N/metrics.json`, table printed by `summarize.py` into `raw/summarize-output.txt`.

| run | workload WAL / archive (MB) | affected | W_post | (a) stream: fixed / left wrong / outside / lost | (b) PITR + copy bad servers: fixed / left wrong / outside / lost | (c) full restore: fixed / left wrong / outside / lost |
|---|---|---|---|---|---|---|
| run-1 | 20 / 101 | 1000 | 1005 | 952 / 48 / 0 / 0 | 697 / 303 / 143 / 478 | 697 / 303 / 668 / 1005 |
| run-2 | 20 / 101 | 1000 | 1005 | 953 / 47 / 0 / 0 | 697 / 303 / 143 / 478 | 697 / 303 / 668 / 1005 |
| run-3 | 20 / 101 | 1000 | 1005 | 952 / 48 / 0 / 0 | 697 / 303 / 143 / 478 | 697 / 303 / 668 / 1005 |
| run-4 (padded) | 20 / 319 | 1000 | 1002 | 953 / 47 / 0 / 0 | 698 / 302 / 142 / 476 | 698 / 302 / 666 / 1002 |

Total rows differing from the oracle after the repair (a / b / c): 48 / 446 / 971 (run-1, run-3), 47 / 446 / 971 (run-2), 47 / 444 / 968 (run-4). The 47 or 48 rows the stream repair left wrong are exactly the rows it flagged as conflicts: a legitimate `qty` change (+5) landed on an affected row after the bad write, so the guard refused to overwrite it and left the capped value plus the later change. They are 4.7 to 4.8% of the affected rows. The repair also reported 10 rows as gone (the legitimate deletes of ids 1401 to 1410), correctly.

Time to repair, in seconds (single measurement per run; a wall clock around `kubectl exec` calls, so each includes about 0.1 to 0.2 s of call overhead):

| run | (a) fetch events + build + apply = total | PITR recovery (copy base backup, replay, promote) | (b) recovery + row copy | (c) recovery |
|---|---|---|---|---|
| run-1 | 0.16 + 0.42 = 0.61 | 0.52 | 0.77 | 0.52 |
| run-2 | 0.16 + 0.44 = 0.63 | 0.65 | 0.89 | 0.65 |
| run-3 | 0.17 + 0.40 = 0.60 | 0.61 | 0.88 | 0.61 |
| run-4 (padded) | 0.15 + 0.41 = 0.59 | 2.04 | 2.27 | 2.04 |

Stream size for the bad writer: its 1,000 events were 3.9 MB of the 10.7 MB event file (about 3.9 KB per event, because `REPLICA IDENTITY FULL` carries the 1.6 KB row twice). The repair issued 1,000 guarded statements.

## Verdicts

| Hypothesis | Verdict | Evidence |
|---|---|---|
| H1: the tag reaches the stream and selects exactly the bad writer | **Held** (4 of 4 runs) | No key with `application`, `client` or `user` in the `source` block (keys seen: connector, db, lsn, name, origin, origin_lsn, schema, sequence, snapshot, table, ts_ms, ts_ns, ts_us, txId, version, xmin). The tagged events' `txId`s equal the ledger's in every run, and 1,000 tagged events match 1,000 ledger rows. |
| H2: stream repair exact except where it flags a conflict | **Held** (4 of 4) | 0 rows wrong outside the affected set, 0 legitimate writes lost, left-wrong set equal to the flagged set, 47 or 48 conflict rows (needed at least 10). |
| H3: full restore correct for the bad writer, discards later good work | **Held** (4 of 4), with a caveat | (c) lost 100% of W_post (needed 95 to 100%), and left 968 to 971 rows wrong against 47 or 48 for (a) (needed at least 10 times). The caveat: the 100% is close to true by construction, since a clone recovered to the restore point is by definition without everything after it. It checks that the restore point was right, not that full restore is bad. |
| H4: surgical PITR copy-back confines its damage but still hurts | **Held** (4 of 4) | 0 rows wrong on servers other than 3, 4, 5; 476 to 478 legitimate writes lost; rows wrong (a) 47 or 48 < (b) 444 to 446 < (c) 968 to 971. |
| H5: at this small scale every strategy is under 10 s and the full restore is faster than the stream repair | **Failed** (the ordering) | All times were under 10 s (the first half held). But (c) was faster than (a) in only 1 of the 3 unpadded runs: run-1 0.52 vs 0.61; run-2 0.65 vs 0.63 and run-3 0.61 vs 0.60 went the other way, by 0.01 to 0.02 s, which is inside the call overhead. The pre-registered prediction that PITR is faster here is not supported; the honest reading is a tie at this scale. |
| H6: PITR time grows with the WAL, stream repair time does not | **Held** (1 padded run) | Recovery 2.04 s against an unpadded mean of 0.59 s (3.4 times; needed at least 2); stream repair 0.59 s against 0.61 s. One padded run only, with about 218 MB of extra archive. |

## What this supports

- In this lab shape, a repair that undoes only the events tagged with the bad writer fixed 95% of the affected rows, touched none of the others, and lost no legitimate write, in under a second. The other 5% were exactly the rows where a legitimate write to the same column came afterwards; the repair named them instead of overwriting them. That is not "fully repaired": those 47 or 48 rows still hold the capped value (plus the later change) and need a person or a rule (here, for instance, replaying the delta on the original value).
- The full restore (c), the choice the real operator made, put every affected row back to a pre-incident value, but discarded all of the 1,002 to 1,005 legitimate writes made after the restore point and left 968 to 971 rows different from the oracle, about 20 times as many as the stream repair. Only 697 or 698 of the 1,000 affected rows ended up equal to the oracle, because the other 302 or 303 also had a legitimate write that the restore discarded.
- The surgical PITR copy-back (b) lost 476 to 478 legitimate writes and left 444 to 446 rows wrong. Of those, 142 or 143 were rows on the bad servers that the bad writer never touched (their `qty` was already at most 100) but that had legitimate writes the copy-back overwrote: knowing the affected servers is not the same as knowing the affected rows.
- Selecting by a tag stamped into the row worked where `txId` alone could not be used. The tag does not come with Debezium: it came from our trigger.

## What did not go as predicted, or needs a caution

- **H5 failed** as stated. I expected the PITR recovery to be faster than the stream repair, from the earlier numbers; at this size they are the same within noise. At 5,000 rows neither speed argument can be made.
- The three unpadded runs were close to identical in every count (same script, same row ids, only the timing of the concurrent writer varies). They are repetitions, not independent samples, and the variation shown (47 or 48 conflict rows, 0.52 to 0.65 s) is all there is.
- W_post is a floor. It does not count writes to rows that were later deleted (10 rows).
- "Legitimate writes lost" relies on the instrumentation column `legit_ver`; a write that changes a value without incrementing it would not be seen. The harness's own writers always increment it.
- (c)'s time is the recovery of a clone only. In real life the restored dataset also has to be made the live database (a cutover, a swap of the service, replication re-sync), which is not measured and is not zero. (b)'s time includes the copy but not the work of deciding which servers to copy.
- Times include `kubectl exec` overhead of 0.1 to 0.2 s per call, similar for all three, but not the same number of calls.

## Raw evidence

Each `raw/run-N/` holds: `run.log` (the console output), `summary.json` (times, sizes, bad `txId`s), `metrics.json` (the scores), `repair-report.json` (the stream repair's own counts, tagged `txId`s, conflict ids, `source` keys), `repair-stdout.txt`, `bad-job.sql`, `bad-ledger.csv` (every row the bad writer changed, with its `txId`), `oracle-pre.csv` (oracle at the restore point), `oracle.csv` (final oracle), `incident.csv` (the table before any repair), `state-a.csv`, `state-b.csv`, `state-c.csv` (the table after each strategy), `events.jsonl.gz` (all events as received), `clone-bad-servers.csv.gz` (what (b) copied). CSV columns are `id, server_id, account, item, qty, note, legit_ver, md5(body)`; `writer_app` is left out of the comparison on purpose, because the trigger stamps it with whichever session wrote last (including the repair). `summarize.py` produced `raw/summarize-output.txt` and applies the hypotheses' criteria literally.

Two things about how the runs came about, for the record. One smoke run (not a registered run, discarded) was made before the four to check that the harness works; it passed the same checks and gave the same shape of numbers. After it I made the row body smaller (50 hashes, 1.6 KB, instead of 200, 6.4 KB) so the raw files are small enough to commit; this changes event sizes and nothing the hypotheses refer to. After the four runs I corrected one column label in `summarize.py` (it said "pad WAL" for the workload WAL; criteria untouched).

## Not covered, and not claimed

(Written before the runs; extended after them.)

- Anything about Bungie's real database, schema, volumes, or the reasons for its decision. This is a lab shape, not a replay.
- Detection of the incident. The tag and the restore point are GIVEN to the strategies, as the bad transaction ids were in the earlier experiments.
- Production-scale volumes: 5,000 rows, local disk, one table, a few seconds of bad writes.
- Managed-service PITR (RDS, Cloud SQL) with its provisioning time; an object store for the archive.
- A durable event store: events are kept in a file in a pod (`emptyDir`).
- Triggers, foreign keys, schema changes between the incident and the repair, other column types.
- Whether the in-band tag (a trigger-stamped column) would be acceptable on a real table, or survive a writer that sets no `application_name`, sets the wrong one, or writes through a pooler that rewrites it. A bad writer that does not identify itself is not repaired by this method at all; that case was not run.
- Added after the runs:
  - Any claim about speed. At 5,000 rows the times are about half a second each, a tie within noise (H5 failed on the ordering). H6 rests on one padded run and one WAL size (about 218 MB extra).
  - The cutover and the downtime of a full restore, the time to decide to restore, and the time to find the restore point or the affected servers. The strategies were handed the tag, the restore point and (for (b)) the server list.
  - Semantic repair of the conflict rows. The 47 or 48 rows the stream repair left wrong could be fixed by replaying the later legitimate delta on the original value; that was not built, and the repair here only flags them.
  - Other bad-write shapes: the bad writer here only updates one column, once per row, with a constant. Deletes, inserts, multi-column changes, repeated passes over the same row, and a bad writer still running during the repair were not tried (the earlier `repair-from-stream` covers some of those with a different, hand-given selection).
  - The cost of `REPLICA IDENTITY FULL` and of the trigger on the write path (P4), and the size of a durable event store sized for a retention window.
  - Other numbers than these: one table shape, one row width, one fraction of affected rows (20% of rows; 1,000 of 5,000), one PostgreSQL version, one machine. The three unpadded runs are repetitions of one script, not independent samples.
  - Any conclusion about what the real operator should have done in 2020. The finding is only that, in this invented shape, a tag-selected stream repair left fewer rows wrong and lost fewer legitimate writes than the two PITR routes, and that it needed a tag, `REPLICA IDENTITY FULL`, a kept stream and a trigger, none of which we know the real system had.

## Reproduce

```
make -C bench setup && bench/collectors/cdc/up.sh
RUN=run-1 bench/experiments/repair-subset-writer/run.sh          # also run-2, run-3
PAD_ROWS=100000 PAD_PASSES=6 RUN=run-4 bench/experiments/repair-subset-writer/run.sh
python3 bench/experiments/repair-subset-writer/summarize.py       # judges H1 to H6 over raw/run-*
```
