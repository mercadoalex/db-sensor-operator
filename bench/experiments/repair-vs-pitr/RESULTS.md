# P3, second half: the same incident, repaired with point-in-time recovery

Script: `run.sh` (throwaway PostgreSQL 16.15 in a kind pod, `archive_mode=on`, base backup taken before the workload,
recovery with `recovery_target_xid` set to the first bad transaction, `recovery_target_inclusive=off`).
Workload identical to `../repair-from-stream`: 5000 rows, a bad job of 13 transactions (1100 updates, 100 deletes,
100 junk inserts), a good writer running during the incident, 560 good writes after it, an oracle table.

## Measured (3 runs, all hypotheses held; the two runs with no extra WAL gave the same counts)

| | no extra WAL | 1 GB of extra WAL before the incident |
|---|---|---|
| WAL since the backup | 2.3 MB (8 segments archived) | 1,015 MB (71 segments) |
| Recovery (copy backup, replay, promote) | 0.5 s | 7.9 s |
| Rows differing from the oracle, clone swapped in | 1224 | 1224 |
| Rows differing from the oracle, row-copy of touched rows | 1024 | 1024 |
| Stream repair, rows differing (from `repair-from-stream`) | 0 | 0 |

- **H1 held.** The recovered clone is correct for the past and wrong for the present: 1224 rows (distinct ids) differ
  from the oracle. It lacks the 794 legitimate writes made after the first bad commit (in that run).
- **H2 held.** Copying only the rows the bad job touched from the clone into the live table took 0.3 to 0.5 s and still
  left 1024 rows different from the oracle, because every legitimate write to those rows after the incident began
  (the concurrent writer, the 300 `note` writes, the 50 `status` writes, the 10 deletes) was overwritten.
  The stream repair, on the same scenario, left 0 and reported the 50 true conflicts for a person.
- **H3 held in one dimension.** Recovery time grew from 0.5 s to 7.9 s as the WAL between the backup and the target
  grew from 2.3 MB to about 1 GB. The stream repair time (0.56 s to apply, in `repair-from-stream`) depends on the events
  of the bad transactions, not on the WAL before them. That comparison is by design, not by a head-to-head run.

## What this does and does not say

- It says: for this incident, PITR does not repair a database that kept accepting legitimate writes; it returns a past
  state. The two ways to use it (swap, or copy rows back) either lose the later legitimate writes or overwrite those
  made to the same rows. The stream repair avoids that because it undoes only the bad transactions' changes, column by
  column, and flags the real conflicts.
- It does not say the stream is better as a whole. PITR needs no `REPLICA IDENTITY FULL`, no operator-owned event store,
  and restores anything the WAL saw, including DDL and tables nobody was watching. The stream repair needs all three of
  those conditions (see `../repair-from-stream/RESULTS.md`).
- A hybrid is possible and was not tried: recover a clone, then reapply the legitimate transactions that came after.
  That is "PITR plus replay", it needs the same list of good transactions, and it is the realistic competitor.

## Not covered, and not claimed

- Times from the two experiments come from different PostgreSQL instances; they are indicative, not a head-to-head.
- One run per configuration beyond the duplicate; 5000 rows; local disk; no object store or managed-service PITR
  (RDS, Cloud SQL), whose restore time includes provisioning a new instance and is minutes to hours, not measured here.
- The bad transaction id is GIVEN, as in the stream repair. Finding it, or the right recovery time, is not measured.
- Foreign keys, triggers and schema changes are untouched.
- The WAL cost of `REPLICA IDENTITY FULL` (needed by the stream repair, not by PITR) is still not measured. This is P4.

## Reproduce

```
bench/experiments/repair-vs-pitr/run.sh
PAD_ROWS=200000 PAD_PASSES=15 bench/experiments/repair-vs-pitr/run.sh
```
