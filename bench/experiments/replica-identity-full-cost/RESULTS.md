# P4: the WAL cost of REPLICA IDENTITY FULL

Script: `run.sh` (throwaway PostgreSQL 16.15, `wal_level=logical`, `fsync` on, `full_page_writes` on,
`wal_compression` off). 20,000 rows per case, fillfactor 70, primary key only. WAL bytes are the difference of
`pg_current_wal_lsn()` around each statement. Three repetitions on fresh tables gave identical numbers (min equals
max in every cell), so the table below is not a range.

Why it matters: the content policies and the repair from the stream need the old row image in the event, and on
PostgreSQL that means `REPLICA IDENTITY FULL`. This is the price of the whole content line.

## Measured: WAL bytes per row

The UPDATE changes one small, non-indexed column (`status`). "Steady" is the second pass, after the full-page images
of the first one; "pass 1" includes them (it is what you pay right after a checkpoint).

| Row | Row bytes | UPDATE steady DEFAULT | UPDATE steady FULL | Ratio | DELETE DEFAULT | DELETE FULL | Ratio |
|---|---|---|---|---|---|---|---|
| narrow | 82 | 175 | 239 | 1.4x | 66 | 123 | 1.9x |
| 1 KB inline | 1,048 | 1,159 | 2,195 | 1.9x | 73 | 1,100 | 15x |
| 1.8 KB inline | 1,840 | 1,972 | 3,802 | 1.9x | 78 | 1,900 | 24x |
| 6.4 KB (TOASTed body) | 6,448 | 186 | 6,637 | **36x** | 6,902 | 13,345 | 1.9x |

Pass 1 (right after a checkpoint): 1.2x, 1.4x, 1.4x and 23x for the four rows.

## Hypotheses

- **H1a refuted.** With DEFAULT, an UPDATE does not cost the same for every row width: 175 B for the narrow row and
  1,972 B for the 1.8 KB row (11x). The new row version is written to the WAL, so the cost follows the inline row
  width. What stays flat is a row whose wide column is TOASTed and unchanged: 186 B for a 6.4 KB row.
- **H1b held.** The FULL/DEFAULT ratio grows with the width, from 1.4x to 36x.
- **H2 held.** A narrow row costs 1.4x per UPDATE under FULL (below the 2x limit).
- **H3 held, and by a wide margin.** The 6.4 KB TOASTed row costs 36x per UPDATE (limit was 5x).
- **H4 held.** DELETE costs more under FULL in every case.

## What this says, and what it does not

- For rows kept inline, FULL roughly doubles the WAL of a small UPDATE (1.4x to 1.9x), because the old row is
  written as well as the new one. That is a modest price.
- The expensive case is a wide column stored out of line (TOAST) that the UPDATE does not touch. With DEFAULT
  the unchanged value is never written; with FULL it is written again on every UPDATE of any column of that row.
  Tables with large text, JSON or bytea columns are where FULL hurts, and this is a property of the row shape, not of
  the number of rows.
- DELETE of an inline row goes from the key only (66 to 78 B) to the whole row (123 B to 1.9 KB): 15x to 24x for wide
  rows. A bulk delete, such as a retention job, is the second place to watch.
- It matches what `repair-from-stream` saw in the events: about 12 KB per event for a 6.4 KB row, because FULL
  carries the row twice.

Arithmetic, not a measurement: at 1,000 UPDATEs per second on the 6.4 KB row, 6.6 MB/s of WAL under FULL against
0.19 MB/s under DEFAULT, which is about 570 GB per day against 16 GB. The rate is an assumption, not data.

## Not covered, and not claimed

- INSERT (identity does not change it), updates that change an indexed column or the key, concurrent load.
- `wal_compression=on`, which may change the numbers, and replication or network cost.
- The storage of the downstream event store (here only the 12 KB per event from `repair-from-stream`).
- Production row shapes: they are not known. These four are synthetic.
- Per-column options: PostgreSQL has no column-level identity; the choice per table (`FULL` only on the tables a
  policy needs) was not evaluated. A table with a USING INDEX identity was not tried either.

## Reproduce

```
bench/experiments/replica-identity-full-cost/run.sh          # N=20000 REPS=3
```
