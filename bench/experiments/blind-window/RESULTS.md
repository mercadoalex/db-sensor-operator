# Blind-window experiment: results

Task A6 of the snapshot policy (ADR-014). Reproduce with
`make -C bench setup && ROWS=100000 bench/experiments/blind-window/run.sh`.

- **Debezium Operator / Server:** 3.6.3, PostgreSQL 16, `pgoutput`
- **Table:** `public.orders`, 100,086 rows before the run
- **Offset store:** in-memory; transport: HTTP sink to a counting receiver
- **Measure:** seconds from applying the `DebeziumServer` until the receiver sees
  the first live change (`op=c`, `snapshot=false`), with one marker INSERT per second
- **Run date:** 2026-10-31, single kind node, one run per mode (plus one earlier run
  that differed, below)

## Results

| `snapshot.mode` | Time to first live change | Snapshot reads delivered before it |
|---|---|---|
| `initial` | **57 s** | 100,089 (the whole table) |
| `no_data` | **6 s** | 0 |

The sensor is blind for the full snapshot under `initial`: nothing live arrives
until every row has been read and delivered. Under `no_data` the first live change
arrives as soon as the Debezium pod is up; the 6 s is pod start, not snapshot work.

## Reading the numbers

- The snapshot rate here was about 1,750 rows per second over the HTTP sink to a
  local receiver. If it held at that rate, a 10-million-row table would be blind for
  roughly 95 minutes. That is **an extrapolation**; it was not run, and real tables,
  row widths, networks and sinks will differ.
- An earlier run of the same experiment measured 110 s for `initial`. It started
  from a cold image pull and used a log-based detector that was later replaced by
  receiver counters, so 57 s is the figure to use; the spread shows these timings
  are not tight.
- Under `initial` the WAL retained by the replication slot grows for the whole
  snapshot. That was not measured here.

## Verified through the operator

On the same 100k-row table a `DatabaseSensor` started without a snapshot and stayed
`Pending` (no events yet), not `Degraded`. `status.snapshot` reported
`strategy: no-data` with `offsetStore: memory` and, after switching to
`persistent`, the matching reason text.

## Not covered

- One run per mode on one host; no spread or repeated measurements.
- Table sizes above 100k rows, and any engine other than PostgreSQL.
- Hard-kill behavior (see `../offset-persistence/RESULTS.md`).
- The quiet-table case (below) was observed, not resolved.

## Open: a quiet table reads as Degraded

The status `lag` is `now - last event`, so a healthy sensor on a table with no
writes becomes `Degraded` after the warning threshold (30 s by default) and
`Failed` after the no-event timeout. This predates the snapshot work. Options: report idle-with-no-writes as its own state, or
derive lag from the replication slot position (WAL lag) so that "no writes" is not
"behind". Either changes the health semantics, so it needs a decision.
