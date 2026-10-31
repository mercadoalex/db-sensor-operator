# Snapshot-mode experiment: results

Task A0 of `.kiro/specs/snapshot-policy/`. Reproduce with
`make -C bench setup && bench/experiments/snapshot-modes/run.sh`.

- **Debezium Operator / Server:** 3.6.3 (the connector reports `3.6.3.Final`)
- **PostgreSQL:** 16, `wal_level=logical`, `pgoutput`
- **Table:** `public.orders`, about 1000 rows
- **Offset store:** in-memory (the Debezium Operator default)
- **Transport:** HTTP sink to a logging receiver
- **Run date:** 2026-10-30, one run per mode on a single kind node

## Results

| `snapshot.mode` | At startup | Live INSERT afterwards | After restarting the Debezium pod |
|---|---|---|---|
| `initial` | 1001 snapshot reads (`op=r`) | delivered, with row data | **re-snapshot**: 1002 snapshot reads |
| `when_needed` | 1003 snapshot reads | delivered, with row data | **re-snapshot**: 1004 snapshot reads |
| `no_data` | 0 events | delivered, with row data | 0 snapshot reads; **1 live event re-delivered** |
| `never` | **rejected** | - | - |

`never` is not a valid value on 3.6.3. The connector refused to start with:
"The 'snapshot.mode' value is invalid: Value must be one of always,
initial_only, configuration_based, when_needed, initial, custom, no_data".

## What this means

1. **`no_data` is the non-blocking default.** It emits no snapshot reads, and
   live changes arrive with full row data from the first one.
2. **The in-memory offset store defeats `initial` and `when_needed`.** Every
   pod restart re-reads the whole table. This confirms the concern in ADR-014.
3. **A restart re-delivers live events, not only snapshot rows.** With
   `no_data` the one INSERT made before the restart arrived a second time.
   The offset is lost, so the connector resumes from the replication slot's
   last confirmed position and replays what it had already sent. I did not log
   the event key, so the replay of that exact row is inferred from "one INSERT
   made, one `c` event re-delivered", not proven. If it holds, the operator's
   counters over-count by the events replayed on each restart, which is
   at-least-once delivery and not the at-most-once the HTTP transport
   notes assumed.

## Not covered

- Only 3.6.3 was run, so this gives no minimum version.
- `initial_only`, `always`, `configuration_based` and `custom` were not run.
- One run per mode; no timing was measured.
- A persistent offset store was not tested (task A4 does that).
