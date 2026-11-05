# Backpressure: a slow consumer retains WAL — results

Reproduce with `make -C bench setup && bench/collectors/cdc/up.sh && bench/experiments/backpressure/run.sh`.

- **Cluster:** kind, PostgreSQL 16, Debezium Operator 3.6.3, `max_slot_wal_keep_size = -1` (default), large disk.
- **Setup:** the sensor's sink pointed at a deliberately slow consumer (sleeps 2s per event); then writes driven faster than it can drain (2000 rows/tick, 6 ticks).
- **Run date:** 2026-11-05, one run. Measures the TREND, not an actual disk-full event (the bench disk is 496G).

## Hypothesis

A consumer slower than the write rate makes the slot retain WAL that grows and does not recover while the lag persists; the operator's inspector sees `retainedBytes` climb.

## Result

**Confirmed for the growth; the inspector sees it; relief not observed in-window; and a real gap found in the diagnosis.**

Retained WAL grew monotonically under the slow sink:

| tick | retained |
|---|---|
| baseline | 64 kB |
| 1 | 413 kB |
| 2 | 720 kB |
| 3 | 1029 kB |
| 4 | 1337 kB |
| 5 | 1644 kB |
| 6 | 1953 kB |

The inspector reported it: `retainedBytes: 1999672`, `confirmedLagBytes: 1999672`, `walStatus: reserved`. So the operator has its own backlog number — the hook a backpressure reaction would use.

## The gap this found (the valuable part)

`SlotRetentionHealthy` stayed **`True` / `SlotHealthy`** the whole time, while WAL was clearly
piling up. Why: with `max_slot_wal_keep_size = -1` (unbounded, the PostgreSQL default), the slot
never enters a size-based risk state (`extended`/`unreserved`/`lost`), so `DiagnoseSlot` calls it
healthy even as retention climbs. **The operator sees `retainedBytes` rising but does not flag
it.** That is a real product gap: on an unbounded slot, growth alone is never reported as a
problem, which is exactly the disk-fill risk we claim to guard against.

Fix direction (not built): the diagnosis should look at the *trend* of `retainedBytes` (growth
across successive inspections) and/or at free disk on `pg_wal`, not only at `wal_status`. On an
unbounded slot, `wal_status` is not enough. Ties to ADR-014's "WAL headroom" note, which is
unbuilt.

## Also noted

- **Relief not observed in-window:** after restoring a fast sink, retained WAL was still 1953 kB
  40s later. Not a conclusion that it does not recover — the drain/confirm needs longer or a
  longer measurement. Redo with a longer relief window to confirm recovery.

## What this establishes

- The slot-fills-the-disk risk is real and measurable: a slow consumer makes retained WAL grow
  steadily, and nothing in the slot advances until the consumer catches up.
- The operator already has the raw signal (`retainedBytes`), but its current diagnosis does not
  turn growth on an unbounded slot into a warning. This matters independently of content policies.

## Not covered

- Actual disk-full (large bench disk; trend only).
- Bounded `max_slot_wal_keep_size` (would reach `unreserved`/`lost`; tested separately in the slot work).
- Confirmed recovery after relief (window too short).
- One run, no variance.
