# Quiet table: a sensor with no writes goes `Failed` after about two minutes

No script: it is an observation on the running bench, plus a code reading. To reproduce, write one row to a captured table (`public.orders`) and read `kubectl -n bench get databasesensor bench-cdc -o jsonpath='{.status.phase} {.status.lag}'` every few seconds, with no further writes.

- **Cluster:** kind, PostgreSQL 16.15, Debezium Operator 3.6.3, Lactifluus from this repo. Default thresholds (the sensor's `spec.thresholds` was empty when read).
- **Run date:** 2026-11-06, one run.
- **Why it was looked at:** `bench-cdc` was found in `Failed` with `Ready=False, LagAboveCritical`, lag 11 min, and nothing was wrong with replication.

## What we saw

One insert at 14:25:24 UTC, then no writes:

| Seconds since the event | `status.phase` | `status.lag` |
|---|---|---|
| 11 | Running | 11.8 s |
| 23 | Running | 23.8 s |
| 35 | **Degraded** | 35.8 s |
| 60 to 96 | Degraded | 60 s to 1 m 36 s |
| 221 (first reading of the second batch) | **Failed** | 3 m 41 s |

I did not capture the exact moment of the switch to `Failed`; it happened between 96 s and 221 s after the event. The code says 120 s.

## Why (code reading, matches the measurement)

- `LagTracker.GetLag()` returns `now - lastEvent` (`internal/health/lag.go`). It is the **age of the last event**, not the replication lag.
- `decidePhase` (`internal/health/checker.go`) with the defaults (warn 30 s, critical 120 s, no-event warn 5 min, no-event fail 30 min): while the last event is under 5 minutes old, the lag arm decides, so silence over 30 s is `Degraded` and over 120 s is `Failed`. Past 5 minutes the "silence 5 to 30 minutes means Degraded" branch is unreachable, because the lag (equal to the silence) is already above the 120 s critical threshold, so it returns `Failed`.

## Verdict

**A table with no writes makes the sensor `Failed` after two minutes**, with the message "Replication lag is above the critical threshold". Nothing is lagging. This is a false alarm, the opposite direction of the 8-hour `Pending` case (a state that says "fine" while blind): here the state says "broken" while healthy.

It contradicts two statements in the repo:
- `docs/HANDOFF.md`, open decisions: "Quiet table ... stays `Degraded`. Decided: conservative is correct." The behavior is `Failed`.
- `docs/overview/stakeholder-brief.md`, glossary "Idle table": says it goes to `Degraded`.

The slot-side signals are correct in the same status (`SlotRetentionHealthy=True`, `confirmedLagBytes` 4720), which is what a real lag signal could use.

## Not verified

- The exact switch time to `Failed` (one reading missing).
- Behaviour with non-default `spec.thresholds`.
- The effect of a heartbeat (none is configured), which HANDOFF says is the way to tell a quiet table from a stalled connector.
- Whether the alert sinks fire on this transition (not checked).
- A fix. None is proposed here; it needs a design decision (real lag from the slot, or a heartbeat).
