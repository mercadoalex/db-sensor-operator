# Design Document: Snapshot Policy Layer

## Overview

This design implements `requirements.md` and [ADR-014](../../../docs/adr/ADR-014-snapshot-policy-layer.md). It makes reconcile go from "generate a static `DebeziumServer` spec" to "inspect, decide, generate a spec", in two phases that ship independently:

- **Phase A** changes only what the operator already does: how the connector starts and how snapshot events are treated. No database connection is added.
- **Phase B** adds an Inspector, a pure Policy, and an opt-in backfill ladder.

Phase C (MySQL) is design-only here.

## What the code does today (verified by reading it)

- `internal/deployer/debezium.go` builds the `DebeziumServer` source config with no snapshot mode, so Debezium's default (`initial`) applies. It sets `plugin.name=pgoutput`, `slot.name`, the table filter, and (since the previous change) the HTTP sink and the Secret environment.
- `internal/events/parser.go` returns `Op` as `payload.op`, so snapshot rows have `Op="r"`. It does not read `source.snapshot`.
- `internal/metrics/exporter.go` `OnEvent` counts only `c`, `u`, `d`, `t` and `ddl`; `r` is dropped silently.
- `internal/events/pipeline.go` `Handle` passes every event to the `LagTracker` and the dashboard ring buffer, including `op=r`. This is the source of the Degraded-after-snapshot behavior seen in the benchmark cluster.
- The Debezium Operator's generated ConfigMap uses `MemoryOffsetBackingStore` and `MemorySchemaHistory` (observed on the benchmark cluster).

## Phase A: non-blocking start and snapshot-event handling

### A1. Snapshot event classification (Requirement 2)

Add a `Snapshot bool` field to `events.CDCEvent`, set by `Parse` from `payload.source.snapshot` (and the un-wrapped `source.snapshot`). It is true for `true`, `first`, `first_in_data_collection`, `last_in_data_collection`, `last` and `incremental`, and for `op=r`; false for `false` or an absent field. An incremental-snapshot row is also a read of an existing row, so it is a Snapshot_Event for lag and counting, the same as a blocking-snapshot row. Phase B (Requirement 6) is where backfill rows get their own visibility.

Handling in `Pipeline.Handle`:

```go
func (p *Pipeline) Handle(ev CDCEvent) {
    if ev.Snapshot {
        // Not a live change: skip lag, counters and (by default) history.
        return
    }
    // existing behavior unchanged
}
```

The exporter already ignores `r`; the explicit early return makes the rule independent of that detail and also covers `source.snapshot` rows whose `op` is `c`. A counter `debezium_snapshot_events_total{db,table}` is a possible Phase B addition and is not part of Phase A.

### A2. Non-blocking default start (Requirement 1)

Set `snapshot.mode: no_data`. Verified in task A0 on Debezium 3.6.3 against a real PostgreSQL (`bench/experiments/snapshot-modes/RESULTS.md`): it emits no snapshot reads, and live changes arrive with full row data from the first one. `never` is **not valid** on 3.6.3 (the connector refuses to start; the accepted values are `always`, `initial_only`, `configuration_based`, `when_needed`, `initial`, `custom`, `no_data`). The value is version-specific, so a different pinned Debezium must repeat the experiment before the mapping is trusted.

`deployer.BuildDebeziumServer` sets `snapshot.mode` from a small function of the sensor spec; with no `spec.snapshot`, it returns the non-blocking value.

### A3. Offset persistence (Requirement 3)

`spec.snapshot.offsetStore` (`memory` default, `persistent`). `persistent` maps to the Debezium Operator's `runtime.storage.data` (persistent, with a claim) and the `source.offset` store (file or ConfigMap). The exact CR shape was read from the operator's documentation (`Offset`, `DataStorage`) and its CRD; the chosen combination is verified on the benchmark cluster by restarting the Debezium pod and checking that no new snapshot occurs. Existing sensors are not changed on upgrade (Requirement 3.4).

Task A0 showed that persistence matters for more than the snapshot. With the in-memory store, a restart re-snapshots under `initial` and `when_needed`, and under `no_data` it re-delivered a live event that had already been sent (inferred, not proven: see `RESULTS.md`). Replayed events would be counted twice, so an in-memory offset can inflate counters after every restart. Task A6 checks whether a persistent store removes the replay as well as the re-snapshot.

### A4. Status (Requirement 4)

A `status.snapshot` block: `strategy`, `reason`, `offsetStore`. Surfaced as a printer column only if it stays readable; otherwise in `kubectl describe`.

## Phase B: inspect, decide, act (opt-in)

```
spec.snapshot.backfill: true
        │
        ▼
 Inspector (per engine)  ──►  TableFacts  ──►  Policy (pure)  ──►  Strategy
 read-only catalog queries                    no I/O               + reason
                                                                       │
                                       ┌───────────────────────────────┤
                                       ▼                               ▼
                           DebeziumServer spec               Signal sender (once per table)
                           (signal.data.collection,          INSERT execute-snapshot into the
                            snapshot.mode, ...)              signaling table, or the read-only channel
```

### B1. TableFacts and the Inspector interface

```go
type TableFacts struct {
    Table         string
    EstimatedRows int64   // from planner statistics, never count(*)
    CanWrite      bool    // may create + insert into a signaling table
    ReadOnly      bool    // pg_is_in_recovery()
    WALLevel      string  // logical required
}

type Inspector interface {
    Inspect(ctx context.Context, conn ConnectionRef, tables []string) ([]TableFacts, error)
}
```

The PostgreSQL Inspector reads `pg_class.reltuples` (estimate), `has_schema_privilege` / `has_table_privilege` (write capability), `pg_is_in_recovery()`, and `SHOW wal_level`. All read-only (Requirement 7.6). It needs a SQL driver; the choice (`pgx`) is an added dependency and is justified in task B1.

### B2. The Policy

A pure function `Decide(facts TableFacts, cfg PolicyConfig, caps Capabilities) Strategy`, where `Capabilities` is derived from the engine and the pinned Debezium version (Requirement 10).

| Facts | Strategy |
|---|---|
| `EstimatedRows <= threshold` | `stream-only` (non-blocking start; existing rows are few, so a one-off signal or none) |
| above threshold, `CanWrite` | `signal-table` |
| above threshold, `!CanWrite`, read-only path supported | `read-only-incremental` |
| above threshold, `!CanWrite`, no read-only path | `none` + `Degraded` with reason |

`PolicyConfig.threshold` defaults to a documented value and is overridable per sensor. A manual `spec.snapshot.strategy` bypasses the table above (Requirement 5.3).

### B3. Acting on the decision

- **signal-table:** the deployer adds `signal.data.collection` and the signaling table to the connector filter; the operator creates the table and sends the `execute-snapshot` row. This is the most invasive step and is opt-in, documented, and idempotent (Requirement 5.5 uses a per-table "requested" marker in the status).
- **read-only-incremental:** uses the engine's read-only watermark. On PostgreSQL this depends on the pinned version (Requirement 10); the delivery of the signal without writing to the database is verified on the benchmark cluster against a read-only replica before this Strategy is enabled.

### B4. Failure behavior (Requirement 8.2)

Inspection runs on its own timeout and never blocks the main reconcile path. A failed inspection keeps the previous Strategy and reports `inspectionError` in the status. The operator does not stop or restart a healthy connector because inspection failed.

### B5. Trust boundary (Requirement 9)

The inspection connection is opened only when backfill is enabled. The password is read from the referenced Secret at inspection time and held only for the duration of the call. RBAC adds Secret `get` scoped by the operator's existing Secret rules; the chart's RBAC is updated and documented.

## Phase C: MySQL (design only)

A `MySQLInspector` reads `@@gtid_mode`, `@@gtid_executed`, `@@read_only` and write privileges. The Policy's `read-only-incremental` row applies when GTID is on. The signal for the read-only MySQL path is documented by Debezium as a Kafka topic; whether a Debezium Server can use a broker-free channel is an open question (Requirement 11.2).

## Verification plan

- **Unit:** the classifier, the Pipeline early return, the Policy table (including every row and the Degraded case), the deployer's `snapshot.mode` mapping.
- **envtest:** the generated `DebeziumServer` for each Strategy; status fields.
- **Real cluster (benchmark kind):** Phase A is verified by a sensor on a table with a large number of rows (the sensor reports Running from the first live change; lag stays accurate during the start), and by restarting the Debezium pod with a persistent offset store (no re-snapshot). Phase B signal-table is verified end to end; read-only is verified against a replica or marked unverified.
- Every claim of "verified" records whether it was real-cluster or unit-level (Requirement 12.3).

## Relationship to the benchmark

The initial-snapshot cost is one of the three CDC costs the benchmark discloses. The non-blocking default is a mitigation. After Phase A, the benchmark can add a variant that runs the CDC Approach with and without the blocking snapshot and reports both, so the cost is shown and the mitigation is measured and not asserted.

## Open questions

1. Exact snapshot-mode names and behavior per connector and pinned version (task A0).
2. Exact Debezium floor per Strategy and engine (Requirement 10).
3. Broker-free signal channel for the read-only MySQL path.
4. Whether any user needs Backfill at all. Phase A should ship and be used before Phase B is built.
