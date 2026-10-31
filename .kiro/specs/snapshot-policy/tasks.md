# Implementation Plan: Snapshot Policy Layer

## Overview

Phase A (core) removes the blind window and stops snapshot events from distorting health, with no database inspection. Phase B (opt-in) adds the Inspector, the pure Policy, and the backfill ladder. Phase C (MySQL) is design-only. See `requirements.md`, `design.md` and ADR-014.

Phase A ships and is used before Phase B starts: the metrics do not need existing rows, so Phase B is built only if someone needs them (ADR-014 open question 1). Tasks marked `*` are optional or deferrable.

Verification uses the benchmark cluster (`bench/`): `make -C bench setup`, then `make -C bench cdc-up`.

## Tasks

- [ ] A0. Verify snapshot modes and the Debezium floor on the real cluster
  - On the benchmark cluster (Debezium Operator 3.6.3), start a `DebeziumServer` against the benchmark PostgreSQL with each candidate `snapshot.mode` (`initial`, `no_data`, `never`) and record: whether schema is captured, whether rows are emitted, and what happens on a pod restart with the in-memory offset store
  - Record the exact mode names and observed behavior in `design.md` (replacing the "candidate" wording) and note the Debezium version they were observed on
  - _Requirements: 1.1, 1.2, 10.1_

- [ ] A1. Classify snapshot events
  - Add `Snapshot bool` to `events.CDCEvent`; set it in `Parse` from `source.snapshot` and `op=r`
  - Unit-test `Parse` for `r`, `first`, `last`, `incremental`, `false`, absent, and the wrapped and un-wrapped envelopes
  - _Requirements: 2.5_

- [ ] A2. Keep snapshot events out of lag, counters and history
  - `Pipeline.Handle` returns early on `Snapshot`; live events are unchanged
  - Unit-test: a snapshot event updates neither the `LagTracker` nor the exporter nor the ring buffer; a live event still updates all three
  - _Requirements: 2.1, 2.2, 2.3, 2.4_

- [ ] A3. Non-blocking default start in the deployer
  - Using the mode confirmed in A0, set `snapshot.mode` in `BuildDebeziumServer` when the spec has no `spec.snapshot`
  - Unit-test the mapping; update the envtest expectations that read the generated source config
  - _Requirements: 1.1, 1.2, 12.1_

- [ ] A4. Offset persistence
  - Add `spec.snapshot.offsetStore` (`memory` default, `persistent`) to the API; map `persistent` to the Debezium Operator's runtime storage and offset store; do not change existing sensors on upgrade
  - Run `make generate manifests` and resync the chart CRD copy
  - _Requirements: 3.1, 3.2, 3.4_

- [ ] A5. Status block and reasons
  - Add `status.snapshot` (`strategy`, `reason`, `offsetStore`); with the in-memory store, set the reason described in Requirement 3.3
  - _Requirements: 3.1, 3.3, 4.1, 4.3_

- [ ] A6. Verify Phase A on the real cluster
  - On the benchmark cluster: load a table with a large number of rows, deploy the sensor, and confirm it reports from the first live change with accurate lag and no `Degraded` caused by the start; restart the Debezium pod with a persistent offset store and confirm no new snapshot
  - Record real-cluster versus unit-level verification in the results note
  - _Requirements: 1.1, 1.3, 2.1, 12.2, 12.3_

- [ ] A7. Checkpoint - Phase A
  - Ensure `go vet`, `go test` and `helm lint` pass; ask the user whether to proceed to Phase B

- [ ]* B1. Inspector interface and PostgreSQL Inspector
  - Define `TableFacts` and `Inspector`; implement the PostgreSQL Inspector with read-only catalog queries (planner-estimate row count, write privileges, `pg_is_in_recovery()`, `wal_level`); justify the added SQL driver dependency
  - _Requirements: 7.1, 7.6, 7.7, 9.1_

- [ ]* B2. Pure Policy
  - Implement `Decide(facts, cfg, caps)` and the capability table per engine and Debezium version; unit-test every row, the threshold boundary, the override, and the Degraded case
  - _Requirements: 5.3, 7.2, 7.3, 7.4, 7.5, 8.1, 10.1, 10.2_

- [ ]* B3. Wire the Inspector and Policy into reconcile
  - Run inspection only when `spec.snapshot.backfill` is true, on its own timeout; keep the previous Strategy on inspection failure; report the Strategy and reason in the status
  - Add the Secret `get` RBAC and document it
  - _Requirements: 4.1, 4.2, 5.1, 5.2, 8.2, 9.1, 9.2, 9.3_

- [ ]* B4. Signaling-table Strategy
  - Create the signaling table, add `signal.data.collection` and the table filter, send `execute-snapshot` once per table, track "requested" in the status
  - Document that this changes the monitored database
  - _Requirements: 5.4, 5.5, 6.1, 6.2, 9.4_

- [ ]* B5. Read-only incremental Strategy (PostgreSQL)
  - Verify against a read-only replica on the benchmark cluster whether the signal can be delivered without writing; enable the Strategy only if verified, otherwise leave it disabled and documented as unverified
  - _Requirements: 7.4, 10.1_

- [ ]* B6. Verify Phase B on the real cluster
  - Signaling-table backfill end to end on a large table; confirm the sensor stays Running during the backfill and backfill rows do not alter rates or lag
  - _Requirements: 5.4, 6.1, 6.2, 12.2_

- [ ]* C1. MySQL Inspector and the open signal-channel question (design and prototype)
  - Document the GTID facts to read; investigate the broker-free signal channel for Debezium Server and record the answer in ADR-014
  - _Requirements: 11.1, 11.2_

- [ ] D1. Documentation
  - Update `ROADMAP.md` status, the README, and `docs/articles/to-watermark-or-not.md` where it states Debezium versions for PostgreSQL read-only snapshots; add the benchmark variant note
  - _Requirements: 1.4, 10.1_
