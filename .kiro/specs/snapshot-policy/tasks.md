# Implementation Plan: Snapshot Policy Layer

## Overview

Phase A (core) removes the blind window and stops snapshot events from distorting health, with no database inspection. Phase B (opt-in) adds the Inspector, the pure Policy, and the backfill ladder. Phase C (MySQL) is design-only. See `requirements.md`, `design.md` and ADR-014.

**Decision (2026-10-30): Phase B is deferred.** Nothing consumes backfilled rows today: the only sink is the operator's `/cdc` endpoint, which drops snapshot events by design (A2). Revisit only when one of these holds:

- a feature needs existing rows (for example a per-table baseline);
- the Kafka transport is wired so other consumers read the topic (the deployer leaves the sink at `none` for Kafka today);
- a real user asks for existing rows.

If it is picked up, start with B1 and B2 (read-only catalog queries and a pure function), then B4 only for a concrete case, and B5 only if the read-only path is first verified on a real replica. A cheaper alternative is an explicit `spec.snapshot.mode` override that lets a user opt in to a blocking snapshot, with the status warning of the cost (57 s for 100k rows in `blind-window/RESULTS.md`).

Phase A ships and is used before Phase B starts: the metrics do not need existing rows, so Phase B is built only if someone needs them (ADR-014 open question 1). Tasks marked `*` are optional or deferrable.

Verification uses the benchmark cluster (`bench/`): `make -C bench setup`, then `make -C bench cdc-up`.

## Tasks

- [x] A0. Verify snapshot modes and the Debezium floor on the real cluster
  - Done on Debezium 3.6.3: `bench/experiments/snapshot-modes/` (run.sh, RESULTS.md). `no_data` is the non-blocking mode, `never` is rejected, and a restart with the in-memory offset re-snapshots under `initial`/`when_needed` and replayed one live event under `no_data`
  - The version floor is still open: only 3.6.3 was run, so this gives no minimum version
  - Original scope: start a `DebeziumServer` against the benchmark PostgreSQL with each candidate `snapshot.mode` and record what is emitted and what happens on a pod restart with the in-memory offset store
  - _Requirements: 1.1, 1.2, 10.1_

- [x] A1. Classify snapshot events
  - Add `Snapshot bool` to `events.CDCEvent`; set it in `Parse` from `source.snapshot` and `op=r`
  - Unit-test `Parse` for `r`, `first`, `last`, `incremental`, `false`, absent, and the wrapped and un-wrapped envelopes
  - _Requirements: 2.5_

- [x] A2. Keep snapshot events out of lag, counters and history
  - `Pipeline.Handle` returns early on `Snapshot`; live events are unchanged
  - Unit-test: a snapshot event updates neither the `LagTracker` nor the exporter nor the ring buffer; a live event still updates all three
  - _Requirements: 2.1, 2.2, 2.3, 2.4_

- [x] A3. Non-blocking default start in the deployer
  - Done for PostgreSQL only (`snapshot.mode: no_data`); other engines are untouched because their mode names are not verified. Verified on the benchmark cluster: the generated `DebeziumServer` carries `no_data`, no `op=r` series appears, and counts match the workload exactly (252/205/43)
  - Observed, not changed: with an idle table the sensor stays `Pending`, and after 35 s without events it goes `Degraded` because the status lag is `now - last event`. A6 should decide whether a quiet table should read as `Degraded`
  - Using the mode confirmed in A0, set `snapshot.mode` in `BuildDebeziumServer` when the spec has no `spec.snapshot`
  - Unit-test the mapping; update the envtest expectations that read the generated source config
  - _Requirements: 1.1, 1.2, 12.1_

- [x] A4. Offset persistence
  - Done: `spec.snapshot.offsetStore` (`memory` default, `persistent`); `persistent` sets the runtime storage, the PVC template and a file offset store with an absolute path. Existing sensors are unchanged. CRDs regenerated and the chart copy resynced
  - Verified on the benchmark cluster through the operator: after a Debezium pod restart the counters stayed at 252/205/43 and no `op="r"` series appeared (`bench/experiments/offset-persistence/RESULTS.md`). A hard kill was not tested
  - Original scope: add the field, map it to the Debezium Operator's runtime storage and offset store, do not change existing sensors on upgrade
  - _Requirements: 3.1, 3.2, 3.4_

- [x] A5. Status block and reasons
  - Done: `status.snapshot` (`strategy`, `offsetStore`, `reason`) computed by the pure `deployer.SnapshotStatusFor` from the spec and written on every status path. PostgreSQL reports `no-data`; other engines report `engine-default` because their non-blocking mode is unverified. With the in-memory store the reason says a restart can re-deliver recent events
  - Not added as a printer column; it is visible in `kubectl get -o yaml` and `describe`
  - Add `status.snapshot` (`strategy`, `reason`, `offsetStore`); with the in-memory store, set the reason described in Requirement 3.3
  - _Requirements: 3.1, 3.3, 4.1, 4.3_

- [x] A6. Verify Phase A on the real cluster
  - Done (`bench/experiments/blind-window/RESULTS.md`): on a 100k-row table `initial` was blind for 57 s while it delivered all 100,089 rows, and `no_data` saw the first live change in 6 s. Through the operator the sensor started `Pending`, not `Degraded`, and `status.snapshot` showed the strategy and offset store. Persistent-offset restart was verified in A4
  - Not covered: tables above 100k rows, hard-kill replay, engines other than PostgreSQL
  - **Decision still open:** a quiet table reads `Degraded` after 30 s because status lag is `now - last event`. Options are in the results note; changing it alters health semantics
  - On the benchmark cluster: load a table with a large number of rows, deploy the sensor, and confirm it reports from the first live change with accurate lag and no `Degraded` caused by the start; restart the Debezium pod with a persistent offset store and confirm no new snapshot
  - Record real-cluster versus unit-level verification in the results note
  - _Requirements: 1.1, 1.3, 2.1, 12.2, 12.3_

- [x] A7. Checkpoint - Phase A
  - Done 2026-10-30: `go vet`, `go test` (all packages except e2e), `helm lint` and `make lint` pass. The user decided to defer Phase B (see the decision note above)

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

- [x] D1. Documentation
  - Done: `ROADMAP.md` status; `README.md` and `README_spanish.md` (new CR fields, the PostgreSQL DDL limitation, a "Start and restarts" section, ADR-014 link); the article's `snapshot.mode=never` example now says `no_data`, with a note that 3.6.3 rejects `never`; the benchmark variant note was added to the benchmark design earlier
  - Not changed: the article's "relatively recent 2.x" for PostgreSQL read-only incremental snapshots. An earlier note here called it wrong; that was a mistake. The current PostgreSQL connector docs list a read-only watermarking option and do not say when it arrived, so the claim is unverified, not contradicted. Read-only *replica* support is the newer piece (fixed in 3.6.2, announced in 3.7), which matches the article's parenthetical
  - _Requirements: 1.4, 10.1_
