# Implementation Plan: CDC Observability Benchmark

## Overview

This plan builds the **CDC Observability Benchmark** as an incremental, reproducible harness that compares four ways to get data-activity (mutation) observability from PostgreSQL — CDC-derived (Lactiflluus over Debezium), polling (`pg_stat_user_tables`), triggers, and `pg_stat_statements` — on four axes (setup cost, source overhead, signal richness, latency), and answers the "CDC-only-for-metrics is too much" claim with data.

The plan follows the design's **"(a) first, then (b)"** depth decision: the **qualitative layer** (capability matrix + setup-cost analysis) is sequenced so it can complete and publish **even if the measured harness is deferred**. Measurement-heavy tasks are marked optional with `*`, mirroring the `db-sensor/tasks.md` convention where unit/property/integration/e2e tests carry `*` and core deliverables do not. Here the qualitative layer and the per-approach collectors are core; the measured overhead/latency harness and its runs are optional (deferrable) — but the **honesty/fairness review and the report's "where CDC does not win" section are core and never optional**.

The harness runs locally with Docker + kind + kubectl + helm only (no cloud), consistent with the repo's `demo/` kit. New harness code lives under `bench/`; the versioned report and results live under `docs/benchmark/`.

## Tasks

- [ ] 1. Scaffold `bench/` and the workload generator on PostgreSQL in kind
  - [ ] 1.1 Create the `bench/` layout and local cluster setup
    - Add `bench/README.md`, `bench/Makefile` (targets: setup, run, collect, report, teardown), `bench/cluster/` (kind config + PostgreSQL manifest with `wal_level=logical`), consistent with `demo/` tone and tooling (Docker, kind, kubectl, helm; no cloud)
    - Provide a teardown path that removes everything it created
    - _Requirements: 6.1, 6.3, 6.4, 10.1_
  - [ ] 1.2 Implement the Workload_Generator with a configurable i/u/d mix
    - `bench/workload/`: apply a deterministic insert/update/delete sequence from a fixed, documented seed, with configurable mix, rate, and duration against the target tables
    - _Requirements: 6.2, 8.1, 3.1_

- [ ] 2. Implement the CDC-derived collector (Lactiflluus over Debezium, HTTP transport)
  - Wire a `DatabaseSensor` CR + the official Debezium Operator against the benchmark PostgreSQL using the HTTP transport (no Kafka broker, per ADR-012); counts derived from the stream per ADR-013
  - Expose per-table insert/update/delete counts/rates, lag, and DDL counts for the collector to read
  - Document what it can observe (rates, lag, DDL, row detail) and its standing costs (Debezium dependency, replication slot, initial snapshot)
  - _Requirements: 1.1, 2.3, 3.2, 4.1_

- [ ] 3. Implement the polling collector (`pg_stat_user_tables` deltas)
  - `bench/collectors/polling/`: timer-driven sampler reading `n_tup_ins/upd/del` per table every configurable interval, computing deltas and handling counter resets
  - Record the sampling interval as a disclosed parameter
  - _Requirements: 1.1, 2.3, 3.4, 4.3_

- [ ] 4. Implement the triggers collector (database-side `AFTER` counters)
  - `bench/collectors/triggers/`: install SQL for per-table `AFTER INSERT/UPDATE/DELETE` triggers that increment a counters table, a reader that samples it, and matching teardown SQL
  - Document that it requires DDL on the monitored DB and write-amplifies every mutation
  - _Requirements: 1.1, 2.3, 3.2_

- [ ] 5. Implement the statement-stats collector (`pg_stat_statements`)
  - `bench/collectors/stmtstats/`: enable the extension and sample call counts / rows-affected, attributing to tables heuristically; note partial per-table attribution and no DDL/lag signal
  - Record that tracking is always-on and not cleanly isolatable per run
  - _Requirements: 1.1, 2.3, 3.5_

- [ ] 6. Checkpoint - collectors wired, one Approach runnable at a time
  - Confirm each collector runs against the benchmark PostgreSQL under the Identical_Workload in isolation; ask the user if questions arise.

- [ ] 7. Author the qualitative capability matrix and setup-cost analysis (PRIMARY, ships independently)
  - [ ] 7.1 Write `docs/benchmark/capability-matrix.md`
    - One row per Approach (CDC, polling, triggers, statement-stats) covering all four axes; per data-activity question mark can/partial/cannot with a one-line justification; state asymmetries explicitly
    - _Requirements: 2.1, 2.2, 2.5_
  - [ ] 7.2 Write the setup-cost analysis section
    - Per approach: components, configuration, dependencies — explicitly the Debezium dependency + snapshot cost for CDC, and the monitored-DB DDL/write-path change for triggers
    - Ensure this layer is complete and publishable without the measured harness
    - _Requirements: 2.3, 2.4_
  - [ ] 7.3 Write the scope framing into the report
    - State data-activity-only scope; operational/infrastructure monitoring (`postgres_exporter`, `pg_stat_activity`) as a separate, complementary, non-competing concern; PostgreSQL-only with MySQL as a future note; app-counters as a mentioned-not-built baseline
    - _Requirements: 1.2, 1.3, 1.4, 1.5_

- [ ]* 8. Implement the source-overhead measurement harness
  - [ ]* 8.1 Implement the overhead sampler in `bench/measure/overhead/`
    - Sample CPU, IO, and active-connection load on the PostgreSQL container at a fixed cadence; run a no-collector baseline of the Identical_Workload and attribute each Approach's overhead as the delta from baseline
    - _Requirements: 3.1, 3.2, 12.1_
  - [ ]* 8.2 Persist overhead results to a machine-readable artifact
    - Write numeric results (not prose) to `bench/results/`; record disclosed parameters (polling interval, trigger presence); flag statement-stats as not-cleanly-isolatable
    - _Requirements: 3.3, 3.4, 3.5_

- [ ]* 9. Implement the write-to-metric latency measurement harness
  - [ ]* 9.1 Implement the marker injector and prober in `bench/measure/latency/`
    - Commit timestamped marker writes; poll each Approach's metric until the marker is reflected; record `t_reflected − t_commit` over many markers for CDC, polling, and triggers
    - _Requirements: 4.1, 4.4_
  - [ ]* 9.2 Emit latency as a distribution and persist results
    - Report at least median and p95; print the sampling interval as the latency floor for timer-driven approaches; store in the same results artifact as overhead
    - _Requirements: 4.2, 4.3, 4.5_

- [ ]* 10. Run the benchmark and collect results with methodology controls
  - Run every measured Approach against the Identical_Workload on the same documented cluster/hardware, with a documented warm-up and multiple runs; emit central tendency and spread
  - Commit summarized results artifacts under `docs/benchmark/results/`
  - _Requirements: 7.1, 7.2, 7.3, 11.1, 8.1_

- [ ] 11. Author the `docs/benchmark/` report body (including the mandatory honesty section)
  - [ ] 11.1 Assemble the report structure and findings
    - `docs/benchmark/README.md`: summary + claim under test + reframe; scope; capability matrix; setup-cost; measured results (if available); "where CDC wins"
    - If the measured layer is deferred, mark the measured sections as pending and ship the qualitative report
    - _Requirements: 2.4, 5.5, 11.1_
  - [ ] 11.2 Write the mandatory "Where CDC does NOT win" section
    - Top-level section (not a footnote), comparable in prominence/detail to "where CDC wins"; name higher setup cost, the Debezium dependency, snapshot cost, and any axis a non-CDC approach won; state non-CDC wins plainly; connect findings back to the "too much" claim and the "multiplier, not a reason to adopt" reframe
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 9.1_
  - [ ] 11.3 Write the "Threats to Validity / Fairness" section
    - Name the confounders (polling interval dominating latency+overhead; trigger write-amplification; CDC's Debezium dependency + snapshot cost; statement-stats always-on tracking) and how each was controlled or why not; disclose tunable parameter choices
    - _Requirements: 7.4, 7.5, 3.4_

- [ ] 12. Final honesty & fairness review (core, never optional)
  - Verify the report satisfies honesty-as-completion: a substantive "where CDC does not win" section exists with comparable prominence; scope framing is consistent (data-activity-only, operational monitoring non-competing, PostgreSQL-only, app-counters not built); every claimed number is backed by a committed results artifact or marked pending; methodology is reproducible from the docs alone
    - _Requirements: 5.1, 5.4, 9.1, 7.3, 12.1_

## Notes

- Tasks marked with `*` (8, 9, 10 and their sub-tasks) are the **measured layer** and are deferrable for a qualitative-first delivery, mirroring the `*`-optional convention in `db-sensor/tasks.md`. The qualitative layer (tasks 1–7) and the report + honesty review (tasks 11–12) are **core and never optional**.
- The "(a) first, then (b)" depth decision is encoded in the ordering: the capability matrix and setup-cost analysis (task 7) complete before — and independently of — the measured harness (tasks 8–10).
- Every `_Requirements:` reference points to a requirement ID defined in `requirements.md` (Requirements 1–12).
- Honesty is a completion criterion (Requirement 9.1), enforced by task 12.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2"] },
    { "id": 2, "tasks": ["2", "3", "4", "5"] },
    { "id": 3, "tasks": ["6"] },
    { "id": 4, "tasks": ["7.1", "7.2", "7.3", "8.1", "9.1"] },
    { "id": 5, "tasks": ["8.2", "9.2"] },
    { "id": 6, "tasks": ["10"] },
    { "id": 7, "tasks": ["11.1", "11.2", "11.3"] },
    { "id": 8, "tasks": ["12"] }
  ]
}
```
