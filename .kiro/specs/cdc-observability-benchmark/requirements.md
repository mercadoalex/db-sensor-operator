# Requirements Document

## Introduction

This document specifies the **CDC Observability Benchmark**: a scoped, reproducible comparison of the ways a team can obtain **data-activity (mutation) observability** from a PostgreSQL database, with the explicit goal of answering — with data, not assertion — whether "setting up a whole CDC pipeline *only* to get metrics is too much."

That claim comes from the Debezium community (relayed by maintainer Mario Fiore Vitale): when metrics are the *sole* payoff, the cost/benefit of standing up Change Data Capture (CDC) does not obviously close. The project's `ROADMAP.md` "Positioning" section argues this may be a **framing problem** rather than a hard cost truth. Netflix's DBLog — *A Watermark Based Change-Data-Capture Framework* (Andreakis & Papapanagiotou, 2020, <https://arxiv.org/abs/2010.12597>) — ran **database activity logging** as one of *three* use cases on CDC infrastructure that already existed for replication and migration. Observability rode along on infrastructure built for another reason; nobody stood up CDC only for metrics.

This benchmark exists to test that reframe honestly. The db-sensor-operator ("Lactiflluus") derives database observability metrics (per-table insert/update/delete rates, replication/change lag, schema/DDL change detection) from a Debezium CDC stream. The benchmark measures where CDC-derived data-activity observability is genuinely cheaper or richer than the alternatives **and where it is not**.

**Intellectual honesty is a first-class, non-negotiable requirement of this benchmark.** Credibility with the Debezium maintainers depends on reporting where CDC *loses* — higher setup cost, a Debezium dependency, snapshot cost — just as prominently as where it wins. Any version of the deliverable that reports only CDC's advantages fails to satisfy these requirements. This honesty obligation recurs across requirements, design, and tasks by intent.

These requirements mirror the structure and the "WHEN … THE SYSTEM SHALL …" acceptance-criteria style of the existing `db-sensor` spec, and are kept numerically stable so the design and tasks can reference them by ID.

## Scope

### In scope

- **Metric family:** data-activity / **mutation** observability only — per-table insert/update/delete rates, replication/change **lag** (time from a committed write to its visibility as a metric), and **schema/DDL change** detection.
- **Database:** **PostgreSQL only** for v1.
- **Approaches compared (all on the same workload and the same four axes):**
  - **CDC-derived** — the Lactiflluus operator consuming a Debezium change stream.
  - **Polling** — periodic sampling on a timer (e.g. `pg_stat_user_tables` tuple-counter deltas, or `SELECT count(*)` / `information_schema`).
  - **Database triggers** — `AFTER INSERT/UPDATE/DELETE` counters maintained database-side in a counters table.
  - **Query-log / statement-stats scraping** — `pg_stat_statements` sampled over time.
- **Two layers of depth:**
  - **(a) Qualitative comparison (PRIMARY deliverable):** a capability matrix (which data-activity questions each approach can answer) plus a setup-cost analysis (components, config, dependencies). This layer must be fully specified and can complete on its own.
  - **(b) Measured harness (SECONDARY):** reproducible measurement of the two axes where numbers matter most — **source overhead** (CPU/IO/connection load on the database under an identical workload) and **write-to-metric latency** (time from a committed write until each approach's metric reflects it).
- **The four evaluation axes (from `ROADMAP.md`):** setup cost, source overhead, signal richness (capability), and latency.
- **Reproducibility target:** a local harness — kind cluster + PostgreSQL + a workload generator with a known insert/update/delete mix — consistent with the repo's existing synthetic, reproducible, no-cloud `demo/` approach.

### Out of scope

- **Operational / infrastructure database metrics** (CPU, connection counts, query latency, buffer-cache hit ratio, lock waits — the territory of `pg_stat_activity`, `performance_schema`, and `postgres_exporter`). These answer a **different question** than data-activity observability and are a **separate, complementary concern — NOT a competitor**. CDC-derived observability and operational exporters are **not substitutes**: one tells you *what data changed and how fast*, the other tells you *how the server is behaving*. The benchmark must state this explicitly and must not frame operational exporters as an approach being beaten.
- **MySQL, MongoDB, and other databases.** MySQL is noted as a *possible future extension* only; it is not specified or measured here.
- **App-level counters** (application code incrementing its own metrics). This is **mentioned as a baseline** — the thing CDC explicitly avoids, because it requires changing application code — but it is **not built and not measured**.
- **Operator/application performance tuning, anomaly detection, or alerting backends.** The benchmark measures observation cost and capability, not downstream alerting.

## Glossary

- **Benchmark_Harness**: The reproducible local system (workload generator + PostgreSQL + per-approach collectors + measurement + report) specified by this document; the system under specification. Shortened to "the harness" in prose.
- **Approach**: One of the compared ways to obtain data-activity observability — CDC-derived, Polling, Triggers, or Statement-stats.
- **CDC_Derived**: The Lactiflluus operator consuming a Debezium change stream (the project's own approach).
- **Polling_Approach**: Timer-driven sampling of `pg_stat_user_tables` tuple counters (or equivalent count queries).
- **Triggers_Approach**: Database-side `AFTER` triggers maintaining per-table/per-operation counters in a counters table.
- **Statement_Stats_Approach**: Periodic sampling of `pg_stat_statements`.
- **App_Counter_Baseline**: Application-code-maintained counters; referenced as a baseline, not built or measured.
- **Workload_Generator**: A component that applies a configurable insert/update/delete mix to PostgreSQL at a controlled, reproducible rate with a fixed seed.
- **Identical_Workload**: The same generated sequence of mutations (same mix, rate, seed, duration) applied while each Approach is measured.
- **Source_Overhead**: The additional CPU, IO, and active-connection load an Approach imposes on the PostgreSQL container under the Identical_Workload.
- **Write_To_Metric_Latency**: The elapsed time from the commit of a database write to the moment an Approach's metric reflects that write.
- **Capability_Matrix**: The Approaches × data-activity-questions table recording what each Approach can and cannot observe.
- **Axis**: One of the four evaluation dimensions — setup cost, source overhead, signal richness (capability), latency.
- **Honesty_Section**: The mandatory part of the report that states, explicitly and prominently, where CDC does **not** win.
- **Report**: The versioned markdown deliverable under `docs/benchmark/` that records methodology, the Capability_Matrix, measured numbers, and the Honesty_Section.

## Requirements

### Requirement 1: Scope the benchmark to data-activity observability on PostgreSQL

**User Story:** As the project author answering the community, I want the benchmark's scope pinned to data-activity metrics on PostgreSQL, so that the comparison is fair, finishable, and not conflated with operational monitoring.

#### Acceptance Criteria

1. THE Benchmark_Harness SHALL evaluate exactly these four Approaches — CDC_Derived, Polling_Approach, Triggers_Approach, and Statement_Stats_Approach — and SHALL evaluate no additional approach as a measured competitor.
2. THE Report SHALL restrict all measured and matrixed metrics to the data-activity family (per-table insert/update/delete rates, replication/change lag, schema/DDL change detection) and SHALL NOT present operational/infrastructure metrics (CPU, connections, query latency, buffer cache) as a benchmarked Approach.
3. THE Report SHALL include a statement that operational/infrastructure database monitoring (e.g. `postgres_exporter`, `pg_stat_activity`, `performance_schema`) is a **separate, complementary concern** that answers a different question, and that CDC-derived observability and operational exporters are **not substitutes** for one another.
4. THE Report SHALL target PostgreSQL only, and SHALL mention MySQL solely as a possible future extension without specifying or measuring it.
5. THE Report SHALL reference the App_Counter_Baseline as the application-code approach that CDC avoids, and SHALL NOT build or measure it.

### Requirement 2: Qualitative capability matrix (primary deliverable)

**User Story:** As an SRE deciding how to get mutation metrics, I want a capability matrix across every approach and axis, so that I can pick an approach from what it can actually observe and what it costs to set up, even before any numbers are measured.

#### Acceptance Criteria

1. THE Capability_Matrix SHALL contain one row per Approach (CDC_Derived, Polling_Approach, Triggers_Approach, Statement_Stats_Approach) and SHALL cover all four Axes (setup cost, source overhead, signal richness, latency) for each Approach.
2. FOR EACH Approach, THE Capability_Matrix SHALL record, for each data-activity question (per-table insert rate, per-table update rate, per-table delete rate, replication/change lag, schema/DDL change detection), whether the Approach **can observe**, **can partially observe**, or **cannot observe** it, with a one-line justification.
3. THE setup-cost analysis SHALL list, for each Approach, the components it requires, the configuration it needs, and the external dependencies it introduces (for CDC_Derived, explicitly the Debezium dependency and the initial snapshot cost; for Triggers_Approach, explicitly the DDL/write-path change to the monitored database).
4. THE Capability_Matrix and setup-cost analysis SHALL be completable and publishable independently of the measured harness (Requirements 3 and 4), so that the qualitative layer can ship even if the measured layer is deferred.
5. WHERE an Approach cannot answer a data-activity question that CDC_Derived can (or vice versa), THE Capability_Matrix SHALL state the asymmetry explicitly rather than omitting the weaker cell.

### Requirement 3: Measured source overhead

**User Story:** As a Debezium maintainer evaluating the claim, I want source-overhead numbers produced under an identical workload, so that "CDC is too much" can be checked against real CPU/IO/connection cost rather than intuition.

#### Acceptance Criteria

1. WHEN the measured harness runs, THE Benchmark_Harness SHALL apply the Identical_Workload (same insert/update/delete mix, rate, seed, and duration) to PostgreSQL once per Approach under measurement.
2. FOR EACH of CDC_Derived, Polling_Approach, and Triggers_Approach, THE Benchmark_Harness SHALL measure Source_Overhead as the additional CPU, IO, and active-connection load observed on the PostgreSQL container while that Approach is collecting, attributed against a no-collector baseline run of the same workload.
3. THE Benchmark_Harness SHALL record Source_Overhead measurements as reproducible numeric values (not prose estimates) and SHALL store them in a machine-readable results artifact under the harness results directory.
4. WHERE an Approach's overhead is dominated by a tunable parameter (e.g. the Polling_Approach sampling interval, or trigger write-amplification), THE Report SHALL state the parameter value used and note that the number is a function of that parameter.
5. IF the Statement_Stats_Approach overhead cannot be cleanly isolated from PostgreSQL's always-on statement tracking, THEN THE Report SHALL say so explicitly rather than reporting a misleading isolated number.

### Requirement 4: Measured write-to-metric latency

**User Story:** As an SRE, I want to know how quickly each approach reflects a committed write, so that I can judge which approach is fast enough for the signal I need.

#### Acceptance Criteria

1. WHEN the measured harness runs, THE Benchmark_Harness SHALL, for each of CDC_Derived, Polling_Approach, and Triggers_Approach, measure Write_To_Metric_Latency as the elapsed time from the commit of a marker write to the moment that Approach's metric reflects it, over multiple sampled writes.
2. THE Benchmark_Harness SHALL report Write_To_Metric_Latency as a distribution (at minimum median and a high-percentile value such as p95) rather than a single number.
3. WHERE an Approach is timer-driven (Polling_Approach, and sampled Statement_Stats_Approach), THE Report SHALL treat the sampling interval as a latency **floor** and SHALL state the interval alongside the measured latency so the floor is visible.
4. THE Benchmark_Harness SHALL measure all Approaches under the same workload and the same cluster/hardware so that latency numbers are directly comparable.
5. THE Benchmark_Harness SHALL store Write_To_Metric_Latency results in the same machine-readable results artifact as the overhead results (Requirement 3.3).

### Requirement 5: Honest "where CDC does not win" reporting

**User Story:** As a Debezium maintainer evaluating the claim, I want the report to call out where CDC loses, so that I can trust the comparison instead of dismissing it as advocacy.

#### Acceptance Criteria

1. THE Report SHALL include a clearly labelled Honesty_Section titled to the effect of "Where CDC does NOT win," positioned as a top-level section of the Report, not a footnote.
2. THE Honesty_Section SHALL name at least the following CDC disadvantages where the data or analysis supports them: higher setup cost, the Debezium runtime dependency, the initial snapshot cost, and any axis on which a non-CDC Approach measured better.
3. WHEN the measured results show a non-CDC Approach winning on an Axis (e.g. Triggers_Approach achieving lower write-to-metric latency, or Polling_Approach being cheaper to set up), THE Report SHALL state that result plainly in the Honesty_Section without softening or omission.
4. THE Report SHALL present the "where CDC wins" and "where CDC does not win" findings with comparable prominence and detail.
5. THE Report SHALL explicitly connect its findings back to the "CDC-only-for-metrics is too much" claim and the `ROADMAP.md` "multiplier, not a reason to adopt" reframe, stating honestly whether the data supports, qualifies, or undercuts the reframe.

### Requirement 6: Local, no-cloud reproducibility

**User Story:** As anyone re-running the benchmark, I want it to run locally with no cloud resources, so that the results are independently reproducible and cheap to verify.

#### Acceptance Criteria

1. THE Benchmark_Harness SHALL run entirely on a local kind cluster with a local PostgreSQL and the Workload_Generator, requiring only tooling consistent with the repo's `demo/` kit (Docker, kind, kubectl, helm) and SHALL NOT require any cloud account, managed service, or paid backend.
2. THE Workload_Generator SHALL produce a reproducible mutation sequence from a fixed, documented seed and a configurable insert/update/delete mix and rate.
3. THE Benchmark_Harness SHALL document the exact commands to stand up the environment, run each Approach, and collect results, such that a second operator can reproduce a run from the documentation alone.
4. THE Benchmark_Harness SHALL provide a teardown path that removes everything it created, consistent with the `demo/` teardown convention.

### Requirement 7: Fairness and methodology rigor

**User Story:** As a reviewer of the benchmark, I want documented fairness controls, so that the comparison is credible and the numbers are not an artifact of uneven conditions.

#### Acceptance Criteria

1. THE Benchmark_Harness SHALL run every measured Approach against the Identical_Workload on the same cluster and the same host hardware, and THE Report SHALL document the hardware/cluster used.
2. THE Benchmark_Harness SHALL perform a documented warm-up before measurement and SHALL execute multiple runs per Approach, and THE Report SHALL present central tendency and spread (e.g. median and a spread measure) rather than a single run.
3. THE Report SHALL document the methodology for each measured Axis in enough detail that the measurement can be independently reproduced and critiqued.
4. THE Report SHALL include a "Threats to Validity / Fairness" section that names the known confounders (e.g. polling-interval choice dominating both polling latency and overhead; trigger write-amplification; CDC's Debezium dependency and snapshot cost; statement-stats always-on tracking) and how each was controlled or why it could not be.
5. WHERE a tunable parameter materially changes an Approach's result, THE Report SHALL state the chosen value and acknowledge that a different choice would move the result.

## Non-Functional Requirements

### Requirement 8: Reproducibility

#### Acceptance Criteria

1. THE Benchmark_Harness SHALL be deterministic in its workload (fixed seed, documented mix/rate) so that two runs of the same configuration apply the same mutation sequence.

### Requirement 9: Honesty as a deliverable property

#### Acceptance Criteria

1. THE Report SHALL be rejected as incomplete if it omits the Honesty_Section (Requirement 5) or presents only results favorable to CDC; honesty is a completion criterion, not an optional addendum.

### Requirement 10: No cloud dependency

#### Acceptance Criteria

1. THE Benchmark_Harness SHALL introduce no dependency on cloud infrastructure or paid external services; all components SHALL run locally.

### Requirement 11: Results versioned in-repo

#### Acceptance Criteria

1. THE Report and the machine-readable results artifacts SHALL live under `docs/benchmark/` in the repository so that results are versioned alongside the code and methodology they were produced from.

### Requirement 12: Methodology rigor

#### Acceptance Criteria

1. THE Benchmark_Harness SHALL isolate the measured PostgreSQL and each Approach's collector so that unrelated workloads do not contaminate Source_Overhead or Write_To_Metric_Latency measurements, and THE Report SHALL document the isolation used.
