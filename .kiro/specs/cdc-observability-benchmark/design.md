# Design Document: CDC Observability Benchmark

## Overview

This design describes a reproducible local benchmark that compares the ways to obtain **data-activity (mutation) observability** from a PostgreSQL database, so that the claim "setting up a whole CDC pipeline *only* to get metrics is too much" can be answered with data. It implements the requirements in `requirements.md`.

The benchmark has **two layers**, deliberately sequenced so the first can ship without the second:

- **(a) Qualitative layer (primary):** a **capability matrix** (what each approach can and cannot observe) and a **setup-cost analysis** (components, config, dependencies). This is the primary deliverable and does not depend on any measurement.
- **(b) Measured layer (secondary):** a harness that measures the two axes where numbers matter most — **source overhead** (CPU/IO/connection load on PostgreSQL) and **write-to-metric latency** (time from a committed write to the metric reflecting it).

Four Approaches are compared on the same four Axes (setup cost, source overhead, signal richness, latency) against one Identical_Workload: **CDC-derived** (the Lactiflluus operator over a Debezium stream), **polling** (`pg_stat_user_tables` deltas), **triggers** (database-side `AFTER` counters), and **statement-stats** (`pg_stat_statements` sampling). App-level counters are referenced as a baseline only — not built, not measured.

Scope is fixed and not re-litigated here (see `requirements.md` §Scope): **data-activity metrics only** (operational/infrastructure monitoring is a separate, complementary concern and not a competitor), **PostgreSQL only** (MySQL is a future note), **honesty is first-class** (the report must state where CDC loses), and the harness runs **locally with no cloud**, consistent with the repo's synthetic `demo/` kit.

**Motivation and prior art.** The reframe under test — CDC observability as "a multiplier on CDC you already have," not "a reason to adopt CDC" — comes from `ROADMAP.md` "Positioning," grounded in Netflix's DBLog (*A Watermark Based Change-Data-Capture Framework*, 2020, <https://arxiv.org/abs/2010.12597>), where **database activity logging** rode along as one of three use cases on shared CDC infrastructure. The CDC_Derived approach's cost profile is shaped by two decisions already recorded in this repo: [ADR-012](../../../docs/adr/ADR-012-event-transport-http-default-kafka-optional.md) (event transport is HTTP by default, Kafka optional — so the benchmark's CDC path needs no broker) and [ADR-013](../../../docs/adr/ADR-013-derived-event-counts-vs-native-jmx-metrics.md) (counts are derived from the stream, not scraped from Debezium's experimental JMX counters — so the measured CDC overhead is the operator consuming events, not a JMX scrape).

---

## Architecture

The harness drives one PostgreSQL instance with a reproducible mutation workload, runs exactly one Approach's collector at a time against it, samples overhead on the Postgres container, injects marker writes to measure latency, and writes results into a machine-readable artifact that the report renders.

```
┌──────────────────────────────────────────────────────────────────────┐
│ Operator runs: bench run --approach <cdc|polling|triggers|stmtstats>   │
└───────────────────────────────┬──────────────────────────────────────┘
                                 │ (one Approach per run, identical workload)
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│ Workload Generator (fixed seed, i/u/d mix, rate, duration)             │
│   + Latency Marker Injector (timestamped marker writes)                │
└───────────────────────────────┬──────────────────────────────────────┘
                                 │ SQL writes (INSERT/UPDATE/DELETE)
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│ PostgreSQL (kind pod / container)         ◀── Overhead Sampler         │
│   WAL · pg_stat_user_tables · triggers ·      (CPU / IO / connections) │
│   pg_stat_statements                                                   │
└───┬───────────────┬───────────────┬───────────────┬───────────────────┘
    │ logical decode │ stat deltas   │ AFTER trigger │ statement stats
    ▼                ▼               ▼               ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────────────┐
│ CDC-derived │ │ Polling     │ │ Triggers    │ │ Statement-stats     │
│ Debezium →  │ │ pg_stat_    │ │ counters    │ │ pg_stat_statements  │
│ Lactiflluus │ │ user_tables │ │ table       │ │ sampler             │
│ (HTTP/kind) │ │ sampler     │ │ reader      │ │                     │
└──────┬──────┘ └──────┬──────┘ └──────┬──────┘ └──────────┬──────────┘
       │               │               │                   │
       └───────────────┴───────┬───────┴───────────────────┘
                               ▼  per-table i/u/d counts, lag, DDL
                    ┌────────────────────────────┐
                    │ Metric Collector / Prober   │
                    │ (reads each approach's       │
                    │  metric; detects marker)     │
                    └──────────────┬──────────────┘
                                   ▼
                    ┌────────────────────────────┐
                    │ Results artifact (JSON)      │  bench/results/
                    │ overhead + latency + config  │
                    └──────────────┬──────────────┘
                                   ▼
                    ┌────────────────────────────┐
                    │ Report (docs/benchmark/)     │
                    │ capability matrix · numbers  │
                    │ · "where CDC does NOT win"   │
                    └────────────────────────────┘
```

App-level counters are **not** a node in this diagram: they would require changing application code and are referenced as a baseline only (per `requirements.md` §Scope and Requirement 1.5). Operational/infrastructure exporters (`postgres_exporter`, `pg_stat_activity`) are also **not** nodes: they answer a different question and are a complementary concern, not a competitor (Requirement 1.3).

---

## Per-Approach Design (PostgreSQL)

Each Approach is wired specifically for PostgreSQL. For each, this section fixes exactly what it **can** and **cannot** observe — this feeds the Capability_Matrix (Requirement 2).

### CDC-derived (Lactiflluus over Debezium)

- **Wiring:** PostgreSQL with `wal_level=logical`; Debezium's PostgreSQL connector (via the official Debezium Operator, driven by a `DatabaseSensor` CR) streams changes. Per [ADR-012](../../../docs/adr/ADR-012-event-transport-http-default-kafka-optional.md), the default transport is **HTTP** (Debezium POSTs events to the operator's `/cdc`), so the benchmark's CDC path needs **no Kafka broker**. Per [ADR-013](../../../docs/adr/ADR-013-derived-event-counts-vs-native-jmx-metrics.md), the operator **derives** per-op/per-table counts and lag from the event stream rather than scraping JMX.
- **Can observe:** per-table insert/update/delete counts and rates; replication/change **lag** (`now() − ts_ms` from the event). Per-row detail is available in the event.
- **Correction (verified in task 2):** schema/DDL changes are **not** observable on PostgreSQL. Logical decoding (`pgoutput`) does not carry DDL, so Debezium's PostgreSQL connector emits no schema-change event; an `ALTER TABLE` on a monitored table produced no `debezium_schema_changes_total` series. Connectors with a schema history (MySQL, SQL Server, Oracle) do emit them. On PostgreSQL, no Approach observes DDL, so it is not a differentiator. See `bench/collectors/cdc/README.md`.
- **Cannot observe (honestly):** nothing until the Debezium initial snapshot completes; it adds a running Debezium process and a replication slot (which retains WAL). It observes only the tables included in the connector filter.
- **Cost drivers for the matrix:** Debezium runtime dependency, replication-slot/WAL retention, initial snapshot cost.

### Polling (`pg_stat_user_tables` deltas)

- **Wiring:** a timer-driven sampler issues `SELECT n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_user_tables` per monitored table every *interval* seconds and computes deltas between samples. (`SELECT count(*)` / `information_schema` are alternatives; `pg_stat_user_tables` is the primary because it gives per-operation tuple counters cheaply.)
- **Can observe:** per-table insert/update/delete **counts and rates** at interval granularity.
- **Cannot observe (honestly):** anything finer than the sampling interval; it misses bursts that cancel within an interval; `pg_stat_user_tables` counters are **cumulative and reset-prone** (e.g. on crash/stat reset), which the sampler must detect. It does **not** see schema/DDL changes, and its "lag" is effectively the sampling interval, not true change lag.
- **Cost drivers:** one connection and a lightweight query every interval; overhead and latency are both dominated by the interval choice.

### Triggers (database-side `AFTER` counters)

- **Wiring:** `AFTER INSERT/UPDATE/DELETE` row-level triggers on each monitored table increment a per-table/per-operation counter row in a dedicated counters table; a reader samples that counters table.
- **Can observe:** per-table insert/update/delete counts and rates with **immediate** (synchronous-to-commit) accuracy; the counters update inside the writing transaction.
- **Cannot observe (honestly):** schema/DDL changes; triggers themselves require DDL on the monitored database (an invasive change to the write path) and **write-amplify** every mutation (extra row update per event), which is itself a source of overhead on the measured database.
- **Cost drivers:** write-amplification proportional to workload write rate; lock/contention on the counters rows under concurrency.

### Statement-stats (`pg_stat_statements` sampling)

- **Wiring:** enable the `pg_stat_statements` extension; a sampler periodically reads call counts and rows affected per normalized statement and attributes them to tables heuristically.
- **Can observe:** aggregate statement call counts and `rows` affected, which approximate mutation volume by statement shape.
- **Cannot observe (honestly):** clean **per-table** attribution (statements are normalized by text, not table; multi-table statements blur attribution); schema/DDL changes; true change lag. Its signal is the weakest of the four for the data-activity questions.
- **Cost drivers:** `pg_stat_statements` tracking is **always on** once enabled (not isolatable per run), so its marginal overhead is hard to attribute cleanly (Requirement 3.5).

### App-level counters (baseline — not built)

- **Referenced only.** Application code increments its own per-operation counters. It is the thing CDC avoids because it requires changing application code. It is listed in the Capability_Matrix narrative as a baseline but is **not** wired or measured (Requirement 1.5).

---

## Measurement Methodology

### Source overhead (Requirement 3)

- **What is sampled:** on the PostgreSQL container, the harness samples CPU, IO (block reads/writes), and active connection count at a fixed cadence throughout each run. In kind, container CPU/IO come from cgroup/pod metrics; connections come from `pg_stat_activity` count (used here as a *measurement instrument*, not as a compared Approach).
- **Fair attribution:** each measured Approach's run is compared against a **baseline run** of the Identical_Workload with **no collector attached**. Source_Overhead for an Approach is the delta (Approach run − baseline run) on each sampled dimension. This isolates the cost the Approach *adds* from the cost of the workload itself.
- **Parameter disclosure:** where overhead depends on a tunable (polling interval, trigger presence), the harness records the parameter value in the results artifact and the report notes the number is a function of it (Requirements 3.4, 7.5).
- **Statement-stats caveat:** because `pg_stat_statements` tracking is always on once enabled, its overhead cannot be cleanly isolated per run; the report states this rather than reporting a misleading isolated number (Requirement 3.5).

### Write-to-metric latency (Requirement 4)

- **Marker writes:** the latency prober commits a distinguishable **marker write** (e.g. a row with a known sentinel key and a client-side commit timestamp) and then polls the Approach's metric until it reflects that marker, recording `t_reflected − t_commit`. This repeats over many markers to build a distribution.
- **Distribution, not a point:** results report at least median and p95 (Requirement 4.2).
- **Interval as a floor:** for timer-driven Approaches (polling, sampled statement-stats), the sampling interval is a hard lower bound on observable latency; the report prints the interval next to the latency so the floor is explicit, and does not claim latency below the interval (Requirement 4.3).
- **Comparability:** all Approaches are probed under the same workload on the same cluster/hardware (Requirement 4.4).

### Confounders and controls (Requirement 7)

- **Warm-up + repeated runs:** each Approach gets a documented warm-up period (discarded) and multiple measured runs; the report shows central tendency and spread (Requirement 7.2).
- **Fixed workload seed:** the Workload_Generator uses a fixed, documented seed so every Approach sees the same mutation sequence (Requirements 6.2, 8.1).
- **Isolation:** one Approach's collector runs at a time against a dedicated PostgreSQL; no unrelated workload shares the instance during measurement (Requirement 12.1).
- **Same hardware/cluster:** all runs execute on one documented host and one kind cluster (Requirements 7.1, 4.4).

---

## Proposed Repository Layout

New harness code under a new `bench/` directory; the report and versioned results under `docs/benchmark/`.

```
bench/                              # reproducible harness (new)
├── README.md                      # how to run; mirrors demo/ tone (Docker+kind, no cloud)
├── Makefile                       # bench targets: setup, run, collect, report, teardown
├── workload/                      # Workload Generator (fixed seed, i/u/d mix, rate)
├── collectors/
│   ├── cdc/                       # DatabaseSensor CR + wiring for the operator over Debezium (HTTP)
│   ├── polling/                   # pg_stat_user_tables delta sampler
│   ├── triggers/                  # AFTER-trigger counters table + reader + install/teardown SQL
│   └── stmtstats/                 # pg_stat_statements sampler
├── measure/
│   ├── overhead/                  # CPU/IO/connection sampler + baseline-delta attribution
│   └── latency/                   # marker-write injector + metric prober
├── cluster/                       # kind config, PostgreSQL manifests (wal_level=logical)
└── results/                       # machine-readable run artifacts (JSON), git-ignored raw/

docs/benchmark/                    # versioned deliverable (new)
├── README.md                      # the Report: methodology, capability matrix, numbers, honesty
├── capability-matrix.md           # qualitative layer (can ship without measured layer)
├── results/                       # committed summarized results artifacts (JSON) + derived tables
└── threats-to-validity.md         # Threats to Validity / Fairness (may be folded into README)
```

The split keeps the qualitative layer (`docs/benchmark/capability-matrix.md`) publishable on its own, and the measured artifacts versioned in-repo (Requirement 11).

---

## Capability-Matrix Template

The qualitative layer produces this table (cells filled by analysis; measured axes cross-referenced to the results artifact). Legend: ✅ can observe · 🟡 partial · ❌ cannot.

### Data-activity questions × approaches

| Question | CDC-derived | Polling (`pg_stat_user_tables`) | Triggers | Statement-stats (`pg_stat_statements`) |
|---|---|---|---|---|
| Per-table insert rate | ✅ | ✅ (interval granularity) | ✅ | 🟡 (attribution by statement, not table) |
| Per-table update rate | ✅ | ✅ (interval granularity) | ✅ | 🟡 |
| Per-table delete rate | ✅ | ✅ (interval granularity) | ✅ | 🟡 |
| Replication / change lag | ✅ (true `now()−ts_ms`) | ❌ (only interval) | ❌ (immediate, no lag concept) | ❌ |
| Schema / DDL change detection | ❌ on PostgreSQL (verified: `pgoutput` carries no DDL; ✅ on MySQL-family connectors) | ❌ | ❌ | ❌ |
| Per-row / change detail | ✅ | ❌ | ❌ | ❌ |

### Four axes × approaches (setup-cost cells qualitative; overhead/latency filled from measured results)

| Axis | CDC-derived | Polling | Triggers | Statement-stats |
|---|---|---|---|---|
| Setup cost | High (Debezium + operator + slot + snapshot) | Low (one sampler + connection) | Medium (DDL on monitored DB: triggers + counters table) | Low–Medium (extension + sampler) |
| Source overhead | *(measured)* | *(measured; interval-dependent)* | *(measured; write-amplifying)* | *(measured; not cleanly isolatable)* |
| Signal richness | Highest (rates + lag + DDL + detail) | Medium (rates only) | Medium (rates only, immediate) | Lowest (approximate, blurred attribution) |
| Latency | *(measured; event-driven)* | *(measured; interval is the floor)* | *(measured; commit-synchronous)* | *(measured; interval is the floor)* |

App-level counters appear only as a narrative baseline row in the report (requires app code changes; not measured).

---

## Depth vs. breadth positioning

This is an **added analysis layer** on top of the head-to-head, not a replacement: the head-to-head methodology above (four Approaches, four Axes, Identical_Workload) is unchanged. This section satisfies **Requirement 13** and feeds the report's final verdict.

### The central hypothesis (restated)

> CDC-derived observability is **narrow-but-deep** for data activity — it produces per-row operation semantics, exact commit-to-metric lag, and additive-vs-breaking DDL detection that the alternatives structurally cannot. The **broader** toolset (native `pg_stat_*` server views, `postgres_exporter`, APM agents, query-log scraping) is **broad-but-shallow** for data activity, but covers a much wider range of database concerns CDC never touches. So the honest pitch is **"install only if you already have CDC"**: not worth standing up CDC *solely* for metrics, but nearly-free depth if CDC already runs for another reason.

The benchmark must be able to **SUPPORT, QUALIFY, or REFUTE** this — including concluding *"CDC is over-engineering for metrics alone"* (Requirements 13.3, 5.6).

### Two-axis mental model

Two orthogonal axes. **Depth** = how rich/exact the *data-activity* signal is (the head-to-head territory). **Breadth** = how wide the tool's *overall* database-observability coverage is across all DB concerns (server health, query latency, connections, locks, slow queries). The data-activity Approaches live mostly on the Depth axis; the operational Broader_Toolset lives on the Breadth axis.

```
                 high DEPTH (data-activity signal)
                              ▲
                              │   ● CDC-derived
                              │     (per-row ops, exact lag, DDL)
                              │
          ● Triggers         │   ● Polling
            (immediate,      │     (interval rates)
             rates only)     │
                              │
   broad BREADTH ◀───────────┼───────────────▶ narrow BREADTH
   (all DB concerns)         │                 (data activity only)
                              │
       ● APM / exporters /    │   ● pg_stat_statements
         pg_stat_* server     │     (approximate, blurred
         views, query-log     │      attribution)
         scraping             │
         (server health,      │
          latency, locks,     │
          slow queries)       ▼
                        low DEPTH (for data activity)
```

Read it as a 2×2: CDC sits **narrow-breadth / high-depth** (deep on one question); the Broader_Toolset sits **broad-breadth / low-depth** (shallow on data activity, wide on everything else). Polling and triggers are mid-depth on data activity and narrow on breadth. The point of the picture is that CDC and the operational toolset are in **different quadrants** — they do not compete, they complement.

### The breadth axis: NOT head-to-head competitors

The breadth axis lists the Broader_Toolset — **native `pg_stat_*` server views** (`pg_stat_activity`, `pg_stat_database`, buffer/lock views), **`postgres_exporter`**, **APM agents**, and **query-log scraping** — explicitly as the tools that cover the *wide* range of DB observability CDC does not. These are the **same tools §Scope and Requirement 1.3 hold out of the head-to-head as a complementary, non-competing concern.** They are not promoted to Approaches here: they are never measured against CDC on the four Axes, and the Capability_Matrix is unchanged. They appear only to characterize *breadth*, so the "complement, not replacement" conclusion (Requirement 13.2) is grounded instead of asserted. This keeps the two framings consistent: the operational tools are out of scope as *competitors* and in scope only as the *breadth yardstick*.

### Reaching a SUPPORT / QUALIFY / REFUTE verdict

The report derives one explicit verdict on the "install only if you already have CDC" thesis from two inputs already produced by the head-to-head:

- **From the Capability_Matrix (Requirement 2):** the data-activity insights CDC produces that each alternative *structurally cannot* — per-row op semantics, additive-vs-breaking DDL, precise commit-to-metric lag (Requirement 13.4a) — alongside the observability dimensions the Broader_Toolset covers that CDC does not — server health, query latency, connections, lock waits, slow queries (Requirement 13.4b).
- **From the measured results (Requirements 3–4):** whether CDC's depth comes at an overhead/latency/setup cost that a cheaper Approach already matches for a team *without* existing CDC.

The verdict rule the report applies (Requirement 13.3):

- **SUPPORT** — CDC is uniquely deep on data-activity questions the alternatives cannot answer, AND its standing cost is only "free-on-top" when CDC already exists → the "install only if you already have CDC" pitch holds.
- **QUALIFY** — CDC's depth is real but partly matched by a cheaper Approach for some questions, OR the cost is tolerable even without existing CDC for specific high-value signals (e.g. exact lag, DDL) → pitch holds with named exceptions.
- **REFUTE** — a cheaper Approach delivers enough of the data-activity signal at materially lower cost that CDC's extra depth does not justify its cost even as an add-on → the report states plainly that *"CDC is over-engineering for metrics alone"* (Requirements 5.6, 13.3).

The verdict's strength depends on which layers were run: with only the qualitative layer it is grounded in the Capability_Matrix and setup-cost analysis; the measured layer, when present, sharpens it with overhead/latency numbers.

---

## Threats to Validity / Fairness

This section is **required for credibility** (Requirement 7.4) and is carried verbatim-in-spirit into the Report. Each threat names a confounder and how the harness controls it (or why it cannot).

- **Polling interval dominates both its latency and its overhead.** A shorter interval lowers latency but raises overhead; a longer one does the reverse. *Control:* the interval is a disclosed parameter, latency is reported with the interval as its floor, and the report states that a different interval would move both numbers. Honestly, polling has no single "fair" interval — the report presents the trade-off, not one winning point.
- **Trigger overhead is write-amplifying.** Every mutation triggers an extra counter write, so the measured overhead scales with the workload's write rate and is not a fixed tax. *Control:* overhead is measured under the Identical_Workload and attributed against the no-collector baseline; the write-amplification is called out as inherent, not incidental.
- **CDC adds a Debezium dependency and a snapshot cost.** The CDC_Derived numbers exclude nothing: the running Debezium process, the replication slot's WAL retention, and the one-time initial snapshot are real costs. *Control:* these are stated as fixed setup/standing costs in the matrix and the Honesty_Section, even where steady-state overhead looks favorable. Per [ADR-012](../../../docs/adr/ADR-012-event-transport-http-default-kafka-optional.md) the benchmark uses HTTP transport (no Kafka), which *understates* the cost of a Kafka-based CDC deployment — the report notes that a durable Kafka transport would add broker cost not measured here.
- **Statement-stats tracking is always on.** Once `pg_stat_statements` is enabled, its cost is paid globally and cannot be isolated to a run. *Control:* the report says so (Requirement 3.5) rather than publishing a misleading isolated overhead number.
- **Measurement instrument overlap.** The harness reads `pg_stat_activity` to count connections as an *instrument*; this is not a compared Approach and its sampling cost is part of every run's baseline, so it does not bias the Approach-vs-baseline delta.
- **Single-host, synthetic workload.** Results come from one local kind host and a synthetic mutation mix; they are reproducible and comparable but are **not** a claim about any specific production system. *Control:* the report states the hardware/cluster and frames results as comparative-under-identical-conditions, not absolute.
- **The reframe is not foreordained.** The benchmark may show that for a team that is **not** already running CDC, polling or triggers are the cheaper way to get mutation rates — which supports the "too much" complaint for that team. *Control:* the Honesty_Section reports this plainly and ties it back to the "multiplier, not a reason to adopt" framing (Requirement 5.5): CDC's case is strongest when the CDC infrastructure already exists for another reason (the DBLog pattern), weakest when it must be stood up solely for metrics.

---

## Reporting Design (honesty is first-class)

The Report under `docs/benchmark/` is structured so the honesty obligation is structural, not optional:

1. **Summary & the claim under test** — states the "CDC-only-for-metrics is too much" claim and the `ROADMAP.md` reframe.
2. **Scope** — data-activity only; operational monitoring is a separate complementary concern (not a competitor); PostgreSQL only; app-counters as baseline.
3. **Capability matrix** — the tables above, filled.
4. **Setup-cost analysis** — components/config/dependencies per approach.
5. **Measured results** — overhead and latency, with methodology and the parameter values used.
6. **Where CDC wins** — findings favorable to CDC.
7. **Where CDC does NOT win** — the mandatory Honesty_Section (Requirement 5), given comparable prominence and detail to §6; names higher setup cost, the Debezium dependency, snapshot cost, and any axis a non-CDC approach won.
8. **Threats to validity / fairness** — the section above.
9. **Depth vs. breadth positioning** — the Positioning_Analysis (Requirement 13): CDC's narrow-deep signal vs. the Broader_Toolset's broad-shallow coverage, the two-axis model, and the enumerated CDC-only insights vs. breadth-only dimensions.
10. **Conclusion** — the explicit **SUPPORT / QUALIFY / REFUTE** verdict on the Central_Hypothesis and the "install only if you already have CDC" pitch (Requirement 13.3), willing to state *"CDC is over-engineering for metrics alone"* where the data points that way (Requirement 5.6), tied back to the `ROADMAP.md` "multiplier, not a reason to adopt" reframe (Requirement 5.5).

A report that renders §6 without a substantive §7 is incomplete by definition (Requirement 9.1).

---

## Dependencies

- **Local tooling only** (Requirements 6.1, 10.1), consistent with `demo/`: Docker, kind, kubectl, helm.
- **PostgreSQL** with `wal_level=logical` (for the CDC approach) and the `pg_stat_statements` extension (for the statement-stats approach).
- **Official Debezium Operator** + the Lactiflluus operator image (for the CDC approach), over the **HTTP** transport per ADR-012 (no Kafka broker required for the benchmark).
- **No cloud account, managed service, or paid backend** at any point.

## Open Questions / Future Iterations

1. **MySQL extension.** Repeating the comparison for MySQL (`performance_schema`-based polling, MySQL triggers, Debezium MySQL connector) is a possible future extension; out of scope for v1.
2. **Kafka-transport CDC overhead.** The benchmark measures the HTTP transport; a future run could add the Kafka variant to quantify the broker cost the HTTP path avoids.
3. **Larger workloads.** v1 targets a single local host; a future iteration could parameterize the workload toward higher rates to see where each approach degrades.
