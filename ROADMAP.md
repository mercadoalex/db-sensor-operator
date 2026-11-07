# Roadmap

Where Lactifluus (the DatabaseSensor operator) could go next. This is a living,
prioritized document — not a commitment. It is grounded in what the operator is
**today** (declarative CDC observability: derived metrics, multi-sink alerting,
built-in dashboard, Helm packaging, hybrid status) versus what teams running
Kubernetes in production actually reach for.

The guiding lens: the gap between "a good MVP" and "a tool an SRE reaches for" is
mostly **trust, ecosystem fit, and reducing the operator's work** — not more
features for their own sake. Tiers below reflect that: Tier 1 earns trust, Tier 2
makes it compelling, Tier 3 serves fleet-scale operability.

**Direction note (2026-11-05).** Since 2026-10-31 the project is also a research effort on
whether Kubernetes can react to the behavior of the data. The tiers below are the product
backlog; the research direction, its evidence and its open questions live in
[`bench/experiments/FINDINGS.md`](bench/experiments/FINDINGS.md) and
[`docs/HANDOFF.md`](docs/HANDOFF.md). Where the two disagree, this file says so.

## Positioning

**Status: a hypothesis, not validated.** The direction is no longer "observability for people
who run CDC." It is **Kubernetes reacts to the behavior of the data**: CDC senses what changed, a
policy decides whether the change is allowed, and Kubernetes acts (alert, ask for approval, stop a
Job). None of the reaction is built, and nobody outside the project has said they want it. That is
what the user conversations are for (R8, [`docs/research/user-conversations.md`](docs/research/user-conversations.md)).

**What we do not pitch**
- **"A reason to adopt CDC to get metrics."** A Debezium maintainer told us that setting up a
  whole CDC pipeline *only* to get metrics felt like too much. Our experiments agree with the
  objection: counting a burst of changes did not need CDC (see below).
- **"A multiplier on CDC you already have."** An earlier version of this file recommended that
  reframe. **Decision (owner, 2026-11-05): we do not support or follow that path.** The evidence
  does not refute it for a team that already runs CDC, where the extra cost of a metrics layer is
  low. What it does show against it (a reading of our own experiments and of the docs, not a
  market finding):
  1. The payoff stays metrics. Both Debezium runtimes we compared already expose JMX counters
     ([`competitive-landscape.md`](docs/research/competitive-landscape.md)), so for those teams our
     layer adds little.
  2. The audience is "teams already running CDC", and we have not measured how many there are.
  3. It leaves out the only things we found that CDC gives and the alternatives do not: the
     content and the order of a change.

**What the experiments say** (6 scenarios, one run each, a local kind cluster, PostgreSQL 16;
evidence from a bench, not from the market):
- CDC is the only source we found for the **content** of a change (a forbidden value transition;
  needs `REPLICA IDENTITY FULL`, the default gives `before=null`) and for its **order** (a skipped
  or duplicate sequence number; the consumer needs a persistent offset and LSN idempotency,
  [ADR-016](docs/adr/ADR-016-stateful-stream-consumer-requirements.md)).
- CDC is **not** needed to count a burst of changes, and for a rerun storm the Kubernetes API
  (repeated job names) is the better signal.
- **Which workload made the change** is not on the wire. It is an extra, not the core, and only an
  exploration so far ([ADR-017](docs/adr/ADR-017-attribution-by-composition.md), Draft).
- A real gap in the product: a slow consumer makes the replication slot retain WAL while the
  operator still reports `SlotHealthy` (see Tier 1).

**Where this sits among existing tools** (docs read, nothing installed; not exhaustive, see
[`docs/research/competitive-landscape.md`](docs/research/competitive-landscape.md)). The database
monitoring tools we read (CloudNativePG, `postgres_exporter`, the OpenTelemetry PostgreSQL
receiver, Datadog Database Monitoring, Dynatrace) get their data by running SQL on statistics
views at an interval. The Debezium and Strimzi operators handle capture. We did not find a policy
on the content of a change among them.

**Prior art at scale.** Netflix's DBLog — *A Watermark Based Change-Data-Capture Framework*
(Andreakis & Papapanagiotou, 2020, <https://arxiv.org/abs/2010.12597>; Netflix Tech Blog:
<https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b>) — is a
production CDC framework (running at Netflix since 2018, ~30 services, the watermark design
Debezium and Flink CDC later adapted). Section 4 describes **three** use cases on the same shared
CDC infrastructure (the "Delta" platform): heterogeneous data replication, **database activity
logging** (changed rows → stream → Elasticsearch and Hive, with Kibana dashboards of operations per
table to inspect data mutation patterns, for example a drop in inserts after a buggy deploy), and
schema migration.

What it shows, and what it does not: at Netflix, activity logging rode along on CDC that already
existed for replication. That is the evidence behind the "multiplier" reframe we decided **not** to
follow (see above). It does not tell us whether anyone on Kubernetes would reach for it, and our
rate-based experiments found that rates do not need CDC.

**What's open and who adopted it (so we cite it correctly).** DBLog itself is a
Netflix-**internal** framework: the paper and design are public, but the code is
not open source (no Netflix repo). Others reimplemented the *approach* — e.g.
[`pgcapture`](https://github.com/replicase/pgcapture), a community "Netflix DBLog
implementation for PostgreSQL." The watermark design, however, was adopted by
major open-source CDC projects: **Debezium** ships it as *incremental snapshots*
(v1.6+), explicitly "based on the solution articulated in the [DBLog] paper" per
its own design doc
([DDD-3](https://github.com/debezium/debezium-design-documents/blob/main/DDD-3.md);
[Incremental Snapshots blog](https://debezium.io/blog/2021/10/07/incremental-snapshots/)),
and **Apache Flink CDC** adopted the same mechanism. So the lineage is
DBLog → Debezium, **not** the reverse: the tool this operator builds on literally
implements Netflix's design. Cite it as **"a production CDC framework (DBLog) that
also applied CDC to activity logging, whose watermark snapshot technique Debezium
later adopted"** — and **never** say "Netflix used Debezium" (they built their own
and had rejected Debezium at the time, 2020).

**Honest caveat:** DBLog's *headline* contribution is the watermark full-state-
capture algorithm (consistent snapshot interleaved with the log, no locks, no
stalling) — a replication concern this operator does **not** need (Debezium
already handles snapshot-vs-stream consistency, via this very technique). Activity
logging is *one application* of DBLog, a few paragraphs in the paper — not its
thesis.

**The "CDC-only-for-metrics isn't worth it" belief.** We first read it as a framing problem. The
experiments point the other way for metrics (counting does not need CDC), so we no longer try to
overturn it. The comparison we proposed, CDC-based vs native database statistics vs
exporters/agents/APM vs query-log scraping, is **partly done** and only on documentation: five
tools read for how they get their data (slide 01 of the community deck and `competitive-landscape.md`). Still
not measured: setup cost, overhead on the source and latency. Status: ⚪ idea; only worth it if the
conversations (R8) show that cost is the objection.

**TODO:** watch the Debezium Showcase session featuring a DBLog author, then fold
any concrete activity-logging details (scale, what they alerted on, what they'd
do differently) back into this positioning + the reply to the maintainer.

Status legend: 🔵 planned · 🟡 partially present · ⚪ idea

---

## Tier 1 — Professional / trustworthy (do first)

The highest-leverage items for "would a team trust this in production."

| Item | Why it matters | Effort | Payoff | Status |
|---|---|---|---|---|
| **Grafana dashboard + curated alert pack** | SREs already run Grafana. Ship a ConfigMap-provisioned dashboard and a vetted set of PrometheusRules so users get value in minutes without building anything. Meets them where they are. (DESIGN.MD "Option B".) | Low–Med | High | 🔵 |
| **Configurable thresholds (per-sensor / per-table)** | Lag warn/crit and no-event timeouts were hardcoded constants. "30s lag" means different things for an audit table vs. payments, so they now live in the CR spec. | Med | High | 🟡 per-sensor spec fields added (`spec.thresholds`); per-table still pending |
| **Validating / defaulting admission webhook** | Reject bad specs at `kubectl apply` time (invalid connector config, missing secret refs, nonsensical thresholds) with a clear error, instead of a later `Failed` status. Separates "a controller" from "a product." Kubebuilder scaffolds it. | Med | High | 🔵 |
| **Snapshot strategy / policy layer (no blocking `Pending`)** | Today the generated connector spec does not set a snapshot mode, so Debezium's default (`initial`, a blocking snapshot) applies and a big table keeps the sensor in `Pending` — a blind window — until it ends. During reconcile the operator would inspect each table (size, write access, GTID/txID availability) and choose how to start the connector and whether to trigger an incremental snapshot: stream-only (skip the blocking snapshot) for small tables; a non-blocking start plus an `execute-snapshot` signal via a signaling table (`signal.data.collection`) when write access exists; read-only incremental snapshots (GTID-set watermarks on MySQL, signal via a Kafka topic) when the connection is read-only; `Degraded` with a clear reason when neither watermark path is available. Incremental snapshots are triggered by signal, not by a `snapshot.mode` value. The sensor then reports from the first log event. Needs a pinned Debezium recent enough (incremental snapshots 1.6+; read-only incremental for MySQL 1.7+; for Postgres the current connector docs list a read-only watermarking option, with the introducing version unverified; read-only *replica* support is recent, fixed in 3.6.2 and announced in 3.7). Split into Phase A (non-blocking start, snapshot events kept out of lag; no DB inspection) and an opt-in Phase B (inspect + backfill ladder). See [ADR-014](docs/adr/ADR-014-snapshot-policy-layer.md), and [the article](docs/articles/to-watermark-or-not.md). | Med | High | 🟡 Phase A done (non-blocking start, offset persistence, status). Phase B: inspector and pure policy built, opt-in and off by default, kind e2e passed 2026-10-31; the plan is recorded but never applied (`applied: false`); backfill deferred |
| **Slot-growth diagnosis (found by experiment)** | A slow consumer makes the replication slot retain WAL (measured: 64 kB → 1953 kB). On an unbounded slot (`max_slot_wal_keep_size = -1`) the operator still reports `SlotHealthy`, and a full disk makes PostgreSQL reject writes for every application. Diagnose the trend of `retainedBytes` and the free space of `pg_wal`, not only a size cap. See [`bench/experiments/backpressure`](bench/experiments/backpressure/RESULTS.md). | Low–Med (estimate) | High | 🔵 not built |
| **Escalate a sensor stuck in `Pending` (verify first)** | Reading `internal/health/checker.go`: a running pod with zero events ever returns `Pending`, by design, and no timeout turns that into a warning. This is a code reading, not a test. Run the experiment first (HANDOFF, pending item 1), then decide. | Low | Med–High | ⚪ verify before any work |
| **Live-DB + real-Debezium E2E in CI** | Prove the full path (Postgres + official Debezium Operator in kind → events → metrics → alert) in CI. The difference between "we think it works" and "it's tested against reality." | Med–High | High | 🟡 synthetic demo exists; kind e2e scaffold present |

**Caveat:** don't add config knobs (thresholds) without the validating webhook to
protect them. The per-sensor `spec.thresholds` knobs now ship ahead of the
webhook and rely on defensive parse-fallback (a malformed/unset value degrades to
the default) in the meantime; the validating webhook remains a tracked follow-up.

---

## Tier 2 — Competitive / differentiating (makes it compelling)

Answers "why this over rolling my own Debezium + Prometheus."

| Item | Why it matters | Effort | Payoff | Status |
|---|---|---|---|---|
| **Mutation-pattern anomaly detection** | Baseline-and-deviation on insert/update/delete rates — "delete rate is 10× its weekly norm." Was listed as the genuinely novel angle. **Under review:** the burst experiment found `pg_stat` counters plus the Kubernetes API saw the same burst, so a rate alone is not CDC-specific. What may still hold is a baseline of one Job's usual behavior (HANDOFF, viability condition), which does not exist today. Start statistical, **not** ML. | Med | Unclear until R8 | ⚪ under review |
| **Content policies and a Kubernetes reaction (research)** | The two things only CDC gave us in the bench: a rule on a **value transition** (`settled` → `pending`) and on a **sequence** (a skipped or duplicate number), then a reaction (alert, approval, stop a Job). Needs `REPLICA IDENTITY FULL` for the first and a persistent, idempotent consumer for the second ([ADR-016](docs/adr/ADR-016-stateful-stream-consumer-requirements.md)). A rule such as "never delete settled payments" can also be enforced inside the database (permissions or a trigger), which beats detecting it afterwards. Policy-gating with an existing engine (Kyverno) is an open exploration (HANDOFF, R12). **Not built, not validated with users.** | High | Unknown (R8) | ⚪ hypothesis |
| **Schema-change (DDL) intelligence** | Already counting DDL events. Go further: capture *what* changed and alert "migration applied to `payments` without a change ticket." A compliance/audit hook (GDPR/SOX/HIPAA — see product.md). | Med | Med–High | 🟡 DDL counted; no detail/alerting. On PostgreSQL a real `ALTER TABLE` produced no CDC event in our test (README, "DDL on PostgreSQL") |
| **Postgres schema-change source (open question)** | For MySQL/Oracle/SQL Server, Debezium can emit an optional public schema-change topic (parse `tableChanges` for column-level detail). For **Postgres** it's unclear whether a public schema-change path exists today — per Debezium maintainers in the community thread, historically not published, and a related feature *may* have been added (unconfirmed). Regardless, `pgoutput` has no statement-level DDL message, so catching *what* changed likely still needs `pg_catalog` polling. **Decision: implement nothing now; revisit when the DDL-intelligence item is picked up — verify against the then-current Postgres connector reference/changelog first.** | — | — | ⚪ note only; verify before any work |
| **OpenTelemetry (OTLP) export** | Teams increasingly standardize on OTel. Emit metrics/traces via OTLP so any backend (Datadog, Grafana Cloud, Honeycomb, …) works through one path, not just Prometheus scrape. Where the ecosystem is heading. **Lowered (2026-11-05):** the experiments say the metrics themselves are not what sets this project apart, so exporting them another way is convenience, not differentiation. | Med | Low–Med (lowered) | ⚪ |
| **Native JMX metrics integration** | Scrape Debezium 3.0's native `NumberOf*EventsSeen` counters as an alternative/complement to deriving counts from the stream — reuses maintainer-supported metrics where a real Debezium/JMX endpoint exists. Today counts are derived from the CDC stream (works for HTTP + Kafka, no JMX, no opt-in). See [ADR-013](docs/adr/ADR-013-derived-event-counts-vs-native-jmx-metrics.md). | Med | Med | ⚪ |
| **Multi-version CRD API + conversion (v1alpha1 → v1)** | Signals maturity; lets the CRD evolve without breaking users. Important once there are real users; premature before that. | Med | Med | ⚪ |

**Caveat:** avoid ML-based anything early, and avoid multi-DB-type breadth you
can't test. Breadth without depth makes operators distrust a tool. Anomaly
detection only lands once the Tier 1 trust foundation is in place.

---

## Tier 3 — Fleet operability (scale & adoption)

What someone running this across many clusters/teams cares about.

| Item | Why it matters | Effort | Payoff | Status |
|---|---|---|---|---|
| **Published Helm repo / OperatorHub (OLM) listing** | Packaging *is* distribution. Being installable from a real chart repo or OperatorHub is how operators discover and adopt. One of the highest-leverage adoption moves. | Med | High (adoption) | 🟡 chart exists; not published |
| **Alertable status conditions + `kubectl wait` support** | Lean into the hybrid `metav1.Condition` status (ADR-011) so GitOps/CI can gate on `Ready`. Fits existing workflows. | Low | Med–High | 🟡 conditions exist |
| **Operator self-observability (SLOs)** | Metrics about the operator itself (reconcile latency, queue depth, error rates) + a Grafana panel. Tools that observe themselves earn trust. | Low–Med | Med | ⚪ |
| **ArgoCD / Flux friendliness** | Clean GitOps behavior: no spurious diffs, server-side apply, health checks. | Low–Med | Med | 🟡 SSA/owner-refs in place |
| **Horizontal scale / sharding** | For very large fleets (many DatabaseSensors). Only matters at real scale — defer until asked. | High | Low (until scale) | ⚪ |

---

## Suggested sequence

If the goal is "something K8s operators actually want to use":

1. **Slot-growth diagnosis** — a real gap we measured; independent of everything else
2. **Configurable thresholds + validating webhook** — table-stakes trust
3. **Live-DB E2E in CI** — credibility
4. **Escalation for a sensor stuck in `Pending`** — only after the experiment (HANDOFF, pending item 1)
5. **User conversations (R8)** — not a build item, but the gate: do people want a rule on the
   content of a change, and do they want an alert, an approval or a stop? This decides what
   gets built next
6. **Content policies and Kubernetes reaction** — only if R8 says yes
7. **Grafana dashboard + alert pack** — adoption convenience, cheap; metrics are not what sets
   this project apart (counting did not need CDC)
8. **Published Helm repo / OperatorHub listing** — distribution
9. **OpenTelemetry export** — lowered: exporting the same metrics through another path is
   convenience, not differentiation

Items 1–3 make it *trustworthy*; item 5 decides the direction; items 6–9 follow from it. Which
item is the differentiator is **not decided**: the experiments narrowed it to content and order,
and only user conversations can say whether anyone wants it. Rate-based anomaly detection is no
longer on the sequence (see Tier 2). Order changed on 2026-11-05 (owner approved): metrics-facing
items moved down, the trust fixes and the conversations moved up.

---

## Deferred / explicitly out of scope for now

Tracked so they aren't lost, but not prioritized:

- `Metric.Push` to sinks is built but not driven (only alert fan-out is wired).
- Thresholds are now per-sensor spec-configurable via `spec.thresholds` (the
  constants remain the defaults); per-table thresholds are still future (see Tier 1).
- Full real-database demo (Postgres + official Debezium Operator) — the synthetic
  demo kit (`demo/`, `DEMO.md`) covers the pipeline today.
- **Gradual adoption ladder (policy-gated stages) — IDEA, to validate.** See
  [ADR-015](docs/adr/ADR-015-gradual-adoption-ladder.md). Nothing built; do not add API
  fields until the first user conversations say a brake is wanted.
- **Attribution (which workload made a change) — EXPLORATION, Draft.** See
  [ADR-017](docs/adr/ADR-017-attribution-by-composition.md). Identity is declared, not discovered;
  the idea is to delegate the "who" (for example to Cilium Hubble) and keep CDC for the "what".
  Nothing built, several claims unverified, and it is an extra to the content policy, not its core.
- **AI-assisted features — UNDER CONSTANT EVALUATION (exploration only, not committed; other priorities first).** Candidate uses:
  statistical-then-ML anomaly detection, natural-language explanation of a diagnosis,
  suggested configuration changes. Constraints decided: **no external/third-party LLM**
  (schemas, table names and data must not leave the cluster); a local or self-hosted model
  would have to be evaluated first and its viability is unknown; rules decide and AI at most
  suggests, with a human approving; start with metrics only, never rows. Until then the
  "start statistical, not ML" note above stands.
- ML-based detection, multi-DB-type breadth beyond what can be tested, and
  horizontal sharding — all premature until real-user demand exists.

See [`DESIGN.MD`](DESIGN.MD) for architecture and the ADRs in
[`docs/adr/`](docs/adr/) for decisions already made.
