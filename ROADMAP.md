# Roadmap

Where Lactiflluus (the DatabaseSensor operator) could go next. This is a living,
prioritized document — not a commitment. It is grounded in what the operator is
**today** (declarative CDC observability: derived metrics, multi-sink alerting,
built-in dashboard, Helm packaging, hybrid status) versus what teams running
Kubernetes in production actually reach for.

The guiding lens: the gap between "a good MVP" and "a tool an SRE reaches for" is
mostly **trust, ecosystem fit, and reducing the operator's work** — not more
features for their own sake. Tiers below reflect that: Tier 1 earns trust, Tier 2
makes it compelling, Tier 3 serves fleet-scale operability.

## Positioning

**Don't pitch this as "a reason to adopt CDC." Pitch it as "a multiplier on CDC
you already have."** Feedback from the Debezium community (Mario Fiore Vitale,
who explored CDC-as-observability before) is that *setting up a whole CDC
pipeline **only** to get metrics felt like too much* — the cost/benefit doesn't
close when metrics is the sole payoff. The reframe that fixes this: teams already
running (or adopting) CDC for data movement, replication, or audit get this
observability layer almost for free on top of infrastructure that already exists
for another reason. That turns the operator from "adopt CDC to get metrics" into
"you're running CDC anyway — here's the observability it unlocks."

**Prior art at scale (and direct evidence for the reframe).** Netflix's DBLog —
*A Watermark Based Change-Data-Capture Framework* (Andreakis & Papapanagiotou,
2020, <https://arxiv.org/abs/2010.12597>; Netflix Tech Blog:
<https://netflixtechblog.com/dblog-a-generic-change-data-capture-framework-69351fb9099b>)
— is a production CDC framework (running at Netflix since 2018, ~30 services,
the watermark design Debezium and Flink CDC later adapted). What matters here is
section 4: DBLog serves **three** use cases on the *same* shared CDC
infrastructure (the "Delta" platform):

1. **Heterogeneous data replication** — changed rows + full state joined into a
   common Elasticsearch search index.
2. **Database activity logging** — changed rows → stream → Elasticsearch
   (short-term) + Hive (long-term), with **Kibana dashboards of operations per
   table** to inspect **data mutation patterns**, explicitly to catch things like
   *a drop in inserts to a table after a buggy service deploy*.
3. **Schema migration** — old→new schema via a Flink job.

This is the direct evidence for the "multiplier, not a reason to adopt" reframe:
at Netflix, activity logging **rode along** on CDC infra that already existed for
replication and migration — nobody stood up CDC *only* for metrics. Use case #2
is, almost line for line, this operator's thesis and Mario's blog example
("operations per table," "mutation patterns," "drop of inserts after a buggy
deploy") — two independent, production-scale sources converged on it.

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

**The "CDC-only-for-metrics isn't worth it" belief may be a misconception worth
testing.** DBLog shows a production system doing "CDC that *also* does
logs/telemetry" — the observability came nearly free on top of CDC run for
replication. That suggests the community pushback Mario heard is partly a framing
problem, not a hard cost truth. Worth backing with evidence: a **benchmark /
comparison of ways to get logs & metrics out of databases** — CDC-based
(Debezium, Flink CDC, this operator) vs. native DB stats (`pg_stat_*`,
`performance_schema`), JMX/exporters, agent/APM, and query-log scraping — on
setup cost, overhead on the source, signal richness (per-table mutation rates,
lag, DDL), and latency. The goal: show *where* CDC-derived observability is
genuinely cheaper/richer (and honestly, where it is not), so the "too much" claim
is answered with data instead of assertion. Status: ⚪ idea; scope before
investing.

**TODO:** watch the Debezium Showcase session featuring a DBLog author, then fold
any concrete activity-logging details (scale, what they alerted on, what they'd
do differently) back into this positioning + the reply to Mario.

Status legend: 🔵 planned · 🟡 partially present · ⚪ idea

---

## Tier 1 — Professional / trustworthy (do first)

The highest-leverage items for "would a team trust this in production."

| Item | Why it matters | Effort | Payoff | Status |
|---|---|---|---|---|
| **Grafana dashboard + curated alert pack** | SREs already run Grafana. Ship a ConfigMap-provisioned dashboard and a vetted set of PrometheusRules so users get value in minutes without building anything. Meets them where they are. (DESIGN.MD "Option B".) | Low–Med | High | 🔵 |
| **Configurable thresholds (per-sensor / per-table)** | Lag warn/crit and no-event timeouts were hardcoded constants. "30s lag" means different things for an audit table vs. payments, so they now live in the CR spec. | Med | High | 🟡 per-sensor spec fields added (`spec.thresholds`); per-table still pending |
| **Validating / defaulting admission webhook** | Reject bad specs at `kubectl apply` time (invalid connector config, missing secret refs, nonsensical thresholds) with a clear error, instead of a later `Failed` status. Separates "a controller" from "a product." Kubebuilder scaffolds it. | Med | High | 🔵 |
| **Snapshot strategy / policy layer (no blocking `Pending`)** | Today the generated connector spec does not set a snapshot mode, so Debezium's default (`initial`, a blocking snapshot) applies and a big table keeps the sensor in `Pending` — a blind window — until it ends. During reconcile the operator would inspect each table (size, write access, GTID/txID availability) and choose how to start the connector and whether to trigger an incremental snapshot: stream-only (skip the blocking snapshot) for small tables; a non-blocking start plus an `execute-snapshot` signal via a signaling table (`signal.data.collection`) when write access exists; read-only incremental snapshots (GTID-set watermarks on MySQL, signal via a Kafka topic) when the connection is read-only; `Degraded` with a clear reason when neither watermark path is available. Incremental snapshots are triggered by signal, not by a `snapshot.mode` value. The sensor then reports from the first log event. Needs a pinned Debezium recent enough (incremental snapshots 1.6+; read-only incremental for MySQL 1.7+; for Postgres the current connector docs list a read-only watermarking option, with the introducing version unverified; read-only *replica* support is recent, fixed in 3.6.2 and announced in 3.7). Split into Phase A (non-blocking start, snapshot events kept out of lag; no DB inspection) and an opt-in Phase B (inspect + backfill ladder). See [ADR-014](docs/adr/ADR-014-snapshot-policy-layer.md), and [the article](docs/articles/to-watermark-or-not.md). | Med | High | 🟡 Phase A done (non-blocking start, offset persistence, status); Phase B deferred |
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
| **Mutation-pattern anomaly detection** | The genuinely novel angle. Baseline-and-deviation on insert/update/delete rates — "delete rate is 10× its weekly norm" — serves the data-engineer persona directly. Start statistical (rolling mean/stddev), **not** ML. | Med | High (differentiation) | ⚪ |
| **Schema-change (DDL) intelligence** | Already counting DDL events. Go further: capture *what* changed and alert "migration applied to `payments` without a change ticket." A compliance/audit hook (GDPR/SOX/HIPAA — see product.md). | Med | Med–High | 🟡 DDL counted; no detail/alerting |
| **Postgres schema-change source (open question)** | For MySQL/Oracle/SQL Server, Debezium can emit an optional public schema-change topic (parse `tableChanges` for column-level detail). For **Postgres** it's unclear whether a public schema-change path exists today — per Debezium maintainers (Mario Fiore Vitale, Jiri Pechanec) in the community thread, historically not published, and a related feature *may* have been added (unconfirmed). Regardless, `pgoutput` has no statement-level DDL message, so catching *what* changed likely still needs `pg_catalog` polling. **Decision: implement nothing now; revisit when the DDL-intelligence item is picked up — verify against the then-current Postgres connector reference/changelog first.** | — | — | ⚪ note only; verify before any work |
| **OpenTelemetry (OTLP) export** | Teams increasingly standardize on OTel. Emit metrics/traces via OTLP so any backend (Datadog, Grafana Cloud, Honeycomb, …) works through one path, not just Prometheus scrape. Where the ecosystem is heading. | Med | High (reach) | ⚪ |
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

1. **Grafana dashboard + alert pack** — meet them where they are
2. **Configurable thresholds + validating webhook** — table-stakes trust
3. **Live-DB E2E in CI** — credibility
4. **OpenTelemetry export** — ecosystem fit, broad reach
5. **Mutation-pattern anomaly detection** — the actual differentiator
6. **Published Helm repo / OperatorHub listing** — distribution

Items 1–3 make it *trustworthy*; items 4–6 make it *compelling*. The anomaly
detection is the novel angle, but it only pays off after the trust foundation.

---

## Deferred / explicitly out of scope for now

Tracked so they aren't lost, but not prioritized:

- `Metric.Push` to sinks is built but not driven (only alert fan-out is wired).
- Thresholds are now per-sensor spec-configurable via `spec.thresholds` (the
  constants remain the defaults); per-table thresholds are still future (see Tier 1).
- Full real-database demo (Postgres + official Debezium Operator) — the synthetic
  demo kit (`demo/`, `DEMO.md`) covers the pipeline today.
- ML-based detection, multi-DB-type breadth beyond what can be tested, and
  horizontal sharding — all premature until real-user demand exists.

See [`DESIGN.MD`](DESIGN.MD) for architecture and the ADRs in
[`docs/adr/`](docs/adr/) for decisions already made.
