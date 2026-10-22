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

Status legend: 🔵 planned · 🟡 partially present · ⚪ idea

---

## Tier 1 — Professional / trustworthy (do first)

The highest-leverage items for "would a team trust this in production."

| Item | Why it matters | Effort | Payoff | Status |
|---|---|---|---|---|
| **Grafana dashboard + curated alert pack** | SREs already run Grafana. Ship a ConfigMap-provisioned dashboard and a vetted set of PrometheusRules so users get value in minutes without building anything. Meets them where they are. (DESIGN.MD "Option B".) | Low–Med | High | 🔵 |
| **Configurable thresholds (per-sensor / per-table)** | Lag warn/crit and no-event timeouts are hardcoded constants today. "30s lag" means different things for an audit table vs. payments. Must live in the CR spec. | Med | High | 🟡 constants exist; need spec fields |
| **Validating / defaulting admission webhook** | Reject bad specs at `kubectl apply` time (invalid connector config, missing secret refs, nonsensical thresholds) with a clear error, instead of a later `Failed` status. Separates "a controller" from "a product." Kubebuilder scaffolds it. | Med | High | 🔵 |
| **Live-DB + real-Debezium E2E in CI** | Prove the full path (Postgres + official Debezium Operator in kind → events → metrics → alert) in CI. The difference between "we think it works" and "it's tested against reality." | Med–High | High | 🟡 synthetic demo exists; kind e2e scaffold present |

**Caveat:** don't add config knobs (thresholds) without the validating webhook to
protect them.

---

## Tier 2 — Competitive / differentiating (makes it compelling)

Answers "why this over rolling my own Debezium + Prometheus."

| Item | Why it matters | Effort | Payoff | Status |
|---|---|---|---|---|
| **Mutation-pattern anomaly detection** | The genuinely novel angle. Baseline-and-deviation on insert/update/delete rates — "delete rate is 10× its weekly norm" — serves the data-engineer persona directly. Start statistical (rolling mean/stddev), **not** ML. | Med | High (differentiation) | ⚪ |
| **Schema-change (DDL) intelligence** | Already counting DDL events. Go further: capture *what* changed and alert "migration applied to `payments` without a change ticket." A compliance/audit hook (GDPR/SOX/HIPAA — see product.md). | Med | Med–High | 🟡 DDL counted; no detail/alerting |
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
- Thresholds are documented constants, not yet spec-configurable (see Tier 1).
- Full real-database demo (Postgres + official Debezium Operator) — the synthetic
  demo kit (`demo/`, `DEMO.md`) covers the pipeline today.
- ML-based detection, multi-DB-type breadth beyond what can be tested, and
  horizontal sharding — all premature until real-user demand exists.

See [`DESIGN.MD`](DESIGN.MD) for architecture and the ADRs in
[`docs/adr/`](docs/adr/) for decisions already made.
