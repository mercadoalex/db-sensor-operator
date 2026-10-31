<p align="center">
  <img src="assets/logo_horizontal_bco.jpeg" alt="Lactifluus — DatabaseSensor Operator" width="520">
</p>

<h1 align="center">Lactifluus — DatabaseSensor Operator</h1>

<p align="center">
  <b>Policy-based CDC intelligence for PostgreSQL on Kubernetes.</b><br>
  A Kubernetes operator built on Debezium (CDC) as a database observability sensor.
</p>

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/overview/lactifluus-overview-dark.svg">
    <img alt="Lactifluus overview: PostgreSQL to Debezium to the Lactifluus operator, with an inspector, three policies, the measured start-up comparison and a verification legend." src="docs/overview/lactifluus-overview-light.svg" width="900">
  </picture>
</p>

<p align="center">
  <a href="docs/overview/stakeholder-brief.md">Technical brief for stakeholders</a>: what was measured, what is built but not yet verified, and what is still a hypothesis.
</p>

---

> [!WARNING]
> **🚧 Work in progress — not production ready.**
> Lactifluus is under active, early-stage development (much of the implementation work is still pending). The `DatabaseSensor` CR API, the Helm chart, and the operator's behavior **may change without notice**. Use it to evaluate, experiment, and give feedback — **do not deploy it to production yet**.

---

## What it is

**Lactifluus** is a Kubernetes operator written in Go that orchestrates database observability from Change Data Capture (CDC). You declare in a YAML "monitor these tables and alert me if X", and the operator automatically orchestrates Debezium + Prometheus + multi-sink alerting.

It is implemented as the `DatabaseSensor` custom resource and acts as an **observability layer** on top of the official Debezium Operator: it derives metrics, creates `ServiceMonitor` / `PrometheusRule`, fans out alerts to multiple backends, and exposes an enriched status on the CR.

### The problem it solves

- **Without an operator:** cron script + `kubectl` + imperative configuration of Debezium, Prometheus, and Alertmanager.
- **With the operator:** a single declarative YAML that deploys, monitors, and alerts.

### Difference from the official Debezium Operator

The official one (Apache 2.0, Java) only manages the Debezium Server lifecycle (deploy, scale, config). It does **not** derive observability metrics, does not create `ServiceMonitor` / `AlertRules`, does not alert on mutation patterns, and does not expose a rich status. Lactifluus fills that gap.

### Prior art & acknowledgements

The CDC-as-observability idea is not ours. The Debezium maintainers' blog post [Detect data mutation patterns with Debezium](https://debezium.io/blog/2024/10/14/Detect-data-mutation-patterns-with-Debezium/) (Mario Fiore Vitale) lays it out clearly: derive business-level signals straight from the change stream instead of instrumenting each service. Lactifluus is the operator-level automation of that idea — one declarative `DatabaseSensor` CR instead of a hand-wired JMX exporter plus Grafana setup. On top of it, the operator adds the signals Debezium doesn't expose on its own: derived lag, DDL intelligence (only for connectors that emit schema changes; not PostgreSQL, see [Derived metrics](#derived-metrics)), and multi-sink alert fan-out. See [ADR-013](docs/adr/ADR-013-derived-event-counts-vs-native-jmx-metrics.md) for how derived counts relate to Debezium 3.0's native counters.

## Key capabilities

- Create and manage a `DebeziumServer` (via the official Debezium Operator) from a single CR.
- Derive metrics: `insert_rate`, `update_rate`, `delete_rate`, `lag`, and `schema_changes` (not available on PostgreSQL, see [Derived metrics](#derived-metrics)).
- Expose `/metrics` (Prometheus format) **always**, regardless of the configured sink.
- Fan out alerts to multiple backends: Prometheus, Datadog, Webhook/Slack.
- Embedded dashboard with live charts.
- Opt-in **database inspection**: a read-only Job checks `wal_level`, the tables and the replication slot, and reports the result in `status.snapshot`. It reports and recommends; it does not block. Verified in kind on PostgreSQL 16. See [Database inspection](#database-inspection).
- Explicit, deterministic **policies** (start-up, per-table backfill plan, WAL retention). Each decision is written to `status` with its reason. They are rules, not machine learning. The backfill plan is computed but never applied.
- Enriched status visible with `kubectl get`: `Phase`, `Lag`, `EventsPerMin`, `LastError`.
- Databases: PostgreSQL (verified against a real cluster) and MySQL (connector mapped, not yet verified). See [Supported databases](#supported-databases).

## Architecture

<p align="center">
  <img src="assets/architecture.jpeg" alt="Lactifluus architecture: the DatabaseSensor CR is reconciled by the operator, which creates a DebeziumServer (managed by the official Debezium Operator), consumes the CDC events, derives metrics, and fans out to Prometheus, Datadog, and Webhook/Dashboard" width="900">
</p>

The diagram above shows the full flow: you apply a `DatabaseSensor` CR, the reconciler (every 30s) creates a `DebeziumServer` that the official Debezium operator deploys, the CDC events are consumed (Kafka or HTTP), metrics are derived (`insert_rate`, `lag`, `schema_changes`), `/metrics` is always exposed in Prometheus format, and depending on `spec.sink` alerts are fanned out to multiple backends. All child resources use owner references, so deleting the `DatabaseSensor` cleans everything up in cascade.

The reconciliation loop in detail:

```
┌─────────────────────────────────────────────────────────────┐
│ User applies DatabaseSensor CR                               │
└──────────────────────────┬──────────────────────────────────┘
                           │ Reconcile (every 30s)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ DatabaseSensor Reconciler (controller-runtime)               │
│   1. EnsureDebeziumServer() → creates the DebeziumServer CR  │
│   2. EnsureServiceMonitor() → Prometheus scrapes /metrics    │
│   3. EnsureAlertRules()     → PrometheusRule (alerts)        │
│   4. EnsureService()        → exposes /metrics + /dashboard  │
│   5. Inspection (opt-in)    → read-only Job → facts, plan    │
│   6. HealthCheck()          → pod status + lag               │
│   7. UpdateStatus()         → Phase, Lag, EventsPerMin...    │
└───────────┬───────────────────┬───────────────┬─────────────┘
            ▼                   ▼               ▼
   ┌─────────────────┐  ┌──────────────┐  ┌──────────────┐
   │ Debezium Op.    │  │ Prometheus   │  │ Datadog /    │
   │ (official, Java)│  │ Operator     │  │ Webhook      │
   └─────────────────┘  └──────────────┘  └──────────────┘
```

The operator does **not** redeploy Debezium from scratch: it creates a `DebeziumServer` CR as a child resource (with an `ownerReference`), and the official Debezium Operator handles the lifecycle. All child resources use owner references, so deleting the `DatabaseSensor` cleans everything up in cascade.

## Usage example

```yaml
apiVersion: sensor.io/v1
kind: DatabaseSensor
metadata:
  name: pg-orders
  namespace: prod
spec:
  database:
    type: postgresql        # postgresql | mysql
    host: postgres-svc.prod
    port: 5432
    user: debezium
    passwordSecret: db-creds   # Secret in the same namespace, password under the key "password"
    name: orders_db         # database to capture; required for PostgreSQL
  tables: ["orders", "users", "payments"]

  transport:
    type: http              # http (default) | kafka
    http:
      url: http://db-sensor-operator.db-sensor.svc:8080/cdc   # where Debezium POSTs events

  snapshot:
    offsetStore: persistent # memory (default) | persistent; see "Start and restarts"
    inspection:             # opt-in, PostgreSQL only; see "Database inspection"
      enabled: true
      intervalSeconds: 600  # 0 = once per spec change, otherwise at least 60
      timeoutSeconds: 30

  sink:
    type: all               # prometheus | datadog | webhook | all
    prometheus:
      namespace: monitoring
      serviceMonitor: true
      alertRules:
        - name: HighLag
          expr: 'debezium_lag_seconds > 30'
          for: 5m
          severity: warning
        - name: NoEvents
          expr: 'rate(debezium_events_total[5m]) == 0'
          for: 15m
          severity: critical
    datadog:
      enabled: true
      apiKeySecret: dd-api-key
      appKeySecret: dd-app-key
      tags: ["env:prod", "db:postgres"]
    webhook:
      enabled: true
      url: https://hooks.slack.com/services/XXX
      triggerOn: [HighLag, NoEvents]

  dashboard:
    enabled: true
    retention: 72h

  thresholds:               # per-sensor; omit to keep the defaults below
    lagWarn: 30s            # lag >= this -> Degraded
    lagCrit: 2m             # lag >  this -> Failed
    noEventWarn: 5m         # no events for this long -> Degraded
    noEventFail: 30m        # no events for this long -> Failed
```

## Configurable thresholds

`spec.thresholds` tunes the health thresholds per DatabaseSensor. Each field is
a Go duration string and falls back to the operator default when unset, so an
omitted block keeps today's behaviour:

| Field | Default | Effect |
|---|---|---|
| `lagWarn` | `30s` | replication lag at or above it → `Degraded` |
| `lagCrit` | `2m` | replication lag above it → `Failed` |
| `noEventWarn` | `5m` | no CDC event for this long → `Degraded` |
| `noEventFail` | `30m` | no CDC event for this long → `Failed` |

Thresholds are **per-sensor** only (not per-table). A malformed value falls back
to the default rather than breaking the sensor; there is no admission webhook
validating them yet.

## Derived metrics

| Metric | Calculation | Alert |
|---|---|---|
| `debezium_events_total{op="c"}` (insert) | Count of `op=c` per window | `< N/min` → did traffic drop? |
| `debezium_events_total{op="u"}` (update) | Count of `op=u` | `> N/min` → bulk update? |
| `debezium_events_total{op="d"}` (delete) | Count of `op=d` | `> N/min` → mass delete? |
| `debezium_schema_changes_total` | DDL event detected (**not on PostgreSQL**, see below) | Any DDL → unannounced migration? |
| `debezium_lag_seconds` | `now() - ts_ms` of the event | `> 30s` → Debezium is falling behind |
| `debezium_events_per_minute` | Event rate | — |

Snapshot reads (`op=r`) are never counted: they report rows that already existed, not changes.

**DDL on PostgreSQL.** PostgreSQL's logical decoding (`pgoutput`) carries no DDL, so Debezium's PostgreSQL connector emits no schema-change event. An `ALTER TABLE` on a monitored table was tested against a real cluster and produced no `debezium_schema_changes_total` series. Connectors that keep a schema history (such as MySQL) are expected to emit schema changes, but only PostgreSQL has been tested.

## Start and restarts

For PostgreSQL the operator starts Debezium with `snapshot.mode=no_data`: it does not read existing rows, so the sensor reports from the first live change instead of waiting for a snapshot. On a 100,000-row table the default `initial` snapshot kept a sensor blind for 57 s while `no_data` saw the first change in 6 s (Debezium 3.6.3, one run; see `bench/experiments/blind-window/RESULTS.md`). The cost is that existing rows are not delivered, which does not affect rates, lag or counts. Other engines keep Debezium's default until their non-blocking mode is verified.

By default the Debezium Operator keeps its position in memory, so a restart of the Debezium pod can re-deliver recent events and inflate counts. `spec.snapshot.offsetStore: persistent` stores the offset on a PersistentVolumeClaim, and a graceful restart then resumed without re-delivery in testing. A hard kill can still replay up to one flush interval; that was not tested. `status.snapshot` shows the strategy, the offset store and a one-line reason. Design and evidence: [ADR-014](docs/adr/ADR-014-snapshot-policy-layer.md).

## Database inspection

Opt-in: set `spec.snapshot.inspection.enabled: true` (off by default). PostgreSQL only, and it needs `spec.database.name`. The operator runs a short read-only Job (the `/inspector` binary from the same image), which reads PostgreSQL's catalogs, and copies the result into `status.snapshot`. The Job has no Kubernetes API token: it hands its result back through the container's termination message, the operator is the only writer of `status`, and the Job is deleted only after the result is stored.

What it reports:

| Condition | Meaning |
|---|---|
| `InspectionAvailable` | The inspection ran, or why it could not (`AuthenticationFailed`, `ConnectionFailed`, `InspectorImageNotConfigured`, ...). A failed run keeps the error and drops any older facts. |
| `LogicalDecodingReady` | `wal_level` is `logical` |
| `TablesFound` | Every listed table exists |
| `SlotRetentionHealthy` | The replication slot is healthy, at risk (`unreserved`) or lost; `SlotMissing` until Debezium creates it |

`status.snapshot.inspection` holds the facts (estimated rows, whether the user can write, the slot's WAL status and retained bytes) and `status.snapshot.plan` holds a backfill choice per table with its reason. The plan always reads `applied: false`: no backfill exists yet (see [ADR-014](docs/adr/ADR-014-snapshot-policy-layer.md)).

Things to know:

- **It reports; it does not block.** Debezium is created whether or not the inspection finds a problem.
- **Slot facts want a periodic interval.** With `intervalSeconds: 0` the first run can happen before Debezium creates its slot, and `status` keeps showing `SlotMissing`. The default (600 s) corrects it.
- **Install with the Helm chart.** It sets `OPERATOR_IMAGE`, which the Job needs. A plain kustomize deploy has none, and the condition says `InspectorImageNotConfigured`.
- **An error message can include the server address and the database user.** It never includes the password.

Verified in kind (PostgreSQL 16, one local cluster, 12 checks, including a wrong password and a namespace enforcing Pod Security `restricted`): see [`bench/experiments/inspection-e2e/RESULTS.md`](bench/experiments/inspection-e2e/RESULTS.md). Not verified: a NetworkPolicy that blocks the Job, PostgreSQL 12 and 17, MySQL. In that Pod Security test the Debezium Server pods created by the Debezium Operator were rejected; the inspector Job was not.

## Status (`kubectl get`)

The CR exposes an enriched status via the status subresource:

| Field | Description |
|---|---|
| `phase` | `Pending` → `Running` → `Degraded` → `Failed` |
| `lag` | Human-readable duration (e.g. `42s`) |
| `eventsPerMin` | Float with the event rate |
| `lastEvent` | Relative time of the last event |
| `lastError` | Error message (empty if none) |
| `conditions` | `DebeziumAvailable`, `ReceivingEvents`, `Ready`, and the inspection conditions when it is enabled |
| `snapshot` | How the connector starts, the offset store and, with inspection on, the facts and the plan |

## Supported databases

| DB | Mechanism | Status |
|---|---|---|
| PostgreSQL | Logical decoding (`pgoutput`) | Verified against a real cluster (Debezium 3.6.3, PostgreSQL 16) |
| MySQL | Binary log (binlog) | Connector and `database.server.id` mapped; **not yet verified** against a real MySQL |

For now the operator supports only these two. The API rejects any other `spec.database.type`. Debezium itself supports more engines (MongoDB, SQL Server, Oracle, Db2, Cassandra); they are not wired here and nothing about them has been tested. The non-blocking start, offset persistence and database inspection described above are implemented for PostgreSQL only.

## Tech stack

- **Go 1.22+**, **Kubebuilder**, **controller-runtime**, **client-go**
- `github.com/prometheus/client_golang` — Prometheus metrics
- `github.com/tidwall/gjson` — CDC event parsing
- `github.com/segmentio/kafka-go` — Kafka consumer (Kafka sink)
- `github.com/DataDog/datadog-api-client-go` — Datadog API
- Multi-stage Docker (`golang:1.22` → `distroless/static`) + Helm chart

## Quick start

### Prerequisites

These operators are installed **separately** — the Lactifluus chart does not include them:

- A Kubernetes cluster.
- **Official Debezium Operator** (manages the Debezium Server lifecycle).
- **Prometheus Operator** (for the `ServiceMonitor` and `PrometheusRule` CRDs).
- Kafka / Redpanda **optional** (only if you use the Kafka transport; with HTTP, the default, no broker is needed).

### Installation

```bash
# 1. Install the official Debezium Operator
helm repo add debezium https://charts.debezium.io
helm install debezium-operator debezium/debezium-operator -n debezium

# 2. Install Lactifluus (creates the namespace on first install)
helm install db-sensor ./charts/db-sensor-operator -n db-sensor --create-namespace

# 3. Apply a sample CR
kubectl apply -n db-sensor -f config/samples/v1_databasesensor_basic.yaml

# 4. Check the sensor status (shortname: dbsensor)
kubectl get databasesensor -n db-sensor
# NAME                   PHASE     READY   LAG   EVENTS/MIN   DB TYPE
# databasesensor-basic   Pending   False               0     postgresql
```

### Samples

Three ready-to-`kubectl apply` examples live in [`config/samples/`](config/samples/):

| Sample | What it shows |
|---|---|
| [`v1_databasesensor_basic.yaml`](config/samples/v1_databasesensor_basic.yaml) | minimal, prometheus-only sink |
| [`v1_databasesensor.yaml`](config/samples/v1_databasesensor.yaml) | full: all sinks + dashboard + transport |
| [`v1_databasesensor_mysql.yaml`](config/samples/v1_databasesensor_mysql.yaml) | MySQL engine, webhook sink, HTTP transport |

### Demo

There is a reproducible local demo (kind + synthetic CDC events, no real DB/Debezium/Kafka) that
spins up the whole observability pipeline. The walkthrough is documented with real output and screenshots in
[DEMO.md](DEMO.md); the scripts are in [`demo/`](demo/).

```bash
./demo/setup.sh                                   # kind + CRDs + image + helm install
kubectl apply -f demo/sample-databasesensor.yaml
kubectl -n db-sensor port-forward svc/db-sensor-db-sensor-operator-operator-http 8080:8080
./demo/generate-events.sh --burst 60              # synthetic events -> /cdc
open http://localhost:8080/dashboard
```

### Endpoints and ports

The operator's manager and HTTP server expose different ports:

| Port | What it serves |
|---|---|
| `:8080` | operator HTTP server: `/metrics` (always), `/dashboard` + `/api/events` (gated by `dashboard.enabled`, 404 when off), `/cdc`, `/healthz`, `/readyz` |
| `:8081` | manager health probes: `/healthz`, `/readyz` |
| `:8443` | manager metrics protected by kube-RBAC |

### Event transport

CDC event ingestion uses **HTTP by default** (no broker, ideal for test/e2e). To use Kafka,
just change `spec.transport.type: kafka` and fill in the CR's `kafka` block — no refactor, since
downstream only sees a `CDCEvent`. It is orthogonal to `spec.sink` (the metrics/alerts output). See
[ADR-012](docs/adr/ADR-012-event-transport-http-default-kafka-optional.md).

### NetworkPolicy & CNI

The chart can **optionally** deploy a `NetworkPolicy` that restricts ingress to the operator's metrics
port. It is **OFF by default**; enable it with `--set networkPolicy.enabled=true`.

It requires a **CNI that enforces** NetworkPolicy (Calico, Cilium, Antrea, Azure CNI). On a CNI
that does not (plain Flannel, kind's default `kindnet`) the policy is installed but is a **silent no-op**
— it blocks nothing. To verify your CNI enforces policies, run a quick functional *default-deny* test
(create a policy that denies everything and confirm traffic is actually cut off). For a full audit against
the NetworkPolicy spec, use [Sonobuoy](https://sonobuoy.io/)
(`sonobuoy run --mode e2e --e2e-focus "\[NetworkPolicy\]"`) or
[Cyclonus](https://github.com/mattfenwick/cyclonus). These audit tools are **references**;
this repo does not run them.

### More documentation

- [DESIGN.MD](DESIGN.MD) — full design (ADRs, diagrams, error handling).
- [ROADMAP.md](ROADMAP.md) — proposed improvements, by tiers (trust / competitive / fleet).
- [docs/TESTING.md](docs/TESTING.md) — practical testing guide.
- [ADR-011](docs/adr/ADR-011-hybrid-status-design.md) — hybrid status (conditions + measurements + phase).
- [ADR-012](docs/adr/ADR-012-event-transport-http-default-kafka-optional.md) — HTTP default / Kafka optional transport.
- [ADR-013](docs/adr/ADR-013-derived-event-counts-vs-native-jmx-metrics.md) — derive event counts from the stream vs. native JMX metrics.
- [ADR-014](docs/adr/ADR-014-snapshot-policy-layer.md) — start the connector without a blocking snapshot, persist the offset, and inspect the database.
- [ADR-015](docs/adr/ADR-015-gradual-adoption-ladder.md) — **draft idea**, not a decision: a ladder of adoption stages opened by policy. Nothing of it is built.
- [docs/overview/stakeholder-brief.md](docs/overview/stakeholder-brief.md) — technical brief for stakeholders ([español](docs/overview/stakeholder-brief_spanish.md)).
- [docs/HANDOFF.md](docs/HANDOFF.md) — current state and pending work.
- [bench/](bench/README.md) — reproducible kind cluster, workload and experiments, with their `RESULTS.md`.
- [docs/articles/to-watermark-or-not.md](docs/articles/to-watermark-or-not.md) — a reflection on CDC snapshot models and why the operator is moving to watermark-based incremental snapshots.

## Development

```bash
make build        # compile
make test         # unit + integration (envtest)
make manifests    # regenerate CRD YAML
make generate     # regenerate deepcopy
make docker-build # build image
make deploy       # deploy to the cluster
make dev          # run locally with envtest
```

### Testing strategy

- **Unit (testify):** pure logic — `LagTracker`, CDC event parser, `Sink` implementations.
- **Integration (envtest):** reconciler against a real API server — state convergence, owner references, status transitions.
- **E2E (kind):** full flow — real RBAC, image pull, leader election.

The principle: test **convergence**, not actions ("the final state is `Running`", not "`Create()` was called"), and verify idempotency with the *second reconcile test*.

You don't need a cluster, a DB, Kafka, or Datadog for most tests: envtest runs an in-memory API server and the event pipeline is tested with synthetic CDC events and `httptest`. Practical guide: [docs/TESTING.md](docs/TESTING.md).

## Project structure

```
db-sensor-operator/
├── api/v1/                 # Spec/Status structs + kubebuilder markers
├── internal/
│   ├── controller/         # Main reconciler
│   ├── deployer/           # Creates/updates the DebeziumServer CR
│   ├── inspection/         # Builds the inspector Job, reads and evaluates its result
│   ├── snapshot/           # Inspector queries, slot diagnosis, per-table plan
│   ├── metrics/            # /metrics, ServiceMonitor, AlertRules
│   ├── sink/               # Fan-out: Prometheus, Datadog, Webhook
│   ├── health/             # Health check + LagTracker
│   └── events/             # Consumer (Kafka/HTTP) + CDC parser
├── cmd/
│   ├── main.go             # Entry point: manager + reconciler + HTTP server
│   └── inspector/          # Read-only inspector binary (runs as a Job)
├── bench/                  # kind cluster, workload and experiments (reproducible)
├── docs/                   # ADRs, brief, articles, design tokens
├── config/                 # CRD, RBAC, manager, samples
├── charts/                 # Helm chart
├── Dockerfile
├── Makefile
└── DESIGN.MD               # Detailed design document
```

## License

Lactifluus is licensed under the **Apache License 2.0** — see [LICENSE](LICENSE). You may use, modify, and distribute it freely; contributions are accepted under the same license.

- Debezium and the official Debezium Operator: Apache 2.0.
- Lactifluus is built on Go, Kubebuilder, and controller-runtime; it does not inherit copyleft from Debezium.

---

<p align="center">
  For the full design (ADRs, sequence diagrams, error handling, and development plan), see <a href="DESIGN.MD">DESIGN.MD</a>.
</p>

<p align="center">
  🌎 <a href="README_spanish.md">Leer este README en español</a>
</p>
