<p align="center">
  <img src="assets/logo_horizontal_bco.jpeg" alt="Lactiflluus — DatabaseSensor Operator" width="520">
</p>

<h1 align="center">Lactifluus — DatabaseSensor Operator</h1>

<p align="center">
  A Kubernetes operator built on Debezium (CDC) as a database observability sensor.
</p>

---

> [!WARNING]
> **🚧 Work in progress — not production ready.**
> Lactiflluus is under active, early-stage development (much of the implementation work is still pending). The `DatabaseSensor` CR API, the Helm chart, and the operator's behavior **may change without notice**. Use it to evaluate, experiment, and give feedback — **do not deploy it to production yet**.

---

## What it is

**Lactiflluus** is a Kubernetes operator written in Go that orchestrates database observability from Change Data Capture (CDC). You declare in a YAML "monitor these tables and alert me if X", and the operator automatically orchestrates Debezium + Prometheus + multi-sink alerting.

It is implemented as the `DatabaseSensor` custom resource and acts as an **observability layer** on top of the official Debezium Operator: it derives metrics, creates `ServiceMonitor` / `PrometheusRule`, fans out alerts to multiple backends, and exposes an enriched status on the CR.

### The problem it solves

- **Without an operator:** cron script + `kubectl` + imperative configuration of Debezium, Prometheus, and Alertmanager.
- **With the operator:** a single declarative YAML that deploys, monitors, and alerts.

### Difference from the official Debezium Operator

The official one (Apache 2.0, Java) only manages the Debezium Server lifecycle (deploy, scale, config). It does **not** derive observability metrics, does not create `ServiceMonitor` / `AlertRules`, does not alert on mutation patterns, and does not expose a rich status. Lactiflluus fills that gap.

## Key capabilities

- Create and manage a `DebeziumServer` (via the official Debezium Operator) from a single CR.
- Derive metrics: `insert_rate`, `update_rate`, `delete_rate`, `lag`, `schema_changes`.
- Expose `/metrics` (Prometheus format) **always**, regardless of the configured sink.
- Fan out alerts to multiple backends: Prometheus, Datadog, Webhook/Slack.
- Embedded dashboard with live charts.
- Enriched status visible with `kubectl get`: `Phase`, `Lag`, `EventsPerMin`, `LastError`.
- Multi-DB support: PostgreSQL, MySQL/MariaDB, MongoDB, SQL Server, Oracle, Db2, Cassandra.

## Architecture

<p align="center">
  <img src="assets/architecture.jpeg" alt="Lactiflluus architecture: the DatabaseSensor CR is reconciled by the operator, which creates a DebeziumServer (managed by the official Debezium Operator), consumes the CDC events, derives metrics, and fans out to Prometheus, Datadog, and Webhook/Dashboard" width="900">
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
│   5. HealthCheck()          → pod status + lag               │
│   6. UpdateStatus()         → Phase, Lag, EventsPerMin...    │
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
    type: postgresql        # postgresql | mysql | mongodb | ...
    host: postgres-svc.prod
    port: 5432
    user: debezium
    passwordSecret: db-creds
  tables: ["orders", "users", "payments"]

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
```

## Derived metrics

| Metric | Calculation | Alert |
|---|---|---|
| `debezium_events_total{op="c"}` (insert) | Count of `op=c` per window | `< N/min` → did traffic drop? |
| `debezium_events_total{op="u"}` (update) | Count of `op=u` | `> N/min` → bulk update? |
| `debezium_events_total{op="d"}` (delete) | Count of `op=d` | `> N/min` → mass delete? |
| `debezium_schema_changes_total` | DDL event detected | Any DDL → unannounced migration? |
| `debezium_lag_seconds` | `now() - ts_ms` of the event | `> 30s` → Debezium is falling behind |
| `debezium_events_per_minute` | Event rate | — |

## Status (`kubectl get`)

The CR exposes an enriched status via the status subresource:

| Field | Description |
|---|---|
| `phase` | `Pending` → `Running` → `Degraded` → `Failed` |
| `lag` | Human-readable duration (e.g. `42s`) |
| `eventsPerMin` | Float with the event rate |
| `lastEvent` | Relative time of the last event |
| `lastError` | Error message (empty if none) |

## Supported databases

| DB | Mechanism |
|---|---|
| PostgreSQL | Logical decoding (pgoutput, wal2json) |
| MySQL / MariaDB | Binary log (binlog) |
| MongoDB | Change Streams / Oplog |
| SQL Server | CDC Tables / LSN |
| Oracle | LogMiner / XStream |
| Db2 | ASN Capture |
| Cassandra | CommitLog (cdc_raw) |

## Tech stack

- **Go 1.22+**, **Kubebuilder**, **controller-runtime**, **client-go**
- `github.com/prometheus/client_golang` — Prometheus metrics
- `github.com/tidwall/gjson` — CDC event parsing
- `github.com/segmentio/kafka-go` — Kafka consumer (Kafka sink)
- `github.com/DataDog/datadog-api-client-go` — Datadog API
- Multi-stage Docker (`golang:1.22` → `distroless/static`) + Helm chart

## Quick start

### Prerequisites

These operators are installed **separately** — the Lactiflluus chart does not include them:

- A Kubernetes cluster.
- **Official Debezium Operator** (manages the Debezium Server lifecycle).
- **Prometheus Operator** (for the `ServiceMonitor` and `PrometheusRule` CRDs).
- Kafka / Redpanda **optional** (only if you use the Kafka transport; with HTTP, the default, no broker is needed).

### Installation

```bash
# 1. Install the official Debezium Operator
helm repo add debezium https://charts.debezium.io
helm install debezium-operator debezium/debezium-operator -n debezium

# 2. Install Lactiflluus (creates the namespace on first install)
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
│   ├── metrics/            # /metrics, ServiceMonitor, AlertRules
│   ├── sink/               # Fan-out: Prometheus, Datadog, Webhook
│   ├── health/             # Health check + LagTracker
│   └── events/             # Consumer (Kafka/HTTP) + CDC parser
├── cmd/main.go             # Entry point: manager + reconciler + HTTP server
├── config/                 # CRD, RBAC, manager, samples
├── charts/                 # Helm chart
├── Dockerfile
├── Makefile
└── DESIGN.MD               # Detailed design document
```

## License

- Debezium and the official Debezium Operator: Apache 2.0.
- Lactiflluus is built on Go, Kubebuilder, and controller-runtime; it does not inherit copyleft from Debezium.

---

<p align="center">
  For the full design (ADRs, sequence diagrams, error handling, and development plan), see <a href="DESIGN.MD">DESIGN.MD</a>.
</p>

<p align="center">
  🌎 <a href="README_spanish.md">Leer este README en español</a>
</p>
