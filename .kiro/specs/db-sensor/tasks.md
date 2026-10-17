# Implementation Plan: Lactiflluus (DatabaseSensor Operator)

## Overview

This plan implements **Lactiflluus**, a Kubernetes Debezium-based operator for database metric observability, multi-sink alerting, and DDL detection, delivered as the `DatabaseSensor` operator. This plan implements the DatabaseSensor Operator in Go (Kubebuilder + controller-runtime) as an incremental, test-driven build. Each task produces working, integrated code that builds on prior tasks, so there is no orphaned code. Pure logic (parser, lag tracker, phase decision, sinks) is implemented and unit/property-tested first, then wired into the reconciler and HTTP server, then validated end-to-end with envtest and kind.

Testing follows the design's pyramid: unit tests with `testify`, property-based tests with `gopter` for correctness properties P4/P5/P7, integration tests with `envtest`, and e2e with `kind`. Property tests reference the correctness properties in `design.md`; test sub-tasks are marked optional with `*` and are skippable for a faster MVP.

Implementation language is **Go** (per `design.md` and the `tech` steering file).

## Tasks

- [ ] 1. Scaffold the operator project with Kubebuilder
  - Run `kubebuilder init --domain sensor.io --repo github.com/tuuser/db-sensor-operator`
  - Run `kubebuilder create api --group sensor --version v1 --kind DatabaseSensor --resource --controller`
  - Confirm the generated layout matches the structure steering (`api/v1`, `internal/controller`, `cmd/main.go`, `config/`)
  - Verify `make build` and `make test` pass on the empty scaffold
  - _Requirements: 11.1, 14.1_

- [ ] 2. Define the API types (Spec/Status) and generate CRD + deepcopy
  - [ ] 2.1 Implement `DatabaseSensorSpec` and nested config types in `api/v1/databasesensor_types.go`
    - Add `DatabaseConfig` (Type, Host, Port, User, PasswordSecret), `SinkConfig`, `PrometheusSink`, `AlertRule`, `DatadogSink`, `WebhookSink`, `DashboardConfig`
    - Add validation markers: `database.type` enum (postgresql;mysql;mariadb;mongodb;sqlserver;oracle;db2;cassandra), `port` 1-65535, `sink.type` enum (prometheus;datadog;webhook;all), required host/user/passwordSecret, `tables` MinItems=1
    - _Requirements: 1.1, 8.4, 4.6_
  - [ ] 2.2 Implement `DatabaseSensorStatus` with the status subresource
    - Fields: Phase (enum Pending;Running;Degraded;Failed), Lag, EventsPerMin, LastEvent, LastError, DebeziumServer
    - Add `+kubebuilder:subresource:status` and printer-column markers for Phase/Lag/EventsPerMin/LastError
    - _Requirements: 5.5, 8.4_
  - [ ] 2.3 Run code generation and verify artifacts
    - Run `make generate` (deepcopy) and `make manifests` (CRD YAML in `config/crd/bases`)
    - Verify `make build` compiles the generated `zz_generated.deepcopy.go`
    - _Requirements: 1.1, 5.5, 8.4_

- [ ] 3. Implement the CDC event model and parser (pure logic, tested first)
  - [ ] 3.1 Define `CDCEvent` and the Debezium envelope `Parse` in `internal/events/parser.go`
    - `CDCEvent{Op, DB, Table, TsMs}`; parse `op` (c/u/d), detect DDL/schema-change envelope -> `Op="ddl"`, extract `ts_ms` using `tidwall/gjson`
    - Return a non-nil error for malformed JSON without panicking; `raw` is read-only
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.7_
  - [ ]* 3.2 Write unit tests for `Parse` in `internal/events/parser_test.go`
    - Cases: insert/update/delete/schema-change envelopes, malformed JSON, missing fields
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.7_

- [ ] 4. Implement the metrics exporter and `/metrics` handler
  - [ ] 4.1 Implement `Exporter` in `internal/metrics/exporter.go`
    - Register `debezium_events_total{op,db,table}`, `debezium_schema_changes_total{db,table}`, `debezium_lag_seconds{db,table}`, `debezium_events_per_minute{db,table}`
    - `OnEvent(CDCEvent)` increments the matching op/DDL counter and sets lag gauge to `now()-ts_ms`, clamped to 0 on clock skew (ts_ms in the future)
    - Provide `Handler()` returning `promhttp.HandlerFor` over the exporter's registry
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.8, 3.1, 3.2_
  - [ ]* 4.2 Write property test for event counting in `internal/metrics/exporter_prop_test.go`
    - **Property 4: Event counting** — for a random sequence of N events, summed `debezium_events_total` equals N, partitioned by op/db/table
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.4** (gopter)
  - [ ]* 4.3 Write property test for per-event lag in `internal/metrics/exporter_prop_test.go`
    - **Property 5: Lag monotonicity per event** — after `OnEvent(tsMs)`, `debezium_lag_seconds` equals `now()-tsMs` (and 0 on skew)
    - **Validates: Requirements 2.5, 2.8** (gopter)

- [ ] 5. Implement the LagTracker
  - [ ] 5.1 Implement `LagTracker` in `internal/health/lag.go`
    - Mutex-guarded `lastTs`; `OnEvent(tsMs int64)` sets `lastTs` and updates the lag gauge; `GetLag()` returns `now()-lastTs`
    - _Requirements: 2.5, 2.8, 7.3_
  - [ ]* 5.2 Write unit tests for `LagTracker` in `internal/health/lag_test.go`
    - Lag computation across event sequences, concurrency safety, clock-skew clamp to 0
    - _Requirements: 2.5, 2.8, 7.3_

- [ ] 6. Implement the health phase decision and Checker
  - [ ] 6.1 Implement pure `decidePhase` in `internal/health/checker.go`
    - Inputs (podRunning, lag, sinceLastEvent, warn, crit) -> Pending/Running/Degraded/Failed per the design table
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 7.3_
  - [ ] 6.2 Implement `Checker.Check` wrapping pod inspection + `decidePhase`
    - Read Debezium pod phase; detect `CrashLoopBackOff`/`Error` -> Failed with last termination message; pod-not-found -> Pending
    - Return (phase, lag, lastError) for the reconciler to persist
    - _Requirements: 7.1, 7.2, 7.4, 5.1, 5.2, 5.3, 5.4_
  - [ ]* 6.3 Write property test for phase determinism in `internal/health/checker_prop_test.go`
    - **Property 7: Phase determinism** — `decidePhase` is a pure function; identical inputs yield identical phases, matching the specification table
    - **Validates: Requirements 5.1, 5.2, 5.3, 5.4** (gopter)
  - [ ]* 6.4 Write unit tests for `decidePhase` truth table in `internal/health/checker_test.go`
    - Cover all (podRunning, lag vs warn/crit, sinceLastEvent vs 5m/30m) combinations
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 7.2, 7.3, 7.4_

- [ ] 7. Checkpoint - pure-logic foundation
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 8. Implement the Sink interface, CompositeSink, Registry, and WebhookSink
  - [ ] 8.1 Implement `Sink`, `CompositeSink`, and `Registry` in `internal/sink/sink.go`
    - `Sink` interface (`Push`, `Alert`); `CompositeSink` fans out and joins errors without aborting other sinks; `Registry` builds the active CompositeSink from `spec.sink.type` (prometheus/datadog/webhook/all)
    - _Requirements: 4.6_
  - [ ] 8.2 Implement `WebhookSink` in `internal/sink/webhook.go`
    - HTTP POST JSON body (alert name, db host, current value, threshold, timestamp, severity); 10s timeout; retry up to 3 times with 5s delay; non-blocking
    - _Requirements: 4.4, 4.8_
  - [ ]* 8.3 Write unit tests for WebhookSink in `internal/sink/webhook_test.go`
    - Use `httptest.Server`: verify JSON body shape, timeout, and retry-on-failure behavior
    - _Requirements: 4.4, 4.8_
  - [ ]* 8.4 Write unit tests for CompositeSink in `internal/sink/composite_test.go`
    - **Property 10: Sink fan-out isolation** — one sink failing does not prevent delivery to others and does not fail the caller
    - **Validates: Requirements 4.4, 4.6**

- [ ] 9. Implement the DatadogSink
  - [ ] 9.1 Implement `DatadogSink` in `internal/sink/datadog.go`
    - Push metric/alert via Datadog HTTP API with 10s timeout; honor `Retry-After` on 429, retry up to 3 times; read API/App keys from K8s Secrets at use time; missing Secret -> skip + lastError
    - Wire into `Registry` so `all`/`datadog` activate it
    - _Requirements: 4.5, 4.9, 12.1_
  - [ ]* 9.2 Write unit tests for DatadogSink in `internal/sink/datadog_test.go`
    - `httptest.Server` returning 429 with Retry-After; verify backoff/retry and missing-secret skip
    - _Requirements: 4.5, 4.9, 12.1_

- [ ] 10. Implement the DebeziumDeployer
  - [ ] 10.1 Implement connector mapping and CR builder in `internal/deployer/debezium.go`
    - `connectorClassFor(dbType)` pure mapping (postgres/mysql+mariadb/mongodb/...) with type-specific fields: postgres `slot.name` (1-63 lowercase/digits/underscore), mysql `database.server.id` (1-4294967295); error listing supported types otherwise
    - `buildDebeziumServer` maps connection params and `spec.tables` -> table include filters
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 1.1_
  - [ ] 10.2 Implement `EnsureDebeziumServer` (SSA + owner ref + secret check)
    - Verify `passwordSecret` exists; if missing return a descriptive error (reconciler sets Failed, no DebeziumServer created); set controller owner reference; apply via Server-Side Apply
    - _Requirements: 1.1, 1.2, 1.3, 12.1_
  - [ ]* 10.3 Write unit tests for connector mapping in `internal/deployer/debezium_test.go`
    - Every supported type + unsupported-type rejection; table-filter mapping; slot.name/server.id generation
    - _Requirements: 8.1, 8.2, 8.3, 8.4_

- [ ] 11. Implement the Service and ServiceMonitor
  - [ ] 11.1 Implement `EnsureService` and `EnsureServiceMonitor` in `internal/metrics/servicemonitor.go`
    - Service exposes port 8080 in the operator namespace, leaving an existing matching Service unchanged; ServiceMonitor targets the Service in `spec.sink.prometheus.namespace` (default operator namespace), owner-referenced
    - If ServiceMonitor CRD is absent, skip creation, set lastError, continue serving `/metrics`
    - _Requirements: 3.3, 3.4, 3.5_

- [ ] 12. Implement the PrometheusRule AlertRules builder
  - [ ] 12.1 Implement `EnsureAlertRules` in `internal/metrics/alertrules.go`
    - Build a `PrometheusRule` from `spec.sink.prometheus.alertRules` (one alert per valid rule mapping expr/for/severity), same namespace, owner-referenced
    - Reject rules missing expr/for/severity (skip that rule, keep valid ones, set lastError identifying the rejected rule/field)
    - _Requirements: 4.1, 4.7_
  - [ ]* 12.2 Write unit tests for AlertRules in `internal/metrics/alertrules_test.go`
    - Valid rules produce alerts; invalid rules are skipped with lastError; valid rules unaffected
    - _Requirements: 4.1, 4.7_

- [ ] 13. Implement the embedded dashboard and ring buffer
  - [ ] 13.1 Implement `RingBuffer` and route handlers in `internal/dashboard/dashboard.go`
    - `RingBuffer` holds last N events sized by `spec.dashboard.retention` (1-10000, default 100); `/api/events` returns newest-first JSON (all stored if fewer than N)
    - `/dashboard` serves HTML + Chart.js fetching `/metrics` and `/api/events`, refresh 1-60s; `RegisterRoutes(mux, enabled, buf)` mounts routes only when enabled (404 otherwise)
    - _Requirements: 6.1, 6.2, 6.3, 6.4_
  - [ ]* 13.2 Write unit tests for the dashboard API in `internal/dashboard/dashboard_test.go`
    - Ring buffer ordering/capacity; `/api/events` fewer-than-N; disabled -> 404 for `/dashboard` and `/api/events`
    - _Requirements: 6.2, 6.3, 6.4_

- [ ] 14. Implement the event consumer and wire the ingestion pipeline
  - [ ] 14.1 Implement the `Consumer` interface and implementations in `internal/events/consumer.go`
    - `Consumer.Start(ctx, handler)`; `KafkaConsumer` (`segmentio/kafka-go`) with exponential reconnect backoff (max 60s); `HTTPConsumer` webhook listener for the HTTP sink
    - _Requirements: 2.7, 9.1_
  - [ ] 14.2 Wire the consumer to Exporter, LagTracker, and RingBuffer
    - On each parsed event call `Exporter.OnEvent`, `LagTracker.OnEvent(ts_ms)`, and `RingBuffer.Add`; run in a dedicated goroutine so ingestion is independent of reconcile latency
    - _Requirements: 2.5, 2.6, 6.2, 9.1_

- [ ] 15. Checkpoint - components complete, tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 16. Implement the Reconciler and SetupWithManager
  - [ ] 16.1 Implement the reconcile loop in `internal/controller/databasesensor_controller.go`
    - Get CR (IgnoreNotFound); EnsureDebeziumServer (missing secret -> Failed, requeue 30s); ensure Service/ServiceMonitor/PrometheusRule; call `Health.Check`; fan out alerts via CompositeSink (non-blocking); return `RequeueAfter: 30s`
    - _Requirements: 1.1, 1.2, 1.3, 4.6, 7.1, 7.2, 7.3, 7.4_
  - [ ] 16.2 Implement status writes with conflict-safe retry
    - Populate phase/lag(duration string)/eventsPerMin/lastEvent/lastError/debeziumServer; `r.Status().Update` wrapped in `retry.RetryOnConflict`; on persistent failure retain prior status + set lastError
    - _Requirements: 5.5, 5.6, 5.7, 4.2, 4.3_
  - [ ] 16.3 Implement `SetupWithManager` with watches
    - `For(&v1.DatabaseSensor{})`, `Owns(DebeziumServer, Service, ServiceMonitor, PrometheusRule)` for cascade GC and owned-resource re-enqueue
    - _Requirements: 1.4, 11.1_

- [ ] 17. Wire the manager, HTTP server, and leader election in cmd/main.go
  - [ ] 17.1 Build the manager with leader election and register the reconciler
    - Add DebeziumServer and monitoring.coreos.com schemes; enable leader election; inject Deployer/Exporter/Health/Sinks into the reconciler
    - _Requirements: 11.1, 14.1_
  - [ ] 17.2 Start the HTTP server on :8080 with all routes
    - `http.ServeMux` serving `/metrics` (always), `/dashboard` + `/api/events` (gated by dashboard.enabled), `/healthz`, `/readyz`; start the consumer goroutine
    - _Requirements: 3.1, 3.2, 6.1, 6.2, 6.4, 9.1_

- [ ] 18. Author least-privilege RBAC
  - [ ] 18.1 Add RBAC markers and generate `config/rbac/role.yaml`
    - Grant only: `sensor.io` CRs, `debezium.io` CRs, apps/deployments (read), core services/configmaps/secrets, monitoring.coreos.com servicemonitors/prometheusrules; run `make manifests`
    - _Requirements: 13.1, 12.1_

- [ ] 19. Write integration tests with envtest
  - [ ]* 19.1 Add envtest suite in `internal/controller/databasesensor_controller_test.go`
    - Apply CR -> DebeziumServer appears with correct connector class/filters/owner ref (**Property 1**); delete CR -> DebeziumServer GC'd (**Property 2**); missing Secret -> Failed (**Property 3**); status transitions Pending->Running->Degraded; `/metrics` served regardless of sink (**Property 6**)
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 3.1, 5.1, 5.2, 5.3, 8.1**
  - [ ]* 19.2 Add a second-reconcile idempotence test
    - **Property 8: Idempotent reconcile** — repeated reconcile on an unchanged spec creates no duplicate resources and does not churn status
    - **Validates: Requirements 14.1**
  - [ ]* 19.3 Add an admission-validation test for unsupported DB type
    - **Property 9: Validation rejects unsupported DB** — a CR with an unsupported `database.type` is rejected by the enum marker
    - **Validates: Requirements 8.4**

- [ ] 20. Package deployment: samples and Helm chart
  - [ ] 20.1 Create sample CRs in `config/samples/`
    - `v1_databasesensor.yaml` (full, all sinks + dashboard), plus `basic` (prometheus only) and `mysql` examples
    - _Requirements: 1.1, 4.6, 8.2_
  - [ ] 20.2 Create the Helm chart in `charts/db-sensor-operator/`
    - `Chart.yaml`, `values.yaml`, templates for Deployment, Service, RBAC, ServiceAccount, and CRD
    - _Requirements: 11.1, 13.1_

- [ ] 21. Build and tooling: Dockerfile and Makefile
  - [ ] 21.1 Write a multi-stage Dockerfile
    - `golang:1.22` builder -> `gcr.io/distroless/static:nonroot` runtime
    - _Requirements: 14.1_
  - [ ] 21.2 Ensure Makefile targets exist
    - build, test, docker-build, docker-push, deploy, undeploy, dev, manifests, generate, lint
    - _Requirements: 11.1, 13.1_

- [ ]* 22. End-to-end test on kind
  - [ ]* 22.1 Add a kind-based e2e test (ginkgo/gomega)
    - Apply CR -> Debezium deploys -> events flow -> `/metrics` reflects counts -> alert fires -> webhook received; validates real RBAC, networking, and leader election
    - **Validates: Requirements 1.1, 3.1, 4.4, 9.1, 11.1, 13.1**

- [ ] 23. Final checkpoint - full suite green
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional (unit, property, integration, and e2e tests) and can be skipped for a faster MVP; core implementation tasks are never optional.
- Property-based tests use `gopter` and map to the design's correctness properties: P4/P5 (metrics), P7 (phase), P10 (sink fan-out), with P1/P2/P3/P6/P8/P9 validated via envtest.
- Each task references the specific requirement sub-criteria it covers for traceability; requirements 1-14 are all covered across the plan.
- Checkpoints (tasks 7, 15, 23) ensure incremental validation at natural boundaries: pure logic, assembled components, and the full system.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["2.1", "2.2", "3.1", "4.1", "5.1", "6.1", "8.1", "13.1"] },
    { "id": 1, "tasks": ["2.3", "3.2", "4.2", "4.3", "5.2", "6.2", "8.2", "9.1", "10.1", "13.2"] },
    { "id": 2, "tasks": ["6.3", "6.4", "8.3", "8.4", "9.2", "10.2", "11.1", "12.1", "14.1"] },
    { "id": 3, "tasks": ["10.3", "12.2", "14.2", "16.1"] },
    { "id": 4, "tasks": ["16.2", "16.3"] },
    { "id": 5, "tasks": ["17.1", "18.1"] },
    { "id": 6, "tasks": ["17.2"] },
    { "id": 7, "tasks": ["19.1", "19.2", "19.3", "20.1", "20.2", "21.1", "21.2"] },
    { "id": 8, "tasks": ["22.1"] }
  ]
}
```
