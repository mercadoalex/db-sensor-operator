
---

## `.kiro/specs/db-sensor/tasks.md`

```markdown
# Implementation Plan

- [ ] 1. Scaffold project with Kubebuilder
  - Run `kubebuilder init --domain sensor.io --repo github.com/tuuser/db-sensor-operator`
  - Run `kubebuilder create api --group sensor --version v1 --kind DatabaseSensor`
  - Verify `make build` and `make test` pass
  - _Requirements: all_

- [ ] 2. Define Spec/Status structs
  - Implement `DatabaseSensorSpec` with fields: Database, Tables, Sink, Dashboard
  - Implement `DatabaseSensorStatus` with fields: Phase, Lag, EventsPerMin, LastEvent, LastError, DebeziumServer
  - Add kubebuilder markers: `+kubebuilder:object:root=true`, `+kubebuilder:subresource:status`, `+kubebuilder:validation`
  - Run `make generate` and `make manifests` to produce deepcopy + CRD YAML
  - _Requirements: 1.1, 1.3, 5.1, 5.2, 5.3, 5.4, 5.5, 8.1, 8.2, 8.3, 8.4_

- [ ] 3. Implement the Reconciler skeleton
  - Create `internal/controller/databasesensor_controller.go`
  - Implement `Reconcile` with: Get CR → handle deletion → placeholder calls → RequeueAfter 30s
  - Implement `SetupWithManager` with `For(&v1.DatabaseSensor{})` and `Owns()`
  - Wire up in `cmd/main.go`
  - _Requirements: 1.1, 1.4, 7.1_

- [ ] 4. Implement DebeziumServer CR creation
  - Create `internal/deployer/debezium.go`
  - Implement `EnsureDebeziumServer`: build DebeziumServer CR from sensor spec, set owner ref, CreateOrUpdate
  - Map `spec.database.type` to correct connector class (postgres, mysql, mongodb)
  - Map `spec.tables` to table filters
  - Handle missing Secret error → set status Failed
  - _Requirements: 1.1, 1.2, 1.3, 8.1, 8.2, 8.3_

- [ ] 5. Implement RBAC and Service
  - Create `config/rbac/role.yaml` with permissions for: sensor.io CRs, debezium.io CRs, apps/deployments, core/services, core/configmaps, core/secrets, monitoring.coreos.com/servicemonitors, monitoring.coreos.com/prometheusrules
  - Create `internal/metrics/servicemonitor.go`: EnsureService + EnsureServiceMonitor
  - _Requirements: 3.1, 3.2, 3.3, NFR-5_

- [ ] 6. Implement Prometheus metrics exporter
  - Create `internal/metrics/exporter.go`
  - Register metrics: `debezium_events_total` (CounterVec), `debezium_lag_seconds` (GaugeVec), `debezium_events_per_minute` (GaugeVec), `debezium_schema_changes_total` (CounterVec)
  - Implement `OnEvent(CDCEvent)` to update counters and gauges
  - Expose `/metrics` on port 8080 via `promhttp.Handler()`
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 3.1_

- [ ] 7. Implement CDC event consumer
  - Create `internal/events/consumer.go` with `Consumer` interface
  - Implement `KafkaConsumer` (using `kafka-go`) for Kafka sink
  - Implement `HTTPConsumer` (webhook listener) for HTTP sink
  - Create `internal/events/parser.go`: parse Debezium JSON → `CDCEvent` struct
  - Wire consumer to `Exporter.OnEvent`
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

- [ ] 8. Implement LagTracker
  - Create `internal/health/lag.go`
  - Implement `LagTracker` with mutex, `OnEvent(tsMs)`, `GetLag()`
  - Update gauge on each event
  - _Requirements: 2.5, 4.2, 7.1_

- [ ] 9. Implement Health Checker
  - Create `internal/health/checker.go`
  - Implement `Check()`: get DebeziumServer status, check pod phase, check lag, check no-event timeout
  - Determine phase: Pending → Running → Degraded → Failed
  - _Requirements: 4.2, 4.3, 5.1, 5.2, 5.3, 5.4, 7.1, 7.2, 7.3_

- [ ] 10. Implement AlertRules (PrometheusRule)
  - Create `internal/metrics/alertrules.go`
  - Implement `EnsureAlertRules`: build PrometheusRule CR from `spec.sink.prometheus.alertRules[]`
  - Set owner reference to DatabaseSensor
  - _Requirements: 4.1_

- [ ] 11. Implement Sink interface + Webhook
  - Create `internal/sink/sink.go`: `Sink` interface, `CompositeSink`, `Registry`
  - Create `internal/sink/webhook.go`: `WebhookSink` with HTTP POST, JSON body, timeout, retry
  - Wire into Reconcile: on alert trigger, call `CompositeSink.Alert()`
  - _Requirements: 4.4, 4.6_

- [ ] 12. Implement Datadog Sink
  - Create `internal/sink/datadog.go`
  - Implement `DatadogSink`: push metrics via Datadog HTTP API, handle 429 with Retry-After
  - Read API keys from K8s Secrets at reconcile time
  - _Requirements: 4.5, 4.6_

- [ ] 13. Implement status updates in Reconcile
  - In Reconcile: call HealthChecker, update `sensor.Status` fields
  - Use `r.Status().Update(ctx, &sensor)`
  - Handle conflict errors (requeue)
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5_

- [ ] 14. Implement built-in dashboard
  - Create `internal/dashboard/` (or serve from `cmd/main.go`)
  - HTML page with Chart.js: line chart for lag, bar chart for events/min, table for recent events
  - `GET /dashboard` → HTML (if `spec.dashboard.enabled`)
  - `GET /api/events` → JSON array of last N events (in-memory ring buffer)
  - _Requirements: 6.1, 6.2, 6.3_

- [ ] 15. Implement HTTP server with probes
  - In `cmd/main.go`: create `http.ServeMux`
  - Routes: `/metrics`, `/dashboard`, `/api/events`, `/healthz`, `/readyz`
  - Listen on port 8080
  - _Requirements: 3.1, 6.1, 6.2, 6.3_

- [ ] 16. Write unit tests
  - `internal/health/lag_test.go`: test LagTracker with various event sequences
  - `internal/events/parser_test.go`: test CDC event parsing (insert, update, delete, schema change)
  - `internal/sink/webhook_test.go`: test WebhookSink with httptest.Server
  - `internal/sink/composite_test.go`: test fan-out behavior
  - _Requirements: 2.1-2.6, 4.4, 4.5_

- [ ] 17. Write integration tests (envtest)
  - `internal/controller/databasesensor_controller_test.go`
  - Test: create CR → DebeziumServer CR appears with correct spec
  - Test: delete CR → DebeziumServer CR is garbage collected
  - Test: missing Secret → status becomes Failed
  - Test: status transitions (Pending → Running → Degraded)
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 5.1-5.5_

- [ ] 18. Create sample CR and Helm chart
  - `config/samples/v1_databasesensor.yaml`: complete example with all fields
  - `charts/db-sensor-operator/`: Chart.yaml, values.yaml, templates (Deployment, Service, RBAC, CRD)
  - _Requirements: all_

- [ ] 19. Dockerfile and Makefile
  - Multi-stage Dockerfile: golang:1.22 → distroless/static
  - Makefile targets: build, test, docker-build, docker-push, deploy, undeploy, dev, manifests, generate, lint
  - _Requirements: NFR-1 through NFR-6_

- [ ] 20. Documentation
  - `README.md`: quick start, architecture diagram, usage examples
  - Update `DESIGN.md` with final decisions
  - Add `config/samples/` with 3 examples: basic (prometheus only), full (all sinks), mysql
  - _Requirements: all_   