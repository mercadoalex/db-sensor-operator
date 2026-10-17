# DatabaseSensor Operator — Requirements

## User Stories

### US-1: Crear un sensor de BD
As a platform engineer, I want to declare a DatabaseSensor CR so that Debezium is automatically deployed and monitoring begins without manual steps.

- **1.1** WHEN a user applies a `DatabaseSensor` CR with a valid `spec.database` (host, port, user, passwordSecret) AND `spec.tables`
  THE SYSTEM SHALL create a `DebeziumServer` CR in the same namespace with the correct database connection parameters and table filters
- **1.2** WHEN the `DebeziumServer` CR is created
  THE SYSTEM SHALL set the `ownerReference` to the `DatabaseSensor` CR so that deletion of the sensor cascades to Debezium
- **1.3** WHEN the `spec.database.passwordSecret` references a non-existent Secret
  THE SYSTEM SHALL set `status.phase` to `Failed` and `status.lastError` to a descriptive message
- **1.4** WHEN a `DatabaseSensor` CR is deleted
  THE SYSTEM SHALL delete the `DebeziumServer` CR and all associated resources (ServiceMonitor, PrometheusRule, Service) via owner references

### US-2: Derivar métricas de observabilidad
As an SRE, I want the operator to derive metrics from CDC events so that I can detect anomalies in database traffic without touching application code.

- **2.1** WHEN a CDC event with `op=c` (insert) is received
  THE SYSTEM SHALL increment the counter `debezium_events_total{op="c", db="<host>", table="<table>"}`
- **2.2** WHEN a CDC event with `op=u` (update) is received
  THE SYSTEM SHALL increment the counter `debezium_events_total{op="u", db="<host>", table="<table>"}`
- **2.3** WHEN a CDC event with `op=d` (delete) is received
  THE SYSTEM SHALL increment the counter `debezium_events_total{op="d", db="<host>", table="<table>"}`
- **2.4** WHEN a schema change event (DDL) is detected
  THE SYSTEM SHALL increment the counter `debezium_schema_changes_total{db="<host>", table="<table>"}`
- **2.5** WHEN the operator receives a CDC event with timestamp `ts_ms`
  THE SYSTEM SHALL update the gauge `debezium_lag_seconds{db="<host>", table="<table>"}` to `now() - ts_ms`
- **2.6** WHEN the operator computes the rate of events per minute
  THE SYSTEM SHALL update the gauge `debezium_events_per_minute{db="<host>", table="<table>"}`

### US-3: Exponer métricas en formato Prometheus
As a monitoring engineer, I want the operator to always expose a `/metrics` endpoint in Prometheus format so that Prometheus can scrape it regardless of the configured sink.

- **3.1** WHEN the operator is running
  THE SYSTEM SHALL expose an HTTP endpoint at `/metrics` (port 8080) with all derived metrics in Prometheus text format
- **3.2** WHEN `spec.sink.prometheus.serviceMonitor` is `true`
  THE SYSTEM SHALL create a `ServiceMonitor` CR in the namespace specified by `spec.sink.prometheus.namespace` that targets the operator's Service on port 8080
- **3.3** WHEN `spec.sink.prometheus.serviceMonitor` is `true`
  THE SYSTEM SHALL create a `Service` resource that exposes the operator's metrics port

### US-4: Configurar alertas
As an SRE, I want to define alert rules in the CR spec so that I get notified when database behavior deviates from expected patterns.

- **4.1** WHEN `spec.sink.prometheus.alertRules` contains a rule with `expr`, `for`, and `severity`
  THE SYSTEM SHALL create a `PrometheusRule` CR with the corresponding alert definition
- **4.2** WHEN the alert `debezium_lag_seconds > threshold` is true for the duration specified in `for`
  THE SYSTEM SHALL update `status.phase` to `Degraded` and `status.lag` to the current lag value
- **4.3** WHEN `rate(debezium_events_total[5m]) == 0` for 15 minutes
  THE SYSTEM SHALL set `status.phase` to `Failed` and `status.lastError` to "No CDC events received"
- **4.4** WHEN `spec.sink.webhook.enabled` is `true` AND an alert from `spec.sink.webhook.triggerOn` fires
  THE SYSTEM SHALL send an HTTP POST to `spec.sink.webhook.url` with a JSON body containing: alert name, db host, current value, threshold, timestamp, severity
- **4.5** WHEN `spec.sink.datadog.enabled` is `true` AND an alert fires
  THE SYSTEM SHALL push the metric and alert to Datadog via the HTTP API using credentials from `spec.sink.datadog.apiKeySecret` and `spec.sink.datadog.appKeySecret`
- **4.6** WHEN `spec.sink.type` is `all`
  THE SYSTEM SHALL activate all configured sinks simultaneously (Prometheus + Datadog + Webhook)

### US-5: Reportar status en el CR
As a platform engineer, I want to check the health of my sensor with `kubectl get` so that I can quickly identify issues without digging into logs.

- **5.1** WHEN the DebeziumServer pod is not yet running
  THE SYSTEM SHALL set `status.phase` to `Pending`
- **5.2** WHEN the DebeziumServer pod is running AND receiving events with lag < threshold
  THE SYSTEM SHALL set `status.phase` to `Running`
- **5.3** WHEN the lag exceeds the warning threshold
  THE SYSTEM SHALL set `status.phase` to `Degraded`
- **5.4** WHEN the lag exceeds the critical threshold OR no events are received for 30 minutes
  THE SYSTEM SHALL set `status.phase` to `Failed`
- **5.5** WHEN the status is updated
  THE SYSTEM SHALL include: `phase`, `lag` (human-readable duration), `eventsPerMin` (float), `lastEvent` (relative time), `lastError` (string, empty if none)

### US-6: Dashboard embebido
As a developer, I want a built-in dashboard so that I can visually inspect metrics without setting up Grafana.

- **6.1** WHEN `spec.dashboard.enabled` is `true`
  THE SYSTEM SHALL serve an HTML page at `/dashboard` (port 8080) with real-time charts (Chart.js) that fetch from `/metrics` and `/api/events`
- **6.2** WHEN the dashboard is active
  THE SYSTEM SHALL serve a JSON API at `/api/events` returning the last N CDC events (configurable via `spec.dashboard.retention`)
- **6.3** WHEN `spec.dashboard.enabled` is `false` (default)
  THE SYSTEM SHALL NOT serve `/dashboard` or `/api/events`

### US-7: Health check periódico
As an SRE, I want the operator to continuously verify that Debezium is healthy so that transient failures are detected and reported.

- **7.1** WHEN the reconciler loop runs (every 30 seconds via RequeueAfter)
  THE SYSTEM SHALL check if the DebeziumServer pod exists and is in `Running` state
- **7.2** WHEN the DebeziumServer pod is in `CrashLoopBackOff` or `Error`
  THE SYSTEM SHALL set `status.phase` to `Failed` and `status.lastError` to the pod's last termination message
- **7.3** WHEN no CDC events have been received for the configured timeout (default: 5 minutes)
  THE SYSTEM SHALL set `status.phase` to `Degraded` and log a warning

### US-8: Soporte multi-BD
As a platform engineer, I want to monitor different database types so that the operator works across our heterogeneous stack.

- **8.1** WHEN `spec.database.type` is `postgresql`
  THE SYSTEM SHALL configure the DebeziumServer with `connector.class=io.debezium.connector.postgresql.PostgresConnector` and `slot.name`
- **8.2** WHEN `spec.database.type` is `mysql`
  THE SYSTEM SHALL configure the DebeziumServer with `connector.class=io.debezium.connector.mysql.MySqlConnector` and `database.server.id`
- **8.3** WHEN `spec.database.type` is `mongodb`
  THE SYSTEM SHALL configure the DebeziumServer with `connector.class=io.debezium.connector.mongodb.MongoDbConnector`
- **8.4** WHEN `spec.database.type` is not one of the supported types
  THE SYSTEM SHALL reject the CR with a validation error listing supported types

---

## Non-Functional Requirements

- **NFR-1**: The operator SHALL handle at least 1000 events/second per DatabaseSensor CR without dropping events
- **NFR-2**: The reconciler SHALL complete in less than 5 seconds under normal conditions
- **NFR-3**: The operator SHALL be HA-capable (leader election via controller-runtime)
- **NFR-4**: All secrets (DB credentials, Datadog keys) SHALL be read from Kubernetes Secrets, never hardcoded
- **NFR-5**: The operator SHALL use least-privilege RBAC (only the permissions it needs)
- **NFR-6**: The operator SHALL be restartable without data loss (state is in K8s objects, not local disk)   