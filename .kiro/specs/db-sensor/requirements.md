# Requirements Document

## Introduction

**Lactiflluus** is a Kubernetes Debezium-based operator that orchestrates database metric observability, multi-sink alerting, and DDL (schema-change) detection, implemented as the `DatabaseSensor` custom resource. The DatabaseSensor Operator is a Kubernetes operator (Go, Kubebuilder + controller-runtime) that turns a single declarative `DatabaseSensor` custom resource into a complete Change Data Capture (CDC) observability pipeline. Rather than deploying Debezium directly, the operator creates a `DebeziumServer` custom resource that the official Debezium Operator reconciles into a running Debezium Server, and layers observability on top: it derives per-operation, per-table metrics from the CDC event stream, always exposes those metrics in Prometheus format, fans alerts out to multiple backends (Prometheus, Datadog, Webhook), serves an optional embedded dashboard, and reports enriched health status back on the custom resource.

These requirements are derived from the approved design document (`design.md`) and are kept numerically aligned with the design's `Validates: Requirements X.Y` references so the two documents remain in sync.

## Glossary

- **DatabaseSensor_Operator**: The Kubernetes operator described by this document; the system under specification. Shortened to "the operator" in prose.
- **DatabaseSensor_CR**: A custom resource of kind `DatabaseSensor` (group `sensor.io`, version `v1`) that declares the database to monitor, the tables, the sink configuration, and the optional dashboard.
- **DebeziumServer_CR**: A custom resource of kind `DebeziumServer` owned by the official Debezium Operator; the operator creates and maintains one per DatabaseSensor_CR.
- **CDC_Event**: A Change Data Capture event emitted by Debezium, carrying an operation code (`op`), source metadata (database host, table), and a source timestamp (`ts_ms`).
- **Reconciler**: The controller-runtime reconciliation loop of the operator.
- **Metrics_Exporter**: The component that owns the Prometheus registry, updates derived metrics from CDC events, and serves `/metrics`.
- **Sink**: An observability backend (Prometheus, Datadog, or Webhook) that receives metrics and/or alerts.
- **Composite_Sink**: The fan-out component that delivers to every active Sink.
- **Health_Checker**: The component that derives the status Phase from Debezium pod state and replication lag.
- **Lag**: The replication delay for a table, computed as `now() - ts_ms` of the most recent CDC_Event.
- **Warning_Threshold**: The lag value above which the Phase becomes `Degraded`.
- **Critical_Threshold**: The lag value above which the Phase becomes `Failed`.
- **Phase**: The status field with values `Pending`, `Running`, `Degraded`, `Failed`.
- **Owner_Reference**: A Kubernetes owner reference that enables cascade garbage collection of child resources.

## Requirements

### Requirement 1: Create a database sensor

**User Story:** As a platform engineer, I want to declare a DatabaseSensor custom resource, so that Debezium is deployed and monitoring begins without manual steps.

#### Acceptance Criteria

1. WHEN a DatabaseSensor_CR with a valid `spec.database` (type is one of the supported database types, host is a non-empty string, port is an integer from 1 to 65535, user is a non-empty string, passwordSecret is a non-empty Secret name) and a `spec.tables` list containing at least 1 entry is applied, THE DatabaseSensor_Operator SHALL create exactly one DebeziumServer_CR in the same namespace as the DatabaseSensor_CR, with database connection parameters (type, host, port, user, passwordSecret) equal to the corresponding `spec.database` values and table include filters equal to the entries in `spec.tables`, within 5 seconds of the apply event.
2. WHEN the DatabaseSensor_Operator creates the DebeziumServer_CR, THE DatabaseSensor_Operator SHALL set an Owner_Reference on the DebeziumServer_CR that points to the DatabaseSensor_CR, with the controller flag set to true.
3. IF `spec.database.passwordSecret` references a Secret that does not exist in the DatabaseSensor_CR namespace at reconcile time, THEN THE DatabaseSensor_Operator SHALL set `status.phase` to `Failed`, set `status.lastError` to a message that names the missing Secret, and SHALL NOT create a DebeziumServer_CR.
4. WHEN a DatabaseSensor_CR is deleted, THE DatabaseSensor_Operator SHALL delete, through Owner_References, every resource associated with that DatabaseSensor_CR, specifically the DebeziumServer_CR, the ServiceMonitor, the PrometheusRule, and the Service, such that none of these resources remain in the namespace after deletion completes.

### Requirement 2: Derive observability metrics from CDC events

**User Story:** As an SRE, I want the operator to derive metrics from CDC events, so that I can detect anomalies in database traffic without changing application code.

#### Acceptance Criteria

1. WHEN a CDC_Event with `op=c` (insert) is received, THE DatabaseSensor_Operator SHALL increment the counter `debezium_events_total` with labels `op="c"`, `db="<host>"`, `table="<table>"`.
2. WHEN a CDC_Event with `op=u` (update) is received, THE DatabaseSensor_Operator SHALL increment the counter `debezium_events_total` with labels `op="u"`, `db="<host>"`, `table="<table>"`.
3. WHEN a CDC_Event with `op=d` (delete) is received, THE DatabaseSensor_Operator SHALL increment the counter `debezium_events_total` with labels `op="d"`, `db="<host>"`, `table="<table>"`.
4. WHEN a schema change (DDL) CDC_Event is received, THE DatabaseSensor_Operator SHALL increment the counter `debezium_schema_changes_total` with labels `db="<host>"`, `table="<table>"`.
5. WHEN a CDC_Event carrying a source timestamp `ts_ms` is received, THE DatabaseSensor_Operator SHALL set the gauge `debezium_lag_seconds` with labels `db="<host>"`, `table="<table>"` to the value `now() - ts_ms` in seconds.
6. WHEN the DatabaseSensor_Operator recomputes the event rate, THE DatabaseSensor_Operator SHALL set the gauge `debezium_events_per_minute` with labels `db="<host>"`, `table="<table>"` to the current per-minute rate.
7. IF a received CDC_Event cannot be parsed into a recognized operation and source metadata, THEN THE DatabaseSensor_Operator SHALL skip metric updates for that event and increment a parse-error counter, without terminating the consumer.
8. WHEN a CDC_Event carries a `ts_ms` greater than the operator current time (clock skew), THE DatabaseSensor_Operator SHALL set the `debezium_lag_seconds` gauge for that `db`/`table` to 0 rather than a negative value.

### Requirement 3: Expose metrics in Prometheus format

**User Story:** As a monitoring engineer, I want the operator to always expose a `/metrics` endpoint in Prometheus format, so that Prometheus can scrape it regardless of the configured sink.

#### Acceptance Criteria

1. WHILE the DatabaseSensor_Operator is running, THE DatabaseSensor_Operator SHALL serve an HTTP endpoint at `/metrics` on port 8080 that returns all derived metrics in Prometheus text format within 5 seconds of each request, independent of the value of `spec.sink.type`, including when `spec.sink.type` has no Prometheus sink configured.
2. WHILE the DatabaseSensor_Operator is running AND no CDC events have yet been received, THE DatabaseSensor_Operator SHALL respond to a request on `/metrics` with a Prometheus-format body containing the registered metric families with their zero/initial values rather than an empty body or an error response.
3. WHERE `spec.sink.prometheus.serviceMonitor` is `true`, THE DatabaseSensor_Operator SHALL create a ServiceMonitor that targets the operator Service on port 8080 in the namespace given by `spec.sink.prometheus.namespace`, defaulting to the operator Service namespace when `spec.sink.prometheus.namespace` is empty or omitted.
4. WHERE `spec.sink.prometheus.serviceMonitor` is `true`, THE DatabaseSensor_Operator SHALL create a Service in the operator Service namespace that exposes the operator metrics port 8080, and SHALL leave an already-existing matching Service unchanged.
5. IF `spec.sink.prometheus.serviceMonitor` is `true` AND the ServiceMonitor custom resource definition is not installed in the cluster, THEN THE DatabaseSensor_Operator SHALL skip ServiceMonitor creation, set `status.lastError` to a message indicating the ServiceMonitor CRD is unavailable, and continue serving the `/metrics` endpoint without terminating.

### Requirement 4: Configure alerts and fan out to sinks

**User Story:** As an SRE, I want to define alert rules in the custom resource spec, so that I am notified when database behavior deviates from expected patterns.

#### Acceptance Criteria

1. WHEN `spec.sink.prometheus.alertRules` contains a rule providing all three fields `expr`, `for`, and `severity`, THE DatabaseSensor_Operator SHALL create a PrometheusRule, in the same namespace as the DatabaseSensor custom resource, containing one alert definition per supplied rule that maps the rule `expr`, `for`, and `severity` into the alert.
2. WHEN the Lag exceeds the threshold referenced by an alert rule's `expr` for a continuous duration equal to that rule's `for` field, THE DatabaseSensor_Operator SHALL set `status.phase` to `Degraded` and set `status.lag` to the current Lag value.
3. IF `rate(debezium_events_total[5m])` equals zero for a continuous duration of 15 minutes, THEN THE DatabaseSensor_Operator SHALL set `status.phase` to `Failed` and set `status.lastError` to a message indicating that no CDC events were received.
4. WHEN `spec.sink.webhook.enabled` is `true` and an alert listed in `spec.sink.webhook.triggerOn` fires, THE DatabaseSensor_Operator SHALL send a non-blocking HTTP POST to `spec.sink.webhook.url`, with a request timeout of 10 seconds, carrying a JSON body containing the alert name, the database host, the current value, the threshold, the timestamp, and the severity.
5. WHEN `spec.sink.datadog.enabled` is `true` and an alert fires, THE DatabaseSensor_Operator SHALL push the metric and alert to the Datadog HTTP API, with a request timeout of 10 seconds, using the credentials read from `spec.sink.datadog.apiKeySecret` and `spec.sink.datadog.appKeySecret`.
6. WHERE `spec.sink.type` is `all`, THE DatabaseSensor_Operator SHALL activate the Prometheus, Datadog, and Webhook sinks simultaneously through the Composite_Sink.
7. IF `spec.sink.prometheus.alertRules` contains a rule that is missing any of the fields `expr`, `for`, or `severity`, THEN THE DatabaseSensor_Operator SHALL reject that rule without creating its alert definition, SHALL leave any valid rules' alert definitions unchanged, and SHALL set `status.lastError` to a message identifying the rejected rule and the missing field.
8. IF a webhook HTTP POST fails or does not complete within the 10-second timeout, THEN THE DatabaseSensor_Operator SHALL retry the delivery up to 3 times with a delay of 5 seconds between attempts, and after the final failed attempt SHALL set `status.lastError` to a message indicating webhook delivery failure, without blocking reconciliation of other sinks.
9. IF the Datadog HTTP API returns a rate-limiting response, THEN THE DatabaseSensor_Operator SHALL wait for the duration indicated by the response's Retry-After value and retry the push up to 3 times, and after the final failed attempt SHALL set `status.lastError` to a message indicating Datadog delivery failure; and IF either `spec.sink.datadog.apiKeySecret` or `spec.sink.datadog.appKeySecret` references a non-existent Secret, THEN THE DatabaseSensor_Operator SHALL skip the Datadog push and set `status.lastError` to a message identifying the missing Secret.

### Requirement 5: Report enriched status on the custom resource

**User Story:** As a platform engineer, I want to check the health of my sensor with `kubectl get`, so that I can identify issues without reading logs.

#### Acceptance Criteria

1. WHILE no Debezium pod exists or the Debezium pod has not yet reached the `Running` state, THE DatabaseSensor_Operator SHALL set `status.phase` to `Pending`.
2. WHILE the Debezium pod is in the `Running` state and the Lag is below the Warning_Threshold, THE DatabaseSensor_Operator SHALL set `status.phase` to `Running`.
3. WHILE the Debezium pod is in the `Running` state and the Lag is greater than or equal to the Warning_Threshold and less than or equal to the Critical_Threshold, THE DatabaseSensor_Operator SHALL set `status.phase` to `Degraded`.
4. WHILE the Lag is greater than the Critical_Threshold, OR no CDC_Event has been received for a continuous period of 30 minutes, THE DatabaseSensor_Operator SHALL set `status.phase` to `Failed`.
5. WHEN the DatabaseSensor_Operator updates status, THE DatabaseSensor_Operator SHALL populate `status.phase` with one of exactly `Pending`, `Running`, `Degraded`, or `Failed`; `status.lag` as a duration string expressed in time units (for example `1m30s`); `status.eventsPerMin` as a floating-point number greater than or equal to 0.0; `status.lastEvent` as the elapsed time since the most recent CDC_Event expressed in time units, or an empty string when no CDC_Event has been received; and `status.lastError` as a string that is empty when no error is present.
6. WHEN 30 seconds have elapsed since the previous status update, THE DatabaseSensor_Operator SHALL re-evaluate pod state and Lag and update the `status` fields so that the reported values reflect observations no older than 30 seconds.
7. IF writing the updated `status` to the custom resource fails, THEN THE DatabaseSensor_Operator SHALL retain the previously persisted `status` values, set `status.lastError` to a message indicating the status update failed, and retry the status update on the next evaluation cycle.

### Requirement 6: Embedded dashboard

**User Story:** As a developer, I want a built-in dashboard, so that I can inspect metrics visually without setting up Grafana.

#### Acceptance Criteria

1. WHERE `spec.dashboard.enabled` is `true`, WHEN an HTTP GET request is received at `/dashboard` on port 8080, THE DatabaseSensor_Operator SHALL respond with HTTP 200 and an HTML page that renders Chart.js charts whose data is fetched from `/metrics` and `/api/events`, refreshing at an interval between 1 and 60 seconds.
2. WHERE `spec.dashboard.enabled` is `true`, WHEN an HTTP GET request is received at `/api/events` on port 8080, THE DatabaseSensor_Operator SHALL respond with HTTP 200 and a JSON array containing the most recent N CDC_Events ordered newest-first, where N equals `spec.dashboard.retention` (an integer from 1 to 10,000, default 100 when unset).
3. IF the number of stored CDC_Events is fewer than N when `/api/events` is requested, THEN THE DatabaseSensor_Operator SHALL return all currently stored CDC_Events as a JSON array without padding or error.
4. WHILE `spec.dashboard.enabled` is `false`, THE DatabaseSensor_Operator SHALL respond with HTTP 404 to any HTTP request for `/dashboard` or `/api/events` on port 8080.

### Requirement 7: Periodic health check

**User Story:** As an SRE, I want the operator to continuously verify that Debezium is healthy, so that transient failures are detected and reported.

#### Acceptance Criteria

1. WHEN the Reconciler loop runs on its 30-second `RequeueAfter` schedule AND the Debezium pod exists and is in the `Running` state, THE DatabaseSensor_Operator SHALL set `status.phase` to `Running` and set `status.lastError` to an empty string.
2. IF the Debezium pod is in `CrashLoopBackOff` or `Error`, THEN THE DatabaseSensor_Operator SHALL set `status.phase` to `Failed` and set `status.lastError` to the pod last termination message, or to an error message indicating the termination reason is unavailable when no last termination message exists.
3. IF no CDC_Event has been received for the configured timeout whose default is 5 minutes (300 seconds), THEN THE DatabaseSensor_Operator SHALL set `status.phase` to `Degraded` and set `status.lastError` to an error message indicating no CDC events were received within the timeout.
4. WHEN the Reconciler loop runs on its 30-second `RequeueAfter` schedule AND the Debezium pod does not exist, THE DatabaseSensor_Operator SHALL set `status.phase` to `Pending` and set `status.lastError` to an error message indicating the Debezium pod is not found.

### Requirement 8: Multi-database support

**User Story:** As a platform engineer, I want to monitor different database types, so that the operator works across a heterogeneous stack.

#### Acceptance Criteria

1. WHEN `spec.database.type` is `postgresql`, THE DatabaseSensor_Operator SHALL configure the DebeziumServer_CR with `connector.class=io.debezium.connector.postgresql.PostgresConnector` and a `slot.name` that is a non-empty value of 1 to 63 characters restricted to lowercase letters, digits, and underscores.
2. WHEN `spec.database.type` is `mysql` or `mariadb`, THE DatabaseSensor_Operator SHALL configure the DebeziumServer_CR with `connector.class=io.debezium.connector.mysql.MySqlConnector` and a `database.server.id` that is a unique integer in the range 1 to 4294967295.
3. WHEN `spec.database.type` is `mongodb`, THE DatabaseSensor_Operator SHALL configure the DebeziumServer_CR with `connector.class=io.debezium.connector.mongodb.MongoDbConnector`.
4. IF `spec.database.type` is absent, empty, or not one of the supported types (postgresql, mysql, mariadb, mongodb, sqlserver, oracle, db2, cassandra), THEN THE DatabaseSensor_Operator SHALL reject the DatabaseSensor_CR with a validation error that lists the supported types and SHALL NOT create a DebeziumServer_CR.

## Non-Functional Requirements

### Requirement 9: Throughput

**User Story:** As an SRE, I want the operator to keep up with busy databases, so that metrics stay accurate under load.

#### Acceptance Criteria

1. WHILE a single DatabaseSensor_CR receives up to 1000 CDC_Events per second, THE DatabaseSensor_Operator SHALL process every event without dropping any.

### Requirement 10: Reconcile latency

**User Story:** As a platform engineer, I want fast reconciliation, so that changes take effect promptly.

#### Acceptance Criteria

1. WHILE operating under normal conditions, THE DatabaseSensor_Operator SHALL complete each reconcile in less than 5 seconds.

### Requirement 11: High availability

**User Story:** As a platform engineer, I want the operator to run with redundancy, so that a single instance failure does not stop reconciliation.

#### Acceptance Criteria

1. WHILE multiple operator replicas are deployed, THE DatabaseSensor_Operator SHALL use controller-runtime leader election so that exactly one replica is the active Reconciler.

### Requirement 12: Secret handling

**User Story:** As a security engineer, I want all credentials sourced from Kubernetes Secrets, so that no secret is hardcoded.

#### Acceptance Criteria

1. WHEN the DatabaseSensor_Operator needs database credentials or Datadog keys, THE DatabaseSensor_Operator SHALL read them from Kubernetes Secrets at use time and SHALL reference secret contents by name rather than embedding secret values in code or logs.

### Requirement 13: Least-privilege RBAC

**User Story:** As a cluster administrator, I want the operator to request only the access it needs, so that its blast radius is limited.

#### Acceptance Criteria

1. THE DatabaseSensor_Operator SHALL request only the RBAC permissions required for the resource kinds it manages: `sensor.io` custom resources, `debezium.io` custom resources, apps deployments, core services, configmaps, secrets, and `monitoring.coreos.com` ServiceMonitors and PrometheusRules.

### Requirement 14: Restartable without data loss

**User Story:** As an SRE, I want the operator to recover from restarts cleanly, so that no durable state is lost.

#### Acceptance Criteria

1. WHEN the DatabaseSensor_Operator restarts, THE DatabaseSensor_Operator SHALL reconstruct its working state from Kubernetes objects and SHALL NOT depend on data persisted to local disk.
