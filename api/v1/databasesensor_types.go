/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// DatabaseConfig describes the database that Debezium captures changes from.
type DatabaseConfig struct {
	// type is the database engine to monitor. The value selects the Debezium
	// connector class and any engine-specific configuration. Only PostgreSQL
	// (verified against a real cluster) and MySQL (connector mapped, not yet
	// verified) are supported; other engines are rejected.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=postgresql;mysql
	Type string `json:"type"`

	// host is the database hostname or service address.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Host string `json:"host"`

	// port is the database port.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	Port int32 `json:"port"`

	// user is the database user Debezium connects as.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	User string `json:"user"`

	// passwordSecret is the name of a Kubernetes Secret, in the same namespace
	// as the DatabaseSensor, that holds the database password under the key
	// "password". The Secret is injected into the Debezium Server pod as
	// environment variables; the password itself is never written into the
	// DebeziumServer spec.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	PasswordSecret string `json:"passwordSecret"`

	// name is the logical database to capture (database.dbname). Required by
	// the PostgreSQL connector; ignored by engines that do not take it.
	// +optional
	Name string `json:"name,omitempty"`
}

// AlertRule maps a Prometheus alerting rule into a generated PrometheusRule.
type AlertRule struct {
	// name is the alert name.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// expr is the PromQL expression that triggers the alert.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Expr string `json:"expr"`

	// for is the duration the expression must be true before the alert fires
	// (for example "5m").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	For string `json:"for"`

	// severity is the alert severity label.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=warning;critical
	Severity string `json:"severity"`
}

// PrometheusSink configures the always-on Prometheus observability backend.
type PrometheusSink struct {
	// namespace is where the ServiceMonitor is created. Defaults to the
	// operator Service namespace when empty.
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// serviceMonitor controls whether a ServiceMonitor (and its backing
	// Service) is created for Prometheus to scrape the operator.
	// +optional
	ServiceMonitor bool `json:"serviceMonitor,omitempty"`

	// alertRules are the alerting rules rendered into a PrometheusRule.
	// +optional
	AlertRules []AlertRule `json:"alertRules,omitempty"`
}

// DatadogSink configures pushing metrics and alerts to Datadog.
type DatadogSink struct {
	// enabled turns the Datadog sink on.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// apiKeySecret is the name of a Kubernetes Secret holding the Datadog API key.
	// +optional
	APIKeySecret string `json:"apiKeySecret,omitempty"`

	// appKeySecret is the name of a Kubernetes Secret holding the Datadog app key.
	// +optional
	AppKeySecret string `json:"appKeySecret,omitempty"`

	// tags are extra tags attached to metrics and alerts pushed to Datadog.
	// +optional
	Tags []string `json:"tags,omitempty"`
}

// WebhookSink configures delivering alerts as HTTP POSTs.
type WebhookSink struct {
	// enabled turns the webhook sink on.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// url is the webhook endpoint that receives the alert JSON payload.
	// +optional
	URL string `json:"url,omitempty"`

	// triggerOn lists the alert names whose firing triggers a webhook POST.
	// +optional
	TriggerOn []string `json:"triggerOn,omitempty"`
}

// SinkConfig selects and configures the observability backends. The Prometheus
// `/metrics` endpoint is always served regardless of this value; the other
// sinks are additive.
type SinkConfig struct {
	// type selects which sinks are active.
	// +optional
	// +kubebuilder:validation:Enum=prometheus;datadog;webhook;all
	Type string `json:"type,omitempty"`

	// prometheus configures the Prometheus sink.
	// +optional
	Prometheus *PrometheusSink `json:"prometheus,omitempty"`

	// datadog configures the Datadog sink.
	// +optional
	Datadog *DatadogSink `json:"datadog,omitempty"`

	// webhook configures the webhook sink.
	// +optional
	Webhook *WebhookSink `json:"webhook,omitempty"`
}

// TransportConfig selects and configures the Debezium delivery transport: how
// CDC events reach the operator. It is orthogonal to spec.sink (ADR-012):
// spec.transport is the ingress for events, while spec.sink is the egress for
// derived metrics and alerts. HTTP is the default because it needs no broker.
type TransportConfig struct {
	// type selects the event transport: "http" (default, no broker required)
	// or "kafka".
	// +optional
	// +kubebuilder:validation:Enum=http;kafka
	// +kubebuilder:default=http
	Type string `json:"type,omitempty"`

	// http configures the HTTP transport; ignored when type is "kafka".
	// +optional
	HTTP *HTTPTransport `json:"http,omitempty"`

	// kafka configures the Kafka transport; ignored when type is "http".
	// +optional
	Kafka *KafkaTransport `json:"kafka,omitempty"`
}

// HTTPTransport configures the HTTP event transport.
type HTTPTransport struct {
	// url is the operator's CDC ingestion endpoint that the Debezium Server
	// HTTP sink POSTs events to (for example
	// "http://db-sensor-operator.db-sensor.svc:8080/cdc"). When empty, the
	// generated DebeziumServer keeps a placeholder sink and delivers nothing;
	// that is only appropriate for the synthetic demo, which POSTs events
	// to /cdc itself.
	// +optional
	URL string `json:"url,omitempty"`
}

// KafkaTransport configures the Kafka event transport.
type KafkaTransport struct {
	// brokers is the list of Kafka bootstrap broker addresses.
	// +optional
	Brokers []string `json:"brokers,omitempty"`

	// topicPrefix is the prefix Debezium uses for its event topics.
	// +optional
	TopicPrefix string `json:"topicPrefix,omitempty"`

	// groupId is the Kafka consumer group the operator joins.
	// +optional
	GroupID string `json:"groupId,omitempty"`

	// topic is the explicit topic to consume; takes precedence over
	// topicPrefix when set.
	// +optional
	Topic string `json:"topic,omitempty"`
}

// ThresholdConfig overrides the operator's health thresholds for a single
// DatabaseSensor. Each field is an optional Go duration string (parsed with
// time.ParseDuration, for example "30s" or "2m"); when a field is unset or
// unparseable the operator falls back to its built-in default constant, so an
// omitted block keeps today's behaviour exactly. Thresholds are per-sensor
// only; per-table thresholds are not supported.
//
// Ordering is expected to be lagCrit >= lagWarn and noEventFail >= noEventWarn.
// This is not enforced by an admission webhook today (the defensive parse
// fallback keeps a malformed value from breaking the checker), so an inverted
// pair is accepted and evaluated as written.
type ThresholdConfig struct {
	// lagWarn is the replication-lag warning threshold; lag at or above it moves
	// the sensor to Degraded (for example "30s"). Defaults to 30s.
	// +optional
	// +kubebuilder:default="30s"
	LagWarn string `json:"lagWarn,omitempty"`

	// lagCrit is the replication-lag critical threshold; lag above it moves the
	// sensor to Failed (for example "2m"). Defaults to 2m.
	// +optional
	// +kubebuilder:default="2m"
	LagCrit string `json:"lagCrit,omitempty"`

	// noEventWarn is how long without a CDC event moves the sensor to Degraded
	// (for example "5m"). Defaults to 5m.
	// +optional
	// +kubebuilder:default="5m"
	NoEventWarn string `json:"noEventWarn,omitempty"`

	// noEventFail is how long without a CDC event moves the sensor to Failed
	// (for example "30m"). Defaults to 30m.
	// +optional
	// +kubebuilder:default="30m"
	NoEventFail string `json:"noEventFail,omitempty"`
}

// DashboardConfig configures the optional embedded dashboard.
type DashboardConfig struct {
	// enabled turns the embedded dashboard (/dashboard, /api/events) on.
	// +optional
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// retention is how long recent events are kept for the dashboard
	// (for example "72h").
	// +optional
	Retention string `json:"retention,omitempty"`
}

// DatabaseSensorSpec defines the desired state of DatabaseSensor
// +kubebuilder:validation:XValidation:rule="!(has(self.snapshot) && has(self.snapshot.inspection) && self.snapshot.inspection.enabled) || self.database.type == 'postgresql'",message="spec.snapshot.inspection is only available for postgresql"
// +kubebuilder:validation:XValidation:rule="!(has(self.snapshot) && has(self.snapshot.inspection) && self.snapshot.inspection.enabled) || (has(self.database.name) && self.database.name != ”)",message="spec.snapshot.inspection requires spec.database.name"
type DatabaseSensorSpec struct {
	// database is the connection to the database to monitor.
	// +kubebuilder:validation:Required
	Database DatabaseConfig `json:"database"`

	// tables are the tables to capture changes from.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Tables []string `json:"tables"`

	// sink configures the observability backends alerts and metrics fan out to.
	// +optional
	Sink SinkConfig `json:"sink,omitempty"`

	// transport configures how Debezium CDC events are delivered to the
	// operator. It is the event ingress and is orthogonal to spec.sink (which
	// is the metrics/alert egress) per ADR-012. Defaults to the HTTP transport.
	// +optional
	Transport *TransportConfig `json:"transport,omitempty"`

	// dashboard configures the optional embedded dashboard.
	// +optional
	Dashboard *DashboardConfig `json:"dashboard,omitempty"`

	// thresholds overrides the per-sensor health thresholds (lag warning/critical
	// and no-event warning/failure timeouts). Each field falls back to the
	// operator default when unset, so an omitted block keeps today's behaviour.
	// +optional
	Thresholds *ThresholdConfig `json:"thresholds,omitempty"`

	// snapshot configures how the Debezium connector starts and how it keeps its
	// position across restarts (ADR-014). An omitted block keeps today's behaviour.
	// +optional
	Snapshot *SnapshotConfig `json:"snapshot,omitempty"`
}

// SnapshotConfig configures Debezium's start behaviour and offset persistence.
type SnapshotConfig struct {
	// offsetStore selects where Debezium keeps its position in the change log.
	// "memory" (default) is lost when the Debezium pod restarts, so a restart
	// can re-deliver events the operator already counted. "persistent" stores the
	// offset on a PersistentVolumeClaim created by the Debezium Operator, so a
	// restart resumes where it stopped. Switching to "persistent" on a running
	// sensor recreates the Debezium pod with a new volume.
	// +optional
	// +kubebuilder:validation:Enum=memory;persistent
	// +kubebuilder:default=memory
	OffsetStore string `json:"offsetStore,omitempty"`

	// inspection configures the read-only inspection of the database that feeds the
	// diagnostics in status.snapshot.inspection. It is off unless enabled.
	// +optional
	Inspection *InspectionConfig `json:"inspection,omitempty"`
}

// InspectionConfig is the user's intent for database inspection. It holds wishes only;
// whatever the inspector measures is reported in status, never here.
type InspectionConfig struct {
	// enabled turns the inspection on. When off (the default) the operator opens no
	// connection to the database and creates no inspection Job.
	// +optional
	// +kubebuilder:default=false
	Enabled bool `json:"enabled,omitempty"`

	// intervalSeconds is how often the database is inspected again. 0 inspects once per
	// change of the spec. A positive value must be at least 60 so a typo cannot turn the
	// inspection into a load on the database.
	// +optional
	// +kubebuilder:default=600
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=86400
	// +kubebuilder:validation:XValidation:rule="self == 0 || self >= 60",message="intervalSeconds must be 0 or at least 60"
	IntervalSeconds *int32 `json:"intervalSeconds,omitempty"`

	// timeoutSeconds is the deadline of one inspection Job. It bounds the Job, not the
	// reconcile, which never waits for it.
	// +optional
	// +kubebuilder:default=30
	// +kubebuilder:validation:Minimum=5
	// +kubebuilder:validation:Maximum=300
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
}

// DatabaseSensorStatus defines the observed state of DatabaseSensor.
//
// This is a hybrid status (see docs/adr/ADR-011-hybrid-status-design.md):
// Conditions are the source of truth for health/readiness state; measurement
// fields (Lag, EventsPerMin, LastEvent, DebeziumServer) stay plain because they
// are point-in-time values; Phase is derived from Conditions as a convenience
// column for `kubectl get`.
type DatabaseSensorStatus struct {
	// conditions represent the current state of the DatabaseSensor resource and
	// are the source of truth for health and readiness. Error state is carried
	// by a condition's reason/message rather than a dedicated field.
	//
	// Standard condition types include:
	// - "Ready": the sensor is healthy end-to-end
	// - "DebeziumAvailable": the child DebeziumServer pod is running
	// - "ReceivingEvents": CDC events have arrived within the no-event timeout
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// phase is a convenience summary derived from Conditions on each reconcile.
	// One of: Pending, Running, Degraded, Failed.
	// +optional
	Phase string `json:"phase,omitempty"`

	// lag is the current replication lag as a duration string (for example "1m30s").
	// +optional
	Lag string `json:"lag,omitempty"`

	// eventsPerMin is the current CDC event rate per minute.
	// +optional
	EventsPerMin float64 `json:"eventsPerMin,omitempty"`

	// lastEvent is the timestamp of the most recent CDC event received.
	// +optional
	LastEvent *metav1.Time `json:"lastEvent,omitempty"`

	// debeziumServer is the name of the managed DebeziumServer resource.
	// +optional
	DebeziumServer string `json:"debeziumServer,omitempty"`

	// snapshot reports how the connector starts and where it keeps its offset,
	// with a one-line reason (ADR-014). It is derived from the spec, so it is
	// present even when the sensor is not healthy.
	// +optional
	Snapshot *SnapshotStatus `json:"snapshot,omitempty"`
}

// SnapshotStatus explains the connector's start behaviour in one line.
type SnapshotStatus struct {
	// strategy is the START MODE of the connector: "no-data" (it starts without reading
	// existing rows) or "engine-default" (the Debezium default snapshot applies). It is not
	// the capture strategy. Whether and how existing rows would be delivered is
	// plan.tables[].backfill. The two sound alike and mean different things: starting without
	// reading rows is how every path begins, including the ones that backfill afterwards.
	// +optional
	Strategy string `json:"strategy,omitempty"`

	// offsetStore is the offset store in effect: "memory" or "persistent".
	// +optional
	OffsetStore string `json:"offsetStore,omitempty"`

	// reason is a one-line explanation of the strategy and its consequences.
	// It never contains secret values or row data.
	// +optional
	Reason string `json:"reason,omitempty"`

	// inspection holds the facts the last inspection observed. It is facts only: what the
	// user asked for lives in spec.snapshot.inspection.
	// +optional
	Inspection *InspectionStatus `json:"inspection,omitempty"`

	// plan is what the backfill policy would do per table. It is information, not
	// execution: nothing consumes backfilled rows yet, so applied is always false.
	// +optional
	Plan *SnapshotPlan `json:"plan,omitempty"`
}

// InspectionStatus is the observed result of one inspection.
type InspectionStatus struct {
	// factsVersion is the version of this facts layout. A status written by an older or newer
	// operator with a different layout must not be trusted field by field.
	FactsVersion int32 `json:"factsVersion"`

	// observedGeneration is the generation of the spec that was inspected. The facts are
	// stale once metadata.generation moves past it.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// observedAt is when the inspection finished, taken from the inspection Job, not from
	// the reconcile that read it.
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`

	// error is why the last inspection produced no facts, empty when it succeeded.
	// It never contains credentials.
	// +optional
	Error string `json:"error,omitempty"`

	// truncated is true when the result did not fit the inspection Job's termination message
	// and some tables are missing from tables.
	// +optional
	Truncated bool `json:"truncated,omitempty"`

	// tables are the facts of each monitored table, common to every engine.
	// +optional
	// +listType=map
	// +listMapKey=name
	Tables []TableInspection `json:"tables,omitempty"`

	// postgres holds the PostgreSQL-only facts. It is absent for other engines, and absent
	// also when the facts could not be read; status.conditions says which.
	// +optional
	Postgres *PostgresInspection `json:"postgres,omitempty"`
}

// TableInspection are the facts of one monitored table.
type TableInspection struct {
	// name is the table as named in spec.tables.
	Name string `json:"name"`

	// found is false when the table does not exist.
	Found bool `json:"found"`

	// estimatedRows is the row estimate, absent when unknown.
	// +optional
	EstimatedRows *int64 `json:"estimatedRows,omitempty"`

	// estimateReliable is false when the statistics behind estimatedRows are stale or missing.
	EstimateReliable bool `json:"estimateReliable"`

	// canWrite is whether the connection may create a signaling table in the table's schema.
	CanWrite bool `json:"canWrite"`
}

// PostgresInspection are the facts that exist only on PostgreSQL.
type PostgresInspection struct {
	// serverVersion is server_version_num, for example 160015.
	// +optional
	ServerVersion int32 `json:"serverVersion,omitempty"`

	// walLevel is the server's wal_level; logical decoding needs "logical".
	// +optional
	WALLevel string `json:"walLevel,omitempty"`

	// inRecovery is true when the server is a standby.
	InRecovery bool `json:"inRecovery"`

	// maxSlotWalKeepSize is the cap, in bytes, on WAL a replication slot may retain. -1 means
	// unlimited, so the free space of pg_wal is the only bound. Absent means it could not be
	// read, for example before PostgreSQL 13.
	// +optional
	MaxSlotWALKeepSize *int64 `json:"maxSlotWalKeepSize,omitempty"`

	// replicationSlot describes the connector's slot. Absent when it does not exist yet or
	// could not be read; it is never a zeroed object standing for "no slot".
	// +optional
	ReplicationSlot *SlotInspection `json:"replicationSlot,omitempty"`
}

// SlotInspection describes the connector's replication slot.
type SlotInspection struct {
	// name is the slot's name.
	Name string `json:"name"`

	// active is true while a connector is attached to the slot.
	Active bool `json:"active"`

	// walStatus is PostgreSQL's own judgment: reserved, extended, unreserved or lost.
	// +optional
	WALStatus string `json:"walStatus,omitempty"`

	// retainedBytes is the WAL kept for the slot, absent when unknown. It is cluster-wide WAL,
	// not only the captured tables.
	// +optional
	RetainedBytes *int64 `json:"retainedBytes,omitempty"`

	// confirmedLagBytes is how far the connector's last confirmed position trails the current
	// WAL position, absent when unknown. A single reading does not show a stalled connector.
	// +optional
	ConfirmedLagBytes *int64 `json:"confirmedLagBytes,omitempty"`

	// safeWalSize is PostgreSQL's own margin before the slot is at risk, in bytes. It is negative
	// once max_slot_wal_keep_size is exceeded (how far past it the slot is) and absent while that
	// cap is -1, because there is then nothing to measure against. It is not exactly the cap minus
	// retainedBytes, so it carries information the other two do not.
	// +optional
	SafeWALSize *int64 `json:"safeWalSize,omitempty"`
}

// SnapshotPlan is what the backfill policy would do. It is not executed: no sink consumes
// backfilled rows, so reading "backfill: signal-table" here does not mean a backfill is running.
type SnapshotPlan struct {
	// applied is false until something consumes backfilled rows; the plan is information only.
	Applied bool `json:"applied"`

	// tables is the decision per table.
	// +optional
	// +listType=map
	// +listMapKey=name
	Tables []TablePlan `json:"tables,omitempty"`
}

// TablePlan is the policy's decision for one table.
type TablePlan struct {
	// name is the table as named in spec.tables.
	Name string `json:"name"`

	// backfill is the capture strategy: whether and how existing rows would be delivered after
	// the connector has started. It is independent of the start mode in status.snapshot.strategy.
	// +kubebuilder:validation:Enum=stream-only;signal-table;read-only-incremental;none
	Backfill string `json:"backfill"`

	// reason is one line explaining the decision. It carries no credentials or row data.
	// +optional
	Reason string `json:"reason,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=dbsensor
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=='Ready')].status`
// +kubebuilder:printcolumn:name="Lag",type=string,JSONPath=`.status.lag`
// +kubebuilder:printcolumn:name="Events/Min",type=number,JSONPath=`.status.eventsPerMin`
// +kubebuilder:printcolumn:name="DB Type",type=string,JSONPath=`.spec.database.type`
// +kubebuilder:printcolumn:name="Last Error",type=string,priority=1,JSONPath=`.status.conditions[?(@.type=='Ready')].message`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// DatabaseSensor is the Schema for the databasesensors API
type DatabaseSensor struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of DatabaseSensor
	// +required
	Spec DatabaseSensorSpec `json:"spec"`

	// status defines the observed state of DatabaseSensor
	// +optional
	Status DatabaseSensorStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// DatabaseSensorList contains a list of DatabaseSensor
type DatabaseSensorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []DatabaseSensor `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &DatabaseSensor{}, &DatabaseSensorList{})
		return nil
	})
}
