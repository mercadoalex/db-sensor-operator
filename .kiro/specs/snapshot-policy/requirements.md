# Requirements Document

## Introduction

This document specifies the **snapshot policy layer** for the `DatabaseSensor` operator (Lactiflluus). Today the generated `DebeziumServer` sets no snapshot mode, so Debezium's default (`initial`, a blocking snapshot) applies and a large table keeps the sensor blind until the snapshot ends. The decision to fix this, and why it is split into two phases, is recorded in [ADR-014](../../../docs/adr/ADR-014-snapshot-policy-layer.md); the motivating discussion is in `docs/articles/to-watermark-or-not.md`.

The central observation is that **the metrics do not need the snapshot.** Snapshot rows arrive as `op=r` and the exporter counts only `c`, `u`, `d`, `t` and `ddl`. So the work has two parts:

- **Phase A (core):** start the connector without a blocking snapshot, and keep snapshot events from distorting lag and the dashboard. No database inspection is needed.
- **Phase B (opt-in):** for users who want existing rows delivered, inspect the table and choose a backfill strategy (stream-only, signaling-table incremental snapshot, or read-only incremental snapshot).

The watermark algorithm stays Debezium's job. The operator chooses how to start the connector and whether to send a signal; it does not reimplement the algorithm.

Scope is **PostgreSQL first.** MySQL (GTID-based read-only snapshots) is designed for but not implemented here (Phase C).

### Glossary

- **Snapshot_Event**: a Debezium event with `op=r`, or whose `source.snapshot` is `true`, `first`, `first_in_data_collection`, `last_in_data_collection` or `last`. It reports an existing row, not a live change.
- **Live_Event**: any event that is not a Snapshot_Event.
- **Strategy**: the start mode plus the snapshot signal (if any) the operator selects for a sensor.
- **Backfill**: delivering existing rows after the connector is already streaming, via an incremental snapshot.
- **Inspector**: the per-engine component that reads facts about a table from the database.
- **Policy**: the engine-independent function from table facts to a Strategy.

## Requirements

### Requirement 1: Non-blocking start by default (Phase A)

**User Story:** As an SRE, I want a new sensor to report from the first live change, so that a large table does not leave it blind for hours.

#### Acceptance Criteria

1. WHEN a `DatabaseSensor` is applied without any snapshot configuration, THE SYSTEM SHALL generate a `DebeziumServer` whose source config starts the connector without a blocking snapshot of table data.
2. THE SYSTEM SHALL start the connector so that schema capture still works and live changes are delivered from the replication slot's start position.
3. WHEN the connector restarts, THE SYSTEM SHALL NOT trigger a new full snapshot because of the restart (see Requirement 3).
4. THE SYSTEM SHALL document that existing rows are not delivered under this default, and that rates, lag and counts are unaffected because they derive from Live_Events.

### Requirement 2: Snapshot events must not distort health or history (Phase A)

**User Story:** As an SRE, I want a snapshot to leave my lag and dashboard accurate, so that a healthy sensor is not reported `Degraded` because of how old a snapshot read is.

#### Acceptance Criteria

1. WHEN a Snapshot_Event is received, THE SYSTEM SHALL NOT update the `LagTracker`.
2. WHEN a Snapshot_Event is received, THE SYSTEM SHALL NOT count it in `debezium_events_total`, `debezium_events_per_minute` or `debezium_schema_changes_total`.
3. WHEN a Snapshot_Event is received, THE SYSTEM SHALL exclude it from the dashboard's recent-events buffer unless the user enabled backfill (Requirement 6).
4. WHEN a Live_Event is received, THE SYSTEM SHALL behave exactly as before this change.
5. THE SYSTEM SHALL identify a Snapshot_Event using the event's own fields (`op`, `source.snapshot`) and SHALL NOT infer it from timestamps.

### Requirement 3: Offset persistence

**User Story:** As a platform engineer, I want a Debezium restart to resume, not re-snapshot, so that the policy decision is not undone by the first pod restart.

#### Acceptance Criteria

1. THE SYSTEM SHALL record whether the generated `DebeziumServer` uses an in-memory or a persistent offset store, and SHALL report it in the sensor status.
2. WHERE a sensor enables a persistent offset store, THE SYSTEM SHALL configure the `DebeziumServer` runtime storage and offset store accordingly, using resources the Debezium Operator supports.
3. IF the offset store is in-memory, THEN THE SYSTEM SHALL state in the status reason that a restart will re-read the stream from the slot position and, when the start mode is not the non-blocking default, will re-snapshot.
4. THE SYSTEM SHALL NOT silently change an existing sensor's offset store on upgrade.

### Requirement 4: Strategy visibility

**User Story:** As a platform engineer, I want to see which strategy was chosen and why, so that a quiet sensor is explained in one line.

#### Acceptance Criteria

1. THE SYSTEM SHALL record the selected Strategy and a one-line reason in the sensor status.
2. WHEN no viable Strategy exists (Requirement 8), THE SYSTEM SHALL set the status to `Degraded` with a reason that names what is missing.
3. THE SYSTEM SHALL NOT include secret values, connection strings with credentials, or row data in any status message or log.

### Requirement 5: Opt-in backfill (Phase B)

**User Story:** As a data engineer, I want to opt in to receiving existing rows, so that I can have them without a blocking snapshot.

#### Acceptance Criteria

1. THE SYSTEM SHALL NOT perform any Backfill unless `spec.snapshot.backfill` is explicitly enabled.
2. WHEN backfill is enabled, THE SYSTEM SHALL inspect each monitored table and select a Strategy per Requirement 7.
3. THE SYSTEM SHALL support a manual override `spec.snapshot.strategy` that takes precedence over the automatic decision, and SHALL report that the override is in effect.
4. WHEN backfill is enabled, THE SYSTEM SHALL keep the connector streaming while the Backfill runs.
5. WHEN a Backfill has been requested for a table, THE SYSTEM SHALL NOT request it again on later reconciles while it is in progress or complete.

### Requirement 6: Backfill events reach the right consumers (Phase B)

#### Acceptance Criteria

1. WHEN backfill is enabled, THE SYSTEM SHALL make the Snapshot_Events of the Backfill visible in a way that does not alter rates, lag or counts.
2. WHEN backfill is enabled, THE SYSTEM SHALL track Backfill progress separately from live health, so a long Backfill never makes the sensor `Degraded` by itself.

### Requirement 7: Strategy selection (Phase B)

**User Story:** As an operator author, I want the strategy chosen from facts about the table, so that the user does not have to.

#### Acceptance Criteria

1. THE Inspector SHALL gather, per monitored table: an estimated row count, whether the connection can write to the database (privileges to create and insert into a signaling table), whether the server is a read-only replica (`pg_is_in_recovery()`), and the server's logical-decoding settings.
2. WHEN the estimated row count is at or below a configurable threshold, THE Policy SHALL select stream-only.
3. WHEN the table is above the threshold AND the connection can write, THE Policy SHALL select a signaling-table incremental snapshot.
4. WHEN the table is above the threshold AND the connection cannot write AND a read-only incremental snapshot is supported for the engine and pinned Debezium version, THE Policy SHALL select the read-only incremental snapshot.
5. THE Policy SHALL be a pure function of its inputs, with no I/O, so it is testable without a cluster.
6. THE Inspector SHALL use only read-only catalog queries.
7. THE SYSTEM SHALL use an estimate (planner statistics) for row count, not `count(*)`.

### Requirement 8: Degraded when no path exists (Phase B)

#### Acceptance Criteria

1. WHEN the table is above the threshold AND the connection cannot write AND no read-only path is available, THE SYSTEM SHALL select no Backfill, keep the connector streaming, and set the status to `Degraded` with the reason.
2. IF the Inspector cannot reach the database, THEN THE SYSTEM SHALL keep the existing Strategy, report an inspection error in the status, and SHALL NOT stop or restart a healthy connector because of it.

### Requirement 9: Trust boundary and invasiveness

**User Story:** As a security-minded platform engineer, I want any new database access to be explicit and minimal.

#### Acceptance Criteria

1. THE SYSTEM SHALL open a database connection from the operator only when backfill is enabled.
2. THE SYSTEM SHALL read the password Secret's value only in order to inspect, only for that sensor's referenced Secret, and SHALL NOT log or store it.
3. THE SYSTEM SHALL request only the Secret `get` permission it needs and SHALL document the RBAC added.
4. THE SYSTEM SHALL create a signaling table only when the signaling-table Strategy is selected, and SHALL document that this changes the monitored database.
5. THE SYSTEM SHALL support the TLS settings the connector uses when connecting for inspection.

### Requirement 10: Debezium version floor

#### Acceptance Criteria

1. THE SYSTEM SHALL document the minimum Debezium version for each Strategy and engine, each confirmed against that version before being stated.
2. IF the pinned Debezium version is below the floor for the selected Strategy, THEN THE SYSTEM SHALL NOT select that Strategy and SHALL report why.

### Requirement 11: Engine extensibility (Phase C, design only)

#### Acceptance Criteria

1. THE design SHALL separate the Inspector (per engine) from the Policy (engine independent), so that a MySQL Inspector can be added without changing the Policy.
2. THE design SHALL record, as an open question, which signal channel a Debezium Server can use for the read-only MySQL path without a Kafka broker (ADR-012).

### Requirement 12: Backward compatibility and verification

#### Acceptance Criteria

1. WHEN backfill is not enabled, THE SYSTEM SHALL change no behavior except the non-blocking default start and the Snapshot_Event handling in Requirements 1 and 2.
2. THE SYSTEM SHALL verify Phase A against a real PostgreSQL and Debezium Server in the benchmark cluster, not only with unit tests.
3. THE SYSTEM SHALL record which behaviors were verified against a real cluster and which only with unit or envtest tests.
