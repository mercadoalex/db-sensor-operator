# CDC-derived collector (Lactiflluus over Debezium, HTTP transport)

The CDC Approach: the official Debezium Operator runs a Debezium Server against
the benchmark PostgreSQL; Lactiflluus creates that `DebeziumServer` from the
`DatabaseSensor` in `sensor.yaml` and derives per-table counts and lag from the
event stream (ADR-013). Events travel over HTTP, so there is no Kafka broker
(ADR-012).

```bash
make -C bench setup        # cluster + PostgreSQL
make -C bench cdc-up       # Debezium Operator + Lactiflluus + the DatabaseSensor
make -C bench run          # drive the workload
bench/collectors/cdc/read.sh          # all debezium_* series
bench/collectors/cdc/read.sh events   # per-table insert/update/delete counts
bench/collectors/cdc/read.sh lag
make -C bench cdc-down     # remove the Approach and its replication slot
```

`read.sh` goes through the Kubernetes API service proxy, so no port-forward is needed.

## What it observes

Verified against the benchmark workload (seed 42, 500 operations):

| Signal | Result |
|---|---|
| Per-table insert / update / delete counts | Exact: 252 / 205 / 43 reported, matching the 252 / 205 / 43 the workload applied |
| Change lag (`now() - ts_ms`) | About 0.2 s under 50 ops/s |
| Per-row detail | Present in each event; Lactiflluus keeps only counts and a recent-events buffer |
| Schema / DDL changes | **Not observed on PostgreSQL** (see below) |

### DDL is not observed on PostgreSQL

An `ALTER TABLE ... ADD COLUMN` followed by `DROP COLUMN` on a monitored table
produced no `debezium_schema_changes_total` series. The next insert was counted
normally, so the stream stayed healthy; the DDL itself is simply absent.
PostgreSQL's logical decoding (`pgoutput`) does not carry DDL statements, so
Debezium's PostgreSQL connector has no schema-change event to emit. Connectors
that keep a schema history (MySQL, SQL Server, Oracle) do emit them.

This contradicts the "DDL ✅" cell for CDC in the design's capability matrix, which
the report must correct for PostgreSQL: **none of the four Approaches observes DDL
on PostgreSQL**, so DDL detection is not a point where CDC beats the alternatives
here.

## Standing costs (disclosed, never hidden)

- The Debezium Operator and a running Debezium Server (a JVM pod).
- A replication slot that retains WAL until consumed. `cdc-down` drops it.
- An initial snapshot of every monitored table on first start. Snapshot rows
  arrive as `op=r` and are deliberately not counted as inserts.
- The default offset store is in-memory, so a Debezium restart loses its offset.
  The experiments in `bench/experiments/` saw a restart re-read the table under
  `initial` and re-deliver a live event under `no_data`, so counts can be
  inflated after a restart. ADR-012 assumed events could only be lost; they can
  also be duplicated. Setting `spec.snapshot.offsetStore: persistent` removed the
  re-delivery on a graceful restart, at the cost of a PersistentVolumeClaim; a
  hard kill can still replay up to one flush interval. `sensor.yaml` keeps the
  in-memory default, and the benchmark must state which store a run used. A
  durable Kafka transport would add broker cost not measured here.

## Wiring notes

- `plugin.name=pgoutput` is set by the deployer; Debezium's default
  (`decoderbufs`) is not present in stock PostgreSQL.
- The password Secret must hold the key `password`. It is exposed to the
  Debezium pod as `DATABASE_password`; the password never appears in the
  `DebeziumServer` spec.
- `spec.database.name` is required for PostgreSQL (`database.dbname`).
- `spec.transport.http.url` points Debezium at the operator's `/cdc` endpoint.
  Without it the generated sink stays `none` (the synthetic demo path).
