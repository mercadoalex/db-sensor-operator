# Demo — synthetic-event walkthrough

A rehearsable ~5-minute demo of the Lactiflluus (DatabaseSensor) operator that
needs **only Docker + kind locally** — no real database, no Debezium Server, no
Kafka, no Datadog.

It works by POSTing **synthetic Debezium events** to the operator's HTTP
ingestion endpoint (`POST /cdc` on `:8080`). The operator consumes them exactly
as it would real Debezium traffic, so the whole observability pipeline lights up:
derived metrics on `/metrics`, the live dashboard on `/dashboard`, recent events
on `/api/events`, and enriched status on `kubectl get databasesensor`.

> Why synthetic? The operator's job is to **consume and interpret** CDC events,
> and events are just JSON. Driving `/cdc` directly gives a reliable, fast demo
> with nothing to flake on stage. A full real-database demo (Postgres + the
> Debezium Operator) is a planned follow-up — see "Later: real database" below.

## Prerequisites

- Docker (running)
- `kind`, `kubectl`, `helm` on PATH — `brew install kind kubectl helm`

## Files

| File | What it is |
|---|---|
| `setup.sh` | Create a kind cluster, install Prometheus Operator CRDs, build+load the image, `helm install` the operator, create the dummy DB Secret |
| `sample-databasesensor.yaml` | The demo `DatabaseSensor` CR (dashboard enabled, HTTP transport, alert rules) |
| `generate-events.sh` | POST synthetic Debezium events to `/cdc` (steady / lag / DDL / burst modes) |
| `teardown.sh` | Remove the CR + release (and optionally the cluster) |

## Walkthrough

### 0. One-time setup

```bash
./demo/setup.sh
```

This stands up `kind-db-sensor-demo`, installs the ServiceMonitor + PrometheusRule
CRDs, builds `db-sensor-operator:demo`, loads it into kind, `helm install`s the
operator into the `db-sensor` namespace with the `:8080` HTTP server enabled, and
creates the `demo-db-creds` Secret the CR references.

### 1. Apply the sensor and watch it come up

```bash
kubectl apply -f demo/sample-databasesensor.yaml
kubectl -n db-sensor get databasesensor -w
```

Talking point: one declarative resource. Show the children the operator created:

```bash
kubectl -n db-sensor get debeziumservers,servicemonitors,prometheusrules,svc
```

Status starts `Pending` (no events yet), with a `Ready` condition.

### 2. Port-forward the operator HTTP server

```bash
# Service name is <release>-<chart>-operator-http; with release "db-sensor" that is:
kubectl -n db-sensor port-forward svc/db-sensor-db-sensor-operator-operator-http 8080:8080
```

> Tip: if unsure of the exact name, run
> `kubectl -n db-sensor get svc -o name | grep operator-http`.

Leave this running. Open the dashboard: <http://localhost:8080/dashboard>

### 3. Feed events — the pipeline lights up

In another terminal:

```bash
./demo/generate-events.sh          # steady ~5 events/sec, Ctrl-C to stop
```

Now show, live:

- Dashboard charts move (lag line, events/min bar, recent-events table).
- Metrics climb:
  ```bash
  curl -s http://localhost:8080/metrics | grep -E 'debezium_(events_total|lag_seconds|events_per_minute)'
  ```
- Recent events API:
  ```bash
  curl -s http://localhost:8080/api/events | jq .
  ```
- Status becomes `Running` with fresh lag/events-per-min:
  ```bash
  kubectl -n db-sensor get databasesensor demo-orders -o wide
  ```

### 4. Show a schema change (DDL)

```bash
./demo/generate-events.sh --ddl
curl -s http://localhost:8080/metrics | grep debezium_schema_changes_total
```

### 5. Show degradation — high lag

Stop the steady stream (Ctrl-C), then emit stale events (timestamps 60s in the
past) so lag crosses the 30s warning threshold:

```bash
./demo/generate-events.sh --lag 60
```

Watch `debezium_lag_seconds` exceed 30 and the status move toward `Degraded`
(the `HighLag` alert rule also matches on the Prometheus side).

### 6. Show degradation — no events

Stop generating entirely and wait. With no events arriving, lag climbs and the
sensor moves to `Degraded` then `Failed` per the no-event timeouts; the `NoEvents`
alert rule is the Prometheus-side counterpart.

```bash
# just stop ./demo/generate-events.sh and watch:
kubectl -n db-sensor get databasesensor demo-orders -w
```

### 7. Delete — everything cleans up

```bash
kubectl delete -f demo/sample-databasesensor.yaml
kubectl -n db-sensor get debeziumservers,servicemonitors,prometheusrules
```

The children are garbage-collected via owner references (same-namespace objects).

## Optional: show the webhook alert fan-out

Set a webhook catcher URL (e.g. a <https://webhook.site> URL) in
`sample-databasesensor.yaml` under `spec.sink.webhook` (`enabled: true`, `url:`),
re-apply, then trigger `--lag 60`. The operator POSTs an alert (name, db host,
value, threshold, timestamp, severity) to that URL. Left disabled by default so
the demo needs no external endpoint.

## Teardown

```bash
./demo/teardown.sh              # remove CR + release, keep the cluster
./demo/teardown.sh --cluster    # also delete the kind cluster
```

## Later: real database

To demo against a real Postgres + the official Debezium Operator (more
impressive, more moving parts), the shape is: install the Debezium Operator via
Helm, deploy a Postgres with logical decoding, point `spec.database` at it, and
let Debezium stream real changes to the operator (HTTP sink → `/cdc`, or Kafka).
That path is a planned follow-up and is intentionally not part of this
synthetic-event kit.

## Notes / caveats

- `kind`'s default CNI (kindnet) does **not** enforce NetworkPolicy; the chart's
  NetworkPolicy is off by default anyway (see the main README's NetworkPolicy &
  CNI section).
- The Prometheus **Operator** CRDs are installed so the operator can create
  ServiceMonitor/PrometheusRule objects, but the full Prometheus server/scraping
  stack is not deployed — the demo reads metrics directly from `/metrics`. Add
  kube-prometheus-stack if you want to show real scraping + Alertmanager.
- `demo-db-creds` is a dummy Secret; no real database is contacted.
