# Testing Guide

How to develop and test the DatabaseSensor operator **without a Kubernetes
cluster, a running database, Kafka, or Datadog**. This guide is the practical
companion to the test-strategy sections in [`DESIGN.MD`](../DESIGN.MD) and
[`.kiro/specs/db-sensor/design.md`](../.kiro/specs/db-sensor/design.md) — read
those for the rationale; read this for how to actually run things.

## TL;DR

You do **not** need real infrastructure to build and test almost all of this
operator. The control-loop logic runs against an in-memory API server
(envtest), and everything that reacts to CDC traffic is tested by feeding
**synthetic events** and stubbing outbound calls with `httptest`. Only the final
end-to-end smoke test needs Docker (via a local [kind](https://kind.sigs.k8s.io/)
cluster), and even that does not need a production cluster or real data.

```bash
make test        # unit + integration (envtest). No cluster, no DB, no Kafka.
make lint        # static analysis
make test-e2e    # end-to-end against a local kind cluster (needs Docker)
```

## The three layers

| Layer | Infrastructure needed | What it covers | When it runs |
|---|---|---|---|
| **Unit** (testify) | none | Pure logic: CDC parser, `LagTracker`, sink implementations, phase derivation, spec→DebeziumServer mapping | every `make test`, milliseconds |
| **Integration** (envtest) | auto-downloaded binaries (no Docker) | Reconciler behavior: status convergence, owner references, missing-Secret handling, idempotency | every `make test` |
| **E2E** (kind) | Docker + kind | What envtest cannot fake: RBAC enforcement, networking, image pull, leader election | `make test-e2e`, local or CI |

The guiding principle (from `DESIGN.MD`): **test convergence to a desired
state, not real infrastructure.** We simulate the inputs the operator reacts to.

## Layer 1 — Unit tests (no Kubernetes at all)

Pure Go functions with inputs and expected outputs. These never touch the
Kubernetes API, so they need nothing installed.

- **CDC parser** (`internal/events`): hand-written Debezium JSON envelopes
  (insert `op=c`, update `op=u`, delete `op=d`, a DDL/schema-change event, and
  malformed input) as fixtures; assert `Parse` returns the right `CDCEvent` and
  errors cleanly on bad JSON without panicking.
- **`LagTracker`** (`internal/health`): feed a `ts_ms` from, e.g., 42 seconds
  ago and assert `GetLag()` ≈ 42s and the gauge is set; exercise concurrency.
- **Sinks** (`internal/sink`): `WebhookSink` and `DatadogSink` are pointed at an
  `httptest.Server` so the POST never leaves the process (see below).
- **Phase derivation** (`derivePhase` / health checker): a truth table of
  `(podRunning, lag, sinceLastEvent)` → `Pending|Running|Degraded|Failed`.
- **Deployer mapping** (`internal/deployer`): spec→DebeziumServer object for
  postgresql/mysql/mongodb (connector class + per-type extras + table filters +
  owner ref), plus the unsupported-type path.

Run just these:

```bash
go test ./internal/events/... ./internal/health/... ./internal/sink/... ./internal/deployer/...
```

## Layer 2 — Integration tests (envtest, still no cluster)

`envtest` is **not** a real cluster. `controller-runtime` downloads a real
`kube-apiserver` + `etcd` binary and runs them locally, in memory. The CRD is
installed, the reconciler runs for real, and tests assert that applying a CR
converges the status. No Docker, no kind, no cloud.

`make test` provisions the envtest binaries automatically (the `setup-envtest`
target derives the Kubernetes version from `go.mod`), so you normally just run:

```bash
make test
```

If you want to run the controller suite directly, point `KUBEBUILDER_ASSETS` at
the downloaded binaries (the Makefile does this for you):

```bash
make setup-envtest
export KUBEBUILDER_ASSETS="$(bin/setup-envtest use -p path)"
go test ./internal/controller/... -v
```

What the envtest suite covers:

- Apply a CR → an owned `DebeziumServer` appears with the correct spec and owner
  reference.
- Delete the CR → the `DebeziumServer` is garbage collected (owner-ref cascade).
- Missing Secret → status becomes `Failed` (Ready=False, Reason `SecretNotFound`).
- Status transitions converge (Pending → Running → Degraded) when the inputs are
  simulated.
- **Idempotency** ("second reconcile test"): reconciling twice does not create
  duplicates or thrash status.

### How envtest fakes external resources

- The **DebeziumServer** CRD has no Go module and the real schema is ~104 KB, so
  the suite installs a **minimal hand-written CRD**
  (`internal/controller/testdata/crd/debeziumservers.debezium.io.yaml`) into
  envtest's `CRDDirectoryPaths`. That is enough to verify `CreateOrUpdate` and
  owner-reference GC without the upstream operator.
- A **DebeziumServer pod state** (running, crashloop, etc.) is fabricated by
  writing the object's status directly, rather than waiting for a real pod.

## Simulating CDC events (the "no database" answer)

The metrics exporter, consumer, lag tracker, and health checker all react to a
stream of Debezium CDC events. We never need a real Postgres emitting changes —
events are just JSON, and the operator's job is to **consume and interpret**
them. We feed synthetic events instead:

| Real component | Test substitute | What it verifies |
|---|---|---|
| Postgres/MySQL/Mongo emitting changes | Hand-written Debezium JSON fixtures | `Parse` → correct `CDCEvent` |
| CDC event stream | Direct calls to `Exporter.OnEvent(ev)` | `/metrics` counters/gauges (scraped via `httptest`) |
| Clock / event age | A controlled `ts_ms` value | `LagTracker.GetLag()` and the lag gauge |
| Kafka broker | A mocked `Consumer` interface | consumer wiring without a broker |
| HTTP (webhook) sink source | `httptest.Server` POSTing a fake event | event reaches the exporter |
| Slack/webhook endpoint | `httptest.Server` capturing the POST | outbound JSON body shape + retry |
| Datadog API | Stub HTTP server (incl. a 429 response) | push payload + `Retry-After` handling |
| DebeziumServer pod phase | Fabricated object status in envtest | health checker → correct phase |

Example — asserting the exporter counts events without any broker:

```go
exp := metrics.NewExporter(prometheus.NewRegistry())
for i := 0; i < 3; i++ {
    exp.OnEvent(events.CDCEvent{Op: "c", DB: "pg", Table: "orders", TsMs: nowMs})
}
// scrape exp.Handler() with httptest and assert
// debezium_events_total{op="c",db="pg",table="orders"} == 3
```

### Prefer the HTTP sink for the dev/test loop

Debezium can deliver events over Kafka or HTTP. The **HTTP** path needs no
broker, so it is the default for local development and testing; the Kafka
consumer sits behind a `Consumer` interface and is unit-tested with a mock.
Nothing in the normal `make test` loop requires a running Kafka.

## Layer 3 — E2E tests (local kind cluster)

The e2e suite (`test/e2e/`) exists for what envtest cannot fake: real RBAC
enforcement, networking, image pull, and leader election. It runs against a
dedicated, disposable [kind](https://kind.sigs.k8s.io/) cluster — **not** a
real/production cluster, and it does **not** require real database data.

```bash
make test-e2e    # requires Docker + kind on PATH
```

This is the only layer that needs Docker. If Docker is unavailable locally,
leave e2e to CI — the scaffold ships `.github/workflows/test-e2e.yml` for that.
A fuller smoke test (throwaway Postgres + the Debezium operator in-cluster →
events flow → metrics appear → alert fires) is optional and, if added, also runs
in kind with synthetic traffic rather than production data.

## What needs what — quick reference

- **Just coding and running `make test`?** Nothing beyond Go and the
  auto-downloaded envtest binaries.
- **Running e2e locally?** Docker + kind.
- **Real database / Kafka / Datadog?** Never required for tests. Only relevant
  when actually deploying the operator to a cluster.

## See also

- [`DESIGN.MD`](../DESIGN.MD) — test pyramid, TDD-by-feature order, the three
  golden rules (convergence over actions, idempotency, 80% envtest).
- [`.kiro/specs/db-sensor/design.md`](../.kiro/specs/db-sensor/design.md) —
  per-component test lists and the `CDCEvent` contract.
- [`AGENTS.md`](../AGENTS.md) — kubebuilder/controller testing conventions.
