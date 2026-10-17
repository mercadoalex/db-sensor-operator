# ADR-012 — Event transport: HTTP default, Kafka optional, selected via `spec.transport`

- **Status:** Accepted
- **Date:** 2026-10-16
- **Context:** Tasks 6–9 (event pipeline) of `.kiro/specs/db-sensor/tasks.md`;
  Task 7 (CDC event consumer) in particular. Relates to NFR-1 (throughput) and
  NFR-6 (restartable without data loss).

## Context

Debezium Server reads the database change log and emits each change as a CDC
event. It must deliver that event to this operator, which derives observability
metrics from it. Debezium supports two relevant delivery transports:

- **HTTP sink** — Debezium POSTs events to an endpoint the operator exposes.
- **Kafka sink** — Debezium publishes to a Kafka topic the operator consumes.

`DESIGN.MD` already treats Kafka/Redpanda as optional ("si el sink de Debezium
es Kafka; si es HTTP, no se necesita"). We need a decision on which is the
default and how a user switches.

## Decision

1. **HTTP is the default transport.** It requires no extra infrastructure
   (no broker), is testable with `httptest` and zero external dependencies, and
   lets the whole pipeline (parser → exporter → lag → health → status →
   dashboard) run end to end locally and in a `kind` e2e cluster without a Kafka
   deployment.
2. **Kafka is implemented up front**, not deferred, behind a shared `Consumer`
   interface (see below). It is unit-tested against a mock so the normal
   `make test` loop never needs a running broker. It is a first-class but
   secondary path, chosen by config.
3. **One `Consumer` interface, two implementations.** Everything downstream of
   the consumer sees only `CDCEvent` and is transport-agnostic. A small factory
   selects the implementation from config:

   ```go
   // internal/events/consumer.go
   type Consumer interface {
       Start(ctx context.Context, handler func(CDCEvent)) error
   }

   type HTTPConsumer  struct{ /* listens for POSTs */ }
   type KafkaConsumer struct{ /* subscribes to a topic */ }

   func NewConsumer(cfg TransportConfig, exporter *Exporter) (Consumer, error) {
       switch cfg.Type {
       case "kafka":
           return NewKafkaConsumer(cfg.Kafka, exporter), nil
       case "http", "":
           return NewHTTPConsumer(cfg.HTTP, exporter), nil
       default:
           return nil, fmt.Errorf("unknown transport %q", cfg.Type)
       }
   }
   ```

4. **A single CR field selects the transport**, kept SEPARATE from `spec.sink`.
   `spec.sink` is about *observability outputs* (Prometheus / Datadog / Webhook);
   the Debezium delivery transport is a different axis and gets its own block:

   ```yaml
   spec:
     database: { ... }
     tables: [ ... ]
     transport:
       type: http          # http (default) | kafka   <-- the single switch
       kafka:              # only read when type: kafka
         brokers: ["redpanda:9092"]
         topicPrefix: dbsensor
     sink:                 # unchanged: observability outputs, a different concept
       type: all
       prometheus: { ... }
       datadog: { ... }
       webhook: { ... }
   ```

5. **The one field drives BOTH ends.** On reconcile, `spec.transport.type`
   determines (a) how the generated `DebeziumServer` CR is configured to emit
   (deployer, Task 4) and (b) which `Consumer` the operator constructs (factory).
   Debezium (producer) and the operator (consumer) therefore always agree,
   because they are derived from the same source of truth.

## How a user switches later

Switching is **not auto-detected** — the operator cannot know a user wants
durability. It is a single declarative change:

1. Edit the CR: `spec.transport.type: http` → `kafka` (and supply `kafka`
   settings), then `kubectl apply`.
2. The reconciler updates the `DebeziumServer` CR (Debezium restarts with the
   Kafka sink) and swaps the operator's consumer: it stops the old consumer
   goroutine and starts the new one via the factory.

No operator redeploy and no code change are required — the Kafka code already
exists; only the selected implementation differs. The consumer lifecycle on a
config change (stop old, start new) is handled in the reconciler's consumer
wiring.

## Rationale

- **Fast, demonstrable pipeline with zero infra.** HTTP-first gets a working
  operator and a green test loop immediately; Kafka would otherwise gate every
  test on a broker.
- **No refactor to adopt Kafka.** Because the transport sits behind the
  `Consumer` interface and downstream code only sees `CDCEvent`, adding or
  switching to Kafka touches only the factory selection, not the metrics / lag /
  health / status logic.
- **Separation of concerns.** `spec.transport` (how events arrive) and
  `spec.sink` (where observability goes) are orthogonal; mixing them into one
  block would conflate a Debezium delivery mechanism with alerting backends.
- **Durability when it matters.** Kafka persists events and lets the consumer
  resume from its committed offset — the honest answer to a literal reading of
  NFR-6 and to sustained high throughput (NFR-1). For an observability operator,
  losing a few in-flight events across a brief restart is usually acceptable
  (metrics self-heal), so HTTP is a sound default; Kafka is available for
  stricter needs.

## Consequences

- Adds a `transport` block to the `DatabaseSensorSpec` (CRD change). This is a
  new field not present in the original flat spec sketch in `DESIGN.MD` §3; it is
  deliberately separate from `spec.sink`.
- The Task 4 deployer gains transport-aware DebeziumServer generation.
- The reconciler owns consumer lifecycle across `spec.transport` changes
  (stop/start on switch).
- Tests: HTTP consumer via `httptest`; Kafka consumer against a mocked broker
  interface; e2e (kind) uses HTTP by default, with an optional Kafka variant.

## Alternatives considered

- **Kafka as the default/primary:** maximal durability and throughput, but forces
  a broker into every dev/test/e2e loop and adds operational burden for a tool
  whose job is observability. Rejected as the default; kept as an opt-in.
- **Defer Kafka entirely (HTTP only for now):** simplest short-term, but leaves a
  half-wired interface and a later refactor. Rejected — the interface cost is
  already paid, so both implementations ship now.
- **Put transport inside `spec.sink`:** fewer top-level fields, but conflates two
  unrelated concepts (event delivery vs. observability output). Rejected for
  clarity.
