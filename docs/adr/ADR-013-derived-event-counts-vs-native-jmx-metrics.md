# ADR-013 — Event counts: derive from the CDC stream now, native JMX scrape as a future path

- **Status:** Accepted
- **Date:** 2026-10-16
- **Context:** Task 6 (Prometheus Exporter) and Tasks 7–8 (event pipeline) of
  the db-sensor implementation plan. Relates to US-2 (derive observability
  metrics) and ADR-012 (HTTP default / Kafka optional transport).
- **Prior art:** Mario Fiore Vitale, *Detect data mutation patterns with
  Debezium* (Debezium blog, 2024-10-14):
  <https://debezium.io/blog/2024/10/14/Detect-data-mutation-patterns-with-Debezium/>.
  The post frames CDC as an observability source and motivated this decision.

## Context

The operator exposes per-operation, per-table counters
(`debezium_events_total{op,db,table}`) and the schema-change counter
(`debezium_schema_changes_total{db,table}`). It builds them by parsing each CDC
event in `internal/events` and incrementing the counters in the `Exporter`
(`OnEvent`).

As of Debezium 3.0, the connector itself exposes equivalent per-table counters
natively over **JMX**:

- `NumberOfCreateEventsSeen`
- `NumberOfUpdateEventsSeen`
- `NumberOfDeleteEventsSeen`
- `NumberOfTruncateEventsSeen`

These native counters are **opt-in** via `internal.advanced.metrics.enable=true`
and are currently **experimental** (the `internal.advanced.` prefix and the
blog's own framing signal that the shape may still change). They exist only
where there is a real Debezium/JMX endpoint to scrape.

Given that Debezium can now emit these counts itself, we need to record why this
operator still derives them from the event stream rather than scraping JMX.

## Decision

1. **Derive event counts from the CDC stream for now.** The parser classifies
   each event's op (`c`/`u`/`d`/`t`, or `ddl` for schema changes) and the
   `Exporter` increments the counters. This is the single source of the
   `debezium_events_total` and `debezium_schema_changes_total` families today.
2. **Treat native JMX counters as an alternative/future source, not a
   replacement.** Scraping Debezium's `NumberOf*EventsSeen` is a candidate future
   path — a complement that reuses maintainer-supported metrics — but it is not
   adopted now.
3. **No JMX dependency and no opt-in flag are introduced.** The operator does not
   require `internal.advanced.metrics.enable=true`, does not open a JMX
   connection, and does not depend on an experimental metric surface to produce
   its counters.

## Rationale

- **Works across both transports (ADR-012).** Stream derivation is identical
  whether events arrive over HTTP (the default) or Kafka, because everything
  downstream of the consumer sees only a `CDCEvent`. The HTTP transport — and the
  synthetic HTTP demo path — has no JMX endpoint at all, so JMX scraping would
  simply not apply there.
- **No experimental opt-in.** Deriving from the stream avoids coupling the
  operator's core metrics to an experimental, flag-gated feature whose shape may
  change between Debezium releases.
- **One code path, already in place.** The parser and exporter already run on
  every event to compute lag and events/min; deriving counts reuses that path
  rather than adding a second collection mechanism.

## Consequences

- The operator **duplicates work the connector can now do natively**. When a real
  Debezium/JMX endpoint exists, two systems count the same events; the operator's
  numbers are derived, not read from the connector's own counters.
- Correctness of the derived counts depends on the operator seeing every event
  (consistent with ADR-012's note that HTTP may drop a few in-flight events
  across a restart, while Kafka resumes from a committed offset). The native JMX
  counters are maintained by the connector regardless of what the operator
  observes.
- Adding the JMX path later is additive: a scrape source can feed the same metric
  families or sit alongside them, without changing the parser/exporter contract.

## Alternatives considered

- **Scrape Debezium's native JMX counters instead of deriving
  (`NumberOf*EventsSeen`):** reuses maintainer-supported metrics and avoids
  duplicating the connector's counting. Rejected as the source of truth *for now*
  because it adds a JMX scrape dependency, requires the experimental
  `internal.advanced.metrics.enable=true` opt-in, and only works where a real
  Debezium/JMX endpoint exists — not the HTTP transport or the synthetic HTTP
  demo path. Kept as a candidate future path (see `ROADMAP.md`).
- **Derive and scrape together (reconcile the two):** most complete, but doubles
  the moving parts and the experimental-flag surface before there is demand.
  Deferred to the same future-path discussion.

## Decision summary

Derive from stream for now; native JMX scrape is a candidate future path — see
[`ROADMAP.md`](../../ROADMAP.md).
