# ADR-014 — Snapshot strategy: a policy layer that decides how the connector starts

- **Status:** Proposed
- **Date:** 2026-10-30
- **Context:** `ROADMAP.md` Tier 2 "Snapshot strategy / policy layer"; the
  article `docs/articles/to-watermark-or-not.md`; the CDC benchmark
  (`.kiro/specs/cdc-observability-benchmark/`). Relates to ADR-012 (HTTP
  transport, no broker) and ADR-013 (counts derived from the stream).
  Implemented by `.kiro/specs/snapshot-policy/`.
- **Prior art:** Netflix DBLog, *A Watermark Based Change-Data-Capture
  Framework* (Andreakis & Papapanagiotou, 2020),
  <https://arxiv.org/abs/2010.12597>. Debezium implements it as incremental
  snapshots; the read-only MySQL variant was documented by Shopify engineers:
  <https://debezium.io/blog/2022/04/07/read-only-incremental-snapshots/>.

## Context

Today the operator generates a `DebeziumServer` and sets no snapshot mode, so
Debezium's default (`initial`, a blocking snapshot) applies. Until the snapshot
ends the connector emits no live changes: on a large table the sensor is blind
for as long as the snapshot takes, and the replication slot retains WAL the
whole time.

Two facts about the current code sharpen the problem and change what a fix
should look like:

1. **The snapshot is worthless to the metrics.** Snapshot rows arrive as
   `op=r`. `Exporter.OnEvent` counts only `c`, `u`, `d`, `t` and `ddl`, so a
   snapshot adds nothing to any counter. The blocking snapshot is a pure cost
   for what this operator derives.
2. **The snapshot is not harmless to health.** `Pipeline.Handle` feeds every
   event, including `op=r`, to the `LagTracker` and the dashboard ring buffer.
   Right after the initial snapshot, before any workload, a sensor in the
   benchmark cluster reported `Degraded` with about 48 s of lag. Snapshot
   events carry the timestamp of the read, not of a live change. The cause is
   read from the code and not yet proven by an isolating test.

A third fact limits any "wait it out" answer: the Debezium Operator's generated
config uses `MemoryOffsetBackingStore` (observed in the ConfigMap it produced
in the benchmark cluster). A restart of the Debezium pod loses the offset and
triggers a full snapshot again. A policy that only picks the start mode is
undone by the first restart unless offset persistence is decided as well.

## Decision

1. **Add a decision step to reconcile.** The reconcile loop goes from
   "generate a static spec" to "inspect, decide, generate a spec". The
   operator is still an operator: it watches a CR, inspects the world, decides,
   reconciles. The watermark algorithm stays Debezium's job; Lactiflluus only
   chooses how to start the connector and whether to send a snapshot signal.
2. **Split the work into two phases, because the metrics do not need rows.**
   - **Phase A, non-blocking start (default).** Start the connector so it
     does not read existing rows, and stop snapshot events from poisoning lag
     and the dashboard. This needs no database inspection. It removes the blind
     window for every sensor and is enough for the observability use case.
   - **Phase B, opt-in backfill ladder.** For users who also want existing rows
     delivered, the operator inspects the table and picks a strategy:
     stream-only for small tables; a signaling table plus an `execute-snapshot`
     signal when it has write access; a read-only incremental snapshot
     (transaction-ID watermarks on PostgreSQL, GTID-set watermarks on MySQL)
     when it does not; `Degraded` with a one-line reason when no watermark
     path exists.
3. **PostgreSQL first, MySQL as a later phase.** The design uses a per-engine
   inspector interface (facts about a table) and an engine-independent policy
   (facts to strategy), so MySQL plugs in without a redesign. Only PostgreSQL
   is implemented and verifiable today.
4. **Decide offset persistence together with the start mode.** Persist the
   offset (the Debezium Operator supports persistent runtime storage and
   offset stores) so a restart resumes and does not re-snapshot.
5. **Surface the decision.** The chosen strategy and its reason appear in the
   sensor status, so "why is my sensor quiet?" has a one-line answer.

## Consequences

- **The operator needs a database connection for the first time.** Phase B
  must read the password Secret's value (today only its existence is checked),
  reach the database from the operator pod, and run read-only catalog queries.
  This is a new trust boundary and a new failure mode (inspection can fail while
  the connector is fine). Phase A needs none of it.
- **The write path is invasive.** A signaling table needs `CREATE TABLE`, an
  `INSERT` privilege for the signal, and inclusion in the connector's table and
  publication filters. It must be opt-in and documented as a change to the
  monitored database, like the triggers Approach in the benchmark.
- **A read-only MySQL signal may need Kafka.** Debezium's documented channel
  for the GTID read-only path is a Kafka topic, which conflicts with ADR-012's
  no-broker default. Whether Debezium Server can use another channel (for
  example `file`) for this is unverified and is an open question for phase 2.
- **The pinned Debezium floor matters.** PostgreSQL read-only replica support
  is recent: 3.6.2 fixed a failure on replicas and 3.7 announces incremental
  snapshots on read-only replicas. The benchmark verified 3.6.3. The exact floor
  per connector must be confirmed before it is documented as a guarantee.
- **The benchmark gains a variant.** The initial-snapshot cost is one of the
  three CDC costs the benchmark discloses. A non-blocking start is a mitigation
  worth measuring as a separate variant, reported next to the default.

## Alternatives considered

- **Keep the static spec and document the blind window.** Zero code, but the
  default experience on a large table is "Pending for hours", which undercuts
  the claim that the sensor is useful from the first event.
- **Always skip the snapshot and nothing else (Phase A only).** Simple and
  sufficient for the metrics, but gives no path for users who want existing
  rows, and does not address why a snapshot would be wanted at all. Kept as
  the default and as the first deliverable, not as the whole design.
- **Re-implement the watermark algorithm in Go.** Rejected. Debezium already
  implements it and it is battle-tested; independent implementations exist
  (pgcapture runs DBLog in Go against a logical replica) but would move the
  project into a different product.
- **Expose the strategy only as a CR field the user sets by hand.** Simpler and
  predictable, but pushes the inspection burden onto the user, which is the
  work an operator exists to do. Kept as an override on top of the automatic
  decision.

## Open questions

1. Is the backfill ladder worth building, given the metrics do not need rows?
   **Deferred (2026-10-30), then reopened (2026-10-31)** when the user ranked the policy layer above the benchmark; B1 and B2 (inspector and pure policy) are built. The original reasoning stands: nothing consumes backfilled rows today. Phase A shipped; Phase B waits for a consumer of
   existing rows: a feature that needs them, a wired Kafka transport, or a real
   user request. The triggers are recorded in the spec's `tasks.md`.
2. What are the exact Debezium floors per connector? The snapshot-mode names
   are answered for 3.6.3 only: `no_data` is the non-blocking mode and `never`
   is rejected (`bench/experiments/snapshot-modes/RESULTS.md`). A different
   pinned version must repeat the experiment.
3. Does an in-memory offset store make the operator's counters over-count
   after a Debezium restart? Yes on a graceful restart: one live event was
   re-delivered, twice in a row (`snapshot-modes` and `offset-persistence`
   experiments). A persistent file offset store removed it
   (`offset-persistence/RESULTS.md`). A hard kill can still replay up to one
   flush interval and was not tested.
4. Which signal channel can a Debezium Server use for the read-only MySQL path
   without a Kafka broker?
