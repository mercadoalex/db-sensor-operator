# ADR-014 — Snapshot strategy: a policy layer that decides how the connector starts

- **Status:** Proposed
- **Date:** 2026-10-30
- **Context:** `ROADMAP.md` Tier 2 "Snapshot strategy / policy layer"; the
  article `docs/articles/to-watermark-or-not.md`; the CDC benchmark
  (`bench/`). Relates to ADR-012 (HTTP
  transport, no broker) and ADR-013 (counts derived from the stream).
  The requirements, design and task plan for this ADR are kept locally and are
  not versioned; the experiments that support it are under `bench/experiments/`.
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

## Deferred design: applies only once a backfill exists

Nothing below is built, and none of it can be measured today: the connector always starts
with `snapshot.mode=no_data`, so there is no blocking snapshot and no backfill to time.
It is recorded so it is not lost and not implemented blind.

**Goal and policy.** The goal is zero blindness: the sensor sees live changes from the
first moment and, when a backfill is wanted, existing rows are delivered without losing
that. Row counts are not the criterion; they plan the cost. A blocking `initial`
snapshot is not zero blindness: in the blind-window experiment no live change arrived
until all 100,089 rows had been delivered (57 s). An incremental snapshot keeps the live
stream flowing. So the mechanism follows capability, not size:

1. With write access or a verified read-only path, use the incremental snapshot, at any size.
2. With neither, use `initial` only if its estimated blind time fits the budget.
3. Otherwise report `Degraded` with the reason. Never omit the backfill silently.

"No backfill" stays an explicit user value (`stream-only`) and the default, so nothing
changes on upgrade. `auto` must never return it. `no_data` remains the start mode of every
path, since an incremental snapshot starts without reading rows and then receives a signal.
Resuming chunks after a restart only holds with a persistent offset store.

**Unit.** The threshold is time, not rows: the longest blind window the user accepts. Rows
only estimate it. The row estimate must come from a reliable source (see the inspector).

**Calibration.** Throughput is a property of one sensor (its sink, row width and network
path), not a constant of the project. It would live in `status.snapshot.calibration`
(`rowsPerSec`, `measuredAt`, `sampleRows`, `source: measured | default | user-override`,
`confidence: low` until three runs). The only measurement so far is one run: 100,089 rows
in 57 s, of which about 6 s was pod start, so roughly 1,960 rows per second net, on one
host with the local HTTP sink. It is a starting default, not a baseline. Storing it before a
backfill can produce samples would be a dead field that suggests a capability that is not there.

**WAL headroom gate.** Retention comes from the whole cluster's WAL, not from the captured
table, and full-page writes can make a small update cost a full page. It only matters for
a blocking snapshot, because a streaming connector keeps acknowledging. The headroom is
bounded by `max_slot_wal_keep_size` (PostgreSQL 13+; `-1` is unlimited), not by
`wal_keep_size`, which does not limit slots. With `-1` the real limit is the free space of
`pg_wal`, which SQL does not expose, so the headroom is unknown and must be treated
conservatively. The WAL rate can be sampled with `pg_wal_lsn_diff` over two
`pg_current_wal_lsn()` readings (not available on a standby), over a window long enough to
smooth checkpoint bursts. None of this was tested.

**Status shape.** The plan is reported as `status.snapshot.plan` with `applied: false` until
a consumer exists, so a status never reads as if a backfill were happening. `captureStrategy:
auto` is not exposed in the spec until it acts.

## Known limitations

These are accepted trade-offs of the table inspector, not bugs.

- **Autovacuum settings are not read.** Whether a row estimate is stale is judged with
  PostgreSQL's default `autovacuum_analyze_threshold` (50) and
  `autovacuum_analyze_scale_factor` (0.1), not the server's or a table's own values.
  If the server's settings are looser, the inspector flags staleness earlier than
  autovacuum would, which is the conservative direction. If they are stricter, the
  inspector is more lenient than autovacuum by at most about 10% of the estimate;
  taking the larger of `reltuples` and `n_live_tup` narrows that error.
- **Statistics counters can be reset.** `n_live_tup` and `n_mod_since_analyze` start
  from zero after crash recovery or `pg_stat_reset()`. Until the next `ANALYZE`, a
  stale `reltuples` can then look reliable. This was not tested.
- **"Can write" is approximated** by the CREATE privilege on the table's schema, not by
  an actual write.
- **PostgreSQL only.** There is no MySQL inspector yet.

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
