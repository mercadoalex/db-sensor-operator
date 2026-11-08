# ADR-018 — Heartbeat: keep the slot moving and tell a quiet table from a stalled connector

- **Status:** Accepted. Phase 1 is implemented (2026-11-07); phases 2 and 3 are not.
- **Date:** 2026-11-06
- **Context:** `docs/HANDOFF.md` "To do" item 4 (Heartbeat) and the open decision "Quiet
  table"; community conversations in `docs/real_user_conversations.md`. Relates to ADR-014
  (snapshot policy, slot inspection, `no_data` start), ADR-011 (hybrid status) and ADR-012
  (HTTP transport default, Kafka optional).
- **Evidence from the Debezium community (Zulip, Feb–Oct 2026):**
  - Everything filtered stalls the slot. Since DBZ-7816 the PostgreSQL connector advances the
    slot only from committed offsets, so a high-write database whose events are all filtered
    produces no records, the slot stops, and WAL accumulates. A maintainer (Chris Cranford)
    called `heartbeat.interval.ms` "the goto solution", often paired with
    `heartbeat.action.query`.
  - A Filter SMT can drop the heartbeat records too, and the slot stalls again (DBZ-9490).
    The maintainer's advice: add a predicate to every SMT in the chain that excludes the
    heartbeat topic.
  - A maintainer on what a heartbeat proves: it is triggered on the event-processing loop, so a
    blocked loop (his example: LogMiner churning 100M rows) delays it; and it shows that
    heartbeat data flows, "not necessarily that there aren't other issues". Its purpose is
    to keep offsets from going stale.
  - A deleted slot (idle-slot cleanup by a hosting provider) means data loss; the only recovery
    is a new snapshot. The advice was to alert in real time when the slot is inactive and the
    confirmed LSN is not advancing.

## Context

The generated `DebeziumServer` sets no heartbeat (`internal/deployer/debezium.go` sets
`snapshot.mode=no_data`, `plugin.name`, `slot.name`, and nothing about heartbeats). Three
consequences in the current code:

1. **WAL can grow with no signal from us.** `snapshot.mode=no_data` plus a narrow
   `table.include.list` is exactly the "mostly filtered" shape. The inspector reports
   `retainedBytes` and `confirmedLagBytes`, but a single reading "does not show a stalled
   connector" (field doc in `SlotInspection`), and nothing keeps the slot moving.
2. **A quiet table and a stalled Debezium look the same.** `decidePhase` keys off
   `LagTracker.LastEvent()`: no event for 5 min is `Degraded`, 30 min is `Failed`. HANDOFF
   decided to stay conservative until a heartbeat exists.
3. **A heartbeat reaching the pipeline today would do damage.** A heartbeat value is
   `{"ts_ms": ...}` with no `source`, `op` or `before/after` (shape to be confirmed, see
   Assumption A1). `Parse` would return `Op=""`, `DB=""`, `Table=""`, `Snapshot=false`.
   `Pipeline.Handle` would then (a) call `LagTracker.OnEvent`, resetting the no-events clock
   on every heartbeat, which hides the stall the heartbeat is meant to expose; and
   (b) call `Exporter.OnEvent`, which sets `debezium_lag_seconds` for the label pair
   `{db="",table=""}`. This is the "decide the order before adding it" warning in HANDOFF.

Two different jobs hide behind the word heartbeat, and they have different owners:

| Job | Who consumes it | Needs the sensor to receive it? |
|---|---|---|
| **A. Slot advancement.** Heartbeat records carry an offset that gets committed, so `confirmed_flush_lsn` moves even when every change is filtered. | PostgreSQL and the connector | No |
| **B. Liveness signal.** A periodic record proves the connector loop is running and the path to our endpoint works. | The sensor (Phase, conditions, alerts) | Yes |

## Decision

1. **Always configure the heartbeat on PostgreSQL sensors.** Add `heartbeat.interval.ms` to
   the generated source config. Default 30 s, configurable, `0` disables. Job A needs
   nothing else from the operator. MySQL gets the same property, as its use there is the same
   (offset freshness); verify before enabling it by default (see Open questions).
2. **Classify heartbeats at the parser, before any metric.** Add `CDCEvent.Heartbeat bool`.
   `Parse` sets it when the payload carries `ts_ms` and none of `source`, `op`, `before`,
   `after`, `ddl`, `historyRecord`, `schemaChange`. Detection uses the event's own fields,
   never a topic name (the HTTP sink does not send one), the same rule `isSnapshot` follows.
3. **Keep two independent clocks.** `Pipeline.Handle` routes a heartbeat to a new
   `HeartbeatTracker` and nowhere else: not the `Exporter` counters, not the `LagTracker`, not
   the dashboard ring buffer. The data-event clock keeps its current meaning, so an idle table
   still shows an old `lastEvent` and nothing is hidden.
4. **Use the heartbeat to refine the state, not to redefine it.**

   | Data events | Heartbeat | Phase | `ReceivingEvents` | Notes |
   |---|---|---|---|---|
   | recent | any | per current rules | True | unchanged |
   | none for ≥ `noEventWarn` | fresh | `Running` | False, reason `Idle` | quiet table; message says heartbeats are arriving |
   | none for ≥ `noEventWarn` | missing for ≥ `heartbeatStale` | `Degraded` | False, reason `HeartbeatMissing` | possible stall |
   | none for ≥ `noEventFail` | missing | `Failed` | existing `NoEventsStale` | unchanged |
   | never any | never any | `Pending` | Unknown | startup, unchanged |

   `heartbeatStale = max(3 × interval, 60 s)`. The generous floor follows the maintainer's note:
   a long transaction can delay a heartbeat without the connector being broken. A missing
   heartbeat alone never produces `Failed`; only the existing 30 min clock does.
   If the heartbeat is disabled (`interval = 0`), the table above collapses to today's
   behavior, so the decision "quiet stays `Degraded`" is unchanged for that case.
5. **Add one condition and two metrics.**
   - Condition `HeartbeatReceived` (`True` / `False` / `Unknown` when disabled or not yet seen).
   - `debezium_heartbeats_total` and `debezium_heartbeat_lag_seconds` (now minus heartbeat
     `ts_ms` at receipt). Label scheme follows the existing exporter metrics; the heartbeat
     carries no `db`/`table`, so these are per-sensor.
   - The heartbeat lag is an honest end-to-end latency for a quiet table, which the data lag
     cannot give.
6. **Use the heartbeat to make slot stall detection meaningful (HANDOFF item 4).** With
   heartbeats flowing, a slot whose `confirmed_flush_lsn` does not move means the slot is not
   advancing even though the pipeline is alive. Add condition `SlotAdvancing` to
   `internal/inspection/evaluate.go`: `False` when `confirmed_flush_lsn` is unchanged across a
   window of at least 2 minutes (twice the longest sawtooth tooth observed, 60 s with a 10 s
   heartbeat; the teeth were 30 s with the 30 s default) and the heartbeat is fresh. Do **not** test "`confirmedLagBytes` grew since the last probe": E1 showed
   that with a healthy heartbeat the lag is a sawtooth that rises for up to about 60 s between
   advances, so that test would fire inside every tooth. The previous reading and its time come
   from the sensor `status` (ADR-008: no controller memory), compared before the new reading
   overwrites it. Without a fresh heartbeat the condition stays `Unknown`, never `False`,
   because then the cause could be the connector and not the slot.
7. **Do not set `heartbeat.action.query` by default.** It needs write access and a table, which
   conflicts with the read-only posture of the inspector and Phase A. It is the second step,
   offered only if experiment E2 shows the interval alone is not enough (see Verification), and
   only after the inspector reports write access. If added, it must be a plain upsert of one
   timestamp row; the community example that merged and deleted two rows per schema bought
   nothing (LogMiner records the change vector even if the value is unchanged).
8. **Generate alert rules that match the signal.** `NoEvents` stays as a data-event rule but is
   downgraded to informational when a heartbeat is configured. Add `HeartbeatMissing`
   (critical) on `time() - debezium_last_heartbeat_timestamp_seconds > heartbeatStale`. This avoids
   paging on a table that is simply quiet.
9. **Kafka transport: heartbeats are on a separate topic.** `topic.prefix` is deterministic, so
   the heartbeat topic is `__debezium-heartbeat.<prefix>`. Today `KafkaConsumer` reads one
   topic. HTTP (the default) needs nothing. For Kafka, add a second reader or a topic list in a
   later phase; until then the sensor must report `HeartbeatReceived=Unknown` with reason
   `NotObservedOnKafka`, not `False`.

## Phasing

- **Phase 1 (smallest useful):** decision 1 only. One config line, a deployer test, and the
  guarantee that the slot keeps moving. No pipeline change, no new field in status. Because a
  heartbeat would then arrive at the HTTP endpoint, decision 2 and the routing in decision 3
  ship in the same change, otherwise the `{db="",table=""}` series and the masked clock appear.
- **Phase 2:** decisions 3–5 and 8 (trackers, state table, condition, metrics, alerts).
- **Phase 3:** decision 6 (`SlotAdvancing`).
- **Later, if needed:** decision 7 (action query) and decision 9 (Kafka).

## Phase 1, as built (2026-11-07)

- **Deployer:** on PostgreSQL the generated `DebeziumServer` sets `heartbeat.interval.ms` (30 s by
  default). Nothing is set for MySQL.
- **API:** `spec.heartbeat.intervalSeconds`, optional. `0` turns the heartbeat off, otherwise 5 to
  3600. The CRD rejects the block on MySQL. Both rules are enforced by CEL and covered by envtest
  specs (`internal/controller/heartbeat_schema_test.go`).
- **Parser:** `CDCEvent.Heartbeat`, set when a record has a timestamp and no op, source, before or
  after. Verified against the body Debezium Server 3.6.3 sent over the HTTP sink.
- **Pipeline:** a heartbeat is dropped before the exporter, the lag tracker and the dashboard
  history. Without this, each beat would have reset the no-events clock and created a series with
  empty `db` and `table` labels.
- **Not built:** the heartbeat tracker and its own clock, the `Idle` and `HeartbeatMissing` states,
  the `HeartbeatReceived` condition, the metrics and alert rules, `SlotAdvancing`, Kafka support.
  A heartbeat is therefore invisible to the user for now; its only effect is that the slot keeps
  moving.
- **Tested:** unit tests for the deployer, parser and pipeline (each fails if its fix is removed),
  and envtest specs for the CRD rules. **Run end to end in kind on 2026-11-08** through the operator
  (`bench/experiments/heartbeat/RESULTS.md`, follow-up): the generated `DebeziumServer` carried the
  property; Debezium logged one record per heartbeat interval; the sensor's `lastEvent` was not
  refreshed by heartbeats and no series with empty labels appeared; with writes to a table outside
  the include list the lag stayed bounded (peak about 310 KiB with the 30 s default and 590 KiB with
  10 s, against 1121 KiB and still growing without a heartbeat); the CRD accepted 0 and 5 to 3600 and
  rejected 3. One run each, `all_tables` publication.

## API surface

One optional block, no change to existing fields:

```yaml
spec:
  heartbeat:
    intervalSeconds: 30   # 0 disables; default 30 on PostgreSQL
```

`intervalSeconds` is validated with `Minimum=0`. A value below 5 would add record traffic
for no gain; this belongs in the validating webhook already on the roadmap, and until then a
CEL `XValidation` keeps it in `0` or `5..3600`. ADR-015 says not to add fields before user
conversations ask; the conversations did (stalled slot, WAL growth), and without the field a
user could not disable a heartbeat that interferes with their pipeline.

## Assumptions to verify before building

- **A1. Debezium Server's HTTP sink delivers heartbeat records to the endpoint, and their body
  is `{"ts_ms":...}` (or the `schema`/`payload` envelope).** **Verified 2026-11-06** (kind,
  PostgreSQL 16, Debezium Server 3.6.3): 9 of 9 expected in a 92 s silent window, gap 10 s, body
  `{"schema":{...,"name":"io.debezium.connector.common.Heartbeat"},"payload":{"ts_ms":...}}`, no
  topic or key header. Control without heartbeat: 0. See
  `bench/experiments/heartbeat/RESULTS.md`.
- **A2. Job A holds with the interval alone when the filtered changes are in the same
  database** (the case reported in the community). **Verified for `publication all_tables`**
  (what the operator creates today): the baseline held 100% of the 2.8 MiB written, with
  heartbeat the peak was 504 KiB. **Refinement (2026-11-08):** the lag stays bounded to about one
  sawtooth tooth, 60 s with a 10 s heartbeat and 30 s with a 30 s one; why is not known, and an earlier
  explanation based on the offset flush period was withdrawn (`RESULTS.md`). Publication mode
  `filtered` not run.
- **A3. Job A may not hold when the database is idle but the server is busy** (other databases
  writing). The connector only knows the position of changes it received. This is why the
  community pairs the interval with an action query, and why Debezium has the Technology
  Preview `lsn.flush.mode=connector_and_driver` (documented for low-activity databases; its docs
  say the slot can then pass the stored offset). Debezium's own startup log (3.6.3, seen
  2026-11-08) says the pgjdbc keepalive flush is disabled in mode `connector` and suggests
  `heartbeat.action.query` for low-activity databases. Unverified; test as E2.
- **A4. The generated config has no SMTs today**, so the "predicate on the heartbeat topic"
  advice does not apply yet. If the spec ever passes SMTs through, add a validation for it.

## Verification plan (kind, PostgreSQL 16, Debezium Server 3.6.3)

- **E1:** table filter excludes the busy table; write steadily to it. Assert
  `confirmedLagBytes` does not grow and the slot's `restart_lsn` advances, with and without
  `heartbeat.interval.ms`. Expect growth without, none with.
- **E2:** idle monitored database, a second database on the same server writing. Same
  assertions. Decides whether decision 7 is needed.
- **E3:** stop writes to the monitored table. Assert `Running` with `Idle`, heartbeat lag stays
  low, and no `NoEvents` page.
- **E4:** pause the Debezium process (`SIGSTOP`). Assert `HeartbeatMissing`, then `Degraded`
  after `heartbeatStale`, then `Failed` only after 30 min.
- **E5:** a long transaction delays heartbeats. Assert no `Failed` and no flapping.
- Unit tests: `Parse` classification (heartbeat, DDL envelope with `source`, snapshot read,
  malformed), `Pipeline.Handle` routing (heartbeat reaches only the tracker), `decidePhase`
  table above, `SlotAdvancing` over probe sequences including a decrease and a missing value.

## Consequences

- Sensors start emitting one extra record per interval into the HTTP endpoint. Negligible
  volume, but the parser must be robust to it from the first release of the change.
- The meaning of "quiet table" gets a cheap, honest answer, which closes the open decision in
  HANDOFF for sensors that keep the heartbeat on.
- We state exactly what a heartbeat proves: that the connector's loop runs and the path to our
  endpoint works. It does not prove that changes to every table are captured, and the docs and
  the condition messages must not say so.
- A new place where a Debezium default is overridden (`heartbeat.interval.ms=0`), so the
  Debezium version pin and the A1 check matter.

## Open questions for the owner

1. Default on for PostgreSQL at 30 s, or opt-in? This ADR proposes on, because the failure it
   prevents is silent and expensive (disk full on the source database).
2. Should MySQL get the heartbeat in Phase 1, or wait until it can be tested? Only PostgreSQL
   is verifiable today.
3. Is `HeartbeatMissing` critical enough to page by default, given the maintainer's note that
   delays happen under heavy load?
4. Do we want a documented recommendation for `lsn.flush.mode`, or ignore it while it is a
   Technology Preview?
