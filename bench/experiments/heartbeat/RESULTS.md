# Heartbeat — results (ADR-018: A1 and E1)

Run: 2026-11-06, kind `db-sensor-bench`, PostgreSQL 16.15, Debezium Server
`quay.io/debezium/server:3.6.3.Final` (image taken from the running `bench-cdc` pod; the probe
pod used the same Debezium Operator 3.6.3 defaults), HTTP sink, publication `all_tables` (what the
operator creates today). One run per variant. Raw data: `bench/results/heartbeat-20261106/`
(`receiver.log`, `samples.csv`, `phases.tsv`, `summary.txt`). Reproduce with `./run.sh`.

## A1 — does the HTTP sink deliver heartbeats, and what do they look like?

**Hypothesis:** with `heartbeat.interval.ms=10000` and no writes, the endpoint gets about one POST
per interval, with `ts_ms` and none of `source`/`op`/`before`/`after`.

**Verdict: PASS.**

| Observation | Value |
|---|---|
| Control (no heartbeat configured), 92 s of silence | 0 requests |
| With heartbeat, 92 s of silence | 9 requests (about 9 expected) |
| Gap between heartbeats | median 10.0 s, max 10.5 s |
| Arrival minus `ts_ms` | median 0.21 s, max 0.44 s |
| Payload keys | `ts_ms` only |
| Envelope | `{"schema": {... "name": "io.debezium.connector.common.Heartbeat"}, "payload": {"ts_ms": ...}}` |
| Headers | `content-type`, `traceparent`, no topic or key header |
| 20 writes to the monitored table, 30 s window | 20 events and 3 heartbeats arrived together |

Consequences for the design: classify by fields (no topic is available over HTTP), as ADR-018
decision 2 says. The schema name `io.debezium.connector.common.Heartbeat` is an extra marker, but it
exists only when schemas are enabled, so it cannot be the rule. `Parse` already reads
`payload.ts_ms`, so a heartbeat today would parse as `Op=""`, `DB=""`, `Table=""` and reach the
`LagTracker` and `Exporter`, which is the problem the ADR describes.

## E1 — does the heartbeat keep `confirmed_flush_lsn` moving when every change is filtered?

**Hypothesis:** without heartbeat the gap `pg_current_wal_lsn() - confirmed_flush_lsn` grows with
the WAL written; with it, the gap stays bounded (peak at most 20% of the baseline growth).

**Verdict: PASS.** The baseline reproduced the problem, so the pass means something.

| Variant | WAL written (244 s) | Lag at end | Peak lag |
|---|---|---|---|
| no heartbeat | 2792 KiB | 2792 KiB | 2792 KiB (grew 100% of the WAL, never moved) |
| heartbeat 10 s | 2800 KiB | 210 KiB | 504 KiB (18% of the baseline) |

**The bound is about 60 s, not 10 s.** With heartbeat the lag is a sawtooth that drops about every
60 s (`confirmed_flush_lsn` advanced at 595, 605, 656, 666, 716, 727, 777 and 787 s, in pairs),
although a heartbeat arrives every 10 s. Peak lag is about 60 s of writes (~500 KiB at this rate).
This matches Debezium's offset flush period: the slot is confirmed when offsets are committed, not
when each heartbeat is emitted. The 60 s default is from a secondary source and the Debezium
Server settings of the probe were not read, so the cause is **inferred**, not proven; the
60 s period itself is observed. Also observed: the first advance came about two minutes after the
connector started.

Consequences for the design:
- The worst-case WAL held back is `write rate × max(heartbeat interval, offset flush interval)`.
  A heartbeat interval below the offset flush interval buys nothing for the slot.
- A stall check based on "`confirmedLagBytes` grew since the last probe" would raise false alarms
  inside a sawtooth tooth. Use "`confirmed_flush_lsn` did not advance over a window longer than
  two offset flush periods, while heartbeats are fresh" (ADR-018 decision 6, updated).

## Not covered (still open)

- **E2:** idle monitored database while another database on the same server writes. Not run.
  Whether the interval alone moves the slot there is still unknown (assumption A3).
- **E1b:** publication mode `filtered` (`PUB_MODE=filtered ./run.sh`). Not run.
- MySQL, Kafka transport, a long transaction delaying heartbeats (E5), a paused process (E4).
- Single run per variant on a laptop. The 60 s period repeated four times within the run, but this is
  not a statistical result, and PostgreSQL was shared with the idle `bench-cdc` sensor.
- The `hb-probe` ran with in-memory offsets (the operator's default), as `bench-cdc` does.
