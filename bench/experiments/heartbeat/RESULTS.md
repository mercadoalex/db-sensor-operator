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

**With a 10 s heartbeat the lag is a sawtooth that drops about every 60 s** (`confirmed_flush_lsn`
advanced at 595, 605, 656, 666, 716, 727, 777 and 787 s, in pairs), although a heartbeat arrives every
10 s. Peak lag is about 60 s of writes (~500 KiB at this rate). Also observed: the first advance came about
two minutes after the connector started.

**Explanation withdrawn (2026-11-08).** On 2026-11-06 this file said the 60 s matched Debezium's offset
flush period and that a heartbeat interval below it buys nothing. The follow-up below contradicts that: with
the same `offset.flush.interval.ms=60000`, a 30 s heartbeat gave a sawtooth of about 30 s. The cause of the
tooth length is **not known**, and no formula is claimed.

Consequences for the design:
- The lag stayed bounded to about one tooth of writes in every run with a heartbeat. At this write rate that
  was roughly 590 KiB with a 10 s heartbeat and 310 KiB with a 30 s one (see below).
- A stall check based on "`confirmedLagBytes` grew since the last probe" would raise false alarms inside a
  tooth. Use "`confirmed_flush_lsn` did not advance over a window longer than the longest tooth seen (60 s),
  for example 2 minutes, while heartbeats are fresh" (ADR-018 decision 6).

## Follow-up through the operator (2026-11-08)

Same question, but through the real operator: the `bench-cdc` sensor, operator image built from `main` after
the heartbeat, Debezium Server 3.6.3, PostgreSQL 16.15, publication `all_tables`, in-memory offsets. Debezium's
startup log printed `offset.flush.interval.ms = 60000`. Load: about 10 writes/s of 1 KiB to a table outside
`table.include.list`; the lag is current WAL minus `confirmed_flush_lsn`, sampled about every 10 s. One run each.

| Setting | Run | What the lag did |
|---|---|---|
| none (operator image from before the heartbeat) | 90 s | 183 KiB to 1121 KiB, never dropped, still growing |
| 30 s (the default, no `spec.heartbeat`) | 240 s | dropped about every 30 s (t = 31, 61, 91, 121, 152, 182, 212 s); between about 60 and 310 KiB, with one higher peak of 433 KiB at the start |
| 10 s (`spec.heartbeat.intervalSeconds: 10`) | 200 s | dropped about every 60 s, in pairs 10 s apart (t = 21, 81, 142 s); peaks of 588, 561 and 554 KiB |

So a heartbeat bounded the lag in both cases, and in these two runs the **30 s default was tighter than 10 s**.
That is only two settings with one run each; do not read a rule from it.

Other things checked in the same session:
- The generated `DebeziumServer` carried `heartbeat.interval.ms: 30000`; `intervalSeconds: 10` gave `10000`;
  `0` removed the property; removing the block returned to `30000`; the API server rejected `3` with
  "intervalSeconds must be 0 or at least 5".
- With the tables quiet, Debezium logged "1 records sent during previous 00:00:30" every 30 s (3 in 90 s): the
  heartbeats. The sensor's `lastEvent` was not refreshed by them and no `debezium_*` series with an empty `db` or
  `table` label appeared (0 of them), so the pipeline change works.
- With those heartbeats flowing and nothing else, the sensor still went `Degraded` and `Ready=False` with reason
  `LagAboveCritical`: the quiet-table behaviour (`bench/experiments/quiet-table`). Phase 2 of ADR-018 is what
  addresses it; phase 1 does not.
- Debezium's own startup log says that with LSN flush mode `connector` the pgjdbc keepalive flush is disabled and
  suggests `heartbeat.action.query` to prevent WAL growth on low-activity databases. That is relevant to E2
  (an idle monitored database while another one writes), which is still not run.
- After the Debezium pod restarted with in-memory offsets, it re-delivered 5 earlier updates (the documented
  behaviour of ADR-014). One sample showed the slot inactive for a moment (`active = f` at t = 0 of the 10 s
  run) and active afterwards; not investigated.

## Not covered (still open)

- **E2:** idle monitored database while another database on the same server writes. Not run.
  Whether the interval alone moves the slot there is still unknown (assumption A3).
- **E1b:** publication mode `filtered` (`PUB_MODE=filtered ./run.sh`). Not run.
- MySQL, Kafka transport, a long transaction delaying heartbeats (E5), a paused process (E4).
- Single run per variant on a laptop; this is not a statistical result. The 2026-11-06 runs used a probe
  `DebeziumServer` written by hand; the 2026-11-08 runs used the operator.
- Both used in-memory offsets (the operator's default).
