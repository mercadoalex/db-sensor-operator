# Offset-persistence experiment: results

Task A4 of the snapshot policy (ADR-014), following
`../snapshot-modes/RESULTS.md`. Reproduce with
`make -C bench setup && bench/experiments/offset-persistence/run.sh`.

- **Debezium Operator / Server:** 3.6.3
- **PostgreSQL:** 16, `pgoutput`, `snapshot.mode=no_data`, `public.orders`
- **Procedure per variant:** one live INSERT, 6 s wait, delete the Debezium pod
  (graceful), wait for it to be ready, 25 s wait, count events that arrive again
  at the receiver, then one more INSERT to prove delivery continues
- **Run date:** 2026-10-30/31, one run per variant on a single kind node

## Results

| Variant | Re-delivered after restart | New INSERT delivered |
|---|---|---|
| `memory` (Operator default) | **1** | yes |
| `file`, default flush (60 s) | 0 | yes |
| `file`, `flushMs: 1000` | 0 | yes |

A persistent file offset store removed the re-delivery. The default 60 s flush
was enough here because a graceful pod deletion lets Debezium commit its offset
on shutdown.

## Two gotchas found while getting it to work

1. **The Debezium Operator rejects `runtime.storage.data.type: persistent`
   without a PVC template.** Its log says `Missing PVC template` and no pod is
   created at all, with no status on the CR. The template goes in
   `runtime.templates.volumeClaim` (`accessModes`, `resources.requests.storage`).
2. **A relative `source.offset.file.fileName` fails silently.** The documentation
   says the name is "relative to data root", but the file is resolved against the
   process working directory, which is not writable. The pod runs and delivers
   events, while every offset flush logs `Flush of the offsets failed` and no
   file is ever written. This variant also showed 1 event re-delivered, so a
   persistent volume with a relative path is no better than memory. The path must
   be absolute: `/debezium/data/offsets.dat`.

## Verified through the operator

A `DatabaseSensor` with `spec.snapshot.offsetStore: persistent` produced a
`DebeziumServer` with the PVC template and the absolute path, and the Debezium
Operator created a bound 100Mi claim. After the workload (252/205/43) and a
restart of the Debezium pod, the operator's counters stayed at 252/205/43 with
no `op="r"` series, and one extra INSERT moved the insert count to 253.

## Not covered

- **Hard failures.** Only a graceful pod deletion was tested. A kill, a node
  loss or an OOM can lose up to one flush interval of offset, so up to
  `flushMs` of events may still be replayed. That case was not run.
- **The re-delivered event is inferred, not proven.** The receiver does not log
  keys; "one INSERT made, one event re-delivered" is consistent but not a
  per-row identity check.
- A single Debezium version, one run per variant, no timing measured.
- Offset stores other than `memory` and `file` (ConfigMap, JDBC, Kafka, Redis).
- Whether the volume survives `kubectl delete` of the CR (it is owned by the
  DebeziumServer, so it is expected to be deleted with it; not checked).
