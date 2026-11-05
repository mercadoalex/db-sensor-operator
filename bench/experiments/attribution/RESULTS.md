# Attribution, approach 1 (session identity): results

Reproduce with `make -C bench setup && bench/collectors/cdc/up.sh && bench/experiments/attribution/run.sh`.

- **Cluster:** kind, PostgreSQL 16, Debezium Operator 3.6.3, Lactifluus from this repo.
- **Question:** can a data change be tied to the Job that made it, without guessing by a time window?
- **Method:** a Job sets `application_name=job/attrib-writer` on its connection, holds the transaction a few seconds, then writes a row. We checked (1) whether the raw CDC event carries the session identity, and (2) whether `pg_stat_activity` can correlate it.
- **Run date:** 2026-11-05, one run.

## What we saw

**Approach 1 — identity in the CDC stream: NO.** The event's `source` block (verbatim):

```
source={"version":"3.6.3.Final","connector":"postgresql","name":"bench-bench-cdc","ts_ms":...,
        "snapshot":"false","db":"bench","schema":"public","table":"orders","txId":778,"lsn":27694504, ...}
```

It carries the WAL position, the transaction id (`txId`), the server and the table. It does **not** carry `application_name`, the database user, or anything about the client. Debezium's source metadata describes the server and the transaction, not who opened the connection. So **CDC alone cannot attribute a change to a workload.**

**Approach 2 — correlate with `pg_stat_activity`: only in the moment, racy.** While the Job held its transaction, the view showed:

```
application_name=job/attrib-writer | state=active | query=... insert into public.orders ...
```

So the session identity exists and a Job can set it. But `pg_stat_activity` is a live view: once the session closes, the row is gone. The CDC event arrives after the commit, by which time the session may have ended. Correlating reliably would need to sample the view at the exact moment of commit, which is a race, not a dependable link.

## Verdict

**FAIL for session identity as a standalone attribution method.** CDC does not carry it, and `pg_stat_activity` does not retain it past the session. On its own it gives at best a best-effort, racy guess.

## The useful lead: `txId`

The CDC event does carry `txId` (778 here). That is a stable key for the transaction. If some other mechanism recorded "transaction 778 was opened by job/attrib-writer" at commit time, the event's `txId` would join the change to the workload exactly, with no time-window guessing. Nothing we have records that today; it is what approach 3 (eBPF) or a database-side hook would need to produce. This reframes the next experiment: not "who holds the session now" but "who owned this txId".

## What stays open

- **Approach: Cilium / Hubble.** Shows which pod connected to the database, at the network layer, independent of the app cooperating. Gives "pod X talked to the DB", still not "pod X made change Y" — would also need joining by time or txId. Needs a CNI that provides it; the bench uses kindnet, so this needs a cluster change. Not run.
- **Approach: eBPF.** Could observe the client/transaction at the kernel or protocol level and emit the txId-to-workload link the lead above wants. Most powerful, most work. Not run.
- **Lean on Kubernetes.** For a Job that is the sole writer in its window, the Job's identity plus the window already attributes, honestly, without any of the above. This is the cheap path when the assumption holds, and it is the one finding that is solid today.
- Whether anyone needs per-change attribution at all: user conversations (R8).

## Not covered

One run, no variance. No managed-PostgreSQL test (session behavior may differ). The `txId`-to-workload recorder was not built; it is a hypothesis.
