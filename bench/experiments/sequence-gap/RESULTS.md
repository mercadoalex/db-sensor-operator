# Sequence gap / duplicate detection (C2): results

Reproduce with `make -C bench setup && bench/collectors/cdc/up.sh && bench/experiments/sequence-gap/run.sh`.

- **Cluster:** kind, PostgreSQL 16, Debezium Operator 3.6.3, Lactifluus from this repo.
- **Scenario:** an `invoices` table with an `invoice_number` sequence. A stateful consumer reads the CDC stream, keeps the last number seen per sequence in memory, and flags a gap (`n != last+1`) or a duplicate (`n` already seen).
- **Run date:** 2026-11-05, one run.

## Hypothesis

A stream consumer can detect a skipped or duplicate sequence number — something a counter or a `GROUP BY` cannot, because it depends on order and history, not totals.

## What happened

| Part | Input | Consumer finding |
|---|---|---|
| 1. Gap | insert 1001..1005, then 1009 | `gap number=1009 prev=1005` ✓ |
| 2. Duplicate | insert 1009 again | `duplicate number=1009` ✓ |
| 3. Restart | kill consumer, then insert 1010 | nothing — state reset, `count_seen=1`, 1010 taken as a fresh baseline |

Verbatim after parts 1–2: `{"last":{"invoices":1009},"count_seen":6,"findings":[{"kind":"gap","number":1009,"prev":1005},{"kind":"duplicate","number":1009}]}`

## Verdict

**PASS for the capability.** Gap and duplicate were detected from the ordered stream. This is a
genuine CDC-only result: neither a `pg_stat` counter nor a periodic `GROUP BY` can find a skipped
or duplicated sequence number, because both report totals, not the order of values. This is a
cleaner "why CDC, not a counter" than the rate case (C1), which a periodic `GROUP BY ... HAVING`
could approximate.

## The honest limits, measured

- **Restart loses state (the big one).** The consumer keeps `last` per sequence in memory. On
  restart it starts empty, so the first number after a restart becomes a new baseline and a gap
  straddling the restart is missed (Part 3: 1010 flagged nothing). For anything that must not
  miss, `last` per sequence has to be persisted — more work, and a place for the state to live.
- **Order dependence.** Detection assumes events arrive in commit order. Debezium delivers in
  commit order, but a restart can re-deliver events (measured in offset-persistence), which would
  re-flag or miscount; the consumer is not idempotent here.
- **"Sequence" must be defined.** This assumed a single global `invoice_number`. Real sequences
  may be per-tenant or per-type, so the key is part of the rule, not a given.

## What this establishes (and what it does not)

- Establishes: the strongest form of the group-C claim. A stateful stream consumer detects
  order/history violations that no aggregate can. The operator is the natural place for that
  state (outside the DB, no trigger, no load on writes).
- Does NOT establish: that anyone needs sequence checking (R8); nor a production-grade consumer
  (state is in memory, not persisted, not idempotent on redelivery).

## Not covered

Persisted state, idempotent reprocessing, per-key sequences, out-of-order under load, one run only.
