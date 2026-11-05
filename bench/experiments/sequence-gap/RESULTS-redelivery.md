# C2 v1 — the confirmed/processed frontier lives in Debezium: results

Reproduce with `bench/experiments/sequence-gap/run-redelivery.sh` (after setup + cdc/up.sh).

- **Cluster:** kind, PostgreSQL 16, Debezium Operator 3.6.3, offset store default (memory).
- **Design (owner's, adapted):** in our architecture the sequence consumer receives HTTP POSTs
  and does not confirm the slot — Debezium owns the offset. So the re-delivery that causes a
  false duplicate must come from restarting **Debezium**, not the consumer.
- **Hypothesis:** with the default in-memory offset, killing Debezium mid-stream makes it
  re-deliver already-seen events, and the stateful consumer raises a false duplicate.
- **Run date:** 2026-11-05, one run.

## Result

**Hypothesis confirmed, with a caveat about the baseline.**

- After Debezium was force-killed (in-memory offset) and came back, the consumer logged a
  **second `duplicate` for 1010** — an event it had already processed before the kill. That is a
  false positive caused by re-delivery, exactly as predicted. The confirmed/processed frontier is
  owned by Debezium, and a memory offset makes it re-deliver, so sequence detection downstream is
  unreliable without a persistent offset (or an idempotent consumer).

## Caveat — the baseline was not clean (experiment flaw, not a result)

Before the kill, the state already showed `out-of-order` for 1001..1009 and a `duplicate` for
1010. So the events did **not** reach the consumer in ascending order even on the clean run:
1010 (or a snapshot-style delivery) arrived first, then the rest. This means the run conflated
two things:
1. Re-delivery after a Debezium restart causes a false finding — **valid, this is the hypothesis.**
2. Delivery was already out of order on the normal run — **noise introduced by the setup** (fast
   serial inserts and/or Debezium startup ordering), not a clean baseline.

The order assumption C2 depends on ("events arrive in commit order") did not hold as set up here.
That is itself a finding: downstream sequence logic cannot assume in-order delivery without
checking it. But it muddied the restart test; a cleaner redo would (a) confirm an in-order clean
baseline first, (b) only then kill Debezium.

## What this establishes

- The restart/offset risk is real and lives in Debezium, not in the consumer. A persistent
  offset (already a feature, `spec.snapshot.offsetStore: persistent`) is a prerequisite for any
  sequence rule, and even then redelivery after a hard kill is possible (measured earlier in
  offset-persistence).
- A sequence consumer must be **idempotent** (dedupe by a stable key, e.g. the event LSN/txId),
  not trust arrival order. Memory-only, order-trusting state is not enough.

## What it does NOT establish

- A clean in-order baseline (the setup failed to guarantee it; redo needed).
- That anyone needs sequence detection (R8).
- Behavior with a persistent offset (not run in this script).

## Next if pursued

Redo with: persistent offset, an in-order clean baseline asserted before the kill, and an
idempotent consumer keyed on LSN. Then the question becomes whether redelivery still slips
through — which is the real production question.
