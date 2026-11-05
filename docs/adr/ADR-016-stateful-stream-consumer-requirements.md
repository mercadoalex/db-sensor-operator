# ADR-016 — Requirements for a stateful stream consumer (if content/sequence policies are built)

- **Status:** Draft. Engineering requirements derived from experiments, NOT a decision to build.
  Whether to build this depends on user conversations (R8). It records what the experiments proved
  so the design is not re-derived later.
- **Date:** 2026-11-05
- **Context:** `bench/experiments/FINDINGS.md` and the group-C experiments
  (`sequence-gap/RESULTS.md`, `sequence-gap/RESULTS-redelivery.md`). Relates to ADR-014 (offset
  store) and ADR-012 (HTTP transport).

## The finding

Group-C policies (per-entity rate, skipped/duplicate sequence, revert pattern) need the operator
to keep state across events, over time. Experiments showed the hard part is not the detection
logic but the **delivery guarantees underneath it**:

1. **The offset frontier lives in Debezium, not in our consumer.** Our consumer receives HTTP
   POSTs and does not confirm the replication slot; Debezium owns the offset. (Measured:
   `sequence-gap/RESULTS-redelivery.md`.)
2. **A default in-memory offset re-delivers on restart.** Killing Debezium re-delivered an
   already-processed event (1010), and the stateful consumer raised a **false duplicate**.
3. **Delivery is not guaranteed in commit order as set up.** The clean baseline already arrived
   out of order (fast inserts / Debezium startup). Order cannot be assumed.
4. **The consumer's own state is in memory.** A consumer restart loses it; a gap straddling the
   restart is missed. (Measured: `sequence-gap/RESULTS.md`.)

Net: a naive "trust order, count in RAM" consumer produces false positives and misses real ones
around any restart. That is unacceptable for a fraud/integrity rule, where a false alert erodes
trust and a missed one defeats the purpose.

## Requirements, if a group-C policy is built

Configuration:
- **Persistent offset is mandatory** (`spec.snapshot.offsetStore: persistent`, ADR-014). An
  in-memory offset is disqualifying for these policies. This does not fully solve it: a hard kill
  can still replay up to a flush interval (measured in offset-persistence).

Consumer logic:
- **Idempotent, keyed on a stable per-change id.** Dedupe by the event LSN (monotonic, unique per
  change). `txId` groups a transaction but is not unique per row, so it is not the dedupe key.
- **Do not trust arrival order.** Order by LSN, or tolerate out-of-order within a bounded window;
  never assume the next event is the next in commit order.
- **Persist the per-sequence / per-entity state**, so a consumer restart does not lose it and
  reopen a blind window. In-memory-only state is a demo, not a feature.
- **Define the sequence/entity key explicitly.** "invoice_number" may be global, per-tenant, or
  per-type; the key is part of the policy, not an assumption.

Operational consequences:
- These are stateful-stream-processing requirements (idempotency, persisted state, replay
  tolerance). They are real work and a real operational surface (where the state lives, how it is
  sized, how it recovers). Pricing this in is part of deciding whether group-C is worth it.

## What is NOT decided here

- Whether to build any group-C policy. That is R8.
- Which policy first. If built, the cheapest is group A (value out of range, new value only, no
  REPLICA IDENTITY FULL, no accumulated state); group C is the most defensible but the most
  demanding operationally.
- The state store (embedded, PVC, external). Out of scope until there is a reason to build.

## Why record it now

The experiments were cheap; re-deriving these guarantees later would not be. If the conversations
say "yes, we want sequence/rate rules", this ADR is the starting design and the honest cost
estimate. If they say no, this is parked with the rest.
