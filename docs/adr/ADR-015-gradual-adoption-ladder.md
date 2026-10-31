# ADR-015 — Gradual adoption: a policy-gated ladder of stages
- **Status:** Draft (an idea to validate, not a decision; nothing here is built)
- **Date:** 2026-10-31
- **Context:** ADR-014 (snapshot policy layer), `docs/overview/stakeholder-brief.md`
  sections 1 and 7, `ROADMAP.md`. Builds on the inspection and the policies that exist
  today: start mode, per-table backfill plan (computed, never applied), WAL retention
  diagnosis.

## Idea

Adopting CDC on a production database is usually all or nothing: switch it on and find out
what breaks. The idea is to make adoption a ladder of stages, where a policy decides when
the next stage may open, and the operator respects that pace.

| Stage | What it does | Opens when (a policy checks it) |
|---|---|---|
| 1. Inspect | Reads the database and reports; nothing is captured | the inspection runs |
| 2. Observe | Starts without a snapshot, metrics only | `wal_level`, tables and slot pass the checks |
| 3. Alert | Enables alert rules and sinks | lag and slot stay healthy for a configured period |
| 4. Backfill | Loads existing rows | the user's blind-window budget allows it (ADR-014) |

Stage 4 does not exist: ADR-014 defers the backfill until something consumes it.

## What exists today, and what does not

Exists: the inspection (opt-in), the slot and table diagnosis, the start-mode policy, a plan
reported with `applied: false`, and independent opt-in switches (offset store, dashboard,
ServiceMonitor, alert rules, thresholds).

Does not exist: any sequence of stages, any policy that opens a stage, any brake. Today the
inspection reports and never blocks, and Debezium is created regardless of its verdict. There
is no separate policy resource either: policies live inside `DatabaseSensor`.

## Why it might matter (hypotheses, not evidence)

1. **Defensibility.** Metrics are a commodity. Knowing when it is safe to take the next step
   (WAL, slots, blind windows) is accumulated knowledge that is harder to copy.
2. **Shorter approval.** The person approving asks "what if it breaks production?". Stages
   with explicit entry conditions answer that by design.
3. **Roadmap order.** Backfill, alerts and anomaly detection stop being loose items and
   become stages opened in sequence.
4. **Each step is verifiable.** A stage has an entry condition that can be tested, in line
   with the rule that the schema does not run ahead of its producer.

## Risks

- Extra complexity nobody asked for: one more thing the user must understand.
- If users only want quick metrics, a ladder gets in the way.
- Much work before knowing whether it matters.
- The comparison with Prometheus/Alertmanager and Datadog ("no phases", "enable all
  monitors") is an unverified generalisation. Both let you enable rules one by one. A
  competitor review is needed before saying this in public.

## Open questions

- Do users want a brake, or only to be told? (brief, section 7, hypothesis 4)
- Is a separate policy resource needed, or is `spec` enough?
- Can a stage close again (rollback) when its condition stops holding?
- What counts as "healthy for a configured period" without inventing thresholds?

## Decision rule: continue or drop

Do not write code or API fields for this before the first 5 to 10 conversations. Continue
if at least some of them say they would rather be held back than just warned. Drop or shrink
it to "inspect and report" (what already exists) if they only want metrics.

## If it goes ahead

Start with the smallest slice: one gate (stage 1 → 2) that blocks creating Debezium while
`LogicalDecodingReady` is `False`, off by default so nothing changes on upgrade. Verify it in
kind end to end first, since the inspection flow is not yet tested against a real operator.
