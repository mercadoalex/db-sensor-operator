# Where CDC adds value for Kubernetes: what the experiments found

Consolidated read of the R11 experiments (2026-11-05, one kind cluster, PostgreSQL 16,
Debezium 3.6.3, one run each). Each claim links to its experiment. This is evidence from a
local cluster, not a market finding; it does not prove anyone needs this (that is R8).

## The question

The pivot is: metrics are not the product; the product is Kubernetes reacting to the behavior
of the data. For that to be worth building, CDC has to show something the cheaper sources
(PostgreSQL's own `pg_stat` counters, and the Kubernetes API) do not. We tested where it does.

## What each source is good at

| Need | Best source | CDC needed? |
|---|---|---|
| How many rows changed (counts, rates) | `pg_stat` counters | No |
| Load / performance during a burst | database load metrics | No |
| Who/when at the workload level | Kubernetes API | No |
| **What a change was (row values, a forbidden transition)** | **CDC** | **Yes** |
| Order of changes within a transaction | CDC (`txId`, LSN) | Yes |
| Capture without touching the app or the database | CDC | Yes (by design) |

## Experiment results

| Experiment | Question | Result |
|---|---|---|
| [scheduler-catchup](scheduler-catchup/RESULTS.md) | Does CDC beat counters when a scheduler fires every missed run at once? | **FAIL.** Counters + the Kubernetes API saw the same burst; CDC's exported counts added nothing. |
| [scheduler-catchup (duplicates)](scheduler-catchup/RESULTS.md) | Can any source tell reruns wrote duplicate rows? | **FAIL for CDC.** The Kubernetes API (repeated job names) was the better signal, not CDC. |
| [row-content](row-content/RESULTS.md) | Can a policy on a value transition (`settled`->`pending`) be evaluated? | **PASS, CDC only.** And only with `REPLICA IDENTITY FULL`; default identity gives `before=null`. |
| [attribution](attribution/RESULTS.md) | Can a change be tied to the Job that made it, without a time guess? | **FAIL for session identity.** CDC carries no client identity; `pg_stat_activity` is racy. Lead: the event carries `txId`. |
| [sequence-gap](sequence-gap/RESULTS.md) | Can a stream consumer detect a skipped/duplicate sequence number? | **PASS, CDC only.** No counter or GROUP BY can; it needs order/history. Limit: in-memory state is lost on restart. |
| [sequence-gap (redelivery)](sequence-gap/RESULTS-redelivery.md) | Where is the restart risk, and does it cause false findings? | **Confirmed:** the offset frontier lives in Debezium; a memory offset re-delivered 1010 and the consumer raised a false duplicate. Caveat: the baseline was not in-order (setup flaw), so order cannot be assumed either. |
| [attribution (in-band, server log)](attribution-inband/RESULTS.md) | Can a CDC `txId` be joined to a workload that declares `application_name`, using the server log? | **PASS, conditional.** Exact join when statements are logged after they run (`log_min_duration_statement = 0`); `xid=0` and no join with `log_statement = mod`. Cost: every statement logged with literals. One run, PostgreSQL 16; poolers, scale and managed databases untested. |
| [argo-events-chain](argo-events-chain/RESULTS.md) | Can Debezium to Kafka to Argo Events detect a forbidden value transition and act on a Job, with no code of ours? | **PASS for detect and act** (Job suspended within 0.72 s, upper bound, one run; legal updates did not trigger). **Limits:** the target Job is fixed (no "who"); sequence checks not tested (read from the docs: stateless per event); 2 of 2 events lost when the Sensor pod was replaced at that moment (later diagnosed in [SENSOR-LOSS-RESULTS.md](argo-events-chain/SENSOR-LOSS-RESULTS.md): the NATS Streaming bus held the event on a dead queue member for 301 to 326 s in 10 of 10 forced kills; no ack-wait or redelivery setting fixes it on the native bus; clean graceful replacements lost 1 of 15); a Kafka outage stalled the slot with nothing reporting it. |
| [quiet-table](quiet-table/RESULTS.md) | What does the sensor report for a table with no writes? | **A product defect.** `lag` is the age of the last event, so with the default thresholds a quiet table goes `Degraded` after 30 s and `Failed` after 120 s. HANDOFF said it stays `Degraded`. |
| [backpressure](backpressure/RESULTS.md) | Does a slow consumer retain WAL, and does the operator see it? | **Confirmed growth** (64 kB -> 1953 kB), inspector sees `retainedBytes`. **Gap found:** on an unbounded slot the diagnosis still says `SlotHealthy` while WAL piles up. |
| [p0-public-postmortems](../../docs/research/p0-public-postmortems.md) | Of 16 real public data-incident postmortems, how many would CDC + a content policy have clearly beaten DB-side prevention and counters? | **1 clear case (Bungie/Destiny 2 2020), 1 partial (Matrix.org 2025).** Neither had a real CDC deployment behind it; both are our own reasoning about the public account. In 0 of 16 did a content-aware alert detect the incident. The winning shape, in both cases: a valid-looking wrong value, on a subset of rows, while legitimate writes kept arriving elsewhere — exactly what `repair-vs-pitr` already measured. |
| [p1-invariant-classification](../../docs/research/p1-invariant-classification.md) | Of 56 real business invariants across 4 OSS projects on PostgreSQL (Mastodon, Discourse, Saleor, Odoo), how many need CDC and not a constraint? | **Only 3 of 56 (5%).** 32% already enforced by the DB; of the 68% that are not, 61% needs cross-row/cross-table state a per-row content policy cannot evaluate, and 75% of what IS row-decidable is a plain `CHECK` the DB could do directly. CDC's edge in this sample is narrow: value-transition rules on tables the owner will not constrain. |
| [repair-subset-writer](repair-subset-writer/RESULTS.md) | Bungie-shaped lab scenario: a tagged bad writer corrupts a subset of rows while a good writer keeps writing; compare stream repair, PITR copy-back, and full restore | **Stream repair wins clearly.** 952-953/1000 rows fixed, 0 legitimate writes lost, 47-48 left wrong (flagged as conflicts, not silently overwritten). PITR copy-back: 697/1000 fixed, ~477 legitimate writes lost. Full restore (the real Bungie choice): 697/1000 fixed, 100% of later legitimate writes lost. One hypothesis failed: PITR was expected to be faster at this scale; it tied with stream repair within noise (5,000 rows). |
| [repair-via-logical-decoding](repair-via-logical-decoding/RESULTS.md) | Matrix-shaped lab scenario: when a DELETE is driven by a corrupted index, do Debezium's delete events still name the rows actually removed? | **PASS for `REPLICA IDENTITY FULL`.** Events matched the removed rows exactly (including the "foreign" collateral rows of other groups), with exact prior content; re-inserting the before-images fully repaired the table. **Unexpected finding:** with the default identity, non-key columns of a delete event are not null as assumed — they are the column's type default (`0`, empty string, epoch date), which looks like real data and is not. Turns our earlier "plausible, not proven" reasoning in `docs/research/p0-public-postmortems.md` into a measured result. |

## What we learned, in one paragraph

CDC's unique value is **row content**: it is the only source that shows what a change was, not
just that one happened. Counting changes does not need CDC, so any product framed around counts
loses to a `pg_stat` exporter — which is exactly the objection we got. The real product would be
**policies on the content of changes** (a forbidden value transition, a field that must not
change, a value out of range), enforced by a Kubernetes reaction. That has a concrete cost
(`REPLICA IDENTITY FULL`, more WAL) and one unsolved hard problem: **attribution** — tying a
change to the workload that caused it. Session identity is not enough; the `txId` in the event
looked like the lead, but eBPF cannot recover it from the wire and neither can Cilium, so
per-change attribution is not cheaply solvable; the one honest path is a Job that is the sole
writer in its window (Kubernetes side).

**Narrower than we first thought.** P0 and P1 (added 2026-11-11) quantify just how narrow. Across
56 real invariants in 4 OSS projects, only 5% are transition rules a `CHECK` cannot express but
CDC could evaluate from a single row; 61% of what the database does not already enforce needs
cross-row or cross-table state, which is a different, bigger product (a stateful stream
processor, out of scope today, and the exact thing that competes with counter-based alerts and
periodic reconciliation queries). Across 16 real public postmortems, only one (Bungie/Destiny 2)
is a clear win and one (Matrix.org) a partial one, and in zero of the 16 did a content-aware
alert detect anything. The winning shape in both is specific: **a valid-looking wrong value, on a
subset of rows, while legitimate writes keep arriving elsewhere** — the same shape
`repair-vs-pitr` already measured, and now confirmed twice more as lab scenarios
(`repair-subset-writer`, `repair-via-logical-decoding`) with the same result: stream repair wins
precision and keeps legitimate writes, full/PITR restore does not. The positioning this supports
is narrow and specific, not broad: value-transition detection and precise subset repair, for
tables the owner will not constrain, not "CDC detects business-rule violations" in general.

## Open, in priority order

1. **Does anyone want a content policy or per-change attribution?** User conversations (R8). Nothing else should be built before this.
2. **Attribution via `txId`.** A mechanism that records `txId -> workload` at commit time (eBPF or a database-side hook) would join exactly. Unbuilt, non-trivial.
3. **Attribution via eBPF — tested, same class of answer as Cilium.** Feasibility probe ran: eBPF loads on the linuxkit kernel and sees the TCP connection to PostgreSQL, but NOT the `txId` (it is assigned inside the server, not on the wire). So eBPF gives "which pod connected", not "which workload owns this txId". The only client-side route to `txId` is a fragile uprobe inside PostgreSQL (not attempted). See `attribution-ebpf/RESULTS.md`.
4. **Attribution via Cilium/Hubble.** Connection-level "which pod talked to the DB"; needs a CNI the bench does not run. Same class as eBPF: the connection, still not the change.
5. **If it goes ahead:** expose `before`/`after` in the operator pipeline (today it keeps only counts), and have the inspector flag tables that need `REPLICA IDENTITY FULL`.

## Engineering requirements if group C is built

The group-C experiments turned up delivery-guarantee requirements that are more work than the
detection logic: a persistent offset, an idempotent consumer keyed on the event LSN, no reliance
on arrival order, and persisted per-key state. Written up in
[ADR-016](../../docs/adr/ADR-016-stateful-stream-consumer-requirements.md) as a draft (not a
decision to build). This is the honest cost of a sequence/rate/revert rule.

## Kill criteria (unchanged, R9)

If the conversations find no one who needs content policies, or attribution cannot be done
honestly, this direction is dropped. The experiments narrow where it could work; they do not
establish that it should be built.
