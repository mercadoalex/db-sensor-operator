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

## Open, in priority order

1. **Does anyone want a content policy or per-change attribution?** User conversations (R8). Nothing else should be built before this.
2. **Attribution via `txId`.** A mechanism that records `txId -> workload` at commit time (eBPF or a database-side hook) would join exactly. Unbuilt, non-trivial.
3. **Attribution via eBPF — tested, same class of answer as Cilium.** Feasibility probe ran: eBPF loads on the linuxkit kernel and sees the TCP connection to PostgreSQL, but NOT the `txId` (it is assigned inside the server, not on the wire). So eBPF gives "which pod connected", not "which workload owns this txId". The only client-side route to `txId` is a fragile uprobe inside PostgreSQL (not attempted). See `attribution-ebpf/RESULTS.md`.
4. **Attribution via Cilium/Hubble.** Connection-level "which pod talked to the DB"; needs a CNI the bench does not run. Same class as eBPF: the connection, still not the change.
5. **If it goes ahead:** expose `before`/`after` in the operator pipeline (today it keeps only counts), and have the inspector flag tables that need `REPLICA IDENTITY FULL`.

## Kill criteria (unchanged, R9)

If the conversations find no one who needs content policies, or attribution cannot be done
honestly, this direction is dropped. The experiments narrow where it could work; they do not
establish that it should be built.
