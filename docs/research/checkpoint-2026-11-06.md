# Checkpoint, 2026-11-06: where the research stands before the first user conversations

A stock-take after about a week of experiments, written so the direction and its evidence are not
re-derived from memory. It is a reading of `docs/HANDOFF.md`, `bench/experiments/FINDINGS.md` and
the experiments themselves. Some of the experiments it cites are in PRs #12 and #13, not yet merged.

## Where we started (2026-10-31, HANDOFF)

- The question: can Kubernetes react to the behavior of the data?
- The user conversations (R8, 5 to 10) "gate everything else": nothing gets built for this
  direction before them.
- Kill criterion (R9): if after the conversations we cannot name more than one real case where
  CDC plus a Kubernetes reaction beats database-side prevention and counter-based alerts, the
  direction does not hold.

## What held (one local kind cluster, PostgreSQL 16, mostly one run each)

- **Only CDC sees the content and the order of a change**: a forbidden value transition (needs
  `REPLICA IDENTITY FULL`) and a skipped or duplicate sequence number. This is the one thing that
  survived every round.
- **Counting changes does not justify CDC**: database counters plus the Kubernetes API saw the same.
- **The reaction is not ours alone**: Debezium to Kafka to Argo Events suspended a Job on a
  forbidden transition in under a second, with no code of ours (`argo-events-chain`).
- **The "who" is expensive**: the only exact route measured needs the server to log every
  statement (`attribution-inband`). Statement-level eBPF (Coroot) was inconclusive on our bench.
- **Three defects in the product today**:
  - a quiet table goes `Failed` after two minutes (`quiet-table`, measured);
  - the slot grows while the operator reports `SlotHealthy` (`backpressure`, measured);
  - `Pending` never escalates (code reading, not tested).

## What fell away

Each round made the thesis narrower: first the metrics, then rate-based detection, then the
reaction as something new. What remains is narrow: rules on the content and the order of a change,
plus the layer that makes CDC trustworthy on Kubernetes (pre-flight checks, slot health, a
stateful consumer).

## The diagnosis

The technical work was honest: each experiment reported what it found, including the ones that
failed. The risk was elsewhere: about ten experiments and a slide deck, and no conversation with a
user yet, although that was meant to come first. Experiments are more comfortable than asking,
and none of them can answer the question that decides everything: does this hurt anyone enough to
install something?

That is about to change: the first conversations are the community talk next week, which is what
the slides were prepared for. So this is not a stubborn loop yet; it is the point just before the
test that decides it. It becomes one if experiments continue in place of the conversations.

## Decisions taken at this checkpoint

1. **Pause experiments until after the talk**, including the VM bench for Coroot.
2. **Use the talk as the first round of R8**, with `r8-capture-sheet.md`.
3. **Apply R9 to what comes out, without moving the goalposts.**
   - Real cases of content or order rules: the direction lives, and the next step is to build.
   - None: the project can stay as a smaller, trustworthy CDC operator, or close with a write-up of
     what was learned. Both are results.
4. **Fix the two measured defects (quiet table, slot growth) whatever R8 says.** They are useful in
   every scenario, and a "trust layer" cannot ship false alarms. They need a design decision from
   the owner; not before the talk unless time allows.

## How to improve the bench (for after the talk)

The lesson from Coroot: before measuring with an eBPF tool, check that it sees something known.
- Two tiers: kind for the operator and CDC (fast, enough for everything that worked); a cluster on
  a VM (for example Multipass with single-node k3s) for anything that touches the kernel.
- A canary check in each eBPF experiment: open a known connection and stop if the tool does not
  report it.
- Scripts take a configurable kube context instead of assuming `kind-db-sensor-bench`.
- Each `RESULTS.md` records kernel, versions and node type automatically.
- Results presented in the talk should be run at least three times.
