# ADR-017 — Attribution by composition: Hubble for the "who", CDC for the "what"

- **Status:** Draft. An exploration, NOT a decision. Nothing here is built, and several claims
  are unverified (listed at the end). Attribution is the extra, not the core of the product.
- **Date:** 2026-11-05
- **Context:** `bench/experiments/FINDINGS.md`, `attribution/RESULTS.md`,
  `attribution-ebpf/RESULTS.md`, `attribution-ebpf/UPROBE-NOTES.md`,
  `docs/research/competitive-landscape.md`. Relates to ADR-015 (adoption ladder) and ADR-016
  (stateful consumer requirements).

## The principle

**Identity is declared, not discovered.** An IP address is a transport address, not an
identity. Inferring "which pod is this" from an IP, a socket or a conntrack entry after the fact
is a bet against the scheduler and IP reuse: it works at rest and breaks under churn. The
question is not "how do I resolve the IP" but "where is identity attached before, or atomically
with, the connection". That is an architecture problem, not a timing problem.

Three ways to avoid the race, cheapest first:
1. **Declare in-band**: the workload states who it is (headers, mTLS/SPIFFE, TLS SNI).
2. **Resolve atomically at the edge**: at `accept()`, look up and attach metadata before the
   socket is released.
3. **eBPF in the accept path**: the pod's cgroup ID is available in the same context as the
   socket, so there is no copy, no cache, no race.

The approaches to avoid: searching conntrack afterwards, reading `/proc/net` asynchronously, or
trusting that an informer is already in sync. All are guesses about what a socket was a moment ago.

## What the experiments found (measured, in our bench)

- The CDC event carries `txId` and the row content, but **no client identity**
  (`attribution/RESULTS.md`): no `application_name`, no user, no IP.
- `pg_stat_activity` has the client address and `application_name`, but only while the session is
  alive; it is a race against a short-lived Job (`attribution/RESULTS.md`).
- A socket/TCP-level eBPF probe sees the **connection** to PostgreSQL, not the `txId`, because
  the txId is assigned inside the server and is not on the wire (`attribution-ebpf/RESULTS.md`).
- Declaring identity **in-band does not apply to PostgreSQL as it does to HTTP**: the channel is
  the PostgreSQL protocol, which has no headers. The closest field is `application_name`, which
  the measurements above show does not reach the CDC event.
- **But the server log can carry the link** (`attribution-inband/RESULTS.md`, 2026-11-06, one run,
  PostgreSQL 16). With `%a` (application_name) and `%x` (transaction id) in `log_line_prefix`, and
  statements logged **after** they run (`log_min_duration_statement = 0`), the xid in the log line of
  a workload's first write equalled the `txId` of its CDC event, for two sessions that committed in
  the same millisecond. With `log_statement = 'mod'` the id is `0` (the line is written before the
  statement runs) and the join fails. The cost is logging every statement, with its literal values;
  see the RESULTS for what is not verified.

## The two halves of the problem (the part the principle alone does not cover)

The principle above resolves **who opened the connection**. Our problem has a second half:

| Question | Source | Status |
|---|---|---|
| Who opened the connection? | Hubble / eBPF (pod identity, atomic) | Delegated, not built by us |
| What changed in the data? | CDC (`txId`, row content) | Ours, measured (`row-content`) |
| How do the two join? | Time window on the connection (the txId is not on the wire), or the `txId` through the server log when the workload declares `application_name` (costly, measured once) | **The open piece** (see "Correlation strategy") |

So even with perfect identity at the network layer, the join "this connection <-> this transaction"
is by time, not atomic. The server-log route above is the one measured way around it. We inherit a smaller race: not in the "who", but in pairing who with what.

## The decision space (exploration, no choice made)

**Delegate the "who" to Cilium Hubble rather than rebuild it.** Hubble already resolves pod
identity at the datapath and exposes flows with source and destination pod names, namespaces,
IPs, ports, protocol and verdict; the Hubble Exporter writes them as JSON lines to a file or
stdout, with filters and field masks (verified from the Cilium docs).

What Hubble gives and does not give, translated to our case (data, not HTTP):

| | Hubble | Us |
|---|---|---|
| Pod -> database connection, with pod identity, atomically | yes | no, delegate |
| Network drops, retransmissions, per-flow metrics | yes | not our concern |
| What changed inside the connection (row, transition) | no (it sees flows) | yes, via CDC |
| Retention beyond Hubble's real-time window (who touched this 2 hours ago) | no | **would have to be built** |
| Join of flow to data change, and the query over history | no | **would have to be built** |

Options kept open, none chosen:
- **A. Integrate with Hubble** where the cluster already runs Cilium (optional, not universal).
- **B. A Cilium plugin/extension** instead of a separate consumer.
- **C. A minimal `sock_ops` program of our own**, only if there is real demand outside Cilium.
  It would reuse the idea (cgroup ID attached to the socket at accept), not Cilium's code.

- **D. Declare in-band and read the server log** (measured once, conditional; see above and
  `attribution-inband/RESULTS.md`). Exact on the `txId`, but needs the workload to set
  `application_name` and the server to log every statement.

Leads from the landscape review (2026-11-06, docs only; see
`docs/research/competitive-landscape.md`):
- **Coroot** already uses eBPF on the client side of the database connection. Its code (read, not
  run) puts the statement text and the client container in each Postgres span. A test on our kind
  bench was inconclusive, probably because the node is nested (`attribution-coroot/RESULTS.md`).
- **pganalyze query tags** let a workload declare who it is in a SQL comment (job name, user ID,
  trace ID). The comment shows in `pg_stat_activity` and statement logs; whether it reaches the CDC
  event is not verified. The log-plus-xid join was then tested (option D); the comment tag appears in
  the logged statement text. How pganalyze itself uses the tags was not tested.

Ruled out: porting a large part of Cilium's datapath. It is a subsystem, not a function, and
maintaining it (eBPF maps, BTF, kernel-version compatibility) is exactly the work Cilium already
does. Reimplementing it to avoid depending on Cilium would be reinventing the wheel.

## Correlation strategy (proposed, unbuilt)

Goal: for a CDC change event, say which pod most likely caused it, and say honestly how sure we
are. The correlation is a join between two independent streams.

**Inputs**
- From CDC (Debezium): the commit time (`ts_ms`), the `txId`, the database and the table. It
  carries no client identity (measured).
- From the network layer (Hubble flows, if available): pod UID (plus name, namespace, labels),
  destination IP and port, and time. It carries no data content.

**What the join key is.** The CDC side does not know the pod and the network side does not know
the row. So the key is the database endpoint plus time: a CDC event on database D at time T is
matched against connections to D's address and port (`dst_ip`, `dst_port`) that were live at T.
The pod comes from the network side as `(pod_uid, dst_ip, dst_port)`.

**Match against live connections, not against connection-open events.** A connection can be
opened long before the write: a pooled or long-lived connection may be open for hours. A rule of
"a flow within +-5 s of the commit" would miss it. The question to ask is "which connections to D
were open at commit time T" (with a tolerance for clock skew), not "which connections opened near
T". Whether Hubble exposes enough to answer that is unverified (see the list below).

**Window.** Starting proposal: +-5 s, configurable per sensor. It covers clock skew and delivery
delay between the two streams; it is not the only criterion. It becomes a field in the
`DatabaseSensor` spec only when a consumer exists (the schema does not run ahead of its
producer). Today it is a design parameter, not an API field.

**Three outcomes, never a silent guess**

| Outcome | When | What we report |
|---|---|---|
| exact | exactly one candidate pod matches in the window | that pod |
| ambiguous | several pods or connections match (a pod can hold many connections; many pods can write at once) | the list of candidates, not a single answer |
| `unknown_source` | no match in the window | `unknown_source`; the change is still recorded |

`unknown_source` is the right default for zero matches. "Ambiguous" is its own outcome and must
not be collapsed into "exact" or into "unknown": reporting one wrong pod is worse than reporting
a short list. A 5 ms gap between a flow and an UPDATE does not prove they belong together, so the
strategy reports a confidence, not a certainty.

**Where identity collapses.** If clients reach PostgreSQL through a pooler or proxy (PgBouncer, a
sidecar), the network layer sees the pooler, not the application pod, and attribution lands on
the pooler. The strategy must say so (a candidate that is a known pooler is flagged "via
pooler") instead of presenting the pooler as the cause. The same applies when NAT or SNAT hides
the source.

**The glue is a real component, not a detail.** It has to:
- consume both streams (Hubble export as JSON lines or metrics, and the Debezium events);
- tolerate events out of order, with a bounded lateness (a watermark): a CDC event that arrives
  after its window has closed is correlated late or marked `unknown_source`, never dropped;
- be idempotent on redelivery, keyed on the event LSN (ADR-016), since redelivery is measured;
- hold state (the recent connections) that survives a restart if the audit view is wanted
  (ADR-016: persisted state).

**Retention** of the correlation record is an open parameter, driven by the use case and not
guessed. Three plausible levels:
- 24 h: enough for real time and for next-day review; smallest store.
- 7 d: covers a week-long investigation.
- 30 d: audit or compliance-style lookback; largest store.

Storage scales with flow volume times retention. We have measured neither, so no sizing is
claimed. Design note: the record should reference the change (table, primary key, `txId`, LSN)
and not copy row content, so sensitive data is not stored a second time.

**Illustrative cases (constructed to show the outcomes, NOT measured data).**

| Case | Network side (Hubble) | CDC side | Outcome |
|---|---|---|---|
| A. One candidate | `pod-A` has a live connection to the DB at commit time | `UPDATE users`, txId 12345 | exact: `pod-A` |
| B. Two candidates | `pod-A` and `pod-B` both hold live connections | `UPDATE users`, txId 12345 | ambiguous: `[pod-A, pod-B]` |
| C. No candidate | no live connection to the DB in the window | `UPDATE users`, txId 12345 | `unknown_source`, change still recorded |
| D. Pooler | only `pgbouncer-0` holds a connection | `UPDATE users`, txId 12345 | `via pooler`: candidate is the pooler, not the real client |

Case B is the structural limit: the txId is assigned inside the server and is not on the wire,
so two writers inside the same window cannot be told apart by time alone. This is inherent to
the approach, not a tuning problem.

**Correlation record (proposed shape, unbuilt).** One record per CDC change, written by the
correlator. It references the change; it does not copy the row.

```go
type Correlation struct {
    ObservedAt time.Time // CDC commit time (ts_ms)
    Table      string    // from CDC
    Operation  string    // c / u / d, from CDC
    PrimaryKey string    // affected key only, never row content
    TxID       int64     // from CDC
    LSN        string    // from CDC; idempotency key (ADR-016)
    Outcome    string    // exact | ambiguous | unknown_source
    Candidates []Pod     // 1 for exact, N for ambiguous, empty for unknown_source
    ViaPooler  bool      // a candidate is a known pooler/proxy
}

type Pod struct {
    Namespace string // from the network side
    Name      string
    UID       string // exact field names depend on what Hubble exports (unverified)
}
```

**Proposed data flow (unbuilt).** The CDC consumer and a tailer of the Hubble export both feed
the correlator. The correlator keeps a bounded in-memory window of recent connections and emits
to what already exists downstream: metrics, sinks and `status`. A "top writers" view in `status`
would be new. Persisting the records (audit view) is a separate data layer, not part of this flow.

**DDL (for example a DROP TABLE).** Knowing who ran a destructive schema change is a legitimate
audit need. But on PostgreSQL the CDC stream does not carry DDL: a real ALTER TABLE produced no
event in our measurements (README, "DDL on PostgreSQL"). This correlation anchors on a CDC event,
so it cannot attribute a DDL on PostgreSQL. That would need another source (for example database
audit logging or event triggers), which is unexplored. Connectors that keep a schema history
(MySQL) are expected to emit schema changes, but we have not tested that.

## Real time and audit are two views of one correlation

They are not exclusive. Both read the correlation described above:
- **Real time** ("who is writing bad data right now?"): the *brake* case. It needs only a short
  in-memory window. It is cheap, and it is what an alert or a Kubernetes reaction consumes.
- **Audit** ("who touched these payments last week?"): the same correlation, persisted. It needs
  a durable store, retention and a query layer. Neither Hubble nor we store this today.

What differs is cost, so the real question is the order: real time first (small), audit later (a
data layer). Which one users need first is a question for the conversations (R8).

## Scope notes

- **Server side vs client side.** Observing the PostgreSQL *server* from the kernel (a uprobe, a
  server-side probe) needs access to the node where the server runs. That is possible when the
  database is in our cluster and impossible on a managed database (RDS, Cloud SQL), whose server
  runs on the provider's machine. Observing the *client* connection works in both cases, because
  the client pod always runs on a node we control. The "who" (this ADR's subject) is a client-side
  question, so it is available for managed databases too.
- Option A depends on Cilium as the CNI. It is an optional integration, not a universal one.

## What is NOT verified (do not present these as facts)

- That Hubble does **not** parse the PostgreSQL protocol at L7. Inferred from the L7 protocols
  seen in the docs (HTTP, DNS, Kafka, gRPC); not confirmed.
- That a join by connection time window is accurate enough in practice for short-lived Jobs.
- Option D at scale: the volume and overhead of logging every statement, poolers passing
  `application_name`, 64-bit transaction ids and wraparound, and managed databases allowing the
  settings. Only a single local run exists.
- The cgroup-ID-at-accept mechanism (`sock_ops` / `sk_msg`) and Hubble's use of it come from
  the author's study notes, not from a test of ours.
- Whether a Cilium plugin is a viable extension point for this.
- That anyone needs per-change attribution at all (R8).
- That Hubble's flow records let us ask "which connections to the database were open at time T",
  as opposed to only emitting events when a connection opens or is observed. This decides
  whether the live-connection match is possible at all.
- How pooled or proxied connections appear with Cilium (the identity-collapse case).
- That +-5 s is the right tolerance. It is a starting value, unmeasured.
- Any source for DDL attribution on PostgreSQL. None has been identified.
- Storage size and cost for each retention level.
- The exact field names in a Hubble flow (for example whether a pod UID is exported, or only pod
  name, namespace and labels). The `Pod` shape above is provisional until checked.
- That "real time first" covers most of what users need. It is a hypothesis for R8, not a figure.

## One-slide summary (what can be said on stage)

- **Principle:** identity is declared, not discovered. Guessing a pod from an IP after the fact
  breaks under churn.
- **Two halves:** the "who" (connection, pod identity) comes from the network layer; the "what"
  (row content, order) comes from CDC. We measured the CDC half. The "who" is delegated.
- **Measured:** the CDC event has no client identity, and a socket probe sees the connection but
  not the transaction. The join between the two is by time, unless the workload declares
  `application_name` and the server log is used (costly, measured once).
- **Honest outcomes:** exact, ambiguous, or `unknown_source`. Never a silent guess. A pooler is
  flagged, not blamed on the application.
- **Not covered:** DDL on PostgreSQL (CDC does not carry it).
- **Status:** exploration. Nothing built. Whether anyone needs it is a question for the users.

## Not decided here

Whether to build any of this. The product's core remains the content of the change; attribution
is an add-on. This ADR records the reasoning so it is not re-derived, and what to ask in the
conversations to decide.
