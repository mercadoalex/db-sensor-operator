# Attribution via a PostgreSQL uprobe: notes (not built)

Parked idea, documented in case it becomes worth it. Not attempted. This is the only route
that could recover the `txId` on the write side; its blockers are about where it can run, not
whether it works.

## The idea

An eBPF uprobe attached to the PostgreSQL function that assigns the transaction id
(`GetNewTransactionId` / `AssignTransactionId` in the source). On fire, read the newly assigned
`txId`; from the backend process's PID and the connection's source IP, map it to the client pod.
Join that `txId -> workload` record to the `txId` the CDC event already carries. That gives exact
per-change attribution, not a time-window guess.

## Pros

- The only way to get the `txId` on the write side; nothing on the network or socket carries it (measured: `attribution-ebpf/RESULTS.md`).
- Exact attribution, not correlation by time.
- Fits the Kubernetes/eBPF data-plane angle; hard for others to copy if it worked.

## Cons (why it is parked)

- **Managed databases: impossible for this approach.** RDS, Cloud SQL, Azure DB do not let you run probes on the server, and a uprobe lives on the server. A large share of users are on managed Postgres, so this approach serves none of them. (Client-side observation, meaning which pod connected, still works there; this note is only about the server-side uprobe.)
- **Needs privileged access on the database node.** Many teams will not grant eBPF on their DB host in production.
- **Version-coupled and fragile.** It hooks an internal function; a different Postgres version, build, or stripped binary breaks it. Every upgrade is a revalidation.
- **Symbols.** Without debug symbols, it means hand-resolved offsets, more fragile still.
- **High maintenance for unproven demand.** Months of work for attribution nobody has yet asked for.

## Cheaper alternative to try first (if the goal is just to correlate)

PostgreSQL can log `application_name` and the `txId` via `log_line_prefix` into its own logs, no
probe. Works on some managed databases (where log access is allowed). Gives `txId -> application_name`
to join with the CDC event's `txId`. Less elegant, far less fragile. Not tested; a hypothesis.

## Decide from R8, not from taste

Two questions settle whether this is ever worth it:
1. Is your PostgreSQL managed, or do you operate the node yourself? (Managed kills the uprobe.)
2. Do you need to know which workload made a change, or is knowing what changed enough?

If most say managed, or that the "what" is enough, this stays parked for good.

## Reminder: attribution is the extra, not the core

CDC's unique value is the content of the change (the "what"), proven in `row-content`. A content
policy — "status must not go settled -> pending: stop and alert" — is useful without knowing which
workload did it. Attribution (the "who") is a hard add-on, not a prerequisite. Do not let the
attribution problem stall the content-policy question, which R8 should settle first.
