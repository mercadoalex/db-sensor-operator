# Attribution, approach 3 (eBPF): feasibility results

A feasibility probe, not a built feature. Run on the `bench` kind cluster, PostgreSQL 16,
Docker Desktop on macOS (arm64), kernel `6.10.14-linuxkit`, bpftrace via
`quay.io/iovisor/bpftrace` (amd64, emulated). 2026-11-05, exploratory.

## Question

The attribution lead was: the CDC event carries `txId`. If something recorded `txId -> workload`
at commit time, the event would attribute exactly. Can eBPF produce that link?

## What was tested, in order

1. **Can eBPF load on this kind at all?** Yes. A `sys_enter_connect` counter attached and
   counted 167 calls in 4 s. BTF is present (`/sys/kernel/btf/vmlinux`), tracefs is mounted,
   `CONFIG_BPF`, `CONFIG_BPF_SYSCALL`, `CONFIG_DEBUG_INFO_BTF` are all `y`.
2. **Can it see a client's connection to PostgreSQL?** Yes. `tracepoint:sock:inet_sock_set_state`
   captured the full TCP lifecycle of a remote client pod connecting to port 5432 (SYN_SENT ->
   ESTABLISHED -> close), with source and destination ports. From PID/comm this reaches the pod
   that opened the connection.
3. **Can it see the `txId`?** No. (See verdict.)

## Verdict

**eBPF gives the connection, not the transaction.** The PostgreSQL transaction id is assigned
inside the server and is not on the wire, so a socket- or TCP-level probe sees the SQL and the
connecting process, never the `txId`. eBPF therefore lands in the same place as Cilium/Hubble:
it answers "which pod connected to the database", not "which workload owns txId 778". The exact
join the lead wanted is not available this way.

The only way to get the `txId` on the client side would be an uprobe inside the PostgreSQL
process (on the function that assigns the xid). That is version-coupled and fragile, and it is
product work, not feasibility, so it was not attempted.

## What this means for attribution

- eBPF and Cilium/Hubble are the same class of answer: connection-level identity, then you still
  join to the change by time or by `txId` you do not have. Neither gives honest per-change
  attribution on its own.
- The one solid path stays the Kubernetes side: a Job that is the sole writer in its window is
  attributable from its own identity plus the window, with no kernel probes.
- Honest conclusion across all attribution experiments: per-change attribution to a workload is
  not cheaply solvable. If a use case needs it, the realistic design is "sole-writer Job +
  window", not network or kernel tracing.

## Environment notes (for anyone repeating this)

- `timeout` is not on macOS; use bpftrace's `interval:s:N { exit(); }` to self-terminate.
- The bpftrace image is amd64; on Apple silicon it runs emulated (slow but works).
- Run the writer from a separate client pod, not `kubectl exec` into the PostgreSQL pod: an
  exec writes over a local socket, not the TCP path a real client uses.
- Reading `struct sock` fields by offset was unreliable here; the stable path was the
  `sock:inet_sock_set_state` tracepoint, whose args carry the ports directly.

## Not done

- No uprobe into PostgreSQL (the only route to `txId` client-side).
- One exploratory session, no RESULTS for a built collector.
- Cilium/Hubble not run (would give the same connection-level answer).
