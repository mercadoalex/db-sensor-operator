# Can a probe on the client node see `SET session_replication_role` and say which pod sent it?

Script: `run.sh` (hypotheses and the negative-but-useful criterion are written in its header, before the first run).
bpftrace on `sys_enter_sendto` (`probe.bt`), run as a privileged container sharing the PID namespace of the kind node
(Docker Desktop, kernel 6.10.14-linuxkit, bpftrace image amd64 emulated on arm64). Clients: `client-a` and `client-b`
(`psql` 16) over TCP to a throwaway PostgreSQL 16.15 with `log_statement=all` (plain on 5432, TLS on 5433).
`bench-postgres` is not involved.

## Answer

**Yes, for this client and without TLS.** The probe reads the text of the query, and the cgroup of the sending process
resolves to the right pod. It detected 7 of the 11 ways of setting the role that were tried, never confused the two pods,
and raised no alarm on the controls. The four it missed are the ones the hypothesis predicted.
Measured in 3 runs (the first with a counting bug in the analysis, explained below; two clean runs after the fix).

## Matrix (3 repetitions each as the superuser; "bypass" = the role was `replica` in the same session)

| Test | What it sends | Bypass | Naive prefix | Substring | Tolerant regex | Server log (same regex) |
|---|---|---|---|---|---|---|
| T1 | `SET session_replication_role = replica` | 3/3 | 3/3 | 3/3 | 3/3 | 3/3 |
| T1a | T1 from the other client | 3/3 | 3/3 | 3/3 | 3/3 | 3/3 |
| T2 | `set SESSION_REPLICATION_ROLE to replica` | 3/3 | 0/3 | 3/3 | 3/3 | 3/3 |
| T3 | `/* x */ SET session_replication_role=replica` | 3/3 | 0/3 | 3/3 | 3/3 | 3/3 |
| T4 | `set_config('session_replication_role',...)` | 3/3 | 0/3 | 3/3 | 3/3 | 3/3 |
| T5 | `BEGIN`, `SET LOCAL ...` (separate messages) | 3/3 | 0/3 | 3/3 | 3/3 | 3/3 |
| T6 | `DO $$ ... EXECUTE 'SET session_' \|\| 'replication_role...'` | 3/3 | 0/3 | 0/3 | 0/3 | 0/3 |
| T6b | `set_config('session_' \|\| 'replication_role',...)` | 3/3 | 0/3 | 0/3 | 0/3 | 0/3 |
| T8 | 300-character comment, then the `SET` | 3/3 | 0/3 | 0/3 | 0/3 | 3/3 |
| T9 | extended protocol (Parse), value as a bind parameter | 3/3 | 0/3 | 3/3 | 3/3 | 3/3 |
| T10 | `SELECT 1; SET ...` in one message | 3/3 | 0/3 | 3/3 | 3/3 | 3/3 |
| T11 | T1 over TLS | 3/3 | 0/3 | 0/3 | 0/3 | 3/3 |
| T7 | control: `SELECT 1` | 0/3 | 0/3 | 0/3 | 0/3 | 0/3 |
| T12 | control: the name inside a string | 0/3 | 0/3 | **3/3 (false alarm)** | 0/3 | 0/3 |
| T13 | control: `SHOW session_replication_role` | 0/3 | 0/3 | **3/3 (false alarm)** | 0/3 | 0/3 |

## Hypotheses

| | Result |
|---|---|
| H0 No lost events, ground truth right | held (every marker arrived; every bypass really set `replica`, controls stayed `origin`) |
| H1 T1 detected and attributed to the right pod, T7 silent | **held** (T1 to `client-b`, T1a to `client-a`, every repetition) |
| H2 Naive prefix catches at most 3 of 11 | held (catches 1: only T1) |
| H3 Regex catches exactly T1 T2 T3 T4 T5 T9 T10 | held |
| H4 Substring false-alarms on T12 and T13; regex does not | held |
| H5 No misattributed event | held |
| H6 Server log catches those 7 plus T8 and T11, and misses T6, T6b | held after a fix to the analysis (below) |
| H7 Non-superuser: no variant sets the role; probe still sees the attempts | held (0 of 11 bypassed, `permission denied`; attempts seen 7) |

Counting bug: in the first run H6 failed because the TLS server log has no markers and no user, and it also held the
one TLS attempt made in the non-superuser pass (4 hits for 3 repetitions). The prediction was not changed; the analysis
now subtracts that attempt. The first run is the only one with the bug.

## What this says

- **The positive result is narrow.** psql, no TLS, simple or extended protocol, the text visible in the first 199
  characters. The cgroup inode resolves to the pod UID in `find /sys/fs/cgroup -inum`, and the UID matches `kubectl get pods`.
- **It does not beat the server log on coverage.** `log_statement=all` caught the same 7 and two more: the one over TLS
  (T11) and the long one (T8). Both rows are things the probe cannot see. Both fail on T6 and T6b, because both read
  the text the client sent. A claim that the server log "catches every variant, including T6" would be false.
  pgaudit may catch T6, because it logs nested statements; it was not installed and not tested.
- **What the probe adds is attribution to the pod without touching the server**, in the client's node. The server log
  also has the client address (`%h`), which maps to the pod IP; that was not measured here, only noted.
- **It detects, it does not prevent.** The probe observes the `sendto` call; the `SET` still reaches the server.
  Run as a role that is not a superuser, PostgreSQL itself refused every `SET` and `set_config` (0 of 11 worked). Removing the privilege is the defence that closes the hole; the probe and
  the log are for seeing who tried.
- **Evasion is trivial for a client that wants it.** T6, T6b and TLS are the evasions that matter: the first two
  need nothing but string concatenation. The probe is useful against mistakes and unmodified tools, not against
  a deliberate caller.
- **Text rules need care.** The substring detector flagged a `SHOW` and a string literal; the tolerant regex did not.
  A regex that strips comments is a heuristic, not a parser.

## Not covered, and not claimed

- Clients other than psql/libpq, syscalls other than `sendto` (`write`, `writev`, `sendmsg`), a query split over several
  sends (the longest here is about 400 bytes), the window raised above 199 characters.
- TLS beyond one negative test: uprobes on `SSL_write` were not tried and depend on each client's library.
- pgaudit, managed services, the cost of the probe (CPU, lost events under load; the emulated bpftrace lost none here
  at this rate, which says nothing about a real workload).
- Joining the event to the CDC change. The CDC event carries `txId` and no source port, and the `SET` is a session
  setting that can precede the change by any time, so the earlier conclusion stands: per-change attribution is only
  honest for a sole writer in a window (`../attribution-ebpf/RESULTS.md`).
- Three repetitions on one local kind.
- Reading the SCRAM exchange: the probe's filter drops it, but the same hook with a different filter would see it.
  Anyone repeating this should treat the probe as able to read client traffic.

## Reproduce

```
bench/experiments/ebpf-session-role/run.sh      # REPS=3; needs Docker and the kind cluster
```
