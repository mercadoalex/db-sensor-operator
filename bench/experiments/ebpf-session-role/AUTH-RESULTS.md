# Does the probe also see the authentication handshake, and what is readable in it?

Script: `auth-handshake.sh` (hypotheses and decision criteria are written in its header, before the first run).
Same hook as `../ebpf-session-role` (`sys_enter_sendto`, `comm == "psql"`, privileged container sharing the kind
node's PID namespace), with the Q/P type filter removed (`probe-raw.bt`). Throwaway PostgreSQL 16.15 requiring
`scram-sha-256` for a disposable role `probeuser` with a disposable, one-time password. Client: `client-a` over TCP,
`sslmode=disable` (same limit as before: with TLS the probe sees ciphertext, not retested here). 2 full runs, 3
repetitions each (6 connections total), all with the same result.

## Answer

**Yes.** The same hook built to watch `SET session_replication_role` also captures the full authentication
handshake, with no extra capability and no change to what the probe is allowed to do. The username is readable in
clear text. The client's half of the SCRAM exchange (mechanism name, nonce, final proof) is readable and
structurally identifiable. No password, or value that is the password, appears anywhere in the capture, which
matches what SCRAM is designed to guarantee and is confirmed here rather than assumed.

## What was captured (one connection, bytes as sent by `psql`)

| Message | Bytes (first 48, hex) | What a human reads from it |
|---|---|---|
| StartupMessage | `...000300007573657200706f...` | `user=probeuser`, `database=postgres`, `application_name=psql`, decoded from the NUL-separated field list |
| SASLInitialResponse | `7000000036534352414d2d5348412d3235360...6e2c2c6e3d2c723d...` | mechanism `SCRAM-SHA-256`, client nonce `n,,n=,r=<64 random chars>` |
| SASLResponse (client-final-message) | `700000006c633d626977732c723d...703d...` | `c=biws,r=<nonce>,p=<base64 proof>` — the proof, not the password |

The server's half of the exchange (`server-first-message`: salt and iteration count) is sent by PostgreSQL over the
socket, not via a `sendto()` from the `psql` process; this hook watches the client process only, so it is not seen
here. The server's own log confirms the method actually used: `connection authenticated: identity="probeuser"
method=scram-sha-256`, ruling out a silent fallback to `trust` (the bench's local-socket `trust` line is unrelated:
these connections are over TCP from a different pod).

## Hypotheses

| | Result |
|---|---|
| H1 The handshake is visible to the same hook that watches queries | **held** |
| H2 The StartupMessage's `user` is readable in clear text | **held** (`probeuser`, decoded from the hex; `str()` from byte 0 alone stops at the first NUL and only shows `user`) |
| H3 The client's half of SCRAM is structurally identifiable (mechanism, client-first nonce, client-final proof) | **held** |
| H4 No plaintext password, or trivial equivalent, appears anywhere captured | **held**, 2 runs, 6 connections |
| H5 Consistent across repetitions | **held**, within each run and across the two runs |

Two fixes made before the first full result, both now reflected in the hypotheses and the scripts, not patched
after the fact:
- `str()` from a fixed offset cuts at the first NUL; a StartupMessage is a sequence of NUL-terminated strings, so
  reading from byte 0 only ever shows `user`. The fix reads the hex dump and decodes the NUL-separated fields
  instead, which is how `database` and `application_name` were confirmed too.
- The SASLInitialResponse carries the mechanism name AND the client nonce in the same message, separated by a NUL;
  `str()` from after the length header stops at that NUL and only shows `SCRAM-SHA-256`. The nonce is read from the
  hex dump by searching for its byte pattern, not from the NUL-truncated text.

## Decision, against the criteria fixed before running

**Modest-but-real finding.** Username and the full handshake structure are exposed; the password is not, matching
SCRAM's design. This is the "modest" branch named in advance: worth one line in a "securing eBPF itself" talk, not a
talk on its own. It needs pairing with the privilege model for loading such a probe and with mitigations to carry
more than that.

## What this says, and what it does not

- A probe that only intends to watch `SET session_replication_role` statements, if it drops its type filter or is
  misconfigured, watches login too. The boundary that matters is the syscall the hook attaches to (here,
  `sys_enter_sendto` for any `psql` process), not the string the probe happens to search for. Anyone who can load
  this hook can read every username connecting through that node in clear text, and the full shape of every login.
- It does not recover the password. SCRAM's point is exactly to avoid that, and the capture over 2 runs confirms it:
  no connection exposed anything equal to, or trivially derivable as, the password placed on the wire.
- It is information disclosure, not a credential leak: account names, the fact that SCRAM is in use, and the timing
  of logins (from the event timestamps) are the things actually exposed.
- It says nothing about a client that is not `psql`, a syscall other than `sendto`, or TLS.

## Not covered, and not claimed

- Whether the password is recoverable through cryptanalysis of the captured proof. Expected no, by SCRAM's design;
  not attempted, and attempting it is out of scope here.
- The Linux privilege model needed to load a probe like this (`CAP_BPF`, `CAP_PERFMON`, or privileged, as used
  here). Not measured.
- Mitigations, or a comparison with Tetragon/Cilium admission or RBAC controls for restricting who can load eBPF
  programs. Not attempted.
- TLS: not retested; `../ebpf-session-role/AUTH-RESULTS.md`'s sibling result already established that a `sendto()`
  probe sees ciphertext once TLS is on.
- Clients other than `psql`/libpq (a JDBC or Node client's exact wire framing was not captured here).
- Two runs on one local kind node. The capture depends on message framing fitting inside 48 captured bytes per
  send, which held for every message seen here (the longest was 109 bytes, where the fields read are near the
  start); a different client or a larger `application_name` could push a field further than that.

## Reproduce

```
bench/experiments/ebpf-session-role/auth-handshake.sh    # REPS=3; needs Docker and the kind cluster
```
