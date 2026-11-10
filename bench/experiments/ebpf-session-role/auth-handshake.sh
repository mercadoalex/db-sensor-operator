#!/usr/bin/env bash
#
# bench/experiments/ebpf-session-role/auth-handshake.sh — does the probe also see the authentication handshake?
#
# Question: probe.bt filters on the first byte of the sendto() buffer to keep only query (Q) and Parse (P) messages.
# Before any of those, every connection does a StartupMessage and, with scram-sha-256, a SCRAM exchange. If the type
# filter is removed, does the same hook (sys_enter_sendto, unprivileged client, no TLS) also capture that handshake?
# If it does, what is actually readable in it: the username, the SCRAM message structure, the password?
#
# Why it matters: a probe built to watch queries can watch login too, with no extra capability. The risk is not in
# what the probe looks for, it is in everything the hook can see. Relevant to "securing eBPF itself": the capability
# boundary is the syscall, not the filter string.
#
# Setup: a throwaway PostgreSQL 16 (one instance, scram-sha-256 required for a disposable role "probeuser" with a
# disposable password used nowhere else), a client pod connecting over TCP with sslmode=disable (same limit as
# ebpf-session-role: with TLS the probe would see ciphertext, not retested here). probe-raw.bt removes the Q/P type
# filter and dumps the first BPFTRACE_STRLEN bytes of every sendto() from psql.
#
# Hypotheses, written BEFORE the first run:
#   H1  The probe captures sendto() calls before the first Q message, i.e. the handshake is visible to the same hook.
#   H2  The StartupMessage carries the "user" parameter in clear text, readable in the capture.
#   H3  The client's half of the SCRAM exchange is identifiable by structure: "SCRAM-SHA-256" (mechanism name),
#       "n,,n=...,r=..." (client-first-message, with a nonce), "c=biws,r=...,p=..." (client-final-message, with
#       the proof). The server's half (salt, iteration count) is sent BY the server, over the TCP socket, not via
#       a sendto() from the psql process; this probe watches the client process only, so it is not expected here
#       and is read from the server's own log instead, for completeness.
#   H4  No plaintext password, and no value trivially equal to the password, appears in any captured message. This is
#       expected by SCRAM's design (the proof is PBKDF2+HMAC-SHA256 over the password, salt and nonces); the point of
#       this run is to confirm it, not assume it.
#   H5  The result is consistent across 3 repetitions with fresh connections and fresh nonces each time.
# Decision criteria (fixed before running, not after):
#   - Modest-but-real finding: username and handshake structure exposed, password not. Worth one slide line, not a
#     full talk; needs pairing with the privilege model and mitigations to carry a talk.
#   - Stronger finding: anything more sensitive appears (e.g. auth silently fell back to trust or password instead of
#     scram-sha-256). If so, verify it is not a bench artifact (pg_hba.conf trust lines for local sockets) before
#     reporting it.
#   - Weaker finding: nothing structured is visible, only protocol noise. Then this direction does not carry a talk.
#
# NOT covered, and not claimed: whether the password is recoverable (expected no, by SCRAM's design); the Linux
# privilege model needed to load a probe like this (CAP_BPF, CAP_PERFMON, or privileged, as used here); mitigations,
# or a comparison with Tetragon/Cilium admission or RBAC controls for this problem; TLS (same limit as before: a
# sendto() probe sees ciphertext).
#
# Prerequisites: a kind cluster (make -C bench setup), Docker (the probe runs as a privileged container sharing the
# node's PID namespace), images postgres:16 and quay.io/iovisor/bpftrace.
# Env: CLUSTER (db-sensor-bench)  NS (bench)  REPS (3)  OUT (results directory)
#
set -uo pipefail
CLUSTER="${CLUSTER:-db-sensor-bench}"; NS="${NS:-bench}"; REPS="${REPS:-3}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${OUT:-${HERE}/../../results/ebpf-auth-$(date +%Y%m%d-%H%M%S)}"
CTX="kind-${CLUSTER}"; NODE="${CLUSTER}-control-plane"
TESTPASS="probe-test-only-$(date +%s)-not-reused-anywhere"
mkdir -p "$OUT"; OUT="$(cd "$OUT" && pwd)"
K() { kubectl --context "$CTX" -n "$NS" "$@"; }
X() { K exec -i ebpf-pg-scram -- "$@"; }
say() { printf '\n== %s\n' "$*"; }
cleanup() {
  say "cleanup"
  docker rm -f ebpf-probe-raw >/dev/null 2>&1
  K delete pod ebpf-pg-scram client-a --ignore-not-found --wait=true >/dev/null 2>&1
  echo "  removed the probe container, the SCRAM server pod and the client pod"
}
trap cleanup EXIT
docker info >/dev/null 2>&1 || { echo "Docker is not running" >&2; exit 1; }
docker ps --format '{{.Names}}' | grep -qx "$NODE" || { echo "kind node ${NODE} not found" >&2; exit 1; }
K delete pod ebpf-pg-scram client-a --ignore-not-found --wait=true >/dev/null 2>&1

say "1. throwaway server requiring scram-sha-256, and a client pod"
K apply -f "${HERE}/server-scram.yaml" >/dev/null
K apply -f "${HERE}/clients.yaml" >/dev/null   # reuses client-a from ebpf-session-role; client-b is not needed here
K wait --for=condition=ready pod/ebpf-pg-scram pod/client-a --timeout=180s >/dev/null || { echo "pods not ready" >&2; exit 1; }
X bash -c "
set -e
initdb -D /work/pg -U postgres --auth=scram-sha-256 --pwfile=<(echo postgres) >/dev/null
cat >> /work/pg/postgresql.conf <<'EOF'
port = 5432
listen_addresses = '*'
unix_socket_directories = '/tmp'
password_encryption = scram-sha-256
log_connections = on
EOF
cat > /work/pg/pg_hba.conf <<'EOF'
local all all trust
host all all 0.0.0.0/0 scram-sha-256
EOF
pg_ctl -D /work/pg -l /work/pg.log -w start >/dev/null
psql -X -q -h /tmp -U postgres -d postgres -c \"create role probeuser login password '${TESTPASS}'\"
psql -X -q -h /tmp -U postgres -d postgres -c \"select rolpassword from pg_authid where rolname='probeuser'\" | head -c 80
" || { echo "server setup failed" >&2; exit 1; }
SRV_IP=$(K get pod ebpf-pg-scram -o jsonpath='{.status.podIP}')
VERIFIER=$(X psql -X -tA -h /tmp -U postgres -d postgres -c "select rolpassword from pg_authid where rolname='probeuser'")
echo
echo "  server at ${SRV_IP}; stored verifier for probeuser (not the password): ${VERIFIER:0:40}..."
echo "  test password used, disposable, not reused anywhere: ${TESTPASS}"

say "2. start the unfiltered probe (same hook as ebpf-session-role, no Q/P type filter)"
docker rm -f ebpf-probe-raw >/dev/null 2>&1
docker run -d --name ebpf-probe-raw --privileged --pid=host -e BPFTRACE_STRLEN=200 -v /sys:/sys -v /lib/modules:/lib/modules \
  -v "${HERE}/probe-raw.bt:/probe.bt" quay.io/iovisor/bpftrace:latest bpftrace /probe.bt >/dev/null 2>&1
for _ in $(seq 1 60); do docker logs ebpf-probe-raw 2>&1 | grep -q "Attaching" && break; sleep 1; done
sleep 4
docker logs ebpf-probe-raw 2>&1 | grep -E "Attaching|error|ERROR" | head -3

say "3. ${REPS} fresh connections as probeuser, sslmode=disable; the marker is the first statement of each connection"
for rep in $(seq 1 "$REPS"); do
  K exec client-a -- env PGSSLMODE=disable PGPASSWORD="${TESTPASS}" psql -X -tA -h "$SRV_IP" -p 5432 -U probeuser -d postgres \
    -c "SELECT 'MARK:AUTH:${rep}'" -c "SELECT 1" > "${OUT}/conn-${rep}.out" 2>&1
  echo "  connection ${rep}: $(cat "${OUT}/conn-${rep}.out" | tr -d '\n')"
  sleep 0.5
done
sleep 2
docker logs ebpf-probe-raw 2>/dev/null > "${OUT}/raw-events.txt"
docker stop ebpf-probe-raw >/dev/null 2>&1
X cat /work/pg.log > "${OUT}/server.log"

say "4. analysis"
OUT="$OUT" REPS="$REPS" TESTPASS="$TESTPASS" python3 "${HERE}/auth-analyze.py"
