#!/usr/bin/env bash
#
# bench/experiments/ebpf-session-role/run.sh — can a probe on the client node see `SET session_replication_role` and say which pod sent it?
#
# Question: a pod runs SET session_replication_role = replica against PostgreSQL, which makes triggers in default mode
# stop firing (see ../trigger-bypass). Can an eBPF program on the node of the client (sys_enter_sendto, bpftrace) read the
# query text, and does the cgroup of the sending process identify the pod? How does that compare with the server's own
# log_statement=all, and with removing the privilege?
#
# Setup: a throwaway PostgreSQL 16 pod with two instances (plain on 5432, TLS on 5433), both with log_statement=all and
# trust authentication; two client pods (client-a, client-b) running psql over TCP, never through the server pod.
# bench-postgres is not touched. The probe prints the first 199 characters of every simple (Q) and Parse (P) message.
#
# Hypotheses, written BEFORE the first run (the checks at the end are exactly these):
#   H0  Instrument: every marker query sent reaches the probe (bpftrace runs emulated, amd64 on arm64) and the ground
#       truth is right (each bypass variant really sets the role; each control leaves it at "origin").
#   H1  POSITIVE RESULT: T1 is detected and attributed to client-b, T1a to client-a, in every repetition, and the control
#       SELECT 1 (T7) raises no alarm.
#   H2  A naive detector (the query starts with the exact text "SET session_replication_role") catches at most 3 of the
#       11 bypass variants (T1..T6, T6b, T8..T11).
#   H3  A tolerant regex (comments removed, SET / SET LOCAL / SET SESSION ... TO|=, or set_config('session_replication_role'))
#       catches exactly T1 T2 T3 T4 T5 T9 T10, and misses T6 and T6b (the name is built at run time), T8 (the SET is
#       beyond the 199-character window the probe reads) and T11 (TLS: the probe sees ciphertext).
#   H4  A plain substring detector raises false alarms on the controls (T12: the name inside a string; T13: SHOW); the regex does not.
#   H5  No detected event is attributed to the wrong pod.
#   H6  The server log (log_statement=all) with the same regex catches the seven of H3 plus T8 and T11 (9 of 11), and
#       misses T6 and T6b too: it logs the same text the client sent, so a text rule cannot see a name built at run time.
#   H7  As a role that is not a superuser, no variant sets the role (permission denied), while the probe still sees the
#       attempts. Removing the privilege prevents what the probe only detects.
# Negative-but-useful outcome, decided now: T1 detected but more than half of the bypass variants evade even the tolerant
# regex. That is reported as the result.
#
# NOT covered, and not claimed: pgaudit (not installed in the image; it logs nested statements, so it may catch T6);
# clients other than psql/libpq or syscalls other than sendto (write, writev, sendmsg); a query split over several
# sends (the longest query here is about 400 bytes); uprobes on the TLS library (SSL_write); managed services;
# the cost of the probe (CPU, lost events under load); joining the event to a CDC change (the session can change rows
# long after the SET, and the CDC event carries no source port).
#
# Prerequisites: a kind cluster (make -C bench setup), Docker (the probe runs as a privileged container sharing the
# node's PID namespace), images postgres:16 and quay.io/iovisor/bpftrace.
# Env: CLUSTER (db-sensor-bench)  NS (bench)  REPS (3)  OUT (results directory)
#
set -uo pipefail
CLUSTER="${CLUSTER:-db-sensor-bench}"; NS="${NS:-bench}"; REPS="${REPS:-3}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${OUT:-${HERE}/../../results/ebpf-$(date +%Y%m%d-%H%M%S)}"
CTX="kind-${CLUSTER}"; NODE="${CLUSTER}-control-plane"
mkdir -p "$OUT"; OUT="$(cd "$OUT" && pwd)"
K() { kubectl --context "$CTX" -n "$NS" "$@"; }
X() { K exec -i ebpf-pg -- "$@"; }
say() { printf '\n== %s\n' "$*"; }
cleanup() {
  say "cleanup"
  docker rm -f ebpf-probe >/dev/null 2>&1
  K delete pod ebpf-pg client-a client-b --ignore-not-found --wait=true >/dev/null 2>&1
  echo "  removed the probe container, the server pod and both client pods"
}
trap cleanup EXIT
docker info >/dev/null 2>&1 || { echo "Docker is not running" >&2; exit 1; }
docker ps --format '{{.Names}}' | grep -qx "$NODE" || { echo "kind node ${NODE} not found" >&2; exit 1; }
K delete pod ebpf-pg client-a client-b --ignore-not-found --wait=true >/dev/null 2>&1

say "1. throwaway server (plain 5432, TLS 5433, log_statement=all) and two client pods"
K apply -f "${HERE}/server.yaml" >/dev/null
K apply -f "${HERE}/clients.yaml" >/dev/null
K wait --for=condition=ready pod/ebpf-pg pod/client-a pod/client-b --timeout=180s >/dev/null || { echo "pods not ready" >&2; exit 1; }
X bash -c '
set -e
for d in plain tls; do initdb -D /work/$d -U postgres --auth=trust >/dev/null; echo "host all all 0.0.0.0/0 trust" >> /work/$d/pg_hba.conf; done
mkdir -p /work/cert && openssl req -new -x509 -days 2 -nodes -subj /CN=ebpf-pg -keyout /work/cert/server.key -out /work/cert/server.crt >/dev/null 2>&1
chmod 600 /work/cert/server.key
cat >> /work/plain/postgresql.conf <<EOF
port = 5432
listen_addresses = '"'"'*'"'"'
unix_socket_directories = '"'"'/tmp'"'"'
log_statement = all
log_line_prefix = '"'"'%m [%p] %h '"'"'
EOF
cat >> /work/tls/postgresql.conf <<EOF
port = 5433
listen_addresses = '"'"'*'"'"'
unix_socket_directories = '"'"'/tmp'"'"'
log_statement = all
log_line_prefix = '"'"'%m [%p] %h '"'"'
ssl = on
ssl_cert_file = '"'"'/work/cert/server.crt'"'"'
ssl_key_file = '"'"'/work/cert/server.key'"'"'
EOF
pg_ctl -D /work/plain -l /work/plain.log -w start >/dev/null
pg_ctl -D /work/tls -l /work/tls.log -w start >/dev/null
for p in 5432 5433; do psql -X -q -h /tmp -p $p -U postgres -d postgres -c "create role app login nosuperuser"; done
' || { echo "server setup failed" >&2; exit 1; }
SRV_IP=$(K get pod ebpf-pg -o jsonpath='{.status.podIP}')
echo "  server at ${SRV_IP}; superuser: $(X psql -X -tA -h /tmp -p 5432 -U postgres -d postgres -c "select rolsuper from pg_roles where rolname='postgres'"), app role created"
echo "  TLS instance: $(X psql -X -tA -h /tmp -p 5433 -U postgres -d postgres -c "show ssl")"

say "2. start the probe (privileged container, shares the node's PID namespace; BPFTRACE_STRLEN=200)"
docker rm -f ebpf-probe >/dev/null 2>&1
docker run -d --name ebpf-probe --privileged --pid=host -e BPFTRACE_STRLEN=200 -v /sys:/sys -v /lib/modules:/lib/modules \
  -v "${HERE}/probe.bt:/probe.bt" quay.io/iovisor/bpftrace:latest bpftrace /probe.bt >/dev/null 2>&1
for _ in $(seq 1 60); do docker logs ebpf-probe 2>&1 | grep -q "Attaching" && break; sleep 1; done
sleep 4
docker logs ebpf-probe 2>&1 | grep -E "Attaching|error|ERROR" | head -3

say "3. run the matrix (${REPS} repetitions as the superuser, plus one pass as a role that is not a superuser)"
OUT="$OUT" SRV_IP="$SRV_IP" REPS="$REPS" NS="$NS" CLUSTER="$CLUSTER" NODE="$NODE" python3 "${HERE}/matrix.py" run || { echo "matrix failed" >&2; exit 1; }
sleep 3
docker logs ebpf-probe 2>/dev/null > "${OUT}/events.txt"
docker stop ebpf-probe >/dev/null 2>&1
X cat /work/plain.log > "${OUT}/server-plain.log"
X cat /work/tls.log > "${OUT}/server-tls.log"

say "4. analysis"
OUT="$OUT" SRV_IP="$SRV_IP" REPS="$REPS" NS="$NS" CLUSTER="$CLUSTER" NODE="$NODE" python3 "${HERE}/matrix.py" analyze
