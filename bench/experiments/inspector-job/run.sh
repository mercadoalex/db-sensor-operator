#!/usr/bin/env bash
#
# bench/experiments/inspector-job/run.sh — does the Job -> termination message mechanism work?
#
# The inspector runs as a Job and reports its result as JSON in the container's termination
# message, which the operator would copy into the sensor's status. This runs it by hand against
# the benchmark PostgreSQL, with the pod hardened the way the operator's Job will be (non-root,
# read-only root filesystem, no capabilities, no Kubernetes API token), and checks:
#
#   1. success: facts, including a replication slot, arrive as JSON and the Job succeeds
#   2. wrong password: the Job fails, the message says AuthenticationFailed, no password in it
#   3. unreachable host: ConnectionFailed
#   4. many tables: the message is cut by us, below the kubelet's 4096-byte limit, and says so
#   5. unsupported engine: InvalidConfiguration
#
# Requires `make -C bench setup`. Builds the operator image (which now carries /inspector).
#
set -euo pipefail

CLUSTER="${CLUSTER:-db-sensor-bench}"
NS="${NS:-bench}"
IMG="${IMG:-db-sensor-operator:bench}"
CTX="kind-${CLUSTER}"
K="kubectl --context ${CTX} -n ${NS}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../../.." && pwd)"
PSQL="$K exec bench-postgres-0 -- psql -U bench -d bench -tAq -c"
FAILS=0

echo "==> building ${IMG} and loading it into kind"
(cd "$REPO_ROOT" && make docker-build IMG="$IMG" >/dev/null)
kind load docker-image "$IMG" --name "$CLUSTER" >/dev/null 2>&1

# A wrong-password Secret, to provoke an authentication failure.
$K create secret generic bench-pg-wrong --from-literal=password='definitely-wrong' --dry-run=client -o yaml | $K apply -f - >/dev/null
# A slot to report on.
$PSQL "select pg_create_logical_replication_slot('inspect_probe','pgoutput') where not exists (select 1 from pg_replication_slots where slot_name='inspect_probe')" >/dev/null || true

job() { # name secret host tables slot engine
  local name="$1" secret="$2" host="$3" tables="$4" slot="$5" engine="$6"
  $K delete job "$name" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  cat <<EOF | $K apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata: { name: ${name} }
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 60
  template:
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      securityContext: { runAsNonRoot: true, runAsUser: 65532, seccompProfile: { type: RuntimeDefault } }
      containers:
        - name: inspector
          image: ${IMG}
          imagePullPolicy: Never
          command: ["/inspector"]
          terminationMessagePolicy: File
          securityContext: { allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: ["ALL"] } }
          resources: { requests: { cpu: 10m, memory: 32Mi }, limits: { memory: 128Mi } }
          envFrom: [ { secretRef: { name: ${secret} }, prefix: DATABASE_ } ]
          env:
            - { name: INSPECT_ENGINE, value: "${engine}" }
            - { name: INSPECT_HOST, value: "${host}" }
            - { name: INSPECT_PORT, value: "5432" }
            - { name: INSPECT_USER, value: bench }
            - { name: INSPECT_DATABASE, value: bench }
            - { name: INSPECT_SSLMODE, value: disable }
            - { name: INSPECT_TABLES, value: "${tables}" }
            - { name: INSPECT_SLOT, value: "${slot}" }
EOF
  # Wait for the Job to finish either way.
  for _ in $(seq 1 60); do
    s=$($K get job "$name" -o jsonpath='{.status.succeeded}' 2>/dev/null || true)
    f=$($K get job "$name" -o jsonpath='{.status.failed}' 2>/dev/null || true)
    [[ "$s" == "1" || "$f" == "1" ]] && break
    sleep 2
  done
}

result() { # name -> termination message
  $K get pod -l job-name="$1" -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.message}'
}
exitcode() { $K get pod -l job-name="$1" -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.exitCode}'; }

check() { # description condition-result
  if [[ "$2" == "ok" ]]; then echo "   PASS  $1"; else echo "   FAIL  $1"; FAILS=$((FAILS + 1)); fi
}
contains() { [[ "$1" == *"$2"* ]] && echo ok || echo no; }

echo; echo "== 1. success"
job insp-ok bench-pg-creds bench-postgres "public.orders,public.users,public.nope" inspect_probe postgresql
m=$(result insp-ok); echo "   exit=$(exitcode insp-ok) bytes=${#m}"; echo "   ${m:0:420}"
check "the Job succeeded with exit 0" "$([[ "$(exitcode insp-ok)" == 0 ]] && echo ok || echo no)"
check "the message is JSON with factsVersion 1" "$(contains "$m" '"factsVersion":1')"
check "the slot facts are present" "$(contains "$m" '"replicationSlot"')"
check "a missing table is reported, not dropped" "$(contains "$m" '"name":"public.nope","found":false')"

echo; echo "== 2. wrong password"
job insp-auth bench-pg-wrong bench-postgres "public.orders" "" postgresql
m=$(result insp-auth); echo "   exit=$(exitcode insp-auth)"; echo "   ${m:0:300}"
check "exit code 4 (authentication)" "$([[ "$(exitcode insp-auth)" == 4 ]] && echo ok || echo no)"
check "the message says AuthenticationFailed" "$(contains "$m" AuthenticationFailed)"
check "the password is not in the message" "$([[ "$m" != *definitely-wrong* ]] && echo ok || echo no)"

echo; echo "== 3. unreachable host"
job insp-conn bench-pg-creds no-such-host.invalid "public.orders" "" postgresql
m=$(result insp-conn); echo "   exit=$(exitcode insp-conn)"; echo "   ${m:0:300}"
check "exit code 3 (connection)" "$([[ "$(exitcode insp-conn)" == 3 ]] && echo ok || echo no)"
check "the message says ConnectionFailed" "$(contains "$m" ConnectionFailed)"

echo; echo "== 4. many tables"
many=$(python3 -c "print(','.join('public.table_number_%04d' % i for i in range(150)))")
job insp-big bench-pg-creds bench-postgres "$many" "" postgresql
m=$(result insp-big); echo "   exit=$(exitcode insp-big) bytes=${#m}"
check "the Job succeeded" "$([[ "$(exitcode insp-big)" == 0 ]] && echo ok || echo no)"
check "the message fits below the kubelet limit (4096)" "$([[ ${#m} -le 4000 ]] && echo ok || echo no)"
check "it is valid JSON and says it was truncated" "$(python3 -c "import json,sys; d=json.loads(sys.argv[1]); print('ok' if d.get('truncated') and 0 < len(d['tables']) < 150 else 'no')" "$m" 2>/dev/null || echo no)"

echo; echo "== 5. unsupported engine"
job insp-eng bench-pg-creds bench-postgres "public.orders" "" mysql
m=$(result insp-eng); echo "   exit=$(exitcode insp-eng)"; echo "   ${m:0:200}"
check "exit code 2 (configuration)" "$([[ "$(exitcode insp-eng)" == 2 ]] && echo ok || echo no)"
check "the message says InvalidConfiguration" "$(contains "$m" InvalidConfiguration)"

echo; echo "== cleanup"
for j in insp-ok insp-auth insp-conn insp-big insp-eng; do $K delete job "$j" --ignore-not-found --wait=false >/dev/null 2>&1 || true; done
$K delete secret bench-pg-wrong --ignore-not-found >/dev/null 2>&1 || true
$PSQL "select pg_drop_replication_slot('inspect_probe') where exists (select 1 from pg_replication_slots where slot_name='inspect_probe')" >/dev/null 2>&1 || true

echo; if [[ "$FAILS" == 0 ]]; then echo "ALL CHECKS PASSED"; else echo "${FAILS} CHECK(S) FAILED"; exit 1; fi
