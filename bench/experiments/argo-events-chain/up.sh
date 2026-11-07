#!/usr/bin/env bash
# Sets up Kafka, a second DebeziumServer, Argo Events v1.9.11 (pinned) and the sensor.
# Changes the bench cluster: installs Argo Events CRDs and controller cluster-wide, adds a status
# column and REPLICA IDENTITY FULL to public.payments. down.sh reverts.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CTX="kind-${CLUSTER:-db-sensor-bench}"
k() { kubectl --context "$CTX" "$@"; }
say() { printf '\n== %s\n' "$*"; }
say "payments: status column + REPLICA IDENTITY FULL"
k -n bench exec -i bench-postgres-0 -- psql -U bench -d bench -v ON_ERROR_STOP=1 \
  -c "alter table public.payments add column if not exists status text not null default 'pending'" \
  -c "alter table public.payments replica identity full" \
  -c "update public.payments set status='settled' where id <= 5"
say "Kafka"
k apply -f "$HERE/kafka.yaml" && k -n bench rollout status deploy/argo-kafka --timeout=180s
say "Argo Events v1.9.11"
k create namespace argo-events --dry-run=client -o yaml | k apply -f -
k apply -n argo-events -f https://github.com/argoproj/argo-events/releases/download/v1.9.11/install.yaml
k -n argo-events rollout status deploy/controller-manager --timeout=180s
cat <<'YAML' | k apply -n argo-events -f -
apiVersion: argoproj.io/v1alpha1
kind: EventBus
metadata: { name: default }
spec: { nats: { native: { replicas: 3, auth: token } } }
YAML
say "Debezium (Kafka sink)"
k apply -f "$HERE/debezium.yaml"
say "EventSource, Sensor, RBAC, target Job"
k apply -f "$HERE/argo.yaml"
