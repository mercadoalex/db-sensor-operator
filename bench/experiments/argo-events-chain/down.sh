#!/usr/bin/env bash
# Reverts up.sh.
set -uo pipefail
CTX="kind-${CLUSTER:-db-sensor-bench}"
k() { kubectl --context "$CTX" "$@"; }
k delete -n argo-events sensor/settled-to-pending eventsource/payments-cdc --ignore-not-found
k delete -n bench job/payments-writer debeziumserver/argo-cdc deploy/argo-kafka svc/argo-kafka \
  role/job-suspender rolebinding/job-suspender --ignore-not-found
k delete -n argo-events eventbus/default --ignore-not-found
k delete -n argo-events -f https://github.com/argoproj/argo-events/releases/download/v1.9.11/install.yaml --ignore-not-found
k delete namespace argo-events --ignore-not-found
# The slot stays active until the Debezium pod is gone; retry for up to a minute.
for i in $(seq 1 12); do
  out="$(k -n bench exec -i bench-postgres-0 -- psql -U bench -d bench -At \
    -c "select pg_drop_replication_slot('argo_test_slot') from pg_replication_slots where slot_name='argo_test_slot'" 2>&1)"
  echo "$out" | grep -q "is active" || break
  sleep 5
done
k -n bench exec -i bench-postgres-0 -- psql -U bench -d bench \
  -c "alter table public.payments replica identity default" \
  -c "alter table public.payments drop column if exists status"
