#!/usr/bin/env bash
#
# demo/teardown.sh — remove the demo resources and/or the kind cluster.
#
# Usage:
#   ./demo/teardown.sh            # delete the CR + helm release, keep the cluster
#   ./demo/teardown.sh --cluster  # also delete the kind cluster
#
set -euo pipefail

CLUSTER="${CLUSTER:-db-sensor-demo}"
NS="${NS:-db-sensor}"
DELETE_CLUSTER=false
[[ "${1:-}" == "--cluster" ]] && DELETE_CLUSTER=true

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> removing the demo DatabaseSensor (children GC via owner refs)"
kubectl -n "$NS" delete -f demo/sample-databasesensor.yaml --ignore-not-found

echo "==> uninstalling the helm release"
helm uninstall db-sensor -n "$NS" || true

echo "==> deleting the dummy Secret"
kubectl -n "$NS" delete secret demo-db-creds --ignore-not-found || true

if $DELETE_CLUSTER; then
  echo "==> deleting kind cluster '$CLUSTER'"
  kind delete cluster --name "$CLUSTER"
else
  echo "Kept kind cluster '$CLUSTER'. Delete it with: kind delete cluster --name $CLUSTER"
fi

echo "Teardown complete."
