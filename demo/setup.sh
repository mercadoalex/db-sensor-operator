#!/usr/bin/env bash
#
# demo/setup.sh — stand up a local kind cluster and install the operator for the
# synthetic-event demo. No real database, Debezium, Kafka, or Datadog required.
#
# What it does:
#   1. Creates a kind cluster (if one named $CLUSTER doesn't already exist).
#   2. Installs the Prometheus Operator CRDs (ServiceMonitor + PrometheusRule)
#      so the operator can create those objects. (The full Prometheus stack is
#      NOT required for the synthetic demo — only the CRDs are, so the operator's
#      EnsureServiceMonitor / EnsureAlertRules succeed.)
#   3. Builds the operator image and loads it into kind.
#   4. Installs the Helm chart into the db-sensor namespace with the operator
#      HTTP server enabled.
#   5. Creates the dummy DB-credentials Secret the sample CR references (so the
#      sensor does not start in Failed on a missing Secret).
#
# Prereqs: docker, kind, kubectl, helm. (brew install kind kubectl helm)
#
# Usage:
#   ./demo/setup.sh            # full setup
#   CLUSTER=mydemo ./demo/setup.sh
#
set -euo pipefail

CLUSTER="${CLUSTER:-db-sensor-demo}"
NS="${NS:-db-sensor}"
IMG="${IMG:-db-sensor-operator:demo}"
CHART_DIR="charts/db-sensor-operator"
PROM_OPERATOR_VERSION="${PROM_OPERATOR_VERSION:-v0.76.0}"

# Resolve repo root so the script works from anywhere.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

need() { command -v "$1" >/dev/null 2>&1 || { echo "ERROR: '$1' not found on PATH. Install it first (e.g. brew install $1)." >&2; exit 1; }; }
need docker; need kind; need kubectl; need helm

echo "==> [1/5] kind cluster '$CLUSTER'"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "    cluster '$CLUSTER' already exists, reusing it"
else
  kind create cluster --name "$CLUSTER"
fi
kubectl cluster-info --context "kind-${CLUSTER}" >/dev/null

echo "==> [2/5] CRDs: Prometheus Operator (ServiceMonitor + PrometheusRule) + DebeziumServer"
# Only the CRDs are needed so the operator can create those objects. Applying the
# bundle CRDs directly keeps the demo light (no full kube-prometheus-stack).
kubectl apply --server-side \
  -f "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/${PROM_OPERATOR_VERSION}/example/prometheus-operator-crd/monitoring.coreos.com_servicemonitors.yaml" \
  -f "https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/${PROM_OPERATOR_VERSION}/example/prometheus-operator-crd/monitoring.coreos.com_prometheusrules.yaml"

# The DebeziumServer CRD (debezium.io/v1alpha1) MUST exist before the operator
# starts, because the controller sets up an .Owns() watch on DebeziumServer — if
# the CRD is absent the watch fails to start and reconcile stalls. In production
# this CRD comes from the official Debezium Operator; the synthetic demo does NOT
# run real Debezium, so we install only the minimal CRD (the same one the envtest
# suite uses) so the operator can create the child DebeziumServer object.
kubectl apply --server-side \
  -f internal/controller/testdata/crd/debeziumservers.debezium.io.yaml

echo "==> [3/5] build operator image and load into kind"
make docker-build IMG="$IMG"
kind load docker-image "$IMG" --name "$CLUSTER"

echo "==> [4/5] helm install the operator into namespace '$NS'"
# operatorHttp.enabled exposes the :8080 server (metrics/dashboard/api/events/cdc).
# image.repository/tag point at the kind-loaded image; pullPolicy Never so kind
# uses the loaded image instead of trying to pull.
helm upgrade --install db-sensor "$CHART_DIR" \
  --namespace "$NS" --create-namespace \
  --set operatorHttp.enabled=true \
  --set-string manager.image.repository="${IMG%:*}" \
  --set-string manager.image.tag="${IMG##*:}" \
  --set-string manager.image.pullPolicy=Never \
  --wait --timeout 180s || {
    echo "    helm --wait timed out; continuing (the deployment may still be rolling out)." >&2
  }

echo "==> [5/5] create the dummy DB-credentials Secret referenced by the sample CR"
# The operator only checks this Secret EXISTS (US-1.3). The value is never used in
# the synthetic demo because no real DB is contacted.
kubectl -n "$NS" create secret generic demo-db-creds \
  --from-literal=password=not-a-real-password \
  --dry-run=client -o yaml | kubectl apply -f -

cat <<EOF

================================================================================
Setup complete.

Cluster:   kind-${CLUSTER}
Namespace: ${NS}
Image:     ${IMG}

Next:
  kubectl apply -f demo/sample-databasesensor.yaml
  kubectl -n ${NS} get databasesensor -w          # watch status
  kubectl -n ${NS} port-forward svc/db-sensor-db-sensor-operator-operator-http 8080:8080

Then in another terminal:
  ./demo/generate-events.sh                        # feed synthetic CDC events
  open http://localhost:8080/dashboard             # live dashboard
  curl -s http://localhost:8080/metrics | grep debezium_

See demo/README.md for the full walkthrough.

Tear down with: ./demo/teardown.sh
================================================================================
EOF
