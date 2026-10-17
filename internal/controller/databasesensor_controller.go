/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
	"github.com/mercadoalex/db-sensor-operator/internal/deployer"
	"github.com/mercadoalex/db-sensor-operator/internal/metrics"
)

// Condition types and derived phases.
const (
	conditionTypeReady = "Ready"
	phasePending       = "Pending"
	phaseRunning       = "Running"
	phaseFailed        = "Failed"
)

// DatabaseSensorReconciler reconciles a DatabaseSensor object
type DatabaseSensorReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=sensor.io,resources=databasesensors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sensor.io,resources=databasesensors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=sensor.io,resources=databasesensors/finalizers,verbs=update
// +kubebuilder:rbac:groups=debezium.io,resources=debeziumservers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=prometheusrules,verbs=get;list;watch;create;update;patch;delete

// RBAC notes (NFR-5, least privilege; no wildcards, no cluster-admin):
//   - apps/deployments is read-only: the operator inspects the Debezium
//     Deployment for health (Task 9) but never mutates it (the Debezium Operator
//     owns it).
//   - services full CRUD: Task 5 creates/updates/deletes the per-sensor metrics
//     Service.
//   - configmaps full CRUD is listed by tasks.md item 5; Task 5 itself writes no
//     ConfigMap, so this MAY be narrowed to get;list;watch in a later task.
//   - secrets stay read-only (presence check only, NFR-4).
//   - monitoring.coreos.com/prometheusrules CRUD is granted now for the complete
//     role, though PrometheusRule creation is Task 10 (no code imports it yet).

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// This is the control-loop skeleton (Task 3). It fetches the CR, handles
// deletion, establishes an initial hybrid status (ADR-011), and requeues for
// the periodic health check (US-7.1). The real reconcile steps (DebeziumServer
// creation, Service/ServiceMonitor, alert rules, health checks) land in later
// tasks and are marked with TODO comments below.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/reconcile
func (r *DatabaseSensorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Fetch the DatabaseSensor. A NotFound error means the object was deleted;
	// child K8s resources are cleaned up by owner-reference GC (ADR-06), so
	// there is nothing to do here (child creation lands in Task 4+).
	var sensor sensoriov1.DatabaseSensor
	if err := r.Get(ctx, req.NamespacedName, &sensor); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	log.Info("Reconciling DatabaseSensor", "name", sensor.Name, "namespace", sensor.Namespace)

	// Handle deletion. Cascade deletion of child K8s objects is handled by owner
	// references (ADR-06), so a finalizer is NOT required for the skeleton.
	// TODO(Task N): finalizer-based external cleanup would go here IF non-K8s
	// resources (e.g. remote slots, external registrations) ever need teardown;
	// owner-reference GC covers child K8s objects today.
	if !sensor.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Ensure the child DebeziumServer (Task 4). The deployer is a package-level
	// function rather than a reconciler field: it has no state of its own, so
	// injecting it would add a dependency without a payoff. The result drives the
	// Ready condition set below.
	// TODO(Task 10): r.Metrics.EnsureAlertRules(ctx, &sensor)
	// TODO(Task 9): phase, lag := r.Health.Check(ctx, &sensor)
	ensureErr := deployer.EnsureDebeziumServer(ctx, r.Client, r.Scheme, &sensor)

	// A transient (non-mapped) error is returned so controller-runtime backs off
	// and retries; it must not take the status path below (DESIGN "API server
	// timeout" row).
	if ensureErr != nil &&
		!errors.Is(ensureErr, deployer.ErrSecretNotFound) &&
		!errors.Is(ensureErr, deployer.ErrUnsupportedDBType) {
		log.Error(ensureErr, "Failed to ensure DebeziumServer")
		return ctrl.Result{}, ensureErr
	}

	// Ensure the metrics scrape path (Task 5): the per-sensor Service (US-3.3)
	// and, when opted in, the ServiceMonitor (US-3.2). These are plain K8s
	// objects with no mapped failure status reason; a non-nil error is returned
	// so controller-runtime backs off and retries, as with transient
	// DebeziumServer errors. Run only once the (possibly mapped) DebeziumServer
	// ensure did not hit a transient error above.
	if err := metrics.EnsureService(ctx, r.Client, r.Scheme, &sensor); err != nil {
		log.Error(err, "Failed to ensure metrics Service")
		return ctrl.Result{}, err
	}
	if err := metrics.EnsureServiceMonitor(ctx, r.Client, r.Scheme, &sensor); err != nil {
		log.Error(err, "Failed to ensure ServiceMonitor")
		return ctrl.Result{}, err
	}

	// Translate the ensure result into the hybrid status (ADR-011): conditions
	// are the source of truth and Phase is derived from them. Persist with
	// conflict retry, re-fetching the latest object inside the closure per
	// AGENTS.md. All status writes live in this single block.
	condition := readyConditionFor(ensureErr)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh sensoriov1.DatabaseSensor
		if err := r.Get(ctx, types.NamespacedName{Name: sensor.Name, Namespace: sensor.Namespace}, &fresh); err != nil {
			return err
		}
		condition.ObservedGeneration = fresh.Generation
		meta.SetStatusCondition(&fresh.Status.Conditions, condition)
		if ensureErr == nil {
			// US-1.1: record the managed child name on success.
			fresh.Status.DebeziumServer = fresh.Name
		}
		fresh.Status.Phase = derivePhase(fresh.Status.Conditions)
		return r.Status().Update(ctx, &fresh)
	}); err != nil {
		log.Error(err, "Failed to update DatabaseSensor status")
		return ctrl.Result{}, err
	}

	// Requeue for the periodic health-check loop (US-7.1). A mapped failure
	// (SecretNotFound/UnsupportedDBType) returns nil here on purpose so the
	// reconcile does not hot-loop; it re-runs on the 30s cadence and recovers
	// once the Secret appears.
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// readyConditionFor maps the EnsureDebeziumServer result to the Ready condition.
// nil → Ready=Unknown/DebeziumServerEnsured (pod health is Task 9). A mapped
// failure → Ready=False with a specific reason that derivePhase turns into
// Failed.
func readyConditionFor(err error) metav1.Condition {
	switch {
	case err == nil:
		return metav1.Condition{
			Type:    conditionTypeReady,
			Status:  metav1.ConditionUnknown,
			Reason:  "DebeziumServerEnsured",
			Message: "DebeziumServer ensured; pod health is evaluated by a later task",
		}
	case errors.Is(err, deployer.ErrSecretNotFound):
		return metav1.Condition{
			Type:    conditionTypeReady,
			Status:  metav1.ConditionFalse,
			Reason:  "SecretNotFound",
			Message: fmt.Sprintf("Database password secret not found: %v", err),
		}
	case errors.Is(err, deployer.ErrUnsupportedDBType):
		return metav1.Condition{
			Type:    conditionTypeReady,
			Status:  metav1.ConditionFalse,
			Reason:  "UnsupportedDBType",
			Message: fmt.Sprintf("Unsupported database type: %v", err),
		}
	default:
		// Should not reach here: transient errors are handled before this call.
		return metav1.Condition{
			Type:    conditionTypeReady,
			Status:  metav1.ConditionFalse,
			Reason:  "ReconcileError",
			Message: err.Error(),
		}
	}
}

// failureReasons are Ready=False reasons that represent a terminal/misconfigured
// state rather than a transient one, so Phase is derived as Failed.
var failureReasons = map[string]bool{
	"SecretNotFound":    true,
	"UnsupportedDBType": true,
}

// derivePhase computes the convenience Phase column from the condition set
// (ADR-011: conditions are the source of truth, Phase is always derived).
// It is a pure function.
//
// Ready=True → Running. Ready=False with a failure reason → Failed. Ready=False
// without such a reason, Ready=Unknown, or no Ready condition → Pending.
// TODO(Task 9): extend to Degraded/Running from the full condition set
// (DebeziumAvailable, ReceivingEvents, lag thresholds).
func derivePhase(conditions []metav1.Condition) string {
	ready := meta.FindStatusCondition(conditions, conditionTypeReady)
	if ready == nil {
		return phasePending
	}
	switch ready.Status {
	case metav1.ConditionTrue:
		return phaseRunning
	case metav1.ConditionFalse:
		if failureReasons[ready.Reason] {
			return phaseFailed
		}
		return phasePending
	default:
		return phasePending
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *DatabaseSensorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// The DebeziumServer type has no importable Go module (Step 0 decision), so
	// watch it as an unstructured object carrying the DebeziumServer GVK. This
	// gives owner-reference-driven requeues on child changes (US-1.4 cascade is
	// GC; this .Owns re-reconciles the sensor when the child drifts).
	debeziumServer := &unstructured.Unstructured{}
	debeziumServer.SetGroupVersionKind(deployer.DebeziumServerGVK)

	return ctrl.NewControllerManagedBy(mgr).
		For(&sensoriov1.DatabaseSensor{}).
		Owns(debeziumServer).
		// The per-sensor metrics Service is always owned by the sensor (Task 5).
		Owns(&corev1.Service{}).
		// Same-namespace ServiceMonitors carry an owner ref, so .Owns re-enqueues
		// the sensor when they drift. Cross-namespace ServiceMonitors have no
		// owner ref (K8s forbids cross-namespace owner refs), so .Owns does not
		// watch them; they are reconciled on the 30s requeue cadence (DECISION 2/3).
		Owns(&monitoringv1.ServiceMonitor{}).
		Named("databasesensor").
		Complete(r)
}
