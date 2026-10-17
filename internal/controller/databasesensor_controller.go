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
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
)

// DatabaseSensorReconciler reconciles a DatabaseSensor object
type DatabaseSensorReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=sensor.io,resources=databasesensors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sensor.io,resources=databasesensors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=sensor.io,resources=databasesensors/finalizers,verbs=update

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

	// Placeholder reconcile steps, in DESIGN.MD §6 / tasks.md order. These are
	// no-ops in the skeleton and are implemented in later tasks.
	// TODO(Task 4): r.Deployer.EnsureDebeziumServer(ctx, &sensor)
	// TODO(Task 5): r.Metrics.EnsureService / EnsureServiceMonitor(ctx, &sensor)
	// TODO(Task 10): r.Metrics.EnsureAlertRules(ctx, &sensor)
	// TODO(Task 9): phase, lag := r.Health.Check(ctx, &sensor)

	// Establish the initial hybrid status (ADR-011): conditions are the source
	// of truth and Phase is derived from them. Persist with conflict retry,
	// re-fetching the latest object inside the closure per AGENTS.md.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh sensoriov1.DatabaseSensor
		if err := r.Get(ctx, types.NamespacedName{Name: sensor.Name, Namespace: sensor.Namespace}, &fresh); err != nil {
			return err
		}
		meta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionUnknown,
			Reason:             "Reconciling",
			Message:            "Initializing sensor",
			ObservedGeneration: fresh.Generation,
		})
		fresh.Status.Phase = derivePhase(fresh.Status.Conditions)
		return r.Status().Update(ctx, &fresh)
	}); err != nil {
		log.Error(err, "Failed to update DatabaseSensor status")
		return ctrl.Result{}, err
	}

	// Requeue for the periodic health-check loop (US-7.1).
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// derivePhase computes the convenience Phase column from the condition set
// (ADR-011: conditions are the source of truth, Phase is always derived).
//
// Skeleton logic: until the Ready condition is True, the sensor is Pending.
// TODO(Task 9): extend this to derive Running/Degraded/Failed from the full
// condition set (DebeziumAvailable, ReceivingEvents, lag thresholds).
func derivePhase(conditions []metav1.Condition) string {
	ready := meta.FindStatusCondition(conditions, "Ready")
	if ready == nil || ready.Status != metav1.ConditionTrue {
		return "Pending"
	}
	return "Running"
}

// SetupWithManager sets up the controller with the Manager.
func (r *DatabaseSensorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&sensoriov1.DatabaseSensor{}).
		// TODO(Task 4): .Owns(&debeziumv1.DebeziumServer{}) — type not imported yet
		// TODO(Task 5): .Owns(&corev1.Service{}) — Service created in Task 5
		Named("databasesensor").
		Complete(r)
}
