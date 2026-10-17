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
	"sync"
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
	"github.com/mercadoalex/db-sensor-operator/internal/dashboard"
	"github.com/mercadoalex/db-sensor-operator/internal/deployer"
	"github.com/mercadoalex/db-sensor-operator/internal/health"
	"github.com/mercadoalex/db-sensor-operator/internal/metrics"
	"github.com/mercadoalex/db-sensor-operator/internal/sink"
)

// DatabaseSensorReconciler reconciles a DatabaseSensor object
type DatabaseSensorReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Exporter is the operator-wide Prometheus metrics exporter and Lag is the
	// operator-wide replication-lag tracker (DECISION 2 wiring). Both are
	// OPTIONAL and nil-safe: the reconcile skeleton does not use them yet. They
	// are injected by cmd/main.go so a later task can read lag/metrics into
	// status without re-plumbing.
	// TODO(Task 13): manage a per-sensor CDC consumer lifecycle here, (re)starting
	// it on spec.transport changes and feeding events through Exporter/Lag; read
	// Lag.GetLag()/LastEvent() into the hybrid status.
	Exporter *metrics.Exporter
	Lag      *health.LagTracker

	// Health derives the DatabaseSensor Phase and conditions from the child
	// DebeziumServer pod state + the lag/last-event signals (Task 9). OPTIONAL
	// and nil-safe: when nil, Reconcile constructs a default
	// health.NewChecker(r.Client, r.Lag) inline so envtest exercises the health
	// path without changing the test harness.
	Health *health.Checker

	// Dashboard is the operator-wide dashboard enablement gate and DashboardBuf is
	// the recent-events ring buffer (Task 14/15, DECISION A+B). Both are OPTIONAL
	// and nil-safe: the controller envtest harness builds the reconciler without
	// them. When set, Reconcile flips the gate on whenever any reconciled
	// DatabaseSensor requests the dashboard (operator-wide, converging across
	// enable/disable/delete) and pushes the maximum retention window to the
	// buffer.
	Dashboard    *dashboard.Gate
	DashboardBuf *dashboard.RingBuffer

	// dashboardMu guards dashboardSet, the set of sensor keys that currently
	// request the dashboard (used to converge the operator-wide gate across
	// multiple sensors). dashboardRetentions holds each requesting sensor's parsed
	// retention so the buffer's window is the maximum across them (DECISION A).
	dashboardMu         sync.Mutex
	dashboardSet        map[types.NamespacedName]struct{}
	dashboardRetentions map[types.NamespacedName]time.Duration
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
		// Drop any dashboard request this sensor held so the operator-wide gate
		// converges on delete (Task 14/15, DECISION B). Nil-safe.
		r.updateDashboardGate(req.NamespacedName, false, 0)
		return ctrl.Result{}, nil
	}

	// Converge the operator-wide dashboard gate and retention window from this
	// sensor's spec (Task 14/15, DECISION A+B). Nil-safe: the envtest harness
	// builds the reconciler without the gate/buffer, so this is a no-op there.
	r.reconcileDashboard(&sensor)

	// Ensure the child DebeziumServer (Task 4). The deployer is a package-level
	// function rather than a reconciler field: it has no state of its own, so
	// injecting it would add a dependency without a payoff. The result drives the
	// Ready condition set below.
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

	// Ensure the PrometheusRule from spec.sink.prometheus.alertRules (Task 10,
	// US-4.1). Unlike the Service/ServiceMonitor (plain K8s objects that back off
	// on error), Task 10 requires surfacing a failure via Ready=False so it is
	// visible on the CR. Capture the error and, if non-nil, write a
	// Ready=False/AlertRulesFailed condition (which derivePhase maps to Failed)
	// and requeue; the normal health path below runs only when alertErr == nil.
	alertErr := metrics.EnsureAlertRules(ctx, r.Client, r.Scheme, &sensor)
	if alertErr != nil {
		log.Error(alertErr, "Failed to ensure PrometheusRule")
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			var fresh sensoriov1.DatabaseSensor
			if err := r.Get(ctx, types.NamespacedName{Name: sensor.Name, Namespace: sensor.Namespace}, &fresh); err != nil {
				return err
			}
			meta.SetStatusCondition(&fresh.Status.Conditions, metav1.Condition{
				Type:               health.ConditionTypeReady,
				Status:             metav1.ConditionFalse,
				Reason:             "AlertRulesFailed",
				Message:            fmt.Sprintf("Failed to ensure PrometheusRule: %v", alertErr),
				ObservedGeneration: fresh.Generation,
			})
			fresh.Status.Phase = derivePhase(fresh.Status.Conditions)
			return r.Status().Update(ctx, &fresh)
		}); err != nil {
			log.Error(err, "Failed to update DatabaseSensor status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, alertErr
	}

	// Health check (Task 9). Only on the non-mapped-failure path: a mapped
	// SecretNotFound/UnsupportedDBType keeps today's Ready=False path exactly and
	// does not consult the checker. The checker is nil-safe: construct a default
	// inline so the envtest harness (which builds the reconciler with only
	// Client/Scheme) still exercises the health path. r.Lag is nil in tests → the
	// event-signal arm is absent → the pod-state arm alone decides the Phase.
	var healthRes health.HealthResult
	if ensureErr == nil {
		checker := r.Health
		if checker == nil {
			checker = health.NewChecker(r.Client, r.Lag)
		}
		res, herr := checker.Check(ctx, &sensor)
		if herr != nil {
			// Genuine API error: back off and retry WITHOUT touching status
			// (DESIGN "API server timeout" row).
			log.Error(herr, "Failed to evaluate DatabaseSensor health")
			return ctrl.Result{}, herr
		}
		healthRes = res
	}

	// Fan the health signal out to the configured alert sinks (Tasks 11/12,
	// US-4.4/4.5/4.6). Only on the non-mapped-failure path: a mapped
	// SecretNotFound/UnsupportedDBType never ran the health check, so there is no
	// health-derived alert to fan out. Sink failures are NON-BLOCKING — maybeAlert
	// logs warnings and never fails the reconcile or touches Phase. A Datadog 429
	// may return a requeue hint so the next cycle backs off per the DESIGN error
	// table.
	var alertRequeueAfter time.Duration
	if ensureErr == nil {
		alertRequeueAfter = r.maybeAlert(ctx, &sensor, healthRes)
	}

	// Translate the ensure/health result into the hybrid status (ADR-011):
	// conditions are the source of truth and Phase is derived from them. Persist
	// with conflict retry, re-fetching the latest object inside the closure per
	// AGENTS.md. All status writes live in this single block.
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var fresh sensoriov1.DatabaseSensor
		if err := r.Get(ctx, types.NamespacedName{Name: sensor.Name, Namespace: sensor.Namespace}, &fresh); err != nil {
			return err
		}

		if ensureErr != nil {
			// Mapped failure (SecretNotFound/UnsupportedDBType): keep today's
			// path exactly — a single Ready=False condition drives Phase=Failed.
			condition := readyConditionFor(ensureErr)
			condition.ObservedGeneration = fresh.Generation
			meta.SetStatusCondition(&fresh.Status.Conditions, condition)
		} else {
			// Health path: set all three conditions from the HealthResult.
			for _, c := range healthConditions(healthRes) {
				c.ObservedGeneration = fresh.Generation
				meta.SetStatusCondition(&fresh.Status.Conditions, c)
			}
			// US-1.1: record the managed child name on success.
			fresh.Status.DebeziumServer = fresh.Name
			// US-5.5: human-readable lag (0 → "0s") and last-event time.
			fresh.Status.Lag = healthRes.Lag.String()
			if healthRes.LastEvent.IsZero() {
				fresh.Status.LastEvent = nil
			} else {
				t := metav1.NewTime(healthRes.LastEvent)
				fresh.Status.LastEvent = &t
			}
			// US-5.5: current aggregate CDC event rate per minute for this
			// sensor's db host (summed across its tables, D1). Nil-safe: envtest
			// builds the reconciler without an Exporter, so EventsPerMin stays 0.
			if r.Exporter != nil {
				fresh.Status.EventsPerMin = r.Exporter.EventsPerMinForDB(sensor.Spec.Database.Host)
			}
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
	// once the Secret appears. A Datadog 429 (alertRequeueAfter > 0) shortens the
	// cadence to the parsed Retry-After when it is below the default 30s so the
	// next push respects the backoff (DESIGN "Datadog 429" row); status is left
	// unchanged on 429.
	requeueAfter := 30 * time.Second
	if alertRequeueAfter > 0 && alertRequeueAfter < requeueAfter {
		requeueAfter = alertRequeueAfter
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

// alertFireSet maps a terminal/degraded Ready reason to the Alert that fires for
// it (Decision 2 of the Task 11/12 plan). A reason absent from the set fires no
// alert. The Severity field tags the fan-out; the Value/Threshold are filled by
// maybeAlert from the HealthResult and the health thresholds.
var alertFireSet = map[string]struct {
	name     string
	severity string
	critical bool // true → threshold is the critical bound, else the warning bound
	lagBased bool // true → lag alert (Value=Lag); false → no-event alert
}{
	health.ReasonLagAboveWarning:  {name: "HighLag", severity: sink.SeverityWarning, critical: false, lagBased: true},
	health.ReasonLagAboveCritical: {name: "HighLag", severity: sink.SeverityCritical, critical: true, lagBased: true},
	health.ReasonNoEventsTimeout:  {name: "NoEvents", severity: sink.SeverityWarning, critical: false, lagBased: false},
	health.ReasonNoEventsStale:    {name: "NoEvents", severity: sink.SeverityCritical, critical: true, lagBased: false},
}

// maybeAlert builds the Alert for a fired health signal (Decision 2) and fans it
// out to the configured sinks. It is NON-BLOCKING: it never returns an error up
// the reconcile and never touches Phase. It returns a requeue hint only for a
// Datadog 429 (the parsed Retry-After), otherwise 0.
//
// A missing Datadog Secret (ErrDatadogSecretMissing) is a SOFT warning: the
// Datadog sink is skipped and the remaining sinks still fire. This is distinct
// from the DB password Secret, whose absence stays a hard Failed in the deployer
// path and never reaches here.
//
// TODO(Task N): optionally push the derived metrics (EventsPerMinForDB, lag) via
// CompositeSink.Push each reconcile, mirroring this non-blocking alert fan-out.
// Left undriven for Task 13 (D5): it adds outbound calls on the 30s cadence with
// no test harness and no Task 13 acceptance tied to it. The always-on Prometheus
// /metrics path already exposes these values.
func (r *DatabaseSensorReconciler) maybeAlert(ctx context.Context, sensor *sensoriov1.DatabaseSensor, res health.HealthResult) time.Duration {
	log := logf.FromContext(ctx)

	// Only fire when Ready=False with a reason in the fire-set.
	if res.Ready.Status != metav1.ConditionFalse {
		return 0
	}
	fire, ok := alertFireSet[res.Ready.Reason]
	if !ok {
		return 0
	}

	// Value/Threshold per the fire-set: lag alerts use the current lag and the
	// matching lag threshold; no-event alerts use the age since the last event
	// and the matching no-event timeout.
	var value, threshold float64
	if fire.lagBased {
		value = res.Lag.Seconds()
		if fire.critical {
			threshold = health.CriticalLag().Seconds()
		} else {
			threshold = health.WarningLag().Seconds()
		}
	} else {
		if !res.LastEvent.IsZero() {
			value = time.Since(res.LastEvent).Seconds()
		}
		if fire.critical {
			threshold = health.NoEventFail().Seconds()
		} else {
			threshold = health.NoEventWarn().Seconds()
		}
	}

	alert := sink.Alert{
		Name:      fire.name,
		DB:        sensor.Spec.Database.Host,
		Value:     value,
		Threshold: threshold,
		Timestamp: time.Now(),
		Severity:  fire.severity,
	}

	composite, buildErr := sink.BuildSinks(ctx, r.Client, sensor)
	if buildErr != nil {
		if errors.Is(buildErr, sink.ErrDatadogSecretMissing) {
			// Soft: skip Datadog, keep fanning out the rest. Not a Failed phase.
			log.Info("Skipped Datadog sink; secret not found", "sensor", sensor.Name, "namespace", sensor.Namespace)
		} else {
			log.Error(buildErr, "Failed to build alert sinks", "sensor", sensor.Name)
		}
	}
	if composite.Len() == 0 {
		return 0
	}

	if err := composite.Alert(ctx, alert); err != nil {
		// Non-blocking: a sink failure is logged, never fails the reconcile.
		log.Error(err, "Failed to deliver alert to one or more sinks",
			"alert", alert.Name, "sensor", sensor.Name)
		var rl *sink.RateLimitError
		if errors.As(err, &rl) && rl.RetryAfter > 0 {
			return rl.RetryAfter
		}
	}
	return 0
}

// readyConditionFor maps the EnsureDebeziumServer result to the Ready condition.
// nil → Ready=Unknown/DebeziumServerEnsured (pod health is Task 9). A mapped
// failure → Ready=False with a specific reason that derivePhase turns into
// Failed.
func readyConditionFor(err error) metav1.Condition {
	switch {
	case err == nil:
		return metav1.Condition{
			Type:    health.ConditionTypeReady,
			Status:  metav1.ConditionUnknown,
			Reason:  "DebeziumServerEnsured",
			Message: "DebeziumServer ensured; pod health is evaluated by a later task",
		}
	case errors.Is(err, deployer.ErrSecretNotFound):
		return metav1.Condition{
			Type:    health.ConditionTypeReady,
			Status:  metav1.ConditionFalse,
			Reason:  "SecretNotFound",
			Message: fmt.Sprintf("Database password secret not found: %v", err),
		}
	case errors.Is(err, deployer.ErrUnsupportedDBType):
		return metav1.Condition{
			Type:    health.ConditionTypeReady,
			Status:  metav1.ConditionFalse,
			Reason:  "UnsupportedDBType",
			Message: fmt.Sprintf("Unsupported database type: %v", err),
		}
	default:
		// Should not reach here: transient errors are handled before this call.
		return metav1.Condition{
			Type:    health.ConditionTypeReady,
			Status:  metav1.ConditionFalse,
			Reason:  "ReconcileError",
			Message: err.Error(),
		}
	}
}

// healthConditions maps a HealthResult into the three status conditions the
// reconciler persists (DebeziumAvailable, ReceivingEvents, Ready). Each shares
// the same LastTransitionTime handling via meta.SetStatusCondition.
func healthConditions(res health.HealthResult) []metav1.Condition {
	return []metav1.Condition{
		{
			Type:    health.ConditionTypeDebeziumAvailable,
			Status:  res.DebeziumAvailable.Status,
			Reason:  res.DebeziumAvailable.Reason,
			Message: res.DebeziumAvailable.Message,
		},
		{
			Type:    health.ConditionTypeReceivingEvents,
			Status:  res.ReceivingEvents.Status,
			Reason:  res.ReceivingEvents.Reason,
			Message: res.ReceivingEvents.Message,
		},
		{
			Type:    health.ConditionTypeReady,
			Status:  res.Ready.Status,
			Reason:  res.Ready.Reason,
			Message: res.Ready.Message,
		},
	}
}

// failureReasons are Ready=False reasons that represent a terminal state, so
// Phase is derived as Failed. It covers both the mapped-failure path
// (SecretNotFound/UnsupportedDBType) and the health path's terminal reasons.
var failureReasons = map[string]bool{
	"SecretNotFound":                 true,
	"UnsupportedDBType":              true,
	"AlertRulesFailed":               true,
	health.ReasonPodCrashLoopBackOff: true,
	health.ReasonLagAboveCritical:    true,
	health.ReasonNoEventsStale:       true,
}

// degradedReasons are Ready=False reasons that represent a non-terminal
// degradation, so Phase is derived as Degraded.
var degradedReasons = map[string]bool{
	health.ReasonLagAboveWarning: true,
	health.ReasonNoEventsTimeout: true,
}

// derivePhase computes the convenience Phase column from the condition set
// (ADR-011: conditions are the source of truth, Phase is always derived).
// It is a pure function. Precedence is worst-state-wins via the reason maps.
//
// Ready=True → Running. Ready=False with a failure reason → Failed; with a
// degraded reason → Degraded. Ready=False with a non-terminal/non-degraded
// reason (PodNotFound/PodNotReady/NoEventsYet/DebeziumServerEnsured),
// Ready=Unknown, or no Ready condition → Pending.
func derivePhase(conditions []metav1.Condition) string {
	ready := meta.FindStatusCondition(conditions, health.ConditionTypeReady)
	if ready == nil {
		return health.PhasePending
	}
	switch ready.Status {
	case metav1.ConditionTrue:
		return health.PhaseRunning
	case metav1.ConditionFalse:
		switch {
		case failureReasons[ready.Reason]:
			return health.PhaseFailed
		case degradedReasons[ready.Reason]:
			return health.PhaseDegraded
		default:
			return health.PhasePending
		}
	default:
		return health.PhasePending
	}
}

// reconcileDashboard reads the sensor's spec.dashboard and converges the
// operator-wide gate/retention via updateDashboardGate. A sensor requests the
// dashboard when spec.dashboard != nil && Enabled; its retention string is parsed
// with time.ParseDuration (empty/unparseable → 0, i.e. no time bound, logged
// nowhere since it is non-fatal, DECISION A).
func (r *DatabaseSensorReconciler) reconcileDashboard(sensor *sensoriov1.DatabaseSensor) {
	wants := sensor.Spec.Dashboard != nil && sensor.Spec.Dashboard.Enabled
	var retention time.Duration
	if wants && sensor.Spec.Dashboard.Retention != "" {
		if d, err := time.ParseDuration(sensor.Spec.Dashboard.Retention); err == nil && d > 0 {
			retention = d
		}
	}
	key := types.NamespacedName{Name: sensor.Name, Namespace: sensor.Namespace}
	r.updateDashboardGate(key, wants, retention)
}

// updateDashboardGate adds or removes a sensor key from the dashboard request set
// and recomputes the operator-wide gate (on when the set is non-empty) and the
// buffer's retention (the maximum window across requesting sensors, DECISION A).
// It is concurrency-safe and fully nil-safe: when the gate/buffer are not
// injected (envtest), it returns immediately.
func (r *DatabaseSensorReconciler) updateDashboardGate(key types.NamespacedName, wants bool, retention time.Duration) {
	if r.Dashboard == nil && r.DashboardBuf == nil {
		return
	}

	r.dashboardMu.Lock()
	if r.dashboardSet == nil {
		r.dashboardSet = make(map[types.NamespacedName]struct{})
		r.dashboardRetentions = make(map[types.NamespacedName]time.Duration)
	}
	if wants {
		r.dashboardSet[key] = struct{}{}
		r.dashboardRetentions[key] = retention
	} else {
		delete(r.dashboardSet, key)
		delete(r.dashboardRetentions, key)
	}
	enabled := len(r.dashboardSet) > 0
	var maxRetention time.Duration
	for _, d := range r.dashboardRetentions {
		if d > maxRetention {
			maxRetention = d
		}
	}
	r.dashboardMu.Unlock()

	if r.Dashboard != nil {
		r.Dashboard.Set(enabled)
	}
	if r.DashboardBuf != nil {
		r.DashboardBuf.SetRetention(maxRetention)
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
		// Same-namespace PrometheusRules carry an owner ref, so .Owns re-enqueues
		// the sensor when they drift (Task 10). Cross-namespace PrometheusRules
		// have no owner ref (K8s forbids cross-namespace owner refs), so .Owns
		// does not watch them; they are reconciled on the 30s requeue cadence.
		Owns(&monitoringv1.PrometheusRule{}).
		Named("databasesensor").
		Complete(r)
}
