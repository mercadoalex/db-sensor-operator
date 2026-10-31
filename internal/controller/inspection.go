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
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
	"github.com/mercadoalex/db-sensor-operator/internal/inspection"
	"github.com/mercadoalex/db-sensor-operator/internal/snapshot"
)

const (
	// inspectionPoll is how soon to look at a running Job again. A Job event also triggers a
	// reconcile, so this is only a safety net.
	inspectionPoll = 5 * time.Second
	// inspectionRetry is how soon to look again after losing a race on the Job.
	inspectionRetry = 2 * time.Second
	// jobNameLabel is set by the Job controller on the pods of a Job.
	jobNameLabel = "job-name"
)

// inspectionOutcome is what one reconcile pass learned about the inspection. The reconcile never
// waits for the Job: it creates it, looks at it on the next pass, and reads its result once.
type inspectionOutcome struct {
	// apply changes the status of a freshly fetched sensor. It is idempotent, since it can run
	// again when the status write conflicts.
	apply func(*sensoriov1.DatabaseSensor)
	// after runs once the status has been written: it deletes a finished Job only when its result
	// is safely stored, so a failed write does not lose the result.
	after func(context.Context) error
	// requeueAfter asks for another pass sooner than the default cadence; 0 means no preference.
	requeueAfter time.Duration
}

func (r *DatabaseSensorReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// reconcileInspection drives the inspection of a sensor's database (ADR-014). The state lives in
// Kubernetes, not in memory: the Job says whether a run is in flight, and the sensor's status holds
// the last result. It never blocks and it never writes the status itself.
func (r *DatabaseSensorReconciler) reconcileInspection(ctx context.Context, sensor *sensoriov1.DatabaseSensor) (inspectionOutcome, error) {
	key := types.NamespacedName{Namespace: sensor.Namespace, Name: inspection.JobName(sensor.Name)}
	var job batchv1.Job
	err := r.Get(ctx, key, &job)
	exists := err == nil
	if err != nil && !apierrors.IsNotFound(err) {
		return inspectionOutcome{}, err
	}

	if !inspection.Enabled(sensor) {
		out := inspectionOutcome{}
		if exists {
			if err := r.deleteJob(ctx, &job); err != nil {
				return out, err
			}
		}
		if hasInspectionState(sensor) {
			out.apply = clearInspection
		}
		return out, nil
	}

	// The CRD already rejects this; the check keeps a Job from being built for an engine with no inspector.
	if sensor.Spec.Database.Type != "postgresql" {
		return inspectionOutcome{apply: markUnavailable(inspection.ReasonEngineNotSupported, inspection.ErrEngine.Error())}, nil
	}

	if !exists {
		return r.startInspection(ctx, sensor)
	}
	if !job.DeletionTimestamp.IsZero() {
		return inspectionOutcome{requeueAfter: inspectionRetry}, nil
	}
	return r.collectInspection(ctx, sensor, &job)
}

// startInspection creates the Job when an inspection is due.
func (r *DatabaseSensorReconciler) startInspection(ctx context.Context, sensor *sensoriov1.DatabaseSensor) (inspectionOutcome, error) {
	due, wait := inspection.Due(sensor, r.now())
	if !due {
		return inspectionOutcome{requeueAfter: wait}, nil
	}

	job, err := inspection.BuildJob(sensor, inspection.JobParams{Image: r.InspectorImage, PullPolicy: r.InspectorPullPolicy})
	if err != nil {
		// Nothing to run until the spec or the operator's configuration changes, so no requeue.
		return inspectionOutcome{apply: markUnavailable(buildFailureReason(err), err.Error())}, nil
	}
	if err := controllerutil.SetControllerReference(sensor, job, r.Scheme); err != nil {
		return inspectionOutcome{}, err
	}
	if err := r.Create(ctx, job); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return inspectionOutcome{requeueAfter: inspectionRetry}, nil
		}
		return inspectionOutcome{}, err
	}
	logf.FromContext(ctx).Info("Started database inspection", "job", inspection.Describe(job), "generation", sensor.Generation)

	out := inspectionOutcome{requeueAfter: inspectionPoll}
	if !hasFacts(sensor) {
		// Say that a first run is in flight, but leave an existing result alone: a refresh must not
		// make the conditions flap.
		out.apply = markUnavailableStatus(metav1.ConditionUnknown, inspection.ReasonRunning, "the first inspection of the database is running")
	}
	return out, nil
}

func buildFailureReason(err error) string {
	switch {
	case errors.Is(err, inspection.ErrNoImage):
		return inspection.ReasonImageNotConfigured
	case errors.Is(err, inspection.ErrEngine):
		return inspection.ReasonEngineNotSupported
	default:
		return snapshot.ErrCodeConfig
	}
}

// collectInspection looks at a Job that exists and, once it has finished, turns what it left into an outcome.
func (r *DatabaseSensorReconciler) collectInspection(ctx context.Context, sensor *sensoriov1.DatabaseSensor, job *batchv1.Job) (inspectionOutcome, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(job.Namespace), client.MatchingLabels{jobNameLabel: job.Name}); err != nil {
		return inspectionOutcome{}, err
	}
	terminated, waiting := inspectorContainerState(pods.Items)
	generation := observedGeneration(job, sensor)
	cleanup := func(c context.Context) error { return r.deleteJob(c, job) }

	switch {
	case terminated != nil:
		at := terminated.FinishedAt
		if at.IsZero() {
			at = metav1.NewTime(r.now())
		}
		out := inspectionResult(sensor, terminated.Message, at, generation)
		out.after = cleanup
		return out, nil

	case jobFailed(job) != nil:
		reason, text := describeFailure(job, waiting)
		out := failureOutcome(reason, text, metav1.NewTime(r.now()), generation)
		out.after = cleanup
		return out, nil

	case jobComplete(job):
		out := failureOutcome(inspection.ReasonNoResult, "the Job finished but its pod left no result", metav1.NewTime(r.now()), generation)
		out.after = cleanup
		return out, nil
	}
	return inspectionOutcome{requeueAfter: inspectionPoll}, nil
}

// inspectionResult parses a termination message into the outcome of a finished run.
func inspectionResult(sensor *sensoriov1.DatabaseSensor, message string, at metav1.Time, generation int64) inspectionOutcome {
	res, perr := inspection.ParseTermination(message)
	if perr != nil {
		return failureOutcome(perr.Reason, perr.Message, at, generation)
	}
	if res.Error != nil {
		return failureOutcome(res.Error.Code, res.Error.Message, at, generation)
	}

	st := inspection.ToStatus(res, at, generation)
	plan, conds := inspection.Evaluate(sensor, st)
	return inspectionOutcome{apply: func(s *sensoriov1.DatabaseSensor) {
		snap := ensureSnapshotStatus(s)
		snap.Inspection = st
		snap.Plan = plan
		for _, c := range conds {
			setCondition(s, c)
		}
		setCondition(s, metav1.Condition{
			Type: inspection.ConditionInspectionAvailable, Status: metav1.ConditionTrue, Reason: inspection.ReasonInspected,
			Message: "the last inspection finished and its facts are in status.snapshot.inspection",
		})
	}}
}

// failureOutcome records a failed run. The facts of an earlier run are dropped on purpose: the status
// shows the last inspection, and showing older facts beside a new error would pass them off as current.
func failureOutcome(reason, message string, at metav1.Time, generation int64) inspectionOutcome {
	failed := snapshot.NewErrorResult(reason, message)
	return inspectionOutcome{apply: func(s *sensoriov1.DatabaseSensor) {
		snap := ensureSnapshotStatus(s)
		snap.Inspection = inspection.ToStatus(failed, at, generation)
		snap.Plan = nil
		for _, t := range inspection.DerivedConditions {
			meta.RemoveStatusCondition(&s.Status.Conditions, t)
		}
		setCondition(s, metav1.Condition{
			Type: inspection.ConditionInspectionAvailable, Status: metav1.ConditionFalse, Reason: reason, Message: failed.Error.Message,
		})
	}}
}

func markUnavailable(reason, message string) func(*sensoriov1.DatabaseSensor) {
	return markUnavailableStatus(metav1.ConditionFalse, reason, message)
}

func markUnavailableStatus(status metav1.ConditionStatus, reason, message string) func(*sensoriov1.DatabaseSensor) {
	return func(s *sensoriov1.DatabaseSensor) {
		setCondition(s, metav1.Condition{Type: inspection.ConditionInspectionAvailable, Status: status, Reason: reason, Message: message})
	}
}

// clearInspection removes everything an inspection wrote, when the user turns it off.
func clearInspection(s *sensoriov1.DatabaseSensor) {
	if s.Status.Snapshot != nil {
		s.Status.Snapshot.Inspection = nil
		s.Status.Snapshot.Plan = nil
	}
	meta.RemoveStatusCondition(&s.Status.Conditions, inspection.ConditionInspectionAvailable)
	for _, t := range inspection.DerivedConditions {
		meta.RemoveStatusCondition(&s.Status.Conditions, t)
	}
}

func setCondition(s *sensoriov1.DatabaseSensor, c metav1.Condition) {
	c.ObservedGeneration = s.Generation
	meta.SetStatusCondition(&s.Status.Conditions, c)
}

func ensureSnapshotStatus(s *sensoriov1.DatabaseSensor) *sensoriov1.SnapshotStatus {
	if s.Status.Snapshot == nil {
		s.Status.Snapshot = &sensoriov1.SnapshotStatus{}
	}
	return s.Status.Snapshot
}

func hasFacts(s *sensoriov1.DatabaseSensor) bool {
	return s.Status.Snapshot != nil && s.Status.Snapshot.Inspection != nil
}

func hasInspectionState(s *sensoriov1.DatabaseSensor) bool {
	if s.Status.Snapshot != nil && (s.Status.Snapshot.Inspection != nil || s.Status.Snapshot.Plan != nil) {
		return true
	}
	for _, t := range append([]string{inspection.ConditionInspectionAvailable}, inspection.DerivedConditions...) {
		if meta.FindStatusCondition(s.Status.Conditions, t) != nil {
			return true
		}
	}
	return false
}

// inspectorContainerState finds the state of the inspector container across the pods of a Job. It
// returns the terminated state when the container has finished, and otherwise the reason it is waiting.
func inspectorContainerState(pods []corev1.Pod) (terminated *corev1.ContainerStateTerminated, waitingReason string) {
	for i := range pods {
		for _, cs := range pods[i].Status.ContainerStatuses {
			if cs.Name != inspection.ContainerName {
				continue
			}
			if cs.State.Terminated != nil {
				return cs.State.Terminated, ""
			}
			if cs.State.Waiting != nil {
				waitingReason = cs.State.Waiting.Reason
			}
		}
	}
	return nil, waitingReason
}

func jobFailed(job *batchv1.Job) *batchv1.JobCondition {
	for i := range job.Status.Conditions {
		if c := &job.Status.Conditions[i]; c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return c
		}
	}
	return nil
}

func jobComplete(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// describeFailure explains a Job that failed without leaving a result, for example one killed by its
// deadline before the container started because the image could not be pulled.
func describeFailure(job *batchv1.Job, waitingReason string) (reason, message string) {
	c := jobFailed(job)
	reason = inspection.ReasonInspectorFailed
	message = "the inspection Job failed"
	if c != nil && c.Reason == "DeadlineExceeded" {
		reason = inspection.ReasonTimedOut
		message = "the inspection did not finish within its deadline"
		if job.Spec.ActiveDeadlineSeconds != nil {
			message = fmt.Sprintf("the inspection did not finish within %d seconds", *job.Spec.ActiveDeadlineSeconds)
		}
	}
	if waitingReason != "" {
		message += "; the inspector container was waiting: " + waitingReason
	}
	return reason, message
}

// observedGeneration is the generation the Job was started for, falling back to the current one.
func observedGeneration(job *batchv1.Job, sensor *sensoriov1.DatabaseSensor) int64 {
	if v, ok := job.Annotations[inspection.GenerationAnnotation]; ok {
		if g, err := strconv.ParseInt(v, 10, 64); err == nil {
			return g
		}
	}
	return sensor.Generation
}

func (r *DatabaseSensorReconciler) deleteJob(ctx context.Context, job *batchv1.Job) error {
	err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground))
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
