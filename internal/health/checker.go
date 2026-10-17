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

// Package health — Checker (Task 9).
//
// The Checker derives the DatabaseSensor Phase (US-5) and the three health
// conditions (DebeziumAvailable, ReceivingEvents, Ready) from three independent
// signals:
//
//  1. the child DebeziumServer pod state, read off its status subresource
//     (Decision B of the Task 9 plan): not-yet-running → Pending (US-5.1,
//     US-7.1); CrashLoopBackOff/Error → Failed with the last termination
//     message (US-7.2);
//  2. the no-event clock, measured off LagTracker.LastEvent(): ≥5m → Degraded
//     and a logged warning (US-7.3); ≥30m → Failed, "No CDC events received"
//     (US-5.4, US-4.3);
//  3. the replication lag, measured off LagTracker.GetLag(): ≥warn → Degraded
//     (US-5.3); >critical → Failed (US-5.4, US-4.2).
//
// Phase is combined worst-state-wins: Failed > Degraded > Running > Pending.
//
// decidePhase is a PURE function over phaseInputs so the truth table is unit
// testable without a cluster; Check does the IO (fetching the unstructured
// child, reading the lag/last-event signals) and delegates the decision.
package health

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
	"github.com/mercadoalex/db-sensor-operator/internal/deployer"
)

// Threshold defaults (Decision A of the Task 9 plan). They are package
// constants, NOT spec fields, so behaviour is identical across sensors.
//
// Sources:
//   - warningLagThreshold / criticalLagThreshold: DESIGN.MD "Niveles de
//     severidad" (lag warning >30s, lag critical >120s) + US-5.3 / US-5.4 /
//     US-4.2.
//   - noEventWarnTimeout: US-7.3 (no event for the configured timeout, default
//     300s → Degraded + logged warning).
//   - noEventFailTimeout: US-5.4 / US-7 (no events for 30m → Failed) and the
//     DESIGN error table.
//
// US-4.3 (rate==0 for 15m → Failed) has no in-process PromQL engine; the honest
// in-process proxy is "no CDC event observed for 15m" off LagTracker. We keep a
// single no-event clock (5m→Degraded / 30m→Failed, matching US-7.3/US-5.4 and
// the DESIGN error table) and leave the 15m rate-zero rule to the generated
// PrometheusRule in Task 10, so there is only one Failed timer here.
//
// TODO(configurable): promote these to spec/operator config in a later task.
const (
	warningLagThreshold  = 30 * time.Second
	criticalLagThreshold = 120 * time.Second
	noEventWarnTimeout   = 5 * time.Minute
	noEventFailTimeout   = 30 * time.Minute
)

// WarningLag / CriticalLag / NoEventWarn / NoEventFail expose the package
// threshold constants so the reconciler can fill Alert.Threshold without copying
// them. They are pure accessors and do not change decidePhase/Check behaviour.
func WarningLag() time.Duration  { return warningLagThreshold }
func CriticalLag() time.Duration { return criticalLagThreshold }
func NoEventWarn() time.Duration { return noEventWarnTimeout }
func NoEventFail() time.Duration { return noEventFailTimeout }

// Phase values. Mirror the controller's phase constants (ADR-011).
const (
	phasePending  = "Pending"
	phaseRunning  = "Running"
	phaseDegraded = "Degraded"
	phaseFailed   = "Failed"
)

// Condition types the Checker reports back to the reconciler.
const (
	ConditionTypeDebeziumAvailable = "DebeziumAvailable"
	ConditionTypeReceivingEvents   = "ReceivingEvents"
	ConditionTypeReady             = "Ready"
)

// Machine-readable reasons (CamelCase per ADR-011). The exact set is shared with
// the decidePhase truth table and the reconciler's phase-derivation maps.
const (
	ReasonPodNotFound         = "PodNotFound"
	ReasonPodNotReady         = "PodNotReady"
	ReasonPodCrashLoopBackOff = "PodCrashLoopBackOff"
	ReasonPodRunning          = "PodRunning"
	ReasonNoEventsYet         = "NoEventsYet"
	ReasonEventsFlowing       = "EventsFlowing"
	ReasonNoEventsTimeout     = "NoEventsTimeout"
	ReasonNoEventsStale       = "NoEventsStale"
	ReasonLagAboveWarning     = "LagAboveWarning"
	ReasonLagAboveCritical    = "LagAboveCritical"
	ReasonHealthyEndToEnd     = "HealthyEndToEnd"
)

// terminationUnavailable is the fallback DebeziumAvailable message when a
// crash-looping pod reports no termination message (US-7.2).
const terminationUnavailable = "termination reason unavailable"

// Reused human-readable condition messages.
const (
	msgNoEventsYet  = "No CDC events observed yet"
	msgNoEventsWarn = "No CDC events received within the warning window"
)

// ConditionInput carries the status/reason/message the reconciler turns into a
// metav1.Condition via meta.SetStatusCondition.
type ConditionInput struct {
	Status  metav1.ConditionStatus
	Reason  string
	Message string
}

// HealthResult is the outcome of a single Check. Phase is derived worst-state-
// wins from the three condition inputs; Lag/LastEvent are point-in-time
// measurements for the hybrid status.
type HealthResult struct {
	Phase             string
	DebeziumAvailable ConditionInput
	ReceivingEvents   ConditionInput
	Ready             ConditionInput
	Lag               time.Duration // 0 when no event / clock skew
	LastEvent         time.Time     // zero when none
	Message           string        // human summary (e.g. termination message)
}

// Checker derives health for a DatabaseSensor. Dependencies are injected so the
// IO path is testable with a fake client and a stub clock; the decision itself
// lives in the pure decidePhase helper.
type Checker struct {
	client client.Client
	now    func() time.Time
	lag    *LagTracker // source of GetLag()/LastEvent(); may be nil

	// Thresholds captured as fields (default to the constants) so tests can
	// override without touching package-level state.
	warningLag  time.Duration
	criticalLag time.Duration
	noEventWarn time.Duration
	noEventFail time.Duration
}

// NewChecker returns a Checker using the real clock. lag may be nil (the event
// signal is then treated as absent — see Check).
func NewChecker(c client.Client, lag *LagTracker) *Checker {
	return NewCheckerWithClock(c, lag, time.Now)
}

// NewCheckerWithClock returns a Checker with an injectable clock for
// deterministic tests. now must not be nil.
func NewCheckerWithClock(c client.Client, lag *LagTracker, now func() time.Time) *Checker {
	return &Checker{
		client:      c,
		now:         now,
		lag:         lag,
		warningLag:  warningLagThreshold,
		criticalLag: criticalLagThreshold,
		noEventWarn: noEventWarnTimeout,
		noEventFail: noEventFailTimeout,
	}
}

// phaseInputs is the pure decision input. No IO, no clock.
type phaseInputs struct {
	childExists    bool
	podRunning     bool
	crashLoop      bool
	termMsg        string
	lag            time.Duration
	haveEvent      bool
	sinceLastEvent time.Duration
}

// conditionSet is the pure decision output alongside the Phase.
type conditionSet struct {
	debeziumAvailable ConditionInput
	receivingEvents   ConditionInput
	ready             ConditionInput
}

// Check fetches the DebeziumServer child for the sensor, reads its pod state
// (Decision B), composes the event/lag signals from the LagTracker, and returns
// the pure decision as a HealthResult. A missing child resolves to Pending, not
// an error; only genuine API errors are returned.
func (ch *Checker) Check(ctx context.Context, sensor *sensoriov1.DatabaseSensor) (HealthResult, error) {
	log := logf.FromContext(ctx)

	in := phaseInputs{}

	// Fetch the unstructured child the deployer creates (same GVK/name/namespace).
	child := &unstructured.Unstructured{}
	child.SetGroupVersionKind(deployer.DebeziumServerGVK)
	key := types.NamespacedName{Name: sensor.Name, Namespace: sensor.Namespace}
	if err := ch.client.Get(ctx, key, child); err != nil {
		if !apierrors.IsNotFound(err) {
			return HealthResult{}, fmt.Errorf("getting DebeziumServer %q: %w", sensor.Name, err)
		}
		// US-7.4: child not created yet → treated as pod-not-found, not an error.
		in.childExists = false
	} else {
		in.childExists = true
		in.podRunning, in.crashLoop, in.termMsg = readPodState(child)
	}

	// Event signals from the LagTracker. Nil-safe: when the reconciler has no
	// per-sensor tracker wired yet (Task 13), treat events as absent so the
	// pod-state arm alone drives Pending/Running/Failed.
	var lastEvent time.Time
	if ch.lag != nil {
		lastEvent = ch.lag.LastEvent()
	}
	if !lastEvent.IsZero() {
		in.haveEvent = true
		// Clock skew: a future last-event means no real age (mirrors US-2.8).
		in.sinceLastEvent = max(ch.now().Sub(lastEvent), 0)
		in.lag = ch.lagValue()
	}

	phase, conds := decidePhase(in, ch.warningLag, ch.criticalLag, ch.noEventWarn, ch.noEventFail)

	// US-7.3: a no-event warning window must log a warning.
	if in.haveEvent && in.sinceLastEvent >= ch.noEventWarn && in.sinceLastEvent < ch.noEventFail {
		log.Info(msgNoEventsWarn,
			"sensor", sensor.Name, "namespace", sensor.Namespace,
			"sinceLastEvent", in.sinceLastEvent.String())
	}

	return HealthResult{
		Phase:             phase,
		DebeziumAvailable: conds.debeziumAvailable,
		ReceivingEvents:   conds.receivingEvents,
		Ready:             conds.ready,
		Lag:               in.lag,
		LastEvent:         lastEvent,
		Message:           conds.debeziumAvailable.Message,
	}, nil
}

// lagValue returns the current lag with the clock-skew guard applied (never
// negative). Nil-safe on the tracker.
func (ch *Checker) lagValue() time.Duration {
	if ch.lag == nil {
		return 0
	}
	lag := ch.lag.GetLag()
	if lag < 0 {
		return 0
	}
	return lag
}

// decidePhase is PURE: it maps the signals to a Phase and the three condition
// inputs with no IO. Precedence is worst-state-wins: Failed > Degraded >
// Running > Pending. Thresholds are passed in so tests drive them directly.
func decidePhase(in phaseInputs, warningLag, criticalLag, noEventWarn, noEventFail time.Duration) (string, conditionSet) {
	// --- DebeziumAvailable + pod-state phase floor ---
	switch {
	case !in.childExists:
		// Row 1: child not created yet → Pending.
		return phasePending, conditionSet{
			debeziumAvailable: ConditionInput{metav1.ConditionFalse, ReasonPodNotFound,
				"DebeziumServer child not found yet"},
			receivingEvents: ConditionInput{metav1.ConditionUnknown, ReasonNoEventsYet,
				msgNoEventsYet},
			ready: ConditionInput{metav1.ConditionUnknown, ReasonPodNotFound,
				"DebeziumServer child not found yet"},
		}
	case in.crashLoop:
		// Row 3: crashloop beats everything → Failed with the termination message.
		msg := in.termMsg
		if msg == "" {
			msg = terminationUnavailable
		}
		return phaseFailed, conditionSet{
			debeziumAvailable: ConditionInput{metav1.ConditionFalse, ReasonPodCrashLoopBackOff, msg},
			receivingEvents:   ConditionInput{metav1.ConditionUnknown, ReasonNoEventsYet, msgNoEventsYet},
			ready: ConditionInput{metav1.ConditionFalse, ReasonPodCrashLoopBackOff,
				"DebeziumServer pod is crash-looping"},
		}
	case !in.podRunning:
		// Row 2: child exists but no running pod/status yet → Pending.
		return phasePending, conditionSet{
			debeziumAvailable: ConditionInput{metav1.ConditionFalse, ReasonPodNotReady,
				"DebeziumServer pod is not running yet"},
			receivingEvents: ConditionInput{metav1.ConditionUnknown, ReasonNoEventsYet,
				msgNoEventsYet},
			ready: ConditionInput{metav1.ConditionUnknown, ReasonPodNotReady,
				"DebeziumServer pod is not running yet"},
		}
	}

	// Pod is running. DebeziumAvailable=True from here on.
	debeziumAvailable := ConditionInput{metav1.ConditionTrue, ReasonPodRunning,
		"DebeziumServer pod is running"}

	// --- ReceivingEvents + no-event clock ---
	if !in.haveEvent {
		// Row 4: pod running but zero events ever → startup → Pending (NOT
		// Degraded). This is why the checker keys off LastEvent().IsZero()
		// rather than a large GetLag().
		return phasePending, conditionSet{
			debeziumAvailable: debeziumAvailable,
			receivingEvents: ConditionInput{metav1.ConditionFalse, ReasonNoEventsYet,
				msgNoEventsYet},
			ready: ConditionInput{metav1.ConditionUnknown, ReasonNoEventsYet,
				"Waiting for the first CDC event"},
		}
	}

	// Events have flowed at least once. Evaluate the no-event and lag arms and
	// combine worst-state-wins.
	switch {
	case in.sinceLastEvent >= noEventFail:
		// Rows 9/10: silence ≥30m → Failed regardless of lag.
		return phaseFailed, conditionSet{
			debeziumAvailable: debeziumAvailable,
			receivingEvents: ConditionInput{metav1.ConditionFalse, ReasonNoEventsTimeout,
				"No CDC events received"},
			ready: ConditionInput{metav1.ConditionFalse, ReasonNoEventsStale,
				"No CDC events received for over the failure timeout"},
		}
	case in.sinceLastEvent >= noEventWarn:
		// Row 8: silence 5–30m → Degraded, UNLESS lag is already critical
		// (row 10 worst-state-wins).
		if in.lag > criticalLag {
			return phaseFailed, conditionSet{
				debeziumAvailable: debeziumAvailable,
				receivingEvents: ConditionInput{metav1.ConditionFalse, ReasonNoEventsTimeout,
					msgNoEventsWarn},
				ready: ConditionInput{metav1.ConditionFalse, ReasonLagAboveCritical,
					"Replication lag is above the critical threshold"},
			}
		}
		return phaseDegraded, conditionSet{
			debeziumAvailable: debeziumAvailable,
			receivingEvents: ConditionInput{metav1.ConditionFalse, ReasonNoEventsTimeout,
				msgNoEventsWarn},
			ready: ConditionInput{metav1.ConditionFalse, ReasonNoEventsTimeout,
				msgNoEventsWarn},
		}
	}

	// Events are recent (sinceLastEvent < noEventWarn). The lag arm decides.
	receiving := ConditionInput{metav1.ConditionTrue, ReasonEventsFlowing, "CDC events are flowing"}
	switch {
	case in.lag > criticalLag:
		// Row 7: lag critical → Failed.
		return phaseFailed, conditionSet{
			debeziumAvailable: debeziumAvailable,
			receivingEvents:   receiving,
			ready: ConditionInput{metav1.ConditionFalse, ReasonLagAboveCritical,
				"Replication lag is above the critical threshold"},
		}
	case in.lag >= warningLag:
		// Row 6: lag warning → Degraded.
		return phaseDegraded, conditionSet{
			debeziumAvailable: debeziumAvailable,
			receivingEvents:   receiving,
			ready: ConditionInput{metav1.ConditionFalse, ReasonLagAboveWarning,
				"Replication lag is above the warning threshold"},
		}
	default:
		// Row 5: pod running, events flowing, lag healthy → Running.
		return phaseRunning, conditionSet{
			debeziumAvailable: debeziumAvailable,
			receivingEvents:   receiving,
			ready: ConditionInput{metav1.ConditionTrue, ReasonHealthyEndToEnd,
				"Sensor is healthy end-to-end"},
		}
	}
}

// readPodState reads the DebeziumServer child's status subresource and reports
// pod liveness (Decision B). All paths are optional and absence-tolerant: a
// missing or wrongly-shaped field never panics and resolves to "not running".
//
// Paths read (open-schema upstream status; fabricatable in envtest):
//   - status.phase (string): "Running" → running; "CrashLoopBackOff"/"Error" →
//     crashloop.
//   - status.conditions[]{type,status,reason,message}: type Available/Ready with
//     status True → running; a condition whose reason/type names a crashloop/
//     error → crashloop.
//   - status.lastTerminationMessage / status.terminationMessage (string):
//     captured as the termination message for a crash-looping pod.
func readPodState(obj *unstructured.Unstructured) (podRunning, crashLoop bool, termMsg string) {
	status, found, err := unstructured.NestedMap(obj.Object, "status")
	if err != nil || !found || status == nil {
		return false, false, ""
	}

	// Termination message (either spelling).
	if msg, ok := status["lastTerminationMessage"].(string); ok && msg != "" {
		termMsg = msg
	} else if msg, ok := status["terminationMessage"].(string); ok && msg != "" {
		termMsg = msg
	}

	phase, _ := status["phase"].(string)
	switch phase {
	case "Running":
		podRunning = true
	case "CrashLoopBackOff", "Error":
		crashLoop = true
	}

	// Conditions can both confirm running and surface a crashloop/error.
	if conds, ok := status["conditions"].([]any); ok {
		for _, c := range conds {
			cm, ok := c.(map[string]any)
			if !ok {
				continue
			}
			ctype, _ := cm["type"].(string)
			cstatus, _ := cm["status"].(string)
			creason, _ := cm["reason"].(string)
			if (ctype == "Available" || ctype == "Ready") && cstatus == "True" {
				podRunning = true
			}
			if isCrashReason(creason) || isCrashReason(ctype) {
				crashLoop = true
			}
			if cmsg, ok := cm["message"].(string); ok && cmsg != "" && crashLoop && termMsg == "" {
				termMsg = cmsg
			}
		}
	}

	// A detected crashloop/error dominates a stale "running" signal.
	if crashLoop {
		podRunning = false
	}
	return podRunning, crashLoop, termMsg
}

// isCrashReason reports whether a condition reason/type names a crashloop/error.
func isCrashReason(s string) bool {
	return s == "CrashLoopBackOff" || s == "Error"
}
