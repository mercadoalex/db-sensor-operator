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

package health

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
	"github.com/mercadoalex/db-sensor-operator/internal/deployer"
)

// Fabricated DebeziumServer status fields used across the Check IO tests.
const (
	statusPhaseKey  = "phase"
	podPhaseRunning = "Running"
)

// decidePhase threshold shorthands for the table tests.
const (
	tWarnLag   = warningLagThreshold
	tCritLag   = criticalLagThreshold
	tNoEvtWarn = noEventWarnTimeout
	tNoEvtFail = noEventFailTimeout
)

func dp(in phaseInputs) (string, conditionSet) {
	return decidePhase(in, tWarnLag, tCritLag, tNoEvtWarn, tNoEvtFail)
}

func TestDecidePhaseTruthTable(t *testing.T) {
	cases := []struct {
		name     string
		in       phaseInputs
		phase    string
		dbStatus metav1.ConditionStatus
		dbReason string
		reStatus metav1.ConditionStatus
		reReason string
		rdStatus metav1.ConditionStatus
		rdReason string
	}{
		{
			name:     "row1 child not found -> Pending",
			in:       phaseInputs{childExists: false},
			phase:    phasePending,
			dbStatus: metav1.ConditionFalse, dbReason: ReasonPodNotFound,
			reStatus: metav1.ConditionUnknown, reReason: ReasonNoEventsYet,
			rdStatus: metav1.ConditionUnknown, rdReason: ReasonPodNotFound,
		},
		{
			name:     "row2 child exists not running -> Pending",
			in:       phaseInputs{childExists: true, podRunning: false},
			phase:    phasePending,
			dbStatus: metav1.ConditionFalse, dbReason: ReasonPodNotReady,
			reStatus: metav1.ConditionUnknown, reReason: ReasonNoEventsYet,
			rdStatus: metav1.ConditionUnknown, rdReason: ReasonPodNotReady,
		},
		{
			name:     "row3 crashloop -> Failed",
			in:       phaseInputs{childExists: true, podRunning: false, crashLoop: true, termMsg: "OOMKilled"},
			phase:    phaseFailed,
			dbStatus: metav1.ConditionFalse, dbReason: ReasonPodCrashLoopBackOff,
			reStatus: metav1.ConditionUnknown, reReason: ReasonNoEventsYet,
			rdStatus: metav1.ConditionFalse, rdReason: ReasonPodCrashLoopBackOff,
		},
		{
			name:     "row4 running no events ever -> Pending",
			in:       phaseInputs{childExists: true, podRunning: true, haveEvent: false},
			phase:    phasePending,
			dbStatus: metav1.ConditionTrue, dbReason: ReasonPodRunning,
			reStatus: metav1.ConditionFalse, reReason: ReasonNoEventsYet,
			rdStatus: metav1.ConditionUnknown, rdReason: ReasonNoEventsYet,
		},
		{
			name:     "row5 running recent low lag -> Running",
			in:       phaseInputs{childExists: true, podRunning: true, haveEvent: true, sinceLastEvent: time.Minute, lag: 5 * time.Second},
			phase:    phaseRunning,
			dbStatus: metav1.ConditionTrue, dbReason: ReasonPodRunning,
			reStatus: metav1.ConditionTrue, reReason: ReasonEventsFlowing,
			rdStatus: metav1.ConditionTrue, rdReason: ReasonHealthyEndToEnd,
		},
		{
			name:     "row6 lag warning -> Degraded",
			in:       phaseInputs{childExists: true, podRunning: true, haveEvent: true, sinceLastEvent: time.Minute, lag: 45 * time.Second},
			phase:    phaseDegraded,
			dbStatus: metav1.ConditionTrue, dbReason: ReasonPodRunning,
			reStatus: metav1.ConditionTrue, reReason: ReasonEventsFlowing,
			rdStatus: metav1.ConditionFalse, rdReason: ReasonLagAboveWarning,
		},
		{
			name:     "row7 lag critical -> Failed",
			in:       phaseInputs{childExists: true, podRunning: true, haveEvent: true, sinceLastEvent: time.Minute, lag: 200 * time.Second},
			phase:    phaseFailed,
			dbStatus: metav1.ConditionTrue, dbReason: ReasonPodRunning,
			reStatus: metav1.ConditionTrue, reReason: ReasonEventsFlowing,
			rdStatus: metav1.ConditionFalse, rdReason: ReasonLagAboveCritical,
		},
		{
			name:     "row8 no-events warn window -> Degraded",
			in:       phaseInputs{childExists: true, podRunning: true, haveEvent: true, sinceLastEvent: 10 * time.Minute, lag: 5 * time.Second},
			phase:    phaseDegraded,
			dbStatus: metav1.ConditionTrue, dbReason: ReasonPodRunning,
			reStatus: metav1.ConditionFalse, reReason: ReasonNoEventsTimeout,
			rdStatus: metav1.ConditionFalse, rdReason: ReasonNoEventsTimeout,
		},
		{
			name:     "row9 no-events fail window -> Failed",
			in:       phaseInputs{childExists: true, podRunning: true, haveEvent: true, sinceLastEvent: 40 * time.Minute, lag: 5 * time.Second},
			phase:    phaseFailed,
			dbStatus: metav1.ConditionTrue, dbReason: ReasonPodRunning,
			reStatus: metav1.ConditionFalse, reReason: ReasonNoEventsTimeout,
			rdStatus: metav1.ConditionFalse, rdReason: ReasonNoEventsStale,
		},
		{
			name:     "row10 no-events warn + lag critical -> Failed (worst wins)",
			in:       phaseInputs{childExists: true, podRunning: true, haveEvent: true, sinceLastEvent: 10 * time.Minute, lag: 200 * time.Second},
			phase:    phaseFailed,
			dbStatus: metav1.ConditionTrue, dbReason: ReasonPodRunning,
			reStatus: metav1.ConditionFalse, reReason: ReasonNoEventsTimeout,
			rdStatus: metav1.ConditionFalse, rdReason: ReasonLagAboveCritical,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			phase, cs := dp(tc.in)
			assert.Equal(t, tc.phase, phase, "phase")
			assert.Equal(t, tc.dbStatus, cs.debeziumAvailable.Status, "DebeziumAvailable.Status")
			assert.Equal(t, tc.dbReason, cs.debeziumAvailable.Reason, "DebeziumAvailable.Reason")
			assert.Equal(t, tc.reStatus, cs.receivingEvents.Status, "ReceivingEvents.Status")
			assert.Equal(t, tc.reReason, cs.receivingEvents.Reason, "ReceivingEvents.Reason")
			assert.Equal(t, tc.rdStatus, cs.ready.Status, "Ready.Status")
			assert.Equal(t, tc.rdReason, cs.ready.Reason, "Ready.Reason")
		})
	}
}

func TestDecidePhaseWorstStateWins(t *testing.T) {
	// no-events-warn + lag-critical -> Failed.
	phase, cs := dp(phaseInputs{childExists: true, podRunning: true, haveEvent: true,
		sinceLastEvent: 10 * time.Minute, lag: 200 * time.Second})
	assert.Equal(t, phaseFailed, phase)
	assert.Equal(t, ReasonLagAboveCritical, cs.ready.Reason)

	// lag-warning + no-events-fail -> Failed (no-events-fail dominates).
	phase, cs = dp(phaseInputs{childExists: true, podRunning: true, haveEvent: true,
		sinceLastEvent: 40 * time.Minute, lag: 45 * time.Second})
	assert.Equal(t, phaseFailed, phase)
	assert.Equal(t, ReasonNoEventsStale, cs.ready.Reason)
}

func TestDecidePhaseCrashLoopFallbackMessage(t *testing.T) {
	_, cs := dp(phaseInputs{childExists: true, crashLoop: true, termMsg: ""})
	assert.Equal(t, terminationUnavailable, cs.debeziumAvailable.Message)
}

// --- Check IO tests (fake client fabricating the DebeziumServer child) ---

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, sensoriov1.AddToScheme(s))
	return s
}

func newChild(status map[string]any) *unstructured.Unstructured {
	child := &unstructured.Unstructured{}
	child.SetGroupVersionKind(deployer.DebeziumServerGVK)
	child.SetName("test-sensor")
	child.SetNamespace("default")
	if status != nil {
		_ = unstructured.SetNestedMap(child.Object, status, "status")
	}
	return child
}

func testSensor() *sensoriov1.DatabaseSensor {
	return &sensoriov1.DatabaseSensor{
		ObjectMeta: metav1.ObjectMeta{Name: "test-sensor", Namespace: "default"},
	}
}

func TestCheckerChildNotFound(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).Build()
	ch := NewChecker(c, nil)

	res, err := ch.Check(context.Background(), testSensor())
	require.NoError(t, err)
	assert.Equal(t, phasePending, res.Phase)
	assert.Equal(t, metav1.ConditionFalse, res.DebeziumAvailable.Status)
	assert.Equal(t, ReasonPodNotFound, res.DebeziumAvailable.Reason)
}

func TestCheckerPodRunningNoEvents(t *testing.T) {
	child := newChild(map[string]any{statusPhaseKey: podPhaseRunning})
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(child).Build()
	ch := NewChecker(c, nil)

	res, err := ch.Check(context.Background(), testSensor())
	require.NoError(t, err)
	// Pod running but no events (nil lag tracker) -> Pending (row 4).
	assert.Equal(t, phasePending, res.Phase)
	assert.Equal(t, metav1.ConditionTrue, res.DebeziumAvailable.Status)
	assert.Equal(t, ReasonPodRunning, res.DebeziumAvailable.Reason)
}

func TestCheckerPodRunningWithRecentEvent(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	child := newChild(map[string]any{statusPhaseKey: podPhaseRunning})
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(child).Build()

	lt := NewLagTrackerWithClock(func() time.Time { return now })
	lt.OnEvent(now.Add(-5 * time.Second).UnixMilli())

	ch := NewCheckerWithClock(c, lt, func() time.Time { return now })
	res, err := ch.Check(context.Background(), testSensor())
	require.NoError(t, err)
	assert.Equal(t, phaseRunning, res.Phase)
	assert.Equal(t, metav1.ConditionTrue, res.Ready.Status)
	assert.Equal(t, ReasonHealthyEndToEnd, res.Ready.Reason)
	assert.InDelta(t, (5 * time.Second).Seconds(), res.Lag.Seconds(), 0.001)
	assert.False(t, res.LastEvent.IsZero())
}

func TestCheckerPodRunningRunningViaCondition(t *testing.T) {
	child := newChild(map[string]any{
		"conditions": []any{
			map[string]any{"type": "Available", "status": "True"},
		},
	})
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(child).Build()
	ch := NewChecker(c, nil)

	res, err := ch.Check(context.Background(), testSensor())
	require.NoError(t, err)
	assert.Equal(t, metav1.ConditionTrue, res.DebeziumAvailable.Status)
	assert.Equal(t, ReasonPodRunning, res.DebeziumAvailable.Reason)
}

func TestCheckerCrashLoop(t *testing.T) {
	child := newChild(map[string]any{
		statusPhaseKey:           "CrashLoopBackOff",
		"lastTerminationMessage": "connector failed: auth error",
	})
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(child).Build()
	ch := NewChecker(c, nil)

	res, err := ch.Check(context.Background(), testSensor())
	require.NoError(t, err)
	assert.Equal(t, phaseFailed, res.Phase)
	assert.Equal(t, metav1.ConditionFalse, res.DebeziumAvailable.Status)
	assert.Equal(t, ReasonPodCrashLoopBackOff, res.DebeziumAvailable.Reason)
	assert.Contains(t, res.Message, "auth error")
}

func TestCheckerNilLagSafe(t *testing.T) {
	child := newChild(map[string]any{statusPhaseKey: podPhaseRunning})
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(child).Build()
	ch := NewChecker(c, nil)

	assert.NotPanics(t, func() {
		res, err := ch.Check(context.Background(), testSensor())
		require.NoError(t, err)
		assert.Equal(t, phasePending, res.Phase)
	})
}

func TestCheckerNoEventFailViaClock(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	child := newChild(map[string]any{statusPhaseKey: podPhaseRunning})
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(child).Build()

	lt := NewLagTrackerWithClock(func() time.Time { return now })
	// Last event 40 minutes ago -> no-event fail window.
	lt.OnEvent(now.Add(-40 * time.Minute).UnixMilli())

	ch := NewCheckerWithClock(c, lt, func() time.Time { return now })
	res, err := ch.Check(context.Background(), testSensor())
	require.NoError(t, err)
	assert.Equal(t, phaseFailed, res.Phase)
	assert.Equal(t, ReasonNoEventsStale, res.Ready.Reason)
}
