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

package inspection

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
	"github.com/mercadoalex/db-sensor-operator/internal/snapshot"
)

const (
	tblOrders = "public.orders"
	walLogic  = "logical"
	testImage = "img"
)

func newSensor() *sensoriov1.DatabaseSensor {
	return &sensoriov1.DatabaseSensor{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-cdc", Namespace: "prod", Generation: 4},
		Spec: sensoriov1.DatabaseSensorSpec{
			Database: sensoriov1.DatabaseConfig{
				Type: "postgresql", Host: "pg.prod.svc", Port: 5432, User: "debezium", Name: "orders_db", PasswordSecret: "db-creds",
			},
			Tables: []string{tblOrders, "public.users"},
			Snapshot: &sensoriov1.SnapshotConfig{
				Inspection: &sensoriov1.InspectionConfig{Enabled: true, TimeoutSeconds: 30},
			},
		},
	}
}

// ---------------------------------------------------------------- Job

func TestJobName(t *testing.T) {
	assert.Equal(t, "orders-cdc-inspect", JobName("orders-cdc"))

	long := strings.Repeat("a", 90)
	name := JobName(long)
	assert.LessOrEqual(t, len(name), maxJobNameLength, "a Job name is also a pod label value")
	assert.Equal(t, name, JobName(long), "stable")
	assert.NotEqual(t, name, JobName(strings.Repeat("a", 89)+"b"), "two long names do not collide")
}

func TestBuildJobIsHardenedAndCarriesNoPassword(t *testing.T) {
	job, err := BuildJob(newSensor(), JobParams{Image: "registry/op:1.2", PullPolicy: corev1.PullNever})
	require.NoError(t, err)

	assert.Equal(t, "orders-cdc-inspect", job.Name)
	assert.Equal(t, "prod", job.Namespace)
	assert.Equal(t, ComponentValue, job.Labels[ComponentLabel])
	assert.Equal(t, "4", job.Annotations[GenerationAnnotation], "the generation inspected is recorded")

	// One attempt and a hard deadline: the operator, not the Job, decides when to retry.
	assert.Equal(t, int32(0), *job.Spec.BackoffLimit)
	assert.Equal(t, int64(30), *job.Spec.ActiveDeadlineSeconds)
	assert.NotNil(t, job.Spec.TTLSecondsAfterFinished)

	pod := job.Spec.Template.Spec
	assert.Equal(t, corev1.RestartPolicyNever, pod.RestartPolicy)
	assert.False(t, *pod.AutomountServiceAccountToken, "the inspector needs no Kubernetes API access")
	assert.True(t, *pod.SecurityContext.RunAsNonRoot)
	assert.Equal(t, corev1.SeccompProfileTypeRuntimeDefault, pod.SecurityContext.SeccompProfile.Type)
	assert.Empty(t, pod.ServiceAccountName, "no dedicated service account, so no RBAC to grant")

	require.Len(t, pod.Containers, 1)
	c := pod.Containers[0]
	assert.Equal(t, "registry/op:1.2", c.Image)
	assert.Equal(t, corev1.PullNever, c.ImagePullPolicy)
	assert.Equal(t, []string{"/inspector"}, c.Command)
	assert.Equal(t, corev1.TerminationMessageReadFile, c.TerminationMessagePolicy)
	assert.False(t, *c.SecurityContext.AllowPrivilegeEscalation)
	assert.True(t, *c.SecurityContext.ReadOnlyRootFilesystem)
	assert.Equal(t, []corev1.Capability{"ALL"}, c.SecurityContext.Capabilities.Drop)

	// The password reaches the container only through the Secret, never through the Job spec.
	require.Len(t, c.EnvFrom, 1)
	assert.Equal(t, "DATABASE_", c.EnvFrom[0].Prefix)
	assert.Equal(t, "db-creds", c.EnvFrom[0].SecretRef.Name)
	for _, e := range c.Env {
		assert.Nil(t, e.ValueFrom, e.Name)
		assert.NotContains(t, strings.ToLower(e.Name), "password")
	}
}

func TestBuildJobEnvironment(t *testing.T) {
	job, err := BuildJob(newSensor(), JobParams{Image: testImage})
	require.NoError(t, err)
	env := map[string]string{}
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		env[e.Name] = e.Value
	}
	assert.Equal(t, "postgresql", env["INSPECT_ENGINE"])
	assert.Equal(t, "pg.prod.svc", env["INSPECT_HOST"])
	assert.Equal(t, "5432", env["INSPECT_PORT"])
	assert.Equal(t, "debezium", env["INSPECT_USER"])
	assert.Equal(t, "orders_db", env["INSPECT_DATABASE"])
	assert.Equal(t, "public.orders,public.users", env["INSPECT_TABLES"])
	assert.Equal(t, "prod_orders_cdc", env["INSPECT_SLOT"], "the slot the deployer creates for this sensor")
	assert.Equal(t, "24", env["INSPECT_TIMEOUT_SECONDS"], "shorter than the Job's own deadline, so a reason can be written")
}

func TestBuildJobTimeouts(t *testing.T) {
	s := newSensor()
	assert.Equal(t, int32(30), TimeoutSeconds(s))
	s.Spec.Snapshot.Inspection.TimeoutSeconds = 0
	assert.Equal(t, int32(DefaultTimeoutSeconds), TimeoutSeconds(s), "unset falls back to the default")
	s.Spec.Snapshot = nil
	assert.Equal(t, int32(DefaultTimeoutSeconds), TimeoutSeconds(s))

	s = newSensor()
	s.Spec.Snapshot.Inspection.TimeoutSeconds = 5
	job, err := BuildJob(s, JobParams{Image: testImage})
	require.NoError(t, err)
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if e.Name == "INSPECT_TIMEOUT_SECONDS" {
			assert.Equal(t, "4", e.Value, "the inner deadline is below the Job's 5s")
		}
	}
}

func TestBuildJobRefusesWhatItCannotInspect(t *testing.T) {
	_, err := BuildJob(newSensor(), JobParams{})
	assert.ErrorIs(t, err, ErrNoImage)

	s := newSensor()
	s.Spec.Database.Name = ""
	_, err = BuildJob(s, JobParams{Image: testImage})
	assert.ErrorIs(t, err, ErrNoDatabaseName)

	s = newSensor()
	s.Spec.Database.Type = "mysql"
	_, err = BuildJob(s, JobParams{Image: testImage})
	assert.ErrorIs(t, err, ErrEngine)
}

func TestEnabled(t *testing.T) {
	s := newSensor()
	assert.True(t, Enabled(s))
	s.Spec.Snapshot.Inspection.Enabled = false
	assert.False(t, Enabled(s))
	s.Spec.Snapshot = nil
	assert.False(t, Enabled(s), "off unless asked")
}

// ------------------------------------------------------------- Result

func okResult(t *testing.T) string {
	t.Helper()
	b, err := snapshot.Encode(snapshot.NewResult(snapshot.Facts{
		Server: snapshot.ServerFacts{Version: 160015, WALLevel: walLogic, MaxSlotWALKeepSizeKnown: true, MaxSlotWALKeepSizeBytes: snapshot.UnlimitedBytes},
		Slot: &snapshot.SlotFacts{Name: "prod_orders_cdc", Found: true, Active: true, WALStatus: "reserved",
			RetainedBytes: 0, ConfirmedLagBytes: snapshot.UnknownBytes, SafeWALBytes: snapshot.UnknownBytes},
		Tables: []snapshot.TableFacts{
			{Table: tblOrders, Found: true, EstimatedRows: 151000, EstimateReliable: true, CanWrite: true},
			{Table: "public.users", Found: true, EstimatedRows: snapshot.UnknownRows},
		},
	}))
	require.NoError(t, err)
	return string(b)
}

func TestParseTermination(t *testing.T) {
	r, perr := ParseTermination(okResult(t))
	require.Nil(t, perr)
	assert.Equal(t, snapshot.FactsVersion, r.FactsVersion)
	assert.Len(t, r.Tables, 2)

	for name, tc := range map[string]struct{ in, reason string }{
		"empty":               {"", ReasonNoResult},
		"only whitespace":     {"  \n ", ReasonNoResult},
		"not json":            {"connection refused", ReasonInvalidResult},
		"a future version":    {`{"factsVersion":2,"tables":[]}`, ReasonUnsupportedFactsVersion},
		"a missing version":   {`{"tables":[]}`, ReasonUnsupportedFactsVersion},
		"an unknown field":    {`{"factsVersion":1,"surprise":true}`, ReasonInvalidResult},
		"larger than allowed": {`{"factsVersion":1,"x":"` + strings.Repeat("a", 5000) + `"}`, ReasonInvalidResult},
	} {
		t.Run(name, func(t *testing.T) {
			_, perr := ParseTermination(tc.in)
			require.NotNil(t, perr)
			assert.Equal(t, tc.reason, perr.Reason)
			assert.NotEmpty(t, perr.Message)
		})
	}
}

func TestToStatusKeepsAZeroAndDropsAnUnknown(t *testing.T) {
	r, perr := ParseTermination(okResult(t))
	require.Nil(t, perr)
	at := metav1.NewTime(time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC))
	st := ToStatus(r, at, 4)

	assert.Equal(t, int32(1), st.FactsVersion)
	assert.Equal(t, int64(4), st.ObservedGeneration)
	assert.True(t, st.ObservedAt.Equal(&at))
	assert.Empty(t, st.Error)
	require.Len(t, st.Tables, 2)
	assert.Equal(t, int64(151000), *st.Tables[0].EstimatedRows)
	assert.Nil(t, st.Tables[1].EstimatedRows, "an unknown estimate stays absent")

	slot := st.Postgres.ReplicationSlot
	require.NotNil(t, slot)
	require.NotNil(t, slot.RetainedBytes)
	assert.Equal(t, int64(0), *slot.RetainedBytes, "a retained 0 is kept")
	assert.Nil(t, slot.ConfirmedLagBytes)
	assert.Nil(t, slot.SafeWALSize, "NULL under an unlimited cap stays absent")
	assert.Equal(t, int64(-1), *st.Postgres.MaxSlotWALKeepSize)
}

func TestToStatusOfAFailureCarriesNoFacts(t *testing.T) {
	r := snapshot.NewErrorResult(snapshot.ErrCodeAuth, "password authentication failed")
	st := ToStatus(r, metav1.Now(), 4)
	assert.Equal(t, "AuthenticationFailed: password authentication failed", st.Error)
	assert.Empty(t, st.Tables)
	assert.Nil(t, st.Postgres, "stale facts are never shown as current")
}

// ----------------------------------------------------------- Evaluate

func statusWith(slot *sensoriov1.SlotInspection, cap *int64, tables ...sensoriov1.TableInspection) *sensoriov1.InspectionStatus {
	return &sensoriov1.InspectionStatus{
		FactsVersion: 1, ObservedGeneration: 4, Tables: tables,
		Postgres: &sensoriov1.PostgresInspection{ServerVersion: 160015, WALLevel: walLogic, MaxSlotWALKeepSize: cap, ReplicationSlot: slot},
	}
}

func cond(conds []metav1.Condition, typ string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == typ {
			return &conds[i]
		}
	}
	return nil
}

func TestEvaluatePlanIsPerTableAndNeverApplied(t *testing.T) {
	st := statusWith(nil, new(int64(-1)),
		sensoriov1.TableInspection{Name: tblOrders, Found: true, EstimatedRows: new(int64(5_000_000)), EstimateReliable: true, CanWrite: true},
		sensoriov1.TableInspection{Name: "public.tiny", Found: true, EstimatedRows: new(int64(10)), EstimateReliable: true},
	)
	plan, _ := Evaluate(newSensor(), st)
	require.NotNil(t, plan)
	assert.False(t, plan.Applied, "nothing consumes backfilled rows")
	require.Len(t, plan.Tables, 2)
	assert.Equal(t, "signal-table", plan.Tables[0].Backfill)
	assert.Equal(t, "stream-only", plan.Tables[1].Backfill)

	// The reason goes into the status, so it must not move while only the estimate does.
	grown := statusWith(nil, new(int64(-1)),
		sensoriov1.TableInspection{Name: tblOrders, Found: true, EstimatedRows: new(int64(8_123_456)), EstimateReliable: true, CanWrite: true},
		sensoriov1.TableInspection{Name: "public.tiny", Found: true, EstimatedRows: new(int64(11)), EstimateReliable: true},
	)
	plan2, _ := Evaluate(newSensor(), grown)
	assert.Equal(t, plan, plan2, "a different row estimate on the same side of the threshold changes nothing")
	for _, p := range plan.Tables {
		assert.NotContains(t, p.Reason, "5000000")
	}
}

func TestEvaluateNothingWithoutFacts(t *testing.T) {
	for _, st := range []*sensoriov1.InspectionStatus{nil, {Error: "ConnectionFailed: x"}, {FactsVersion: 1}} {
		plan, conds := Evaluate(newSensor(), st)
		assert.Nil(t, plan)
		assert.Empty(t, conds)
	}
}

func TestEvaluateConditions(t *testing.T) {
	healthy := &sensoriov1.SlotInspection{Name: "prod_orders_cdc", Active: true, WALStatus: "reserved", RetainedBytes: new(int64(1 << 20))}

	_, conds := Evaluate(newSensor(), statusWith(healthy, new(int64(-1)),
		sensoriov1.TableInspection{Name: tblOrders, Found: true, EstimateReliable: true}))
	assert.Equal(t, metav1.ConditionTrue, cond(conds, ConditionSlotRetention).Status)
	assert.Equal(t, metav1.ConditionTrue, cond(conds, ConditionLogicalDecoding).Status)
	assert.Equal(t, metav1.ConditionTrue, cond(conds, ConditionTablesFound).Status)

	// A slot past its cap: PostgreSQL's own state, not a threshold of ours.
	atRisk := &sensoriov1.SlotInspection{Name: "s", Active: true, WALStatus: "unreserved", SafeWALSize: new(int64(-112066448))}
	_, conds = Evaluate(newSensor(), statusWith(atRisk, new(int64(64<<20))))
	c := cond(conds, ConditionSlotRetention)
	assert.Equal(t, metav1.ConditionFalse, c.Status)
	assert.Equal(t, snapshot.CodeSlotAtRisk, c.Reason)

	// A slot that does not exist yet is not a problem.
	_, conds = Evaluate(newSensor(), statusWith(nil, new(int64(-1))))
	c = cond(conds, ConditionSlotRetention)
	assert.Equal(t, metav1.ConditionUnknown, c.Status)
	assert.Equal(t, snapshot.CodeSlotMissing, c.Reason)
	assert.Contains(t, c.Message, "prod_orders_cdc", "it names the slot the deployer will create")

	// wal_level and missing tables, reported before the connector fails opaquely.
	st := statusWith(healthy, new(int64(-1)), sensoriov1.TableInspection{Name: "public.b"}, sensoriov1.TableInspection{Name: "public.a"})
	st.Postgres.WALLevel = "replica"
	_, conds = Evaluate(newSensor(), st)
	assert.Equal(t, metav1.ConditionFalse, cond(conds, ConditionLogicalDecoding).Status)
	assert.Contains(t, cond(conds, ConditionLogicalDecoding).Message, "logical")
	tf := cond(conds, ConditionTablesFound)
	assert.Equal(t, metav1.ConditionFalse, tf.Status)
	assert.Equal(t, "these tables do not exist: public.a, public.b", tf.Message, "sorted, so the message is stable")
}

func TestEvaluateMessagesDoNotChangeWithTheByteCounts(t *testing.T) {
	small := &sensoriov1.SlotInspection{Name: "s", Active: true, WALStatus: "extended", RetainedBytes: new(int64(1 << 20)), ConfirmedLagBytes: new(int64(1 << 20))}
	big := &sensoriov1.SlotInspection{Name: "s", Active: true, WALStatus: "extended", RetainedBytes: new(int64(8 << 30)), ConfirmedLagBytes: new(int64(8 << 30))}
	_, a := Evaluate(newSensor(), statusWith(small, new(int64(-1))))
	_, b := Evaluate(newSensor(), statusWith(big, new(int64(-1))))
	assert.Equal(t, a, b, "the conditions are judgments, so they do not churn while the situation does not change")
}

// ---------------------------------------------------------------- Due

func withInspection(s *sensoriov1.DatabaseSensor, gen int64, at time.Time, errText string) *sensoriov1.DatabaseSensor {
	mt := metav1.NewTime(at)
	s.Status.Snapshot = &sensoriov1.SnapshotStatus{Inspection: &sensoriov1.InspectionStatus{
		FactsVersion: 1, ObservedGeneration: gen, ObservedAt: &mt, Error: errText}}
	return s
}

func TestDue(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)

	due, _ := Due(newSensor(), now)
	assert.True(t, due, "never inspected")

	s := withInspection(newSensor(), 4, now.Add(-time.Minute), "")
	due, wait := Due(s, now)
	assert.False(t, due)
	assert.Equal(t, 9*time.Minute, wait, "default interval 600s, 60s elapsed")

	s = withInspection(newSensor(), 4, now.Add(-10*time.Minute), "")
	due, _ = Due(s, now)
	assert.True(t, due, "the interval has elapsed")

	s = withInspection(newSensor(), 3, now, "")
	due, _ = Due(s, now)
	assert.True(t, due, "a new generation always makes it due")

	s = withInspection(newSensor(), 4, now.Add(-24*time.Hour), "")
	s.Spec.Snapshot.Inspection.IntervalSeconds = new(int32(0))
	due, wait = Due(s, now)
	assert.False(t, due, "0 means once per generation")
	assert.Zero(t, wait, "and nothing to schedule")

	s = withInspection(newSensor(), 4, now.Add(-time.Minute), "ConnectionFailed: x")
	due, wait = Due(s, now)
	assert.False(t, due, "a failure is not retried at once")
	assert.Equal(t, failureRetryDelay-time.Minute, wait)

	s = withInspection(newSensor(), 4, now.Add(-failureRetryDelay), "ConnectionFailed: x")
	due, _ = Due(s, now)
	assert.True(t, due, "a failure is retried after the delay")

	s = withInspection(newSensor(), 4, now, "")
	s.Status.Snapshot.Inspection.ObservedAt = nil
	due, _ = Due(s, now)
	assert.True(t, due, "facts without a time cannot be trusted")
}

func TestStatusIsValidJSON(t *testing.T) {
	r, perr := ParseTermination(okResult(t))
	require.Nil(t, perr)
	plan, conds := Evaluate(newSensor(), ToStatus(r, metav1.Now(), 4))
	_, err := json.Marshal(plan)
	require.NoError(t, err)
	for _, c := range conds {
		assert.NotEmpty(t, c.Reason, c.Type)
		assert.Regexp(t, `^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`, c.Reason, "a valid condition reason")
	}
}
