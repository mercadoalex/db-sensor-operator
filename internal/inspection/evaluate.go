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
	"slices"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
	"github.com/mercadoalex/db-sensor-operator/internal/deployer"
	"github.com/mercadoalex/db-sensor-operator/internal/snapshot"
)

// Condition types written from an inspection. A condition is a judgment about the facts, not a
// fact: it changes only when the judgment changes, which also gives it hysteresis for free.
const (
	ConditionInspectionAvailable = "InspectionAvailable"
	ConditionSlotRetention       = "SlotRetentionHealthy"
	ConditionLogicalDecoding     = "LogicalDecodingReady"
	ConditionTablesFound         = "TablesFound"
)

// DerivedConditions are the conditions that only make sense while there are facts behind them.
var DerivedConditions = []string{ConditionSlotRetention, ConditionLogicalDecoding, ConditionTablesFound}

// failureRetryDelay is how long to wait after a failed inspection before trying again, unless the
// spec changes first. It keeps an unreachable database from being hammered.
const failureRetryDelay = 5 * time.Minute

// defaultIntervalSeconds applies when spec.snapshot.inspection.intervalSeconds is not set.
const defaultIntervalSeconds = 600

// Evaluate turns the facts of a successful inspection into the backfill plan and the conditions.
// Condition generations are left for the caller to set.
func Evaluate(sensor *sensoriov1.DatabaseSensor, st *sensoriov1.InspectionStatus) (*sensoriov1.SnapshotPlan, []metav1.Condition) {
	if st == nil || st.Error != "" || st.Postgres == nil {
		return nil, nil
	}
	pg := st.Postgres

	server := snapshot.ServerFacts{Version: int(pg.ServerVersion), InRecovery: pg.InRecovery, WALLevel: pg.WALLevel}
	if pg.MaxSlotWALKeepSize != nil {
		server.MaxSlotWALKeepSizeKnown, server.MaxSlotWALKeepSizeBytes = true, *pg.MaxSlotWALKeepSize
	}

	// The slot is always requested, so a missing one in a result means it does not exist yet.
	slot := &snapshot.SlotFacts{Name: deployer.SlotName(sensor)}
	if s := pg.ReplicationSlot; s != nil {
		slot = &snapshot.SlotFacts{
			Name: s.Name, Found: true, Active: s.Active, WALStatus: s.WALStatus,
			RetainedBytes:     orUnknown(s.RetainedBytes),
			ConfirmedLagBytes: orUnknown(s.ConfirmedLagBytes),
			SafeWALBytes:      orUnknown(s.SafeWALSize),
		}
	}

	plan := &sensoriov1.SnapshotPlan{Applied: false}
	var missing []string
	for _, t := range st.Tables {
		rows := snapshot.UnknownRows
		if t.EstimatedRows != nil {
			rows = *t.EstimatedRows
		}
		d := snapshot.Decide(snapshot.TableFacts{
			Table: t.Name, Found: t.Found, EstimatedRows: rows, EstimateReliable: t.EstimateReliable,
			ModsSinceAnalyze: snapshot.UnknownRows, CanWrite: t.CanWrite,
			ReadOnlyServer: pg.InRecovery, WALLevel: pg.WALLevel,
		}, snapshot.PolicyConfig{}, snapshot.Capabilities{})
		plan.Tables = append(plan.Tables, sensoriov1.TablePlan{Name: t.Name, Backfill: string(d.Kind), Reason: d.Reason})
		if !t.Found {
			missing = append(missing, t.Name)
		}
	}

	risk := snapshot.DiagnoseSlot(server, slot)
	return plan, []metav1.Condition{
		slotCondition(risk),
		logicalDecodingCondition(pg),
		tablesCondition(missing, st.Truncated),
	}
}

func orUnknown(v *int64) int64 {
	if v == nil {
		return snapshot.UnknownBytes
	}
	return *v
}

func slotCondition(r snapshot.SlotRisk) metav1.Condition {
	status := metav1.ConditionUnknown
	switch r.Level {
	case snapshot.LevelOK:
		status = metav1.ConditionTrue
	case snapshot.LevelWarning, snapshot.LevelCritical:
		status = metav1.ConditionFalse
	case snapshot.LevelInfo, snapshot.LevelUnknown:
	}
	return metav1.Condition{Type: ConditionSlotRetention, Status: status, Reason: r.Code, Message: r.Reason}
}

func logicalDecodingCondition(pg *sensoriov1.PostgresInspection) metav1.Condition {
	c := metav1.Condition{Type: ConditionLogicalDecoding}
	if pg.WALLevel == "logical" {
		c.Status, c.Reason, c.Message = metav1.ConditionTrue, "WALLevelLogical", "wal_level is logical"
	} else {
		c.Status, c.Reason = metav1.ConditionFalse, "WALLevelNotLogical"
		c.Message = "wal_level is \"" + pg.WALLevel + "\" and logical decoding needs \"logical\": the connector cannot create its replication slot"
	}
	return c
}

func tablesCondition(missing []string, truncated bool) metav1.Condition {
	if len(missing) == 0 {
		msg := "every inspected table exists"
		if truncated {
			msg += "; the list was truncated, so tables beyond it were not checked"
		}
		return metav1.Condition{Type: ConditionTablesFound, Status: metav1.ConditionTrue, Reason: "AllTablesFound", Message: msg}
	}
	slices.Sort(missing)
	return metav1.Condition{
		Type: ConditionTablesFound, Status: metav1.ConditionFalse, Reason: "TablesNotFound",
		Message: "these tables do not exist: " + strings.Join(missing, ", "),
	}
}

// Due reports whether an inspection should start now and, when it should not, how long until it
// is due (0 means never, until the spec changes). A new generation always makes it due; a failed
// inspection is retried after failureRetryDelay; a successful one after intervalSeconds, where 0
// means once per generation.
func Due(sensor *sensoriov1.DatabaseSensor, now time.Time) (due bool, wait time.Duration) {
	var st *sensoriov1.InspectionStatus
	if sensor.Status.Snapshot != nil {
		st = sensor.Status.Snapshot.Inspection
	}
	if st == nil || st.ObservedAt == nil || st.ObservedGeneration != sensor.Generation {
		return true, 0
	}

	var next time.Time
	switch {
	case st.Error != "":
		next = st.ObservedAt.Add(failureRetryDelay)
	default:
		interval := int32(defaultIntervalSeconds)
		if c := config(sensor); c != nil && c.IntervalSeconds != nil {
			interval = *c.IntervalSeconds
		}
		if interval == 0 {
			return false, 0
		}
		next = st.ObservedAt.Add(time.Duration(interval) * time.Second)
	}
	if !now.Before(next) {
		return true, 0
	}
	return false, next.Sub(now)
}
