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
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
	"github.com/mercadoalex/db-sensor-operator/internal/snapshot"
)

// Reasons of the InspectionAvailable condition, beyond the inspector's own error codes
// (ConnectionFailed, AuthenticationFailed, InspectionFailed, InvalidConfiguration).
const (
	ReasonInspected               = "Inspected"
	ReasonRunning                 = "InspectionRunning"
	ReasonNoResult                = "NoResult"
	ReasonInvalidResult           = "InvalidResult"
	ReasonUnsupportedFactsVersion = "UnsupportedFactsVersion"
	ReasonTimedOut                = "InspectionTimedOut"
	ReasonInspectorFailed         = "InspectorFailed"
	ReasonImageNotConfigured      = "InspectorImageNotConfigured"
	ReasonEngineNotSupported      = "EngineNotSupported"
)

// maxTerminationMessage is the most a kubelet keeps of a termination message.
const maxTerminationMessage = 4096

// ResultError is a result the operator could not use, with the reason to report.
type ResultError struct {
	Reason  string
	Message string
}

func (e *ResultError) Error() string { return e.Reason + ": " + e.Message }

// ParseTermination reads the termination message of the inspector container. It checks the
// version before anything else, so a Job image and an operator that disagree on the layout say
// so, and then decodes strictly: the same version means the same fields.
func ParseTermination(message string) (snapshot.Result, *ResultError) {
	if strings.TrimSpace(message) == "" {
		return snapshot.Result{}, &ResultError{ReasonNoResult, "the inspector left no result in its termination message"}
	}
	if len(message) > maxTerminationMessage {
		return snapshot.Result{}, &ResultError{ReasonInvalidResult, "the termination message is larger than a kubelet keeps"}
	}

	var head struct {
		FactsVersion int `json:"factsVersion"`
	}
	if err := json.Unmarshal([]byte(message), &head); err != nil {
		return snapshot.Result{}, &ResultError{ReasonInvalidResult, "the termination message is not valid JSON"}
	}
	if head.FactsVersion != snapshot.FactsVersion {
		return snapshot.Result{}, &ResultError{ReasonUnsupportedFactsVersion, fmt.Sprintf(
			"the inspector reported facts version %d, this operator reads version %d: the Job image and the operator differ",
			head.FactsVersion, snapshot.FactsVersion)}
	}

	var r snapshot.Result
	dec := json.NewDecoder(bytes.NewReader([]byte(message)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return snapshot.Result{}, &ResultError{ReasonInvalidResult, "the result does not match the facts layout: " + err.Error()}
	}
	return r, nil
}

// ToStatus converts a result into status.snapshot.inspection. A failed inspection yields a status
// with the error and no facts, so stale facts are never shown as current.
func ToStatus(r snapshot.Result, observedAt metav1.Time, generation int64) *sensoriov1.InspectionStatus {
	st := &sensoriov1.InspectionStatus{
		FactsVersion:       int32(r.FactsVersion), //nolint:gosec // a small constant, checked by ParseTermination
		ObservedGeneration: generation,
		ObservedAt:         &observedAt,
		Truncated:          r.Truncated,
	}
	if r.Error != nil {
		st.Error = r.Error.Code + ": " + r.Error.Message
		return st
	}
	for _, t := range r.Tables {
		st.Tables = append(st.Tables, sensoriov1.TableInspection{
			Name: t.Name, Found: t.Found, EstimatedRows: t.EstimatedRows,
			EstimateReliable: t.EstimateReliable, CanWrite: t.CanWrite,
		})
	}
	if pg := r.Postgres; pg != nil {
		out := &sensoriov1.PostgresInspection{
			ServerVersion:      int32(pg.ServerVersion), //nolint:gosec // server_version_num fits in int32
			WALLevel:           pg.WALLevel,
			InRecovery:         pg.InRecovery,
			MaxSlotWALKeepSize: pg.MaxSlotWALKeepSize,
		}
		if s := pg.ReplicationSlot; s != nil {
			out.ReplicationSlot = &sensoriov1.SlotInspection{
				Name: s.Name, Active: s.Active, WALStatus: s.WALStatus,
				RetainedBytes: s.RetainedBytes, ConfirmedLagBytes: s.ConfirmedLagBytes, SafeWALSize: s.SafeWALSize,
			}
		}
		st.Postgres = out
	}
	return st
}
