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

package snapshot

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// The inspection Job reports its result as JSON in its termination message, and the operator
// copies it into the sensor's status. This file is that contract. It is plain data with no
// Kubernetes types, so the inspector binary stays small, and it is versioned so an operator
// and a Job image of different versions can tell they disagree.

const (
	// FactsVersion is the version of the result layout. It matches status.snapshot.inspection.factsVersion.
	FactsVersion = 1

	// MaxResultBytes is the largest result the inspector writes. The kubelet keeps only the first
	// 4096 bytes of a termination message, so staying below it avoids a silently cut JSON document.
	MaxResultBytes = 4000

	maxErrorMessage = 300
)

// Error codes of a failed inspection, stable so the operator can map them to a condition reason.
const (
	ErrCodeConfig     = "InvalidConfiguration"
	ErrCodeConnect    = "ConnectionFailed"
	ErrCodeAuth       = "AuthenticationFailed"
	ErrCodeInspection = "InspectionFailed"
	ErrCodeOutput     = "ResultNotWritten"
)

// Process exit codes of the inspector, one per error code, so a failed Job says why even before
// its message is read.
const (
	ExitOK         = 0
	ExitConfig     = 2
	ExitConnect    = 3
	ExitAuth       = 4
	ExitInspection = 5
	ExitOutput     = 6
)

// ErrConnect marks an error that happened while connecting, as opposed to while querying.
var ErrConnect = errors.New("connection failed")

// Result is one inspection's output.
type Result struct {
	FactsVersion int          `json:"factsVersion"`
	Error        *ResultError `json:"error,omitempty"`
	// Truncated is true when tables were dropped to fit MaxResultBytes.
	Truncated bool            `json:"truncated,omitempty"`
	Tables    []ResultTable   `json:"tables,omitempty"`
	Postgres  *ResultPostgres `json:"postgres,omitempty"`
}

// ResultError says why an inspection produced no facts. The message never carries credentials.
type ResultError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ResultTable are the facts of one table.
type ResultTable struct {
	Name             string `json:"name"`
	Found            bool   `json:"found"`
	EstimatedRows    *int64 `json:"estimatedRows,omitempty"`
	EstimateReliable bool   `json:"estimateReliable"`
	CanWrite         bool   `json:"canWrite"`
}

// ResultPostgres are the PostgreSQL-only facts.
type ResultPostgres struct {
	ServerVersion      int         `json:"serverVersion"`
	WALLevel           string      `json:"walLevel"`
	InRecovery         bool        `json:"inRecovery"`
	MaxSlotWALKeepSize *int64      `json:"maxSlotWalKeepSize,omitempty"`
	ReplicationSlot    *ResultSlot `json:"replicationSlot,omitempty"`
}

// ResultSlot describes the connector's slot.
type ResultSlot struct {
	Name              string `json:"name"`
	Active            bool   `json:"active"`
	WALStatus         string `json:"walStatus,omitempty"`
	RetainedBytes     *int64 `json:"retainedBytes,omitempty"`
	ConfirmedLagBytes *int64 `json:"confirmedLagBytes,omitempty"`
	// SafeWALSize is PostgreSQL's own margin before the slot is at risk; negative once the cap is
	// exceeded, absent while the cap is -1 (there is nothing to measure against).
	SafeWALSize *int64 `json:"safeWalSize,omitempty"`
}

// NewResult converts the facts of a successful inspection. A value that is unknown is left out
// (a nil pointer) so that a real zero and "unknown" stay distinguishable.
func NewResult(f Facts) Result {
	r := Result{FactsVersion: FactsVersion}
	for _, t := range f.Tables {
		rt := ResultTable{Name: t.Table, Found: t.Found, EstimateReliable: t.EstimateReliable, CanWrite: t.CanWrite}
		if t.Found && t.EstimatedRows != UnknownRows {
			rows := t.EstimatedRows
			rt.EstimatedRows = &rows
		}
		r.Tables = append(r.Tables, rt)
	}

	pg := &ResultPostgres{ServerVersion: f.Server.Version, WALLevel: f.Server.WALLevel, InRecovery: f.Server.InRecovery}
	if f.Server.MaxSlotWALKeepSizeKnown {
		v := f.Server.MaxSlotWALKeepSizeBytes
		pg.MaxSlotWALKeepSize = &v
	}
	if f.Slot != nil && f.Slot.Found {
		s := &ResultSlot{Name: f.Slot.Name, Active: f.Slot.Active, WALStatus: f.Slot.WALStatus}
		if f.Slot.RetainedBytes != UnknownBytes {
			v := f.Slot.RetainedBytes
			s.RetainedBytes = &v
		}
		if f.Slot.ConfirmedLagBytes != UnknownBytes {
			v := f.Slot.ConfirmedLagBytes
			s.ConfirmedLagBytes = &v
		}
		if f.Slot.SafeWALBytes != UnknownBytes {
			v := f.Slot.SafeWALBytes
			s.SafeWALSize = &v
		}
		pg.ReplicationSlot = s
	}
	r.Postgres = pg
	return r
}

// NewErrorResult builds the result of a failed inspection. The message is cut to a short length
// and has the secrets scrubbed, so a driver error that echoes a connection string cannot leak.
func NewErrorResult(code, message string, secrets ...string) Result {
	for _, s := range secrets {
		if s != "" {
			message = strings.ReplaceAll(message, s, "[redacted]")
		}
	}
	if r := []rune(message); len(r) > maxErrorMessage {
		message = string(r[:maxErrorMessage]) + "..."
	}
	return Result{FactsVersion: FactsVersion, Error: &ResultError{Code: code, Message: message}}
}

// Encode renders the result as compact JSON no larger than MaxResultBytes. When it does not fit,
// tables are dropped from the end and Truncated is set; a result that cannot fit even without
// tables is an error.
func Encode(r Result) ([]byte, error) {
	for {
		b, err := json.Marshal(r)
		if err != nil {
			return nil, fmt.Errorf("encoding result: %w", err)
		}
		if len(b) <= MaxResultBytes {
			return b, nil
		}
		if len(r.Tables) == 0 {
			return nil, fmt.Errorf("result is %d bytes even without tables, above the %d limit", len(b), MaxResultBytes)
		}
		r.Tables = r.Tables[:len(r.Tables)-1]
		r.Truncated = true
	}
}

// Classify maps an inspection error to its stable code and exit code.
func Classify(err error) (code string, exit int) {
	if errors.Is(err, ErrConnect) {
		var pg *pgconn.PgError
		// SQLSTATE class 28 is "invalid authorization specification" (wrong password, no access).
		if errors.As(err, &pg) && strings.HasPrefix(pg.Code, "28") {
			return ErrCodeAuth, ExitAuth
		}
		return ErrCodeConnect, ExitConnect
	}
	return ErrCodeInspection, ExitInspection
}
