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
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	gib             = int64(1) << 30
	walLogical      = "logical"
	reasonUnbounded = "unbounded"
)

func unlimited() ServerFacts {
	return ServerFacts{Version: 160004, WALLevel: walLogical, MaxSlotWALKeepSizeKnown: true, MaxSlotWALKeepSizeBytes: UnlimitedBytes}
}

func capped(bytes int64) ServerFacts {
	return ServerFacts{Version: 160004, WALLevel: walLogical, MaxSlotWALKeepSizeKnown: true, MaxSlotWALKeepSizeBytes: bytes}
}

func slot(active bool, status string, retained int64) *SlotFacts {
	return &SlotFacts{Name: "s1", Found: true, Active: active, WALStatus: status, RetainedBytes: retained, ConfirmedLagBytes: UnknownBytes, SafeWALBytes: UnknownBytes}
}

func TestDiagnoseSlot(t *testing.T) {
	tests := []struct {
		name   string
		server ServerFacts
		slot   *SlotFacts
		want   Level
		code   string
		reason []string
	}{
		{name: "active and within max_wal_size is ok", server: unlimited(), slot: slot(true, "reserved", 5<<20), want: LevelOK, code: CodeSlotHealthy, reason: []string{"active", reasonUnbounded}},
		{name: "ok with a cap says the cap", server: capped(4 * gib), slot: slot(true, "reserved", 5<<20), want: LevelOK, code: CodeSlotHealthy, reason: []string{"capped at 4.0 GiB"}},
		{name: "extended and unbounded explains what extended means and that safe_wal_size is NULL", server: unlimited(), slot: slot(true, "extended", 8*gib), want: LevelWarning, code: CodeRetentionExtendedUnbounded,
			reason: []string{"wal_status=extended", "beyond max_wal_size", "by the slot or by wal_keep_size", "safe_wal_size is NULL", "pg_wal volume"}},
		{name: "the unbounded advice warns that a cap can lose the slot", server: unlimited(), slot: slot(true, "extended", 8*gib), want: LevelWarning, code: CodeRetentionExtendedUnbounded,
			reason: []string{"invalidates the slot", "changes since are lost", "longest outage"}},
		{name: "extended with a cap warns and names the cap", server: capped(10 * gib), slot: slot(true, "extended", 8*gib), want: LevelWarning, code: CodeRetentionExtended, reason: []string{"capped at 10.0 GiB"}},
		{name: "unreserved is critical", server: capped(10 * gib), slot: slot(true, "unreserved", 11*gib), want: LevelCritical, code: CodeSlotAtRisk, reason: []string{"next checkpoint", "past max_slot_wal_keep_size"}},
		{name: "lost is critical and says the changes are gone", server: capped(10 * gib), slot: slot(false, "lost", UnknownBytes), want: LevelCritical, code: CodeSlotLost, reason: []string{"lost", "cannot be recovered", "recreated"}},
		{name: "inactive slot warns", server: unlimited(), slot: slot(false, "reserved", 2*gib), want: LevelWarning, code: CodeSlotInactive, reason: []string{"inactive", reasonUnbounded}},
		{name: "unknown retention changes nothing", server: unlimited(), slot: slot(true, "reserved", UnknownBytes), want: LevelOK, code: CodeSlotHealthy},
		{name: "missing slot is informational", server: unlimited(), slot: &SlotFacts{Name: "s1"}, want: LevelInfo, code: CodeSlotMissing, reason: []string{"does not exist yet"}},
		{name: "standby cannot be inspected", server: ServerFacts{InRecovery: true}, slot: nil, want: LevelUnknown, code: CodeSlotNotInspected, reason: []string{"standby"}},
		{name: "not requested", server: unlimited(), slot: nil, want: LevelUnknown, code: CodeSlotNotInspected},
		{name: "before PostgreSQL 13 there is no cap to report", server: ServerFacts{Version: 120000}, slot: slot(true, "", 1<<20), want: LevelOK, code: CodeSlotHealthy, reason: []string{"before PostgreSQL 13"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := DiagnoseSlot(tc.server, tc.slot)
			assert.Equal(t, tc.want, got.Level)
			assert.Equal(t, tc.code, got.Code)
			assert.NotEmpty(t, got.Reason)
			for _, want := range tc.reason {
				assert.Contains(t, got.Reason, want)
			}
		})
	}
}

// The message goes into a status condition, so it must not change while the situation does
// not. Retained bytes and lag grow on every probe; they are facts, not part of the message.
func TestDiagnoseSlotMessageIgnoresVolatileNumbers(t *testing.T) {
	for _, status := range []string{"reserved", "extended", "unreserved", "lost"} {
		for _, active := range []bool{true, false} {
			small := slot(active, status, 1<<20)
			small.ConfirmedLagBytes = 1 << 20
			big := slot(active, status, 8*gib)
			big.ConfirmedLagBytes = 8 * gib
			for _, server := range []ServerFacts{unlimited(), capped(4 * gib)} {
				a, b := DiagnoseSlot(server, small), DiagnoseSlot(server, big)
				assert.Equal(t, a, b, "status=%s active=%v", status, active)
			}
		}
	}
}

// An unbounded cap is the normal default and a healthy, active slot within max_wal_size on it
// is not a problem: the unbounded case alone must not raise a warning.
func TestDiagnoseSlotUnboundedAloneIsNotAWarning(t *testing.T) {
	assert.Equal(t, LevelOK, DiagnoseSlot(unlimited(), slot(true, "reserved", 0)).Level)
}

func TestFormatBytes(t *testing.T) {
	assert.Equal(t, "0 B", formatBytes(0))
	assert.Equal(t, "1023 B", formatBytes(1023))
	assert.Equal(t, "1.0 KiB", formatBytes(1024))
	assert.Equal(t, "1.5 MiB", formatBytes(3<<19))
	assert.Equal(t, "8.0 GiB", formatBytes(8*gib))
	assert.Equal(t, "1.0 TiB", formatBytes(1<<40))
}
