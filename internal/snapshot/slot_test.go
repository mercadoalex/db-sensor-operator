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
		reason []string
	}{
		{name: "active and within max_wal_size is ok", server: unlimited(), slot: slot(true, "reserved", 5<<20), want: LevelOK, reason: []string{"active", reasonUnbounded}},
		{name: "ok with a cap says the cap", server: capped(4 * gib), slot: slot(true, "reserved", 5<<20), want: LevelOK, reason: []string{"capped at 4.0 GiB"}},
		{name: "extended and unbounded warns and says the disk is the only limit", server: unlimited(), slot: slot(true, "extended", 8*gib), want: LevelWarning, reason: []string{"beyond max_wal_size", "8.0 GiB", reasonUnbounded, "pg_wal"}},
		{name: "extended with a cap warns", server: capped(10 * gib), slot: slot(true, "extended", 8*gib), want: LevelWarning, reason: []string{"capped at 10.0 GiB"}},
		{name: "unreserved is critical", server: capped(10 * gib), slot: slot(true, "unreserved", 11*gib), want: LevelCritical, reason: []string{"next checkpoint", "11.0 GiB"}},
		{name: "lost is critical and says it must be recreated", server: capped(10 * gib), slot: slot(false, "lost", UnknownBytes), want: LevelCritical, reason: []string{"lost", "recreated"}},
		{name: "inactive slot warns", server: unlimited(), slot: slot(false, "reserved", 2*gib), want: LevelWarning, reason: []string{"inactive", "2.0 GiB", reasonUnbounded}},
		{name: "unknown retention is not printed", server: unlimited(), slot: slot(true, "reserved", UnknownBytes), want: LevelOK},
		{name: "missing slot is informational", server: unlimited(), slot: &SlotFacts{Name: "s1"}, want: LevelInfo, reason: []string{"does not exist yet"}},
		{name: "standby cannot be inspected", server: ServerFacts{InRecovery: true}, slot: nil, want: LevelUnknown, reason: []string{"standby"}},
		{name: "not requested", server: unlimited(), slot: nil, want: LevelUnknown},
		{name: "before PostgreSQL 13 there is no cap to report", server: ServerFacts{Version: 120000}, slot: slot(true, "", 1<<20), want: LevelOK, reason: []string{"before PostgreSQL 13"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := DiagnoseSlot(tc.server, tc.slot)
			assert.Equal(t, tc.want, got.Level)
			assert.NotEmpty(t, got.Reason)
			for _, want := range tc.reason {
				assert.Contains(t, got.Reason, want)
			}
		})
	}
}

// A healthy-looking unbounded slot must say so without alarming: the unbounded case is the
// normal default, and only an extended or inactive slot warns.
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
