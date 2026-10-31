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
	"fmt"
	"math"
)

const (
	// UnlimitedBytes is ServerFacts.MaxSlotWALKeepSizeBytes when max_slot_wal_keep_size is -1:
	// a slot may retain WAL without limit, so the only ceiling is the disk of pg_wal, which
	// SQL cannot see.
	UnlimitedBytes int64 = -1
	// UnknownBytes marks a byte count that could not be determined. It is not -1 because
	// SafeWALBytes is legitimately negative once the cap is exceeded (observed: -112 MB).
	UnknownBytes int64 = math.MinInt64
)

// ServerFacts are the server-wide facts of one inspection.
type ServerFacts struct {
	// Version is server_version_num (for example 160004).
	Version int
	// InRecovery is true on a standby.
	InRecovery bool
	// WALLevel is the server's wal_level; logical decoding needs "logical".
	WALLevel string
	// MaxSlotWALKeepSizeKnown is false on PostgreSQL before 13, where the setting does not exist.
	MaxSlotWALKeepSizeKnown bool
	// MaxSlotWALKeepSizeBytes is the cap on WAL a slot may retain, or UnlimitedBytes. Only
	// meaningful when MaxSlotWALKeepSizeKnown. Not the same as wal_keep_size, which keeps WAL
	// for streaming standbys and does not bound slots.
	MaxSlotWALKeepSizeBytes int64
}

// SlotFacts describe the connector's replication slot.
type SlotFacts struct {
	Name string
	// Found is false when the slot does not exist (the connector has not created it yet).
	Found bool
	// Active is true while a connector is attached to the slot.
	Active bool
	// WALStatus is reserved, extended, unreserved or lost (PostgreSQL 13+; empty before).
	WALStatus string
	// RetainedBytes is the WAL kept for the slot, from its restart_lsn to the current
	// position, or UnknownBytes. This is cluster-wide WAL, not just the captured tables.
	RetainedBytes int64
	// ConfirmedLagBytes is how far the connector's last confirmed position trails the current
	// WAL position (current minus confirmed_flush_lsn), or UnknownBytes. One reading cannot
	// tell a stalled connector from a quiet one: that needs two readings over time.
	ConfirmedLagBytes int64
	// SafeWALBytes is how much more WAL can be written before the slot is at risk (PostgreSQL
	// 13+), or UnknownBytes. The server reports it only when retention is capped, and it is
	// negative once the cap is exceeded.
	SafeWALBytes int64
}

// Level is how serious a finding is.
type Level string

const (
	LevelOK       Level = "ok"
	LevelInfo     Level = "info"
	LevelWarning  Level = "warning"
	LevelCritical Level = "critical"
	LevelUnknown  Level = "unknown"
)

// Reason codes of a SlotRisk. They are stable, so a status condition keyed on one of them
// does not change while the situation does not.
const (
	CodeSlotHealthy                = "SlotHealthy"
	CodeSlotMissing                = "SlotMissing"
	CodeSlotNotInspected           = "SlotNotInspected"
	CodeSlotInactive               = "SlotInactive"
	CodeSlotAtRisk                 = "SlotAtRisk"
	CodeSlotLost                   = "SlotLost"
	CodeRetentionExtended          = "RetentionExtended"
	CodeRetentionExtendedUnbounded = "RetentionExtendedUnbounded"
)

// SlotRisk is the diagnosis of the replication slot.
type SlotRisk struct {
	Level Level
	// Code is a stable reason code for a status condition.
	Code string
	// Reason is the message for the sensor status. It names states and configured limits but
	// carries no value that changes from one inspection to the next (retained bytes, lag):
	// those are facts, and putting them here would rewrite the status on every probe.
	Reason string
}

// unboundedAdvice explains the cost of the obvious fix, because setting a cap trades one
// risk for another and a DBA following the advice blindly can lose the connector's position.
const unboundedAdvice = "Setting max_slot_wal_keep_size bounds the disk use, but PostgreSQL then invalidates the slot, " +
	"and the changes since are lost, if the connector falls further behind than the cap: size it for the longest outage you accept, " +
	"or monitor disk usage out of band."

// DiagnoseSlot judges the replication slot. It is a pure function and uses no threshold of
// its own: only the states PostgreSQL defines in wal_status and whether retention is capped.
// The most common production failure of a Debezium PostgreSQL connector is a slot that
// retains WAL until the disk fills or the server drops the slot, and this is what it reports.
//
// wal_status reserved means the slot's required WAL is within max_wal_size, so a checkpoint
// would have kept it anyway. extended means it is beyond max_wal_size but still retained,
// by the slot or by wal_keep_size. With max_slot_wal_keep_size at -1 PostgreSQL has no bound
// to measure against, so safe_wal_size is NULL and a slot cannot newly become unreserved or
// lost; one that was lost under an earlier cap stays lost after the cap is lifted.
func DiagnoseSlot(server ServerFacts, slot *SlotFacts) SlotRisk {
	if server.InRecovery {
		return SlotRisk{LevelUnknown, CodeSlotNotInspected, "the server is a standby, so the replication slot cannot be inspected"}
	}
	if slot == nil {
		return SlotRisk{LevelUnknown, CodeSlotNotInspected, "the replication slot was not inspected"}
	}
	if !slot.Found {
		return SlotRisk{LevelInfo, CodeSlotMissing, fmt.Sprintf("replication slot %q does not exist yet", slot.Name)}
	}

	switch slot.WALStatus {
	case "lost":
		return SlotRisk{LevelCritical, CodeSlotLost, fmt.Sprintf(
			"replication slot %q is lost: the WAL it needed was removed, so the changes since then cannot be recovered, "+
				"the connector cannot resume and the slot must be recreated", slot.Name)}
	case "unreserved":
		return SlotRisk{LevelCritical, CodeSlotAtRisk, fmt.Sprintf(
			"replication slot %q is past max_slot_wal_keep_size and no longer retains the WAL it needs: "+
				"PostgreSQL will remove it at the next checkpoint unless the connector catches up", slot.Name)}
	}

	unbounded := server.MaxSlotWALKeepSizeKnown && server.MaxSlotWALKeepSizeBytes == UnlimitedBytes
	switch {
	case slot.WALStatus == "extended" && unbounded:
		return SlotRisk{LevelWarning, CodeRetentionExtendedUnbounded, fmt.Sprintf(
			"Slot %s has wal_status=extended (its required WAL is beyond max_wal_size but still retained, by the slot or by wal_keep_size) "+
				"and max_slot_wal_keep_size is -1, so PostgreSQL has no bound to measure against and safe_wal_size is NULL. "+
				"The only limit is the free space of the pg_wal volume, which SQL cannot read. %s", slot.Name, unboundedAdvice)}
	case slot.WALStatus == "extended":
		return SlotRisk{LevelWarning, CodeRetentionExtended, fmt.Sprintf(
			"Slot %s has wal_status=extended (its required WAL is beyond max_wal_size but still retained); %s",
			slot.Name, boundText(server))}
	case !slot.Active:
		return SlotRisk{LevelWarning, CodeSlotInactive, fmt.Sprintf(
			"replication slot %q is inactive (no connector attached); %s", slot.Name, boundText(server))}
	}
	return SlotRisk{LevelOK, CodeSlotHealthy, fmt.Sprintf(
		"replication slot %q is active and its required WAL is within max_wal_size; %s", slot.Name, boundText(server))}
}

// boundText says whether the slot's retention is capped, being honest when it cannot tell.
func boundText(s ServerFacts) string {
	switch {
	case !s.MaxSlotWALKeepSizeKnown:
		return "the retention cap (max_slot_wal_keep_size) does not exist before PostgreSQL 13"
	case s.MaxSlotWALKeepSizeBytes == UnlimitedBytes:
		return "retention is unbounded (max_slot_wal_keep_size is -1), so the free space of pg_wal is the only limit"
	default:
		return "retention is capped at " + formatBytes(s.MaxSlotWALKeepSizeBytes) + " (max_slot_wal_keep_size)"
	}
}

// formatBytes renders a byte count with binary units.
func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
