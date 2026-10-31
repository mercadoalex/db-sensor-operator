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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const tblOrders = "public.orders"

func facts(rows int64, canWrite, replica bool) TableFacts {
	return TableFacts{Table: tblOrders, Found: true, EstimatedRows: rows, EstimateReliable: true, CanWrite: canWrite, ReadOnlyServer: replica, WALLevel: "logical"}
}

func TestDecide(t *testing.T) {
	roCaps := Capabilities{ReadOnlyIncremental: true}

	tests := []struct {
		name     string
		facts    TableFacts
		cfg      PolicyConfig
		caps     Capabilities
		want     StrategyKind
		degraded bool
		reason   string
	}{
		{name: "small table streams only", facts: facts(500, true, false), want: StrategyStreamOnly, reason: "not backfilled"},
		{name: "small table streams only without write access", facts: facts(500, false, false), want: StrategyStreamOnly},
		{name: "at the threshold is still small", facts: facts(DefaultSmallTableRows, true, false), want: StrategyStreamOnly},
		{name: "one row above the threshold is large", facts: facts(DefaultSmallTableRows+1, true, false), want: StrategySignalTable},
		{name: "large with write access uses the signaling table", facts: facts(5_000_000, true, false), want: StrategySignalTable, reason: "signaling table"},
		{name: "large without write uses read-only when verified", facts: facts(5_000_000, false, false), caps: roCaps, want: StrategyReadOnlyIncremental},
		{name: "write access wins over read-only", facts: facts(5_000_000, true, false), caps: roCaps, want: StrategySignalTable},
		{name: "large without write and no read-only path is degraded", facts: facts(5_000_000, false, false), want: StrategyNone, degraded: true, reason: "no write access"},
		{name: "replica never gets the signaling table", facts: facts(5_000_000, true, true), want: StrategyNone, degraded: true, reason: "read-only replica"},
		{name: "replica uses read-only when verified", facts: facts(5_000_000, true, true), caps: roCaps, want: StrategyReadOnlyIncremental},
		{name: "unknown size is treated as large", facts: facts(UnknownRows, true, false), want: StrategySignalTable, reason: "never analyzed"},
		{name: "unknown size without any path is degraded", facts: facts(UnknownRows, false, false), want: StrategyNone, degraded: true},
		{name: "a small but unreliable estimate is treated as large", facts: staleFacts(1_000, 800_000), want: StrategySignalTable, reason: "stale"},
		{name: "a stale estimate without write access is degraded", facts: func() TableFacts { f := staleFacts(1_000, 800_000); f.CanWrite = false; return f }(), want: StrategyNone, degraded: true, reason: "stale"},
		{name: "custom threshold", facts: facts(2_000, true, false), cfg: PolicyConfig{SmallTableRows: 1_000}, want: StrategySignalTable},
		{name: "custom threshold keeps small tables small", facts: facts(900, true, false), cfg: PolicyConfig{SmallTableRows: 1_000}, want: StrategyStreamOnly},
		{name: "non-positive threshold falls back to the default", facts: facts(500, true, false), cfg: PolicyConfig{SmallTableRows: -5}, want: StrategyStreamOnly},
		{name: "missing table is degraded", facts: TableFacts{Table: tblOrders, WALLevel: "logical"}, want: StrategyNone, degraded: true, reason: "not found"},
		{name: "wal_level replica is degraded", facts: TableFacts{Table: tblOrders, Found: true, EstimatedRows: 10, WALLevel: "replica"}, want: StrategyNone, degraded: true, reason: "wal_level"},
		{name: "override wins over everything", facts: facts(5_000_000, false, true), cfg: PolicyConfig{Override: StrategyStreamOnly}, want: StrategyStreamOnly, reason: "set manually"},
		{name: "override wins even over a failed precondition", facts: TableFacts{Table: tblOrders}, cfg: PolicyConfig{Override: StrategySignalTable}, want: StrategySignalTable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Decide(tc.facts, tc.cfg, tc.caps)
			assert.Equal(t, tc.want, got.Kind)
			assert.Equal(t, tc.degraded, got.Degraded)
			assert.NotEmpty(t, got.Reason)
			assert.Contains(t, got.Reason, tblOrders, "the reason names the table")
			if tc.reason != "" {
				assert.Contains(t, got.Reason, tc.reason)
			}
		})
	}
}

// staleFacts is a table whose small reltuples survived a bulk load without ANALYZE.
func staleFacts(rows, mods int64) TableFacts {
	f := facts(rows, true, false)
	f.EstimateReliable = false
	f.ModsSinceAnalyze = mods
	return f
}

// Degraded is set exactly for the "none" strategy that is not a manual override.
func TestDecideDegradedOnlyWhenNothingWorks(t *testing.T) {
	for _, rows := range []int64{UnknownRows, 0, 10, DefaultSmallTableRows, DefaultSmallTableRows + 1, 50_000_000} {
		for _, canWrite := range []bool{true, false} {
			for _, replica := range []bool{true, false} {
				for _, ro := range []bool{true, false} {
					name := fmt.Sprintf("rows=%d write=%v replica=%v ro=%v", rows, canWrite, replica, ro)
					got := Decide(facts(rows, canWrite, replica), PolicyConfig{}, Capabilities{ReadOnlyIncremental: ro})
					assert.Equal(t, got.Kind == StrategyNone, got.Degraded, name)
				}
			}
		}
	}
}

func TestDecideReasonNeverHasCredentials(t *testing.T) {
	f := facts(5_000_000, false, false)
	got := Decide(f, PolicyConfig{}, Capabilities{})
	require.NotEmpty(t, got.Reason)
	assert.NotContains(t, got.Reason, "password")
}
