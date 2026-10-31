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

// Package snapshot decides how a sensor's connector should backfill existing
// rows (ADR-014, Phase B). It has two independent parts:
//
//   - an Inspector (per engine) that reads facts about each monitored table with
//     read-only catalog queries, and
//   - a Policy (engine independent) that maps those facts to a Strategy. Decide is
//     a pure function with no I/O, so it is testable without a cluster.
//
// The connector always starts without a blocking snapshot (Phase A). A Strategy
// only says whether, and how, existing rows are then filled in.
package snapshot

import "fmt"

// StrategyKind names a backfill strategy.
type StrategyKind string

const (
	// StrategyStreamOnly starts streaming and does not backfill: the table is
	// small enough that existing rows are not worth a signal.
	StrategyStreamOnly StrategyKind = "stream-only"
	// StrategySignalTable backfills with an incremental snapshot triggered through a
	// signaling table. It needs write access to the database.
	StrategySignalTable StrategyKind = "signal-table"
	// StrategyReadOnlyIncremental backfills with a read-only incremental snapshot,
	// which writes nothing to the database.
	StrategyReadOnlyIncremental StrategyKind = "read-only-incremental"
	// StrategyNone means no backfill is possible; the sensor keeps streaming and
	// reports why.
	StrategyNone StrategyKind = "none"
)

// DefaultSmallTableRows is the default row-count threshold at or below which a
// table is treated as small. It is a judgment call anchored on one measurement:
// a 100,000-row table took about a minute to snapshot over the HTTP sink
// (bench/experiments/blind-window/RESULTS.md), so "small" here means a snapshot
// that would finish in about a minute.
const DefaultSmallTableRows int64 = 100_000

// UnknownRows is TableFacts.EstimatedRows when the planner has no statistics for
// the table (it has never been analyzed).
const UnknownRows int64 = -1

// TableFacts are the inputs of the Policy for one monitored table.
type TableFacts struct {
	// Table is the table as the user named it.
	Table string
	// Found is false when the table does not exist (or is not a table).
	Found bool
	// EstimatedRows is the row estimate, or UnknownRows. It is the larger of the
	// planner's reltuples and the live-tuple counter n_live_tup: when they
	// disagree, overestimating the size is the safe direction, because "large"
	// leads to the non-blocking paths. It is an estimate and not count(*).
	EstimatedRows int64
	// EstimateReliable is false when the statistics cannot be trusted: the table was
	// never analyzed, or enough rows changed since the last ANALYZE that reltuples is
	// stale. A stale estimate is worse than an unknown one because it looks
	// trustworthy (a table bulk-loaded without ANALYZE keeps its old, small
	// reltuples), so Decide never lets an unreliable estimate say "small".
	EstimateReliable bool
	// ModsSinceAnalyze is n_mod_since_analyze (rows inserted, updated or deleted
	// since the last ANALYZE), or UnknownRows when unavailable. It is kept so the
	// reason can say why the estimate was not trusted.
	ModsSinceAnalyze int64
	// CanWrite reports whether the connection may create a signaling table in the
	// table's schema. It is an approximation of "has write access".
	CanWrite bool
	// ReadOnlyServer is true when the server is a read-only replica (in recovery).
	ReadOnlyServer bool
	// WALLevel is the server's wal_level; logical decoding needs "logical".
	WALLevel string
}

// Capabilities describe what the engine and the pinned Debezium version support.
type Capabilities struct {
	// ReadOnlyIncremental is true only when the read-only incremental snapshot has
	// been verified for the engine and the pinned Debezium version.
	ReadOnlyIncremental bool
}

// PolicyConfig tunes Decide.
type PolicyConfig struct {
	// SmallTableRows is the largest estimated row count still treated as small.
	// Zero or negative means DefaultSmallTableRows.
	SmallTableRows int64
	// Override, when non-empty, bypasses the automatic decision (spec.snapshot.strategy).
	Override StrategyKind
}

// Strategy is the Policy's answer for one table.
type Strategy struct {
	Kind StrategyKind
	// Degraded is true when the sensor should report Degraded because no backfill
	// path is available or a precondition failed.
	Degraded bool
	// Reason is one line for the sensor status. It names tables and numbers but never
	// carries credentials or row data.
	Reason string
}

// Decide maps the facts of one table to a Strategy. It is a pure function.
//
// Rules, in order:
//  1. A manual override wins and is reported as such.
//  2. A missing table, or wal_level other than "logical", is a precondition
//     failure: no Strategy can work, so the result is Degraded.
//  3. At or below the small-table threshold, with a reliable estimate: stream-only.
//  4. Above it, or with an unknown or unreliable estimate, and able to write:
//     signal-table.
//  5. Above it, unable to write, and read-only incremental verified: that.
//  6. Otherwise: none, Degraded, naming what is missing.
//
// A read-only replica cannot write, so it never gets signal-table.
// Unknown or unreliable statistics are treated as large, because such a table
// could be of any size and the large-table paths are the non-blocking ones.
func Decide(f TableFacts, cfg PolicyConfig, caps Capabilities) Strategy {
	if cfg.Override != "" {
		return Strategy{Kind: cfg.Override, Reason: fmt.Sprintf("%s: strategy set manually by spec.snapshot.strategy", f.Table)}
	}

	if !f.Found {
		return Strategy{Kind: StrategyNone, Degraded: true, Reason: fmt.Sprintf("%s: table not found", f.Table)}
	}
	if f.WALLevel != "logical" {
		return Strategy{Kind: StrategyNone, Degraded: true,
			Reason: fmt.Sprintf("%s: wal_level is %q, logical decoding needs \"logical\"", f.Table, f.WALLevel)}
	}

	threshold := cfg.SmallTableRows
	if threshold <= 0 {
		threshold = DefaultSmallTableRows
	}

	if f.EstimatedRows != UnknownRows && f.EstimateReliable && f.EstimatedRows <= threshold {
		return Strategy{Kind: StrategyStreamOnly,
			Reason: fmt.Sprintf("%s: at or below %d rows, so existing rows are not backfilled", f.Table, threshold)}
	}

	// The reason goes into the sensor status, so it carries the configured threshold and never
	// the estimate: the estimate is a fact and changes with every ANALYZE.
	size := fmt.Sprintf("above %d rows", threshold)
	switch {
	case f.EstimatedRows == UnknownRows:
		size = "size unknown (never analyzed), treated as large"
	case !f.EstimateReliable && f.EstimatedRows <= threshold:
		size = "the statistics are stale (more changes since the last ANALYZE than autovacuum tolerates), treated as large"
	}

	if f.CanWrite && !f.ReadOnlyServer {
		return Strategy{Kind: StrategySignalTable,
			Reason: fmt.Sprintf("%s: %s; backfilling through a signaling table", f.Table, size)}
	}
	if caps.ReadOnlyIncremental {
		return Strategy{Kind: StrategyReadOnlyIncremental,
			Reason: fmt.Sprintf("%s: %s and no write access; using a read-only incremental snapshot", f.Table, size)}
	}

	missing := "no write access for a signaling table"
	if f.ReadOnlyServer {
		missing = "the server is a read-only replica"
	}
	return Strategy{Kind: StrategyNone, Degraded: true,
		Reason: fmt.Sprintf("%s: %s, %s, and no verified read-only path; existing rows are not backfilled", f.Table, size, missing)}
}
