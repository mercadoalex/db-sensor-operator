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

// Package health tracks the liveness of a DatabaseSensor's CDC stream. Task 8
// provides the LagTracker: a small, concurrency-safe holder of the most recent
// event time, used by the health checker and status reporter (Task 9/13) to
// derive Phase, Lag, and LastEvent.
//
// DECISION 1 (lag ownership): the LagTracker is a SCALAR, unlabeled tracker and
// registers NO Prometheus metric. The labeled debezium_lag_seconds{db,table}
// GaugeVec is owned and set by the metrics Exporter. Keeping the two separate
// avoids a double-registration panic and keeps the health path (a single
// human-readable lag for status) decoupled from the per-table metric path.
package health

import (
	"sync"
	"time"
)

// LagTracker remembers when the most recent CDC event was produced (its source
// ts_ms) and reports how far behind the stream currently is. It is safe for
// concurrent use.
type LagTracker struct {
	mu     sync.Mutex
	lastTs time.Time
	now    func() time.Time
}

// NewLagTracker returns a LagTracker using the real clock.
func NewLagTracker() *LagTracker {
	return NewLagTrackerWithClock(time.Now)
}

// NewLagTrackerWithClock returns a LagTracker with an injectable clock for
// deterministic tests. now must not be nil.
func NewLagTrackerWithClock(now func() time.Time) *LagTracker {
	return &LagTracker{now: now}
}

// OnEvent records that an event with the given source timestamp (Unix
// milliseconds) was observed, advancing the last-event mark.
func (l *LagTracker) OnEvent(tsMs int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastTs = time.UnixMilli(tsMs)
}

// GetLag returns the age of the most recent event (now - lastEvent). It returns
// 0 before any event has been recorded.
func (l *LagTracker) GetLag() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lastTs.IsZero() {
		return 0
	}
	return l.now().Sub(l.lastTs)
}

// LastEvent returns the source timestamp of the most recent event, or the zero
// time if none has been recorded. Used by the status reporter (Task 13).
func (l *LagTracker) LastEvent() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastTs
}
