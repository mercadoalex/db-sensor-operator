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

// Package dashboard (Task 14 + 15) is the operator's single HTTP surface. It
// holds an in-memory ring buffer of recent CDC events, serves the embedded
// dashboard (/dashboard) and events API (/api/events), and runs the unified
// http.ServeMux as a controller-runtime Runnable that also carries /metrics,
// /cdc, /healthz, and /readyz (DECISIONS A-F of the Task 14/15 plan).
//
// Retention (DECISION A): the ring is bounded by a hard MaxEvents count cap so
// memory stays bounded under NFR-1 (1000 ev/s) regardless of config, and
// spec.dashboard.retention is interpreted as a time window applied on read.
// Events older than now-retention are pruned lazily; an empty/zero retention
// disables the time bound and the buffer behaves as a pure last-MaxEvents ring.
// Record time is the operator's receive time (now()), not the source ts_ms, so
// pruning is monotonic and unaffected by out-of-order source timestamps.
package dashboard

import (
	"sync"
	"time"

	"github.com/mercadoalex/db-sensor-operator/internal/events"
)

// MaxEvents is the hard upper bound on how many recent events the ring buffer
// retains. It is a fixed memory cap independent of spec.dashboard.retention so
// the buffer stays bounded under sustained load (NFR-1).
const MaxEvents = 1000

// Event is the dashboard's view of a single recorded CDC event: the flat
// CDCEvent fields plus the operator receive time used for time-window pruning
// and for display in the events table. It is the JSON shape returned by
// /api/events.
type Event struct {
	Op         string    `json:"op"`
	DB         string    `json:"db"`
	Table      string    `json:"table"`
	TsMs       int64     `json:"tsMs"`
	RecordedAt time.Time `json:"recordedAt"`
}

// RingBuffer is a concurrency-safe, fixed-capacity ring of recently recorded
// CDC events. The consumer pipeline writes via Add; the HTTP handler reads via
// Recent. Writes overwrite the oldest entry once MaxEvents is reached.
type RingBuffer struct {
	mu        sync.Mutex
	buf       []Event
	next      int  // index of the next write slot
	full      bool // whether the ring has wrapped at least once
	retention time.Duration
	now       func() time.Time
}

// NewRingBuffer returns a RingBuffer with the given retention time window and
// clock. A zero (or negative) retention disables time-window pruning. now may
// be nil, in which case time.Now is used; injecting it keeps tests
// deterministic.
func NewRingBuffer(retention time.Duration, now func() time.Time) *RingBuffer {
	if now == nil {
		now = time.Now
	}
	return &RingBuffer{
		buf:       make([]Event, MaxEvents),
		retention: retention,
		now:       now,
	}
}

// Add records one CDC event, stamping it with the current receive time. When the
// ring is full the oldest entry is overwritten. It is safe to call concurrently
// with Recent.
func (r *RingBuffer) Add(ev events.CDCEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.buf[r.next] = Event{
		Op:         ev.Op,
		DB:         ev.DB,
		Table:      ev.Table,
		TsMs:       ev.TsMs,
		RecordedAt: r.now(),
	}
	r.next++
	if r.next == len(r.buf) {
		r.next = 0
		r.full = true
	}
}

// Recent returns the retained events newest-first. When a positive retention is
// configured, events older than now-retention are excluded (time-window pruning
// per DECISION A). The returned slice is a copy and safe for the caller to hold.
func (r *RingBuffer) Recent() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()

	var cutoff time.Time
	if r.retention > 0 {
		cutoff = r.now().Add(-r.retention)
	}

	out := make([]Event, 0, r.size())
	// Walk from newest to oldest. The newest entry is at next-1 (mod len).
	for i := 0; i < r.size(); i++ {
		idx := (r.next - 1 - i + len(r.buf)) % len(r.buf)
		ev := r.buf[idx]
		if r.retention > 0 && ev.RecordedAt.Before(cutoff) {
			// Entries are strictly older as we walk back, so once one is outside
			// the window every remaining entry is too.
			break
		}
		out = append(out, ev)
	}
	return out
}

// SetRetention updates the time window applied on read (DECISION A: the
// reconciler pushes the maximum retention across enabled sensors). A zero or
// negative value disables time-window pruning.
func (r *RingBuffer) SetRetention(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.retention = d
}

// size returns the number of recorded entries currently in the ring. Callers
// must hold r.mu.
func (r *RingBuffer) size() int {
	if r.full {
		return len(r.buf)
	}
	return r.next
}
