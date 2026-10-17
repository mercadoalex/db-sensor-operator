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

// This file (Task 6) adds the Prometheus Exporter: the component that turns a
// stream of CDCEvents into the four operator-wide metric families and serves
// them in Prometheus text format on /metrics (US-2, US-3.1).
//
// DECISION 1 (lag ownership): the Exporter owns the labeled
// debezium_lag_seconds{db,table} GaugeVec and sets it in OnEvent. The health
// LagTracker (internal/health) keeps a SEPARATE unlabeled scalar lag for status
// and registers no Prometheus metric, so there is no double-registration panic.
//
// Style follows servicemonitor.go: a small set of constructors, dependencies
// injected (the registry and the clock), no globals.
package metrics

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/mercadoalex/db-sensor-operator/internal/events"
)

const (
	// ratePerMinWindow is the width of the sliding window used to compute
	// debezium_events_per_minute. One minute of per-second buckets keeps the
	// update O(1) amortized per event (NFR-1: 1000 ev/s).
	ratePerMinWindow = time.Minute

	// rateBuckets is the number of 1-second buckets in the sliding window.
	rateBuckets = 60

	// labelTable is the metric label name for the source table.
	labelTable = "table"
)

// Exporter registers and updates the four Task 6 metric families on a single
// Prometheus registry and serves them via Handler(). It is safe for concurrent
// use by multiple consumer goroutines.
type Exporter struct {
	reg prometheus.Registerer

	eventsTotal   *prometheus.CounterVec
	schemaChanges *prometheus.CounterVec
	lagSeconds    *prometheus.GaugeVec
	eventsPerMin  *prometheus.GaugeVec

	// now is the injectable clock (defaults to time.Now) so lag and the rolling
	// rate window are deterministic in tests.
	now func() time.Time

	mu    sync.Mutex
	rates map[string]*rateWindow
}

// NewExporter builds an Exporter and registers its four metric families exactly
// once on reg, using the real clock. reg is typically a fresh
// prometheus.NewRegistry() in tests or controller-runtime's global metrics
// registry in the manager.
func NewExporter(reg prometheus.Registerer) *Exporter {
	return NewExporterWithClock(reg, time.Now)
}

// NewExporterWithClock is NewExporter with an injectable clock for deterministic
// tests. now must not be nil.
func NewExporterWithClock(reg prometheus.Registerer, now func() time.Time) *Exporter {
	factory := promauto.With(reg)
	return &Exporter{
		reg: reg,
		now: now,
		eventsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "debezium_events_total",
			Help: "Total CDC data-change events by operation, database, and table.",
		}, []string{"op", "db", labelTable}),
		schemaChanges: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "debezium_schema_changes_total",
			Help: "Total CDC schema-change (DDL) events by database and table.",
		}, []string{"db", labelTable}),
		lagSeconds: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "debezium_lag_seconds",
			Help: "Age in seconds of the most recent CDC event by database and table.",
		}, []string{"db", labelTable}),
		eventsPerMin: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "debezium_events_per_minute",
			Help: "Rolling 60-second rate of CDC data-change events by database and table.",
		}, []string{"db", labelTable}),
		rates: make(map[string]*rateWindow),
	}
}

// OnEvent updates the metrics for a single CDC event. Data-change events (c/u/d)
// increment debezium_events_total and feed the rolling events-per-minute gauge;
// DDL events increment debezium_schema_changes_total. Every event refreshes
// debezium_lag_seconds for its {db,table} to now - ts_ms (DECISION 1).
func (e *Exporter) OnEvent(ev events.CDCEvent) {
	switch ev.Op {
	case "c", "u", "d":
		e.eventsTotal.WithLabelValues(ev.Op, ev.DB, ev.Table).Inc()
		e.updateRate(ev.DB, ev.Table)
	case "ddl":
		e.schemaChanges.WithLabelValues(ev.DB, ev.Table).Inc()
	}

	// Lag applies to any timestamped event. A zero/absent ts_ms would produce a
	// meaningless huge lag, so only set it when we actually have a timestamp.
	if ev.TsMs > 0 {
		lag := e.now().Sub(time.UnixMilli(ev.TsMs)).Seconds()
		e.lagSeconds.WithLabelValues(ev.DB, ev.Table).Set(lag)
	}
}

// EventsPerMinForDB returns the current aggregate CDC event rate per minute for
// a database host, summed across every table of that host the Exporter has
// observed (Task 13, Decision D1). The DatabaseSensor status is a per-sensor
// summary and a sensor targets one spec.database.host across its spec.tables, so
// summing the per-{db,table} rolling windows for that host yields one meaningful
// events/min number without the reconciler enumerating tables.
//
// It is concurrency-safe and READ-ONLY with respect to the rolling windows: it
// reads each window's count at e.now() via countAt without expiring buckets, so
// it never corrupts the state OnEvent mutates. An unknown host returns 0.
func (e *Exporter) EventsPerMinForDB(db string) float64 {
	e.mu.Lock()
	defer e.mu.Unlock()

	nowSec := e.now().Unix()
	var total int64
	for key, w := range e.rates {
		// key is db + "\x00" + table; match on the db segment.
		keyDB, _, ok := strings.Cut(key, "\x00")
		if !ok {
			continue
		}
		if keyDB == db {
			total += w.countAt(nowSec)
		}
	}
	return float64(total)
}

// Handler returns an http.Handler that serves this Exporter's registry in
// Prometheus text format. Task 15 mounts it at /metrics on the operator's HTTP
// server; this method intentionally does not build the full ServeMux.
func (e *Exporter) Handler() http.Handler {
	gatherer, ok := e.reg.(prometheus.Gatherer)
	if !ok {
		// A Registerer that is not also a Gatherer cannot be scraped; fall back to
		// the default gatherer so /metrics still responds.
		gatherer = prometheus.DefaultGatherer
	}
	return promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{})
}

// updateRate records one data-change event in the per-{db,table} sliding window
// and refreshes the events_per_minute gauge with the count observed over the
// last ratePerMinWindow.
func (e *Exporter) updateRate(db, table string) {
	key := db + "\x00" + table
	now := e.now()

	e.mu.Lock()
	w := e.rates[key]
	if w == nil {
		w = newRateWindow()
		e.rates[key] = w
	}
	count := w.observe(now)
	e.mu.Unlock()

	e.eventsPerMin.WithLabelValues(db, table).Set(float64(count))
}

// rateWindow is a ring of per-second counters covering a rolling 60-second
// window. observe adds one event at the current second, expires buckets older
// than the window, and returns the total count still inside the window. All
// access is serialized by the Exporter mutex, so the struct itself needs no
// lock. The ring is O(1) amortized per event because at most rateBuckets stale
// buckets are cleared between calls.
type rateWindow struct {
	counts [rateBuckets]int64
	// bucketSec is the absolute second (unix seconds) currently stored in each
	// ring slot, used to detect and expire stale buckets.
	bucketSec [rateBuckets]int64
}

func newRateWindow() *rateWindow {
	w := &rateWindow{}
	for i := range w.bucketSec {
		w.bucketSec[i] = -1
	}
	return w
}

// observe records an event at time t and returns the number of events within the
// trailing ratePerMinWindow (inclusive of this one).
func (w *rateWindow) observe(t time.Time) int64 {
	sec := t.Unix()
	idx := int(sec % rateBuckets)

	// If this slot holds a different (older) second, it is stale: reset it before
	// recording the new event.
	if w.bucketSec[idx] != sec {
		w.bucketSec[idx] = sec
		w.counts[idx] = 0
	}
	w.counts[idx]++

	return w.sum(sec)
}

// countAt totals the buckets whose second is within the trailing window ending
// at nowSec WITHOUT expiring stale buckets. It is the read-only counterpart of
// sum, used by EventsPerMinForDB so a status read never mutates the window that
// OnEvent maintains. The cutoff math mirrors sum exactly.
func (w *rateWindow) countAt(nowSec int64) int64 {
	cutoff := nowSec - int64(ratePerMinWindow/time.Second) + 1
	var total int64
	for i := range w.counts {
		if w.bucketSec[i] < cutoff {
			continue
		}
		total += w.counts[i]
	}
	return total
}

// sum totals the buckets whose second is within the trailing window ending at
// nowSec, discarding stale entries as it goes.
func (w *rateWindow) sum(nowSec int64) int64 {
	cutoff := nowSec - int64(ratePerMinWindow/time.Second) + 1
	var total int64
	for i := range w.counts {
		if w.bucketSec[i] < cutoff {
			// Outside the window: drop it so future sums stay cheap.
			w.bucketSec[i] = -1
			w.counts[i] = 0
			continue
		}
		total += w.counts[i]
	}
	return total
}
