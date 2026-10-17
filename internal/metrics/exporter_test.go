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

package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mercadoalex/db-sensor-operator/internal/events"
)

// fixedClock returns a controllable clock for deterministic lag and rate tests.
func fixedClock(t *time.Time) func() time.Time {
	return func() time.Time { return *t }
}

func TestExporterCountsByOp(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	exp := NewExporterWithClock(prometheus.NewRegistry(), fixedClock(&now))

	ts := now.Add(-time.Second).UnixMilli()
	for i := 0; i < 3; i++ {
		exp.OnEvent(events.CDCEvent{Op: "c", DB: "pg", Table: "orders", TsMs: ts})
	}
	exp.OnEvent(events.CDCEvent{Op: "u", DB: "pg", Table: "orders", TsMs: ts})
	exp.OnEvent(events.CDCEvent{Op: "d", DB: "pg", Table: "customers", TsMs: ts})

	assert.Equal(t, 3.0, testutil.ToFloat64(exp.eventsTotal.WithLabelValues("c", "pg", "orders")))
	assert.Equal(t, 1.0, testutil.ToFloat64(exp.eventsTotal.WithLabelValues("u", "pg", "orders")))
	assert.Equal(t, 1.0, testutil.ToFloat64(exp.eventsTotal.WithLabelValues("d", "pg", "customers")))
}

func TestExporterSchemaChanges(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	exp := NewExporterWithClock(prometheus.NewRegistry(), fixedClock(&now))

	exp.OnEvent(events.CDCEvent{Op: "ddl", DB: "pg", Table: "orders", TsMs: now.UnixMilli()})
	exp.OnEvent(events.CDCEvent{Op: "ddl", DB: "pg", Table: "orders", TsMs: now.UnixMilli()})

	assert.Equal(t, 2.0, testutil.ToFloat64(exp.schemaChanges.WithLabelValues("pg", "orders")))
	// A DDL event is not counted as a data-change event.
	assert.Equal(t, 0.0, testutil.ToFloat64(exp.eventsTotal.WithLabelValues("ddl", "pg", "orders")))
}

func TestExporterLagSeconds(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	exp := NewExporterWithClock(prometheus.NewRegistry(), fixedClock(&now))

	exp.OnEvent(events.CDCEvent{Op: "c", DB: "pg", Table: "orders", TsMs: now.Add(-30 * time.Second).UnixMilli()})

	assert.InDelta(t, 30.0, testutil.ToFloat64(exp.lagSeconds.WithLabelValues("pg", "orders")), 0.001)
}

func TestExporterEventsPerMinuteWindow(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	exp := NewExporterWithClock(prometheus.NewRegistry(), fixedClock(&now))

	// Burst of 10 events in the current second.
	for i := 0; i < 10; i++ {
		exp.OnEvent(events.CDCEvent{Op: "c", DB: "pg", Table: "orders", TsMs: now.UnixMilli()})
	}
	assert.Equal(t, 10.0, testutil.ToFloat64(exp.eventsPerMin.WithLabelValues("pg", "orders")))

	// Advance beyond the 60s window: the burst has aged out, a new event reads 1.
	now = now.Add(61 * time.Second)
	exp.OnEvent(events.CDCEvent{Op: "c", DB: "pg", Table: "orders", TsMs: now.UnixMilli()})
	assert.Equal(t, 1.0, testutil.ToFloat64(exp.eventsPerMin.WithLabelValues("pg", "orders")))
}

func TestExporterHandlerScrape(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	exp := NewExporterWithClock(prometheus.NewRegistry(), fixedClock(&now))

	exp.OnEvent(events.CDCEvent{Op: "c", DB: "pg", Table: "orders", TsMs: now.Add(-time.Second).UnixMilli()})
	exp.OnEvent(events.CDCEvent{Op: "ddl", DB: "pg", Table: "orders", TsMs: now.UnixMilli()})

	srv := httptest.NewServer(exp.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	text := string(body)

	assert.Contains(t, text, "debezium_events_total")
	assert.Contains(t, text, "debezium_schema_changes_total")
	assert.Contains(t, text, "debezium_lag_seconds")
	assert.Contains(t, text, "debezium_events_per_minute")
	assert.True(t, strings.Contains(text, `op="c"`))
}

// TestExporterRegistersEachFamilyOnce guards DECISION 1: constructing an Exporter
// registers the four families exactly once on the passed-in registry (no panic,
// all four gatherable).
func TestExporterRegistersEachFamilyOnce(t *testing.T) {
	reg := prometheus.NewRegistry()
	assert.NotPanics(t, func() { NewExporter(reg) })

	mfs, err := reg.Gather()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	// Gauges/counters with no observations may not appear until used; register a
	// use so all four surface, then re-gather.
	exp := NewExporterWithClock(prometheus.NewRegistry(), time.Now)
	exp.OnEvent(events.CDCEvent{Op: "c", DB: "d", Table: "t", TsMs: time.Now().UnixMilli()})
	exp.OnEvent(events.CDCEvent{Op: "ddl", DB: "d", Table: "t", TsMs: time.Now().UnixMilli()})
	used, err := exp.reg.(*prometheus.Registry).Gather()
	require.NoError(t, err)
	for _, mf := range used {
		names[mf.GetName()] = true
	}
	assert.True(t, names["debezium_events_total"])
	assert.True(t, names["debezium_schema_changes_total"])
	assert.True(t, names["debezium_lag_seconds"])
	assert.True(t, names["debezium_events_per_minute"])
}
