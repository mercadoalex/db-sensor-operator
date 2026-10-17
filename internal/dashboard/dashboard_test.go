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

package dashboard_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mercadoalex/db-sensor-operator/internal/dashboard"
	"github.com/mercadoalex/db-sensor-operator/internal/events"
	"github.com/mercadoalex/db-sensor-operator/internal/metrics"
)

// newTestServer builds a Server wired with a real Exporter and a CDC handler
// backed by a Pipeline, mirroring the production wiring in cmd/main.go.
func newTestServer(t *testing.T, gate *dashboard.Gate, buf *dashboard.RingBuffer) (*dashboard.Server, *metrics.Exporter) {
	t.Helper()
	exp := metrics.NewExporter(prometheus.NewRegistry())
	pipe := &events.Pipeline{Exporter: exp, Recorder: buf}
	httpc := &events.HTTPConsumer{}
	cdcH := httpc.Handler(pipe.Handle)
	srv := dashboard.NewServer(":0", exp.Handler(), cdcH, buf, gate)
	return srv, exp
}

func TestDashboardGatedOff(t *testing.T) {
	gate := &dashboard.Gate{} // off by default
	buf := dashboard.NewRingBuffer(0, nil)
	srv, _ := newTestServer(t, gate, buf)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for _, path := range []string{"/dashboard", "/api/events"} {
		resp, err := http.Get(ts.URL + path)
		require.NoError(t, err)
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusNotFound, resp.StatusCode, "%s should 404 when gate off", path)
	}
}

func TestDashboardGatedOnServesHTML(t *testing.T) {
	gate := &dashboard.Gate{}
	gate.Enable()
	buf := dashboard.NewRingBuffer(0, nil)
	srv, _ := newTestServer(t, gate, buf)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/dashboard")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/html")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	html := string(body)
	assert.Contains(t, html, "chart.js", "page must reference the Chart.js CDN script")
	assert.Contains(t, html, "/api/events", "page must fetch the events API")
	assert.Contains(t, html, "/metrics", "page must fetch metrics")
}

func TestEventsAPIReturnsRecordedEvents(t *testing.T) {
	gate := &dashboard.Gate{}
	gate.Enable()
	buf := dashboard.NewRingBuffer(0, func() time.Time { return time.Unix(0, 0) })
	srv, _ := newTestServer(t, gate, buf)

	for i := range 3 {
		buf.Add(events.CDCEvent{Op: "c", DB: "pg", Table: strconv.Itoa(i), TsMs: int64(i)})
	}

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/events")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")

	var got []dashboard.Event
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Len(t, got, 3)
	// Newest-first.
	assert.Equal(t, "2", got[0].Table)
	assert.Equal(t, "0", got[2].Table)
}

func TestEventsAPIEmptyArray(t *testing.T) {
	gate := &dashboard.Gate{}
	gate.Enable()
	buf := dashboard.NewRingBuffer(0, nil)
	srv, _ := newTestServer(t, gate, buf)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/events")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "[]", strings.TrimSpace(string(body)))
}

// TestMetricsAlwaysOn asserts /metrics serves Prometheus text regardless of the
// dashboard gate (US-3.1 / ADR-04).
func TestMetricsAlwaysOn(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		gate := &dashboard.Gate{}
		gate.Set(enabled)
		buf := dashboard.NewRingBuffer(0, nil)
		srv, exp := newTestServer(t, gate, buf)
		exp.OnEvent(events.CDCEvent{Op: "c", DB: "pg", Table: "orders", TsMs: time.Now().UnixMilli()})

		ts := httptest.NewServer(srv.Handler())

		resp, err := http.Get(ts.URL + "/metrics")
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.NoError(t, err)
		ts.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Contains(t, string(body), "debezium_events_total",
			"/metrics must expose derived metrics with gate enabled=%v", enabled)
	}
}

func TestHealthzReadyzAlwaysOn(t *testing.T) {
	gate := &dashboard.Gate{} // off
	buf := dashboard.NewRingBuffer(0, nil)
	srv, _ := newTestServer(t, gate, buf)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := http.Get(ts.URL + path)
		require.NoError(t, err)
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode, "%s should be 200", path)
	}
}

// TestCDCRouteFeedsBuffer asserts POST /cdc ingests a valid Debezium event into
// the exporter and buffer, and rejects malformed bodies without recording.
func TestCDCRouteFeedsBuffer(t *testing.T) {
	gate := &dashboard.Gate{}
	gate.Enable()
	buf := dashboard.NewRingBuffer(0, func() time.Time { return time.Unix(0, 0) })
	srv, _ := newTestServer(t, gate, buf)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	valid := `{"payload":{"op":"c","ts_ms":1,"source":{"db":"pg","table":"orders"}}}`
	resp, err := http.Post(ts.URL+"/cdc", "application/json", strings.NewReader(valid))
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.Len(t, buf.Recent(), 1)
	assert.Equal(t, "orders", buf.Recent()[0].Table)

	// Malformed body: 400 and nothing recorded.
	bad, err := http.Post(ts.URL+"/cdc", "application/json", strings.NewReader("not json"))
	require.NoError(t, err)
	_ = bad.Body.Close()
	assert.Equal(t, http.StatusBadRequest, bad.StatusCode)
	assert.Len(t, buf.Recent(), 1, "malformed event must not be recorded")
}
