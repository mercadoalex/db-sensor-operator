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

package events_test

import (
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

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
	"github.com/mercadoalex/db-sensor-operator/internal/events"
	"github.com/mercadoalex/db-sensor-operator/internal/health"
	"github.com/mercadoalex/db-sensor-operator/internal/metrics"
)

// TestPipelineEndToEndHTTP exercises the full FEAT-001 path: an HTTP POST of a
// Debezium fixture -> HTTPConsumer.Handler -> Parse -> Pipeline.Handle ->
// Exporter + LagTracker, then asserts both the scraped /metrics counts and the
// LagTracker state.
func TestPipelineEndToEndHTTP(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }

	exp := metrics.NewExporterWithClock(prometheus.NewRegistry(), clock)
	lag := health.NewLagTrackerWithClock(clock)
	pipe := &events.Pipeline{Exporter: exp, Lag: lag}

	consumer, err := events.NewConsumer(sensoriov1.TransportConfig{Type: "http"})
	require.NoError(t, err)
	httpc, ok := consumer.(*events.HTTPConsumer)
	require.True(t, ok)

	ingest := httptest.NewServer(httpc.Handler(pipe.Handle))
	defer ingest.Close()

	eventTs := now.Add(-10 * time.Second).UnixMilli()
	fixture := `{"payload":{"op":"c","ts_ms":` + strconv.FormatInt(eventTs, 10) + `,"source":{"db":"pg","table":"orders"}}}`

	resp, err := http.Post(ingest.URL, "application/json", strings.NewReader(fixture))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// LagTracker got the event.
	assert.InDelta(t, (10 * time.Second).Seconds(), lag.GetLag().Seconds(), 0.001)

	// Metrics reflect the event via the Exporter's /metrics handler.
	scrape := httptest.NewServer(exp.Handler())
	defer scrape.Close()
	mResp, err := http.Get(scrape.URL)
	require.NoError(t, err)
	defer func() { _ = mResp.Body.Close() }()
	body, err := io.ReadAll(mResp.Body)
	require.NoError(t, err)
	text := string(body)

	assert.Contains(t, text, `debezium_events_total{db="pg",op="c",table="orders"} 1`)
	assert.Contains(t, text, `debezium_lag_seconds{db="pg",table="orders"} 10`)
}
