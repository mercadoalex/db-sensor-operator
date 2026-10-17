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

package sink

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Shared literals used across the Datadog sink tests.
const (
	testDDAPIKey = "api-123"
	testDDTag    = "env:test"
)

func TestDatadogSinkPushSeries(t *testing.T) {
	var path, apiKey, appKey string
	var payload datadogSeries
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		apiKey = r.Header.Get(datadogAPIKeyHeader)
		appKey = r.Header.Get(datadogAppKeyHeader)
		body, _ := io.ReadAll(r.Body)
		require.NoError(t, json.Unmarshal(body, &payload))
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	dd := NewDatadogSink(srv.URL, testDDAPIKey, "app-456", []string{testDDTag}, srv.Client())
	m := Metric{Name: "debezium_lag_seconds", Value: 42, Tags: []string{"table:orders"}, Timestamp: time.Unix(1700000000, 0)}
	require.NoError(t, dd.Push(context.Background(), m))

	assert.Equal(t, datadogSeriesPath, path)
	assert.Equal(t, testDDAPIKey, apiKey)
	assert.Equal(t, "app-456", appKey)
	require.Len(t, payload.Series, 1)
	assert.Equal(t, "debezium_lag_seconds", payload.Series[0].Metric)
	assert.Equal(t, "gauge", payload.Series[0].Type)
	assert.Equal(t, []string{testDDTag, "table:orders"}, payload.Series[0].Tags)
	require.Len(t, payload.Series[0].Points, 1)
	assert.Equal(t, float64(1700000000), payload.Series[0].Points[0][0])
	assert.Equal(t, float64(42), payload.Series[0].Points[0][1])
}

func TestDatadogSinkAlertEvent(t *testing.T) {
	var path string
	var payload datadogEvent
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		require.NoError(t, json.Unmarshal(body, &payload))
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	dd := NewDatadogSink(srv.URL, testDDAPIKey, "", []string{testDDTag}, srv.Client())
	a := testAlert()
	a.Severity = SeverityCritical
	require.NoError(t, dd.Alert(context.Background(), a))

	assert.Equal(t, datadogEventsPath, path)
	assert.Equal(t, "error", payload.AlertType)
	assert.Contains(t, payload.Title, "HighLag")
	assert.Contains(t, payload.Title, "pg-svc.default")
	assert.Equal(t, []string{testDDTag}, payload.Tags)
}

func TestDatadogSinkRateLimitedSoftError(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	dd := NewDatadogSink(srv.URL, testDDAPIKey, "", nil, srv.Client())
	dd.maxRetryAfter = time.Millisecond // keep the test fast
	err := dd.Push(context.Background(), Metric{Name: "x", Value: 1})

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDatadogRateLimited), "429 must surface as the soft rate-limit error")
	var rl *RateLimitError
	require.True(t, errors.As(err, &rl))
	assert.Equal(t, time.Second, rl.RetryAfter, "Retry-After seconds must be parsed")
	assert.Equal(t, int32(datadogMaxAttempts), attempts.Load())
}

func TestDatadogSinkNon2xxReturnsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	dd := NewDatadogSink(srv.URL, testDDAPIKey, "", nil, srv.Client())
	err := dd.Push(context.Background(), Metric{Name: "x", Value: 1})
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrDatadogRateLimited))
}
