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
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testAlert() Alert {
	return Alert{
		Name:      "HighLag",
		DB:        "pg-svc.default",
		Value:     120,
		Threshold: 30,
		Timestamp: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		Severity:  SeverityWarning,
	}
}

func TestWebhookSinkAlertPOSTsUS44Body(t *testing.T) {
	var got webhookPayload
	var contentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType = r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		require.NoError(t, json.Unmarshal(body, &got))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ws := NewWebhookSink(srv.URL, nil, srv.Client())
	require.NoError(t, ws.Alert(context.Background(), testAlert()))

	assert.Equal(t, "application/json", contentType)
	assert.Equal(t, "HighLag", got.Alert)
	assert.Equal(t, "pg-svc.default", got.DB)
	assert.Equal(t, float64(120), got.Value)
	assert.Equal(t, float64(30), got.Threshold)
	assert.Equal(t, "2026-01-01T12:00:00Z", got.Timestamp)
	assert.Equal(t, SeverityWarning, got.Severity)
}

func TestWebhookSinkPushIsNoOp(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer srv.Close()

	ws := NewWebhookSink(srv.URL, nil, srv.Client())
	require.NoError(t, ws.Push(context.Background(), Metric{Name: "x", Value: 1}))
	assert.False(t, called, "Push must not contact the webhook")
}

func TestWebhookSinkTriggerOnFiltering(t *testing.T) {
	var posts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Name in triggerOn -> POST.
	ws := NewWebhookSink(srv.URL, []string{"HighLag"}, srv.Client())
	require.NoError(t, ws.Alert(context.Background(), testAlert()))
	assert.Equal(t, int32(1), posts.Load())

	// Name not in a non-empty triggerOn -> filtered, no POST.
	ws = NewWebhookSink(srv.URL, []string{"NoEvents"}, srv.Client())
	require.NoError(t, ws.Alert(context.Background(), testAlert()))
	assert.Equal(t, int32(1), posts.Load())

	// Empty triggerOn -> fire for all names.
	ws = NewWebhookSink(srv.URL, nil, srv.Client())
	require.NoError(t, ws.Alert(context.Background(), testAlert()))
	assert.Equal(t, int32(2), posts.Load())
}

func TestWebhookSinkRetriesThenSucceeds(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ws := NewWebhookSink(srv.URL, nil, srv.Client())
	ws.retryDelay = time.Millisecond
	require.NoError(t, ws.Alert(context.Background(), testAlert()))
	assert.Equal(t, int32(3), attempts.Load())
}

func TestWebhookSinkRetryExhaustionReturnsError(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	ws := NewWebhookSink(srv.URL, nil, srv.Client())
	ws.retryDelay = time.Millisecond
	err := ws.Alert(context.Background(), testAlert())
	require.Error(t, err)
	assert.Equal(t, int32(webhookMaxAttempts), attempts.Load())
}
