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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// Step 0 decision (see .agents/tasks/task11-12-plan.md): the Datadog sink is a
// thin net/http client, NOT the datadog-api-client-go SDK. US-4.5 only needs to
// POST a metric series and an event with header auth and to honour a 429, which
// is a few dozen lines of stdlib and keeps the pinned deps untouched and the
// sink trivially pointable at an httptest.Server.
const (
	// datadogDefaultBaseURL is the production intake host; tests inject an
	// httptest.Server URL instead.
	datadogDefaultBaseURL = "https://api.datadoghq.com"

	// datadogSeriesPath / datadogEventsPath are the v1 intake endpoints.
	datadogSeriesPath = "/api/v1/series"
	datadogEventsPath = "/api/v1/events"

	// datadogAPIKeyHeader / datadogAppKeyHeader carry the credentials.
	datadogAPIKeyHeader = "DD-API-KEY"
	datadogAppKeyHeader = "DD-APPLICATION-KEY"

	// datadogDefaultTimeout bounds a single Datadog request.
	datadogDefaultTimeout = 10 * time.Second

	// datadogMaxAttempts is the total number of attempts on a 429.
	datadogMaxAttempts = 3

	// datadogMaxRetryAfter caps how long a Retry-After sleep may be so a single
	// reconcile never blocks unbounded on a throttled Datadog.
	datadogMaxRetryAfter = 5 * time.Second
)

// ErrDatadogRateLimited is returned (wrapped) when Datadog responds 429 and the
// retries are exhausted. It is a SOFT error: the reconciler leaves status
// unchanged and uses RetryAfter to schedule the next requeue rather than
// hammering the API.
var ErrDatadogRateLimited = errors.New("datadog rate limited")

// RateLimitError carries the Retry-After hint parsed from a 429 so the
// reconciler can requeue after it. It wraps ErrDatadogRateLimited.
type RateLimitError struct {
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("datadog rate limited; retry after %s", e.RetryAfter)
}

func (e *RateLimitError) Unwrap() error { return ErrDatadogRateLimited }

// DatadogSink pushes metrics and alerts to the Datadog v1 HTTP intake using
// header auth. The API/App key values are supplied by the Registry (resolved
// from K8s Secrets at build time) and never read from the K8s API or logged by
// this type (NFR-4). baseURL is injectable for httptest.
type DatadogSink struct {
	baseURL       string
	apiKey        string
	appKey        string
	tags          []string
	client        *http.Client
	maxAttempts   int
	maxRetryAfter time.Duration
}

// datadogSeries is the /api/v1/series body.
type datadogSeries struct {
	Series []datadogSeriesPoint `json:"series"`
}

type datadogSeriesPoint struct {
	Metric string      `json:"metric"`
	Points [][]float64 `json:"points"`
	Type   string      `json:"type"`
	Tags   []string    `json:"tags,omitempty"`
}

// datadogEvent is the /api/v1/events body.
type datadogEvent struct {
	Title        string   `json:"title"`
	Text         string   `json:"text"`
	AlertType    string   `json:"alert_type"`
	DateHappened int64    `json:"date_happened"`
	Tags         []string `json:"tags,omitempty"`
}

// NewDatadogSink builds a DatadogSink. An empty baseURL defaults to the
// production intake; a nil client defaults to one with datadogDefaultTimeout.
func NewDatadogSink(baseURL, apiKey, appKey string, tags []string, c *http.Client) *DatadogSink {
	if baseURL == "" {
		baseURL = datadogDefaultBaseURL
	}
	if c == nil {
		c = &http.Client{Timeout: datadogDefaultTimeout}
	}
	return &DatadogSink{
		baseURL:       baseURL,
		apiKey:        apiKey,
		appKey:        appKey,
		tags:          tags,
		client:        c,
		maxAttempts:   datadogMaxAttempts,
		maxRetryAfter: datadogMaxRetryAfter,
	}
}

// Push submits a metric sample to /api/v1/series as a gauge point.
func (d *DatadogSink) Push(ctx context.Context, m Metric) error {
	ts := m.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}
	body, err := json.Marshal(datadogSeries{
		Series: []datadogSeriesPoint{{
			Metric: m.Name,
			Points: [][]float64{{float64(ts.Unix()), m.Value}},
			Type:   "gauge",
			Tags:   d.mergeTags(m.Tags),
		}},
	})
	if err != nil {
		return fmt.Errorf("marshalling datadog series: %w", err)
	}
	return d.send(ctx, datadogSeriesPath, body)
}

// Alert submits a fired alert to /api/v1/events. A critical severity maps to
// alert_type "error"; anything else maps to "warning".
func (d *DatadogSink) Alert(ctx context.Context, a Alert) error {
	ts := a.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}
	alertType := "warning"
	if a.Severity == SeverityCritical {
		alertType = "error"
	}
	body, err := json.Marshal(datadogEvent{
		Title: fmt.Sprintf("%s on %s", a.Name, a.DB),
		Text: fmt.Sprintf("Alert %s fired for database %s: value %g crossed threshold %g",
			a.Name, a.DB, a.Value, a.Threshold),
		AlertType:    alertType,
		DateHappened: ts.Unix(),
		Tags:         d.mergeTags(nil),
	})
	if err != nil {
		return fmt.Errorf("marshalling datadog event: %w", err)
	}
	return d.send(ctx, datadogEventsPath, body)
}

// mergeTags combines the sink's configured tags with any per-call tags,
// preserving order (configured first) and leaving nil when both are empty.
func (d *DatadogSink) mergeTags(extra []string) []string {
	if len(d.tags) == 0 && len(extra) == 0 {
		return nil
	}
	out := make([]string, 0, len(d.tags)+len(extra))
	out = append(out, d.tags...)
	out = append(out, extra...)
	return out
}

// send POSTs the body to baseURL+path with the Datadog auth headers, retrying on
// 429 up to maxAttempts (sleeping the bounded Retry-After between tries). On
// exhausted 429s it returns a *RateLimitError (soft) carrying the Retry-After.
func (d *DatadogSink) send(ctx context.Context, path string, body []byte) error {
	var lastRetryAfter time.Duration
	for attempt := 1; attempt <= d.maxAttempts; attempt++ {
		retryAfter, err := d.post(ctx, path, body)
		if err == nil {
			return nil
		}
		if retryAfter < 0 {
			// Non-429 failure: return immediately (soft to the caller).
			return err
		}
		// 429: honour the bounded Retry-After and retry, unless out of attempts.
		lastRetryAfter = retryAfter
		if attempt == d.maxAttempts {
			break
		}
		sleep := min(retryAfter, d.maxRetryAfter)
		select {
		case <-ctx.Done():
			return fmt.Errorf("datadog request cancelled: %w", ctx.Err())
		case <-time.After(sleep):
		}
	}
	return &RateLimitError{RetryAfter: lastRetryAfter}
}

// post performs a single Datadog request. It returns (retryAfter >= 0, err) when
// the response is 429 so the caller can back off; a non-429 failure returns
// (-1, err); success returns (-1, nil).
func (d *DatadogSink) post(ctx context.Context, path string, body []byte) (time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return -1, fmt.Errorf("building datadog request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(datadogAPIKeyHeader, d.apiKey)
	if d.appKey != "" {
		req.Header.Set(datadogAppKeyHeader, d.appKey)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return -1, fmt.Errorf("sending datadog request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return parseRetryAfter(resp.Header.Get("Retry-After")), fmt.Errorf("datadog returned 429")
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return -1, fmt.Errorf("datadog returned status %d", resp.StatusCode)
	default:
		return -1, nil
	}
}

// parseRetryAfter reads a Retry-After header expressed in seconds. An absent or
// unparseable value yields 0 (retry promptly, still bounded by maxRetryAfter).
func parseRetryAfter(h string) time.Duration {
	if h == "" {
		return 0
	}
	secs, err := strconv.Atoi(h)
	if err != nil || secs < 0 {
		return 0
	}
	return time.Duration(secs) * time.Second
}
