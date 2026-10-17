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
	"fmt"
	"net/http"
	"slices"
	"time"
)

const (
	// webhookDefaultTimeout bounds a single webhook POST (repo-task 4.8).
	webhookDefaultTimeout = 10 * time.Second

	// webhookMaxAttempts is the total number of POST attempts (1 try + retries).
	webhookMaxAttempts = 3

	// webhookRetryDelay is the pause between attempts; exposed as a field so
	// tests can shrink it to ~1ms.
	webhookRetryDelay = 5 * time.Second
)

// WebhookSink delivers alerts as an HTTP POST of a JSON body (US-4.4). Push is a
// no-op: the webhook carries alerts, not metric samples. A delivery failure is
// returned to the caller, which treats it as NON-BLOCKING (logs a warning,
// never fails the reconcile).
type WebhookSink struct {
	url         string
	client      *http.Client
	triggerOn   []string
	maxAttempts int
	retryDelay  time.Duration
}

// webhookPayload is the JSON body POSTed to the webhook URL. The field names are
// the US-4.4 contract: alert name, DB host, current value, threshold, timestamp
// (RFC3339), severity.
type webhookPayload struct {
	Alert     string  `json:"alert"`
	DB        string  `json:"db"`
	Value     float64 `json:"value"`
	Threshold float64 `json:"threshold"`
	Timestamp string  `json:"timestamp"`
	Severity  string  `json:"severity"`
}

// NewWebhookSink builds a WebhookSink for the given URL and triggerOn filter. A
// nil client defaults to a client with webhookDefaultTimeout.
func NewWebhookSink(url string, triggerOn []string, c *http.Client) *WebhookSink {
	if c == nil {
		c = &http.Client{Timeout: webhookDefaultTimeout}
	}
	return &WebhookSink{
		url:         url,
		client:      c,
		triggerOn:   triggerOn,
		maxAttempts: webhookMaxAttempts,
		retryDelay:  webhookRetryDelay,
	}
}

// Push is a no-op for the webhook sink (webhooks carry alerts, not metrics).
func (w *WebhookSink) Push(_ context.Context, _ Metric) error {
	return nil
}

// Alert POSTs the alert as JSON to the webhook URL, honouring triggerOn. When
// triggerOn is non-empty and the alert name is not in it, the alert is filtered
// (no POST, nil error). An empty triggerOn fires for ALL alert names (documented
// default: webhook enabled means notify me). On a transport error or a non-2xx
// response the POST is retried up to maxAttempts with retryDelay between tries;
// the last error is returned for the caller to log.
func (w *WebhookSink) Alert(ctx context.Context, a Alert) error {
	if len(w.triggerOn) > 0 && !slices.Contains(w.triggerOn, a.Name) {
		return nil
	}

	body, err := json.Marshal(webhookPayload{
		Alert:     a.Name,
		DB:        a.DB,
		Value:     a.Value,
		Threshold: a.Threshold,
		Timestamp: a.Timestamp.Format(time.RFC3339),
		Severity:  a.Severity,
	})
	if err != nil {
		return fmt.Errorf("marshalling webhook payload: %w", err)
	}

	var lastErr error
	for attempt := 1; attempt <= w.maxAttempts; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return fmt.Errorf("webhook POST cancelled: %w", ctx.Err())
			case <-time.After(w.retryDelay):
			}
		}
		lastErr = w.post(ctx, body)
		if lastErr == nil {
			return nil
		}
	}
	return fmt.Errorf("webhook POST to %q failed after %d attempts: %w", w.url, w.maxAttempts, lastErr)
}

// post performs a single webhook POST and classifies the result. A non-2xx
// status is an error so the caller retries.
func (w *WebhookSink) post(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("building webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("sending webhook request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}
	return nil
}
