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

// Package sink is the multi-sink fan-out layer for a DatabaseSensor's derived
// metrics and alerts (Tasks 11 + 12, US-4.4/4.5/4.6). It is the egress side of
// the observability path: the reconciler derives a health signal, maps it to an
// Alert, and fans that Alert out to every configured sink.
//
// The Prometheus sink is intentionally NOT represented here: the /metrics
// endpoint is always exposed by the manager (ADR-04), so Prometheus is a passive
// pull target rather than an active push sink. BuildSinks therefore only ever
// activates the webhook and Datadog sinks.
//
// Sinks never fail the reconcile. A sink Push/Alert error is collected and
// surfaced to the caller, which logs a warning and keeps going (the data path is
// Debezium, not these egress calls). The one exception the caller cares about is
// a Datadog 429, which is surfaced via a typed error so the reconciler can
// requeue after Retry-After instead of hammering the API.
package sink

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
)

// Sink type values mirror spec.sink.type (api/v1).
const (
	sinkTypePrometheus = "prometheus"
	sinkTypeDatadog    = "datadog"
	sinkTypeWebhook    = "webhook"
	sinkTypeAll        = "all"
)

// Severity values carried by an Alert.
const (
	SeverityWarning  = "warning"
	SeverityCritical = "critical"
)

// Datadog Secret data-key conventions. The spec names the Secret but not the
// key inside it, so the Registry reads these conventional keys and falls back to
// the single entry when the Secret has exactly one (documented assumption in the
// Task 11/12 plan).
const (
	datadogAPIKeyDataKey = "api-key"
	datadogAppKeyDataKey = "app-key"
)

// ErrDatadogSecretMissing is returned by BuildSinks (wrapped) when the Datadog
// sink is enabled but its API/App key Secret is absent or unreadable. It is a
// SOFT failure: BuildSinks still returns a usable CompositeSink built from the
// other sinks, and the reconciler logs a warning rather than flipping Phase to
// Failed (distinct from the DB password Secret, which is a hard failure handled
// in the deployer).
var ErrDatadogSecretMissing = errors.New("datadog secret not found")

// Metric is a single numeric sample pushed to a sink. Timestamp is the moment
// the sample was taken; Tags are sink-agnostic "key:value" strings.
type Metric struct {
	Name      string
	Value     float64
	Tags      []string
	Timestamp time.Time
}

// Alert is a fired health signal fanned out to the sinks. It carries the US-4.4
// webhook body fields: alert name, DB host, the current value, the threshold it
// crossed, the time it fired, and the severity.
type Alert struct {
	Name      string
	DB        string
	Value     float64
	Threshold float64
	Timestamp time.Time
	Severity  string
}

// Sink is one observability backend. Push delivers a metric sample; Alert
// delivers a fired alert. Implementations must be safe to call with a cancelled
// context (returning the context error) and must not panic on zero-value input.
type Sink interface {
	Push(ctx context.Context, m Metric) error
	Alert(ctx context.Context, a Alert) error
}

// CompositeSink fans a Push/Alert out to every member sink in insertion order.
// It calls EVERY sink even if an earlier one errors, and returns the aggregate
// via errors.Join (nil when all succeed). This guarantees one failing sink never
// starves the others (Property 10 of the plan).
type CompositeSink struct {
	sinks []Sink
}

// NewCompositeSink builds a CompositeSink over the given sinks, preserving the
// argument order as the deterministic fan-out order.
func NewCompositeSink(sinks ...Sink) *CompositeSink {
	return &CompositeSink{sinks: sinks}
}

// Len reports how many active sinks the composite holds.
func (c *CompositeSink) Len() int {
	return len(c.sinks)
}

// Push fans the metric out to all member sinks; see CompositeSink.
func (c *CompositeSink) Push(ctx context.Context, m Metric) error {
	errs := make([]error, 0, len(c.sinks))
	for _, s := range c.sinks {
		if err := s.Push(ctx, m); err != nil {
			errs = append(errs, fmt.Errorf("sink push: %w", err))
		}
	}
	return errors.Join(errs...)
}

// Alert fans the alert out to all member sinks; see CompositeSink.
func (c *CompositeSink) Alert(ctx context.Context, a Alert) error {
	errs := make([]error, 0, len(c.sinks))
	for _, s := range c.sinks {
		if err := s.Alert(ctx, a); err != nil {
			errs = append(errs, fmt.Errorf("sink alert: %w", err))
		}
	}
	return errors.Join(errs...)
}

// BuildSinks assembles the active CompositeSink for a DatabaseSensor from
// spec.sink. Activation is driven by spec.sink.type and the per-sink enabled
// flags; the fan-out order is deterministic (webhook, then Datadog).
//
// Prometheus is passive (ADR-04): it is never added here. A disabled or
// unconfigured sink is skipped. The Datadog API/App keys are resolved from their
// K8s Secrets at build time (NFR-4: read at use, never cached) via c.Get; a
// missing/unreadable Datadog Secret is SOFT — BuildSinks returns the composite
// built from the remaining sinks ALONGSIDE a wrapped ErrDatadogSecretMissing so
// the reconciler can log a warning and continue.
func BuildSinks(ctx context.Context, c client.Client, sensor *sensoriov1.DatabaseSensor) (*CompositeSink, error) {
	cfg := sensor.Spec.Sink
	var sinks []Sink
	var softErr error

	wantWebhook := cfg.Type == sinkTypeWebhook || cfg.Type == sinkTypeAll
	wantDatadog := cfg.Type == sinkTypeDatadog || cfg.Type == sinkTypeAll

	// Webhook sink: active when selected, enabled, and a URL is configured.
	if wantWebhook && cfg.Webhook != nil && cfg.Webhook.Enabled && cfg.Webhook.URL != "" {
		sinks = append(sinks, NewWebhookSink(cfg.Webhook.URL, cfg.Webhook.TriggerOn, nil))
	}

	// Datadog sink: active when selected and enabled. Secrets are resolved now;
	// a missing/unreadable Secret downgrades to a soft error and the sink is
	// skipped.
	if wantDatadog && cfg.Datadog != nil && cfg.Datadog.Enabled {
		apiKey, appKey, err := resolveDatadogKeys(ctx, c, sensor.Namespace, cfg.Datadog)
		if err != nil {
			softErr = err
		} else {
			sinks = append(sinks, NewDatadogSink("", apiKey, appKey, cfg.Datadog.Tags, nil))
		}
	}

	return NewCompositeSink(sinks...), softErr
}

// resolveDatadogKeys reads the Datadog API/App keys from their named Secrets in
// the sensor's namespace. A missing/unreadable Secret yields a wrapped
// ErrDatadogSecretMissing (soft). The key values are never logged (NFR-4).
func resolveDatadogKeys(ctx context.Context, c client.Client, namespace string, dd *sensoriov1.DatadogSink) (apiKey, appKey string, err error) {
	apiKey, err = secretValue(ctx, c, namespace, dd.APIKeySecret, datadogAPIKeyDataKey)
	if err != nil {
		return "", "", err
	}
	// The app key Secret is optional: Datadog metric/event submission is
	// authenticated by the API key alone. Resolve it only when configured.
	if dd.AppKeySecret != "" {
		appKey, err = secretValue(ctx, c, namespace, dd.AppKeySecret, datadogAppKeyDataKey)
		if err != nil {
			return "", "", err
		}
	}
	return apiKey, appKey, nil
}

// secretValue reads a single value out of a named Secret. It prefers the
// conventional data key and falls back to the sole entry when the Secret has
// exactly one. A missing Secret or an empty/ambiguous payload is a soft
// ErrDatadogSecretMissing. The value is never logged (NFR-4).
func secretValue(ctx context.Context, c client.Client, namespace, name, preferredKey string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("%w: no secret name configured", ErrDatadogSecretMissing)
	}
	var secret corev1.Secret
	key := types.NamespacedName{Name: name, Namespace: namespace}
	if err := c.Get(ctx, key, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("%w: %q", ErrDatadogSecretMissing, name)
		}
		return "", fmt.Errorf("getting datadog secret %q: %w", name, err)
	}
	if v, ok := secret.Data[preferredKey]; ok && len(v) > 0 {
		return string(v), nil
	}
	if len(secret.Data) == 1 {
		for _, v := range secret.Data {
			if len(v) > 0 {
				return string(v), nil
			}
		}
	}
	return "", fmt.Errorf("%w: secret %q has no usable %q key", ErrDatadogSecretMissing, name, preferredKey)
}
