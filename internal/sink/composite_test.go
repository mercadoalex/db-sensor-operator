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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
)

// Shared literals used across the sink builder tests.
const (
	testWebhookURL = "http://hook"
	testDDSecret   = "dd-api"
)

// fakeSink records whether each method was called and can be made to fail.
type fakeSink struct {
	name        string
	err         error
	pushCalled  bool
	alertCalled bool
	order       *[]string
}

func (f *fakeSink) Push(_ context.Context, _ Metric) error {
	f.pushCalled = true
	if f.order != nil {
		*f.order = append(*f.order, f.name)
	}
	return f.err
}

func (f *fakeSink) Alert(_ context.Context, _ Alert) error {
	f.alertCalled = true
	if f.order != nil {
		*f.order = append(*f.order, f.name)
	}
	return f.err
}

func TestCompositeSinkFanOutContinuesAfterError(t *testing.T) {
	var order []string
	boom := errors.New("boom")
	a := &fakeSink{name: "a", order: &order}
	b := &fakeSink{name: "b", err: boom, order: &order}
	c := &fakeSink{name: "c", order: &order}

	comp := NewCompositeSink(a, b, c)

	err := comp.Alert(context.Background(), testAlert())
	require.Error(t, err)
	assert.True(t, errors.Is(err, boom), "aggregate must contain the failing sink's error")
	assert.True(t, a.alertCalled && b.alertCalled && c.alertCalled, "every sink must be called despite the failure")
	assert.Equal(t, []string{"a", "b", "c"}, order, "fan-out order is deterministic insertion order")

	order = nil
	err = comp.Push(context.Background(), Metric{Name: "m"})
	require.Error(t, err)
	assert.True(t, a.pushCalled && b.pushCalled && c.pushCalled)
	assert.Equal(t, []string{"a", "b", "c"}, order)
}

func TestCompositeSinkAllSucceed(t *testing.T) {
	a := &fakeSink{name: "a"}
	b := &fakeSink{name: "b"}
	comp := NewCompositeSink(a, b)
	assert.NoError(t, comp.Alert(context.Background(), testAlert()))
	assert.Equal(t, 2, comp.Len())
}

func buildScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(s))
	require.NoError(t, sensoriov1.AddToScheme(s))
	return s
}

func ddSecret(name, namespace, key, value string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Data:       map[string][]byte{key: []byte(value)},
	}
}

func sensorWith(sink sensoriov1.SinkConfig) *sensoriov1.DatabaseSensor {
	return &sensoriov1.DatabaseSensor{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "default"},
		Spec:       sensoriov1.DatabaseSensorSpec{Sink: sink},
	}
}

func newFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(buildScheme(t)).WithObjects(objs...).Build()
}

func TestBuildSinksWebhookOnly(t *testing.T) {
	sensor := sensorWith(sensoriov1.SinkConfig{
		Type:    sinkTypeWebhook,
		Webhook: &sensoriov1.WebhookSink{Enabled: true, URL: testWebhookURL},
		Datadog: &sensoriov1.DatadogSink{Enabled: true, APIKeySecret: "dd"},
	})
	c := newFakeClient(t)

	comp, err := BuildSinks(context.Background(), c, sensor)
	require.NoError(t, err)
	require.Equal(t, 1, comp.Len())
	_, ok := comp.sinks[0].(*WebhookSink)
	assert.True(t, ok, "type=webhook builds only the webhook sink")
}

func TestBuildSinksDatadogOnlyResolvesSecret(t *testing.T) {
	sensor := sensorWith(sensoriov1.SinkConfig{
		Type:    sinkTypeDatadog,
		Datadog: &sensoriov1.DatadogSink{Enabled: true, APIKeySecret: testDDSecret},
	})
	c := newFakeClient(t, ddSecret(testDDSecret, "default", datadogAPIKeyDataKey, "secret-key"))

	comp, err := BuildSinks(context.Background(), c, sensor)
	require.NoError(t, err)
	require.Equal(t, 1, comp.Len())
	dd, ok := comp.sinks[0].(*DatadogSink)
	require.True(t, ok, "type=datadog builds only the datadog sink")
	assert.Equal(t, "secret-key", dd.apiKey, "api key must be read from the named Secret")
}

func TestBuildSinksDatadogSingleEntryFallback(t *testing.T) {
	sensor := sensorWith(sensoriov1.SinkConfig{
		Type:    sinkTypeDatadog,
		Datadog: &sensoriov1.DatadogSink{Enabled: true, APIKeySecret: testDDSecret},
	})
	// Secret has a single, non-conventional key -> fallback picks it.
	c := newFakeClient(t, ddSecret(testDDSecret, "default", "token", "fallback-key"))

	comp, err := BuildSinks(context.Background(), c, sensor)
	require.NoError(t, err)
	require.Equal(t, 1, comp.Len())
	assert.Equal(t, "fallback-key", comp.sinks[0].(*DatadogSink).apiKey)
}

func TestBuildSinksAllActivatesConfigured(t *testing.T) {
	sensor := sensorWith(sensoriov1.SinkConfig{
		Type:    sinkTypeAll,
		Webhook: &sensoriov1.WebhookSink{Enabled: true, URL: testWebhookURL},
		Datadog: &sensoriov1.DatadogSink{Enabled: true, APIKeySecret: testDDSecret},
	})
	c := newFakeClient(t, ddSecret(testDDSecret, "default", datadogAPIKeyDataKey, "secret-key"))

	comp, err := BuildSinks(context.Background(), c, sensor)
	require.NoError(t, err)
	require.Equal(t, 2, comp.Len())
	_, okWebhook := comp.sinks[0].(*WebhookSink)
	_, okDatadog := comp.sinks[1].(*DatadogSink)
	assert.True(t, okWebhook, "fan-out order is webhook first")
	assert.True(t, okDatadog, "then datadog")
}

func TestBuildSinksDisabledFlagsExcluded(t *testing.T) {
	sensor := sensorWith(sensoriov1.SinkConfig{
		Type:    sinkTypeAll,
		Webhook: &sensoriov1.WebhookSink{Enabled: false, URL: testWebhookURL},
		Datadog: &sensoriov1.DatadogSink{Enabled: false, APIKeySecret: testDDSecret},
	})
	c := newFakeClient(t)

	comp, err := BuildSinks(context.Background(), c, sensor)
	require.NoError(t, err)
	assert.Equal(t, 0, comp.Len(), "disabled sinks are excluded")
}

func TestBuildSinksDatadogSecretMissingIsSoft(t *testing.T) {
	sensor := sensorWith(sensoriov1.SinkConfig{
		Type:    sinkTypeAll,
		Webhook: &sensoriov1.WebhookSink{Enabled: true, URL: testWebhookURL},
		Datadog: &sensoriov1.DatadogSink{Enabled: true, APIKeySecret: "absent"},
	})
	c := newFakeClient(t) // no secret

	comp, err := BuildSinks(context.Background(), c, sensor)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDatadogSecretMissing), "missing datadog secret is a soft error")
	// The webhook sink is still built despite the datadog soft failure.
	require.Equal(t, 1, comp.Len())
	_, ok := comp.sinks[0].(*WebhookSink)
	assert.True(t, ok)
}

func TestBuildSinksPrometheusIsPassive(t *testing.T) {
	sensor := sensorWith(sensoriov1.SinkConfig{
		Type:       sinkTypePrometheus,
		Prometheus: &sensoriov1.PrometheusSink{ServiceMonitor: true},
	})
	c := newFakeClient(t)

	comp, err := BuildSinks(context.Background(), c, sensor)
	require.NoError(t, err)
	assert.Equal(t, 0, comp.Len(), "prometheus is passive: no active sink in the composite")
}
