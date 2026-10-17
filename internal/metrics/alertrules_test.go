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
	"testing"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Shared literals used across the alert-rule builder tests.
const (
	sevWarning = "warning"
	exprGt1    = "x > 1"
)

func TestBuildPrometheusRuleRendersRules(t *testing.T) {
	sensor := &sensoriov1.DatabaseSensor{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-orders", Namespace: "default"},
		Spec: sensoriov1.DatabaseSensorSpec{
			Database: sensoriov1.DatabaseConfig{Host: "pg-svc.default"},
			Sink: sensoriov1.SinkConfig{
				Prometheus: &sensoriov1.PrometheusSink{
					AlertRules: []sensoriov1.AlertRule{
						{Name: "HighLag", Expr: "debezium_lag_seconds > 60", For: "5m", Severity: sevWarning},
						{Name: "NoEvents", Expr: "rate(debezium_events_total[5m]) == 0", For: "15m", Severity: "critical"},
					},
				},
			},
		},
	}

	rule := buildPrometheusRule(sensor)

	assert.Equal(t, "db-sensor-pg-orders-alerts", rule.Name)
	assert.Equal(t, "default", rule.Namespace)

	require.Len(t, rule.Spec.Groups, 1)
	g := rule.Spec.Groups[0]
	assert.Equal(t, "db-sensor.alerts", g.Name)
	require.Len(t, g.Rules, 2)

	r0 := g.Rules[0]
	assert.Equal(t, "HighLag", r0.Alert)
	assert.Equal(t, "debezium_lag_seconds > 60", r0.Expr.String())
	require.NotNil(t, r0.For)
	assert.Equal(t, monitoringv1.Duration("5m"), *r0.For)
	assert.Equal(t, "warning", r0.Labels[severityLabel])
	assert.NotEmpty(t, r0.Annotations["summary"])
	assert.NotEmpty(t, r0.Annotations["description"])

	r1 := g.Rules[1]
	assert.Equal(t, "NoEvents", r1.Alert)
	assert.Equal(t, "rate(debezium_events_total[5m]) == 0", r1.Expr.String())
	require.NotNil(t, r1.For)
	assert.Equal(t, monitoringv1.Duration("15m"), *r1.For)
	assert.Equal(t, "critical", r1.Labels[severityLabel])
}

func TestBuildPrometheusRuleNamespaceResolution(t *testing.T) {
	// Explicit sink namespace wins.
	explicit := &sensoriov1.DatabaseSensor{
		ObjectMeta: metav1.ObjectMeta{Name: "s1", Namespace: "sensor-ns"},
		Spec: sensoriov1.DatabaseSensorSpec{
			Sink: sensoriov1.SinkConfig{
				Prometheus: &sensoriov1.PrometheusSink{
					Namespace:  "monitoring",
					AlertRules: []sensoriov1.AlertRule{{Name: "A", Expr: exprGt1, For: "1m", Severity: sevWarning}},
				},
			},
		},
	}
	assert.Equal(t, "monitoring", buildPrometheusRule(explicit).Namespace)

	// Empty sink namespace defaults to the sensor namespace.
	defaulted := &sensoriov1.DatabaseSensor{
		ObjectMeta: metav1.ObjectMeta{Name: "s2", Namespace: "sensor-ns"},
		Spec: sensoriov1.DatabaseSensorSpec{
			Sink: sensoriov1.SinkConfig{
				Prometheus: &sensoriov1.PrometheusSink{
					AlertRules: []sensoriov1.AlertRule{{Name: "A", Expr: exprGt1, For: "1m", Severity: sevWarning}},
				},
			},
		},
	}
	assert.Equal(t, "sensor-ns", buildPrometheusRule(defaulted).Namespace)
}

func TestBuildPrometheusRuleSkipsInvalidRules(t *testing.T) {
	sensor := &sensoriov1.DatabaseSensor{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "default"},
		Spec: sensoriov1.DatabaseSensorSpec{
			Sink: sensoriov1.SinkConfig{
				Prometheus: &sensoriov1.PrometheusSink{
					AlertRules: []sensoriov1.AlertRule{
						{Name: "Valid", Expr: exprGt1, For: "1m", Severity: sevWarning},
						{Name: "MissingExpr", Expr: "", For: "1m", Severity: "critical"},
					},
				},
			},
		},
	}

	rule := buildPrometheusRule(sensor)
	require.Len(t, rule.Spec.Groups, 1)
	require.Len(t, rule.Spec.Groups[0].Rules, 1)
	assert.Equal(t, "Valid", rule.Spec.Groups[0].Rules[0].Alert)
}
