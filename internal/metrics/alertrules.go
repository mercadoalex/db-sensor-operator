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
	"context"
	"fmt"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
)

const (
	// alertsNamePrefix / alertsNameSuffix form the PrometheusRule name
	// db-sensor-<sensor.Name>-alerts (per tasks.md item 10). This is distinct
	// from the metricsResourceName used for the Service/ServiceMonitor.
	alertsNamePrefix = "db-sensor-"
	alertsNameSuffix = "-alerts"

	// alertRuleGroupName is the single RuleGroup that holds every rule from
	// spec.sink.prometheus.alertRules[]. The spec passes through a flat list
	// with no group field, so one group keeps buildPrometheusRule pure/total.
	alertRuleGroupName = "db-sensor.alerts"

	// severityLabel is the Prometheus label carrying the rule severity.
	severityLabel = "severity"
)

// alertRulesResourceName is the PrometheusRule name for a sensor:
// db-sensor-<sensor.Name>-alerts (tasks.md item 10).
func alertRulesResourceName(sensor *sensoriov1.DatabaseSensor) string {
	return alertsNamePrefix + sensor.Name + alertsNameSuffix
}

// alertRulesNamespace resolves where the PrometheusRule is created:
// spec.sink.prometheus.namespace, defaulting to the sensor's namespace when
// empty (identical to the ServiceMonitor rule).
func alertRulesNamespace(sensor *sensoriov1.DatabaseSensor) string {
	if prom := sensor.Spec.Sink.Prometheus; prom != nil && prom.Namespace != "" {
		return prom.Namespace
	}
	return sensor.Namespace
}

// buildPrometheusRule renders spec.sink.prometheus.alertRules[] into a typed
// PrometheusRule. It is pure (no client), so it is unit-testable in isolation
// and acts as the single source of truth for the object spec that
// EnsureAlertRules writes.
//
// Each AlertRule becomes a monitoringv1.Rule in the single "db-sensor.alerts"
// group: Alert=r.Name, Expr=r.Expr (as intstr), For=&Duration(r.For),
// Labels{severity: r.Severity}, plus synthesized summary/description annotations
// (the AlertRule spec models no annotation fields, and Task 10 forbids adding
// any, so they are derived from existing fields; a later task can enrich them).
//
// Invalid-rule handling: a rule missing any of Name/Expr/For/Severity is skipped
// (US-4.1 scopes the generated rule to those WITH expr/for/severity; the CRD
// already requires them, so this is defensive and keeps the builder total). The
// builder is pure and cannot log; EnsureAlertRules logs the skipped count by
// comparing input length to the built rule count.
func buildPrometheusRule(sensor *sensoriov1.DatabaseSensor) *monitoringv1.PrometheusRule {
	var rules []monitoringv1.Rule
	if prom := sensor.Spec.Sink.Prometheus; prom != nil {
		for _, r := range prom.AlertRules {
			if r.Name == "" || r.Expr == "" || r.For == "" || r.Severity == "" {
				// Skip incomplete rules (defensive; CRD validation normally
				// prevents this). EnsureAlertRules reports the skipped count.
				continue
			}
			// Fresh variable per iteration so each &dur is a distinct address
			// (For is *monitoringv1.Duration, and Duration is a string type).
			dur := monitoringv1.Duration(r.For)
			rules = append(rules, monitoringv1.Rule{
				Alert: r.Name,
				Expr:  intstr.FromString(r.Expr),
				For:   &dur,
				Labels: map[string]string{
					severityLabel: r.Severity,
				},
				Annotations: map[string]string{
					"summary": r.Name,
					"description": fmt.Sprintf(
						"Alert %s on database %s: %s for %s",
						r.Name, sensor.Spec.Database.Host, r.Expr, r.For,
					),
				},
			})
		}
	}

	return &monitoringv1.PrometheusRule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      alertRulesResourceName(sensor),
			Namespace: alertRulesNamespace(sensor),
		},
		Spec: monitoringv1.PrometheusRuleSpec{
			Groups: []monitoringv1.RuleGroup{{
				Name:  alertRuleGroupName,
				Rules: rules,
			}},
		},
	}
}

// EnsureAlertRules creates or updates the per-sensor PrometheusRule when the
// Prometheus sink declares alert rules (spec.sink.prometheus.alertRules, US-4.1);
// otherwise it is a no-op. Like EnsureServiceMonitor, it does NOT delete an
// existing PrometheusRule when the list becomes empty.
//
// The rule is created in spec.sink.prometheus.namespace, defaulting to the
// sensor's namespace.
//
// Owner-ref decision (mirrors EnsureServiceMonitor, DECISION 2 in task5-plan):
// Kubernetes forbids cross-namespace owner references, so the controller owner
// ref is set only when the PrometheusRule lives in the sensor's namespace. For a
// cross-namespace rule the object is instead stamped with traceability labels
// and the fact that its GC is label-based is logged. Idempotent via
// CreateOrUpdate.
func EnsureAlertRules(ctx context.Context, c client.Client, scheme *runtime.Scheme, sensor *sensoriov1.DatabaseSensor) error {
	log := logf.FromContext(ctx)

	// US-4.1 is opt-in: nothing to do unless the Prometheus sink declares rules.
	prom := sensor.Spec.Sink.Prometheus
	if prom == nil || len(prom.AlertRules) == 0 {
		return nil
	}

	ns := alertRulesNamespace(sensor)
	sameNamespace := ns == sensor.Namespace

	desired := buildPrometheusRule(sensor)
	built := len(desired.Spec.Groups[0].Rules)
	if skipped := len(prom.AlertRules) - built; skipped > 0 {
		log.Info("Skipped invalid alert rules missing required fields",
			"skipped", skipped, "sensor", sensor.Name)
	}

	rule := &monitoringv1.PrometheusRule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desired.Name,
			Namespace: ns,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, c, rule, func() error {
		rule.Spec = desired.Spec
		if sameNamespace {
			return controllerutil.SetControllerReference(sensor, rule, scheme)
		}
		// Cross-namespace: no owner ref is possible; stamp traceability labels so
		// the owning sensor is discoverable and label-based cleanup is feasible
		// for a later finalizer task.
		if rule.Labels == nil {
			rule.Labels = map[string]string{}
		}
		rule.Labels[labelSensorName] = sensor.Name
		rule.Labels[labelSensorNS] = sensor.Namespace
		return nil
	})
	if err != nil {
		return fmt.Errorf("ensuring PrometheusRule %q: %w", rule.Name, err)
	}

	if sameNamespace {
		log.Info("Ensured PrometheusRule", "name", rule.Name, "namespace", rule.Namespace)
	} else {
		log.Info("Ensured cross-namespace PrometheusRule; GC is label-based, not owner-ref-based",
			"name", rule.Name, "namespace", rule.Namespace, "sensor", sensor.Name, "sensorNamespace", sensor.Namespace)
	}
	return nil
}
