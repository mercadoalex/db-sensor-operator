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

// Package metrics reconciles the observability scrape path for a DatabaseSensor:
// a per-sensor metrics Service (US-3.3) and, when opted in, a ServiceMonitor
// (US-3.2) that points Prometheus at the operator's /metrics endpoint on port
// 8080 (US-3.1; the endpoint handler itself lands in a later task).
//
// Step 0 dependency decision (resolved during planning, see
// .agents/tasks/task5-plan.md): unlike the Debezium API, the Prometheus Operator
// publishes an importable Go module, so ServiceMonitor is modelled with the typed
// monitoringv1.ServiceMonitor rather than unstructured.
//
// These are stateless package-level functions (no reconciler field), mirroring
// internal/deployer: injecting them would add a dependency without a payoff.
//
// NOTE: the metrics are operator-wide — a single /metrics server runs in the
// manager pod on :8080 (wired in a later task). The Service created here is
// per-sensor: a stable, labeled scrape target that selects the manager pod, so a
// DatabaseSensor's observability path is created and garbage-collected with it.
package metrics

import (
	"context"
	"fmt"
	"maps"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
)

const (
	// metricsPortName is the named port on both the Service and the
	// ServiceMonitor endpoint (plaintext :8080, distinct from the scaffold's
	// :8443 secure metrics Service/monitor which Task 5 leaves untouched).
	metricsPortName = "metrics"

	// metricsPort is the operator's US-3 /metrics port.
	metricsPort int32 = 8080

	// metricsPath is the HTTP path the ServiceMonitor scrapes.
	metricsPath = "/metrics"

	// labelName / labelManagedBy are the shared labels. labelName must match the
	// manager pod labels (config/manager/manager.yaml) so the Service selector
	// resolves the controller-manager pods.
	labelName = "app.kubernetes.io/name"
	appName   = "db-sensor-operator"

	// labelControlPlane targets the manager pod alongside labelName.
	labelControlPlane = "control-plane"
	controllerManager = "controller-manager"
	labelSensorName   = "sensor.io/sensor-name"
	labelSensorNS     = "sensor.io/sensor-namespace"
	metricsNameSuffix = "-metrics"
)

// metricsResourceName is the shared name of the per-sensor Service and
// ServiceMonitor: <sensor.Name>-metrics (1:1 with the sensor, matching the
// child-naming convention used for the DebeziumServer).
func metricsResourceName(sensor *sensoriov1.DatabaseSensor) string {
	return sensor.Name + metricsNameSuffix
}

// serviceSelector targets the operator's controller-manager pods. The metrics
// are served operator-wide by the manager, so every per-sensor Service selects
// the same pods (verified labels: config/manager/manager.yaml).
func serviceSelector() map[string]string {
	return map[string]string{
		labelControlPlane: controllerManager,
		labelName:         appName,
	}
}

// smSelectorLabels are the labels stamped on the Service and matched by the
// ServiceMonitor's selector so the monitor resolves exactly this sensor's
// Service.
func smSelectorLabels(sensor *sensoriov1.DatabaseSensor) map[string]string {
	return map[string]string{
		labelName:       appName,
		labelSensorName: sensor.Name,
	}
}

// EnsureService creates or updates the per-sensor metrics Service (US-3.3). The
// Service lives in the sensor's namespace, exposes the named port "metrics" on
// 8080, selects the manager pods, and is owner-referenced to the DatabaseSensor
// so it is garbage-collected with it (ADR-06, US-1.4). Idempotent via
// CreateOrUpdate.
func EnsureService(ctx context.Context, c client.Client, scheme *runtime.Scheme, sensor *sensoriov1.DatabaseSensor) error {
	log := logf.FromContext(ctx)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      metricsResourceName(sensor),
			Namespace: sensor.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, c, svc, func() error {
		if svc.Labels == nil {
			svc.Labels = map[string]string{}
		}
		maps.Copy(svc.Labels, smSelectorLabels(sensor))
		svc.Spec.Selector = serviceSelector()
		svc.Spec.Ports = []corev1.ServicePort{{
			Name:       metricsPortName,
			Port:       metricsPort,
			TargetPort: intstr.FromInt32(metricsPort),
			Protocol:   corev1.ProtocolTCP,
		}}
		return controllerutil.SetControllerReference(sensor, svc, scheme)
	})
	if err != nil {
		return fmt.Errorf("ensuring metrics Service %q: %w", svc.Name, err)
	}
	log.Info("Ensured metrics Service", "name", svc.Name, "namespace", svc.Namespace)
	return nil
}

// EnsureServiceMonitor creates or updates the per-sensor ServiceMonitor when the
// Prometheus sink opts in (spec.sink.prometheus.serviceMonitor == true, US-3.2);
// otherwise it is a no-op. The monitor is created in
// spec.sink.prometheus.namespace, defaulting to the sensor's namespace.
//
// Owner-ref decision (DECISION 2 in task5-plan): Kubernetes forbids
// cross-namespace owner references, so the controller owner ref is set only when
// the ServiceMonitor lives in the sensor's namespace. For a cross-namespace
// monitor the object is instead stamped with traceability labels and the fact
// that its GC is label-based (not owner-ref-based) is logged. Idempotent via
// CreateOrUpdate.
func EnsureServiceMonitor(ctx context.Context, c client.Client, scheme *runtime.Scheme, sensor *sensoriov1.DatabaseSensor) error {
	log := logf.FromContext(ctx)

	// US-3.2 is opt-in: nothing to do unless the Prometheus sink requests it.
	prom := sensor.Spec.Sink.Prometheus
	if prom == nil || !prom.ServiceMonitor {
		return nil
	}

	ns := prom.Namespace
	if ns == "" {
		ns = sensor.Namespace
	}
	sameNamespace := ns == sensor.Namespace

	sm := &monitoringv1.ServiceMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      metricsResourceName(sensor),
			Namespace: ns,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, c, sm, func() error {
		sm.Spec.Selector = metav1.LabelSelector{MatchLabels: smSelectorLabels(sensor)}
		sm.Spec.Endpoints = []monitoringv1.Endpoint{{
			Port: metricsPortName,
			Path: metricsPath,
		}}
		if sameNamespace {
			return controllerutil.SetControllerReference(sensor, sm, scheme)
		}
		// Cross-namespace: no owner ref is possible; stamp traceability labels so
		// the owning sensor is discoverable and label-based cleanup is feasible
		// for a later finalizer task.
		if sm.Labels == nil {
			sm.Labels = map[string]string{}
		}
		sm.Labels[labelSensorName] = sensor.Name
		sm.Labels[labelSensorNS] = sensor.Namespace
		return nil
	})
	if err != nil {
		return fmt.Errorf("ensuring ServiceMonitor %q: %w", sm.Name, err)
	}

	if sameNamespace {
		log.Info("Ensured ServiceMonitor", "name", sm.Name, "namespace", sm.Namespace)
	} else {
		log.Info("Ensured cross-namespace ServiceMonitor; GC is label-based, not owner-ref-based",
			"name", sm.Name, "namespace", sm.Namespace, "sensor", sensor.Name, "sensorNamespace", sensor.Namespace)
	}
	return nil
}
