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

package controller

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"

	"github.com/prometheus/client_golang/prometheus"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
	"github.com/mercadoalex/db-sensor-operator/internal/deployer"
	"github.com/mercadoalex/db-sensor-operator/internal/events"
	"github.com/mercadoalex/db-sensor-operator/internal/health"
	"github.com/mercadoalex/db-sensor-operator/internal/metrics"
)

var _ = Describe("DatabaseSensor Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			resourceName      = "test-resource"
			resourceNamespace = "default"
		)

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: resourceNamespace,
		}
		const secretName = "db-creds"
		const secretPasswordKey = "password"
		const secretPasswordVal = "s3cret"
		const tableOrders = "orders"

		databasesensor := &sensoriov1.DatabaseSensor{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind DatabaseSensor")
			err := k8sClient.Get(ctx, typeNamespacedName, databasesensor)
			if err != nil && errors.IsNotFound(err) {
				resource := &sensoriov1.DatabaseSensor{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: resourceNamespace,
					},
					Spec: sensoriov1.DatabaseSensorSpec{
						Database: sensoriov1.DatabaseConfig{
							Type:           "postgresql",
							Host:           "postgres-svc.default",
							Port:           5432,
							User:           "debezium",
							PasswordSecret: secretName,
						},
						Tables: []string{tableOrders},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &sensoriov1.DatabaseSensor{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance DatabaseSensor")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())

			By("Cleanup the password Secret if present")
			secret := &corev1.Secret{}
			secretKey := types.NamespacedName{Name: secretName, Namespace: resourceNamespace}
			if err := k8sClient.Get(ctx, secretKey, secret); err == nil {
				Expect(k8sClient.Delete(ctx, secret)).To(Succeed())
			}

			By("Cleanup the DebeziumServer child if present")
			// envtest has no garbage collector, so owner-reference GC does not run;
			// delete the child explicitly to isolate specs.
			child := &unstructured.Unstructured{}
			child.SetGroupVersionKind(deployer.DebeziumServerGVK)
			if err := k8sClient.Get(ctx, typeNamespacedName, child); err == nil {
				Expect(k8sClient.Delete(ctx, child)).To(Succeed())
			}

			By("Cleanup the metrics Service and ServiceMonitor if present")
			// Same reason as the child above: no GC in envtest, so delete the
			// owned Task 5 resources explicitly to isolate specs.
			metricsKey := types.NamespacedName{Name: resourceName + "-metrics", Namespace: resourceNamespace}
			svc := &corev1.Service{}
			if err := k8sClient.Get(ctx, metricsKey, svc); err == nil {
				Expect(k8sClient.Delete(ctx, svc)).To(Succeed())
			}
			sm := &monitoringv1.ServiceMonitor{}
			if err := k8sClient.Get(ctx, metricsKey, sm); err == nil {
				Expect(k8sClient.Delete(ctx, sm)).To(Succeed())
			}

			By("Cleanup the PrometheusRule if present")
			// Same reason: no GC in envtest, so delete the owned Task 10
			// PrometheusRule explicitly to isolate specs.
			alertsKey := types.NamespacedName{Name: "db-sensor-" + resourceName + "-alerts", Namespace: resourceNamespace}
			pr := &monitoringv1.PrometheusRule{}
			if err := k8sClient.Get(ctx, alertsKey, pr); err == nil {
				Expect(k8sClient.Delete(ctx, pr)).To(Succeed())
			}
		})

		It("should create an owned DebeziumServer when the Secret exists (US-1.1/1.2)", func() {
			By("Creating the referenced password Secret")
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: resourceNamespace},
				StringData: map[string]string{secretPasswordKey: secretPasswordVal},
			})).To(Succeed())

			By("Reconciling the created resource")
			controllerReconciler := &DatabaseSensorReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("Deriving a Pending phase when the child exists but its pod is not running yet")
			// The envtest child has no fabricated status, so the health checker
			// reads it as pod-not-ready (row 2 of the Task 9 truth table):
			// DebeziumAvailable=False/PodNotReady, Ready=Unknown/PodNotReady,
			// Phase=Pending.
			Eventually(func(g Gomega) {
				fetched := &sensoriov1.DatabaseSensor{}
				g.Expect(k8sClient.Get(ctx, typeNamespacedName, fetched)).To(Succeed())
				g.Expect(fetched.Status.Phase).To(Equal("Pending"))
				g.Expect(fetched.Status.DebeziumServer).To(Equal(resourceName))
				ready := meta.FindStatusCondition(fetched.Status.Conditions, "Ready")
				g.Expect(ready).NotTo(BeNil())
				g.Expect(ready.Status).To(Equal(metav1.ConditionUnknown))
				g.Expect(ready.Reason).To(Equal("PodNotReady"))
				dbAvail := meta.FindStatusCondition(fetched.Status.Conditions, "DebeziumAvailable")
				g.Expect(dbAvail).NotTo(BeNil())
				g.Expect(dbAvail.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(dbAvail.Reason).To(Equal("PodNotReady"))
			}).Should(Succeed())

			By("Finding the DebeziumServer child with an owner reference to the sensor")
			child := &unstructured.Unstructured{}
			child.SetGroupVersionKind(deployer.DebeziumServerGVK)
			Expect(k8sClient.Get(ctx, typeNamespacedName, child)).To(Succeed())
			owners := child.GetOwnerReferences()
			Expect(owners).To(HaveLen(1))
			Expect(owners[0].Name).To(Equal(resourceName))
			Expect(owners[0].Kind).To(Equal("DatabaseSensor"))

			By("Reconciling a second time converges without duplicating the child (idempotency)")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(deployer.DebeziumServerGVK)
			Expect(k8sClient.List(ctx, list)).To(Succeed())
			Expect(list.Items).To(HaveLen(1))
		})

		It("should keep the inspection facts and plan when it refreshes status.snapshot", func() {
			By("Creating the referenced password Secret")
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: resourceNamespace},
				StringData: map[string]string{secretPasswordKey: secretPasswordVal},
			})).To(Succeed())
			controllerReconciler := &DatabaseSensorReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Recording inspection facts and a plan, as the inspection step will")
			sensor := &sensoriov1.DatabaseSensor{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, sensor)).To(Succeed())
			Expect(sensor.Status.Snapshot).NotTo(BeNil())
			sensor.Status.Snapshot.Inspection = &sensoriov1.InspectionStatus{
				FactsVersion: 1,
				Tables:       []sensoriov1.TableInspection{{Name: tableOrders, Found: true, EstimateReliable: true}},
			}
			sensor.Status.Snapshot.Plan = &sensoriov1.SnapshotPlan{
				Tables: []sensoriov1.TablePlan{{Name: tableOrders, Backfill: "stream-only", Reason: "small"}},
			}
			Expect(k8sClient.Status().Update(ctx, sensor)).To(Succeed())

			By("Reconciling again, which rewrites the start strategy")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Finding the facts and the plan untouched and the strategy still reported")
			fetched := &sensoriov1.DatabaseSensor{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, fetched)).To(Succeed())
			Expect(fetched.Status.Snapshot.Strategy).To(Equal("no-data"))
			Expect(fetched.Status.Snapshot.Inspection).NotTo(BeNil(), "the reconcile must not drop the inspection facts")
			Expect(fetched.Status.Snapshot.Inspection.Tables).To(HaveLen(1))
			Expect(fetched.Status.Snapshot.Plan).NotTo(BeNil(), "the reconcile must not drop the plan")
		})
		It("should derive Running when the child pod is running and events are recent (US-5.2)", func() {
			By("Creating the referenced password Secret")
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: resourceNamespace},
				StringData: map[string]string{secretPasswordKey: secretPasswordVal},
			})).To(Succeed())

			By("Reconciling once to create the child DebeziumServer")
			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			lt := health.NewLagTrackerWithClock(func() time.Time { return now })
			lt.OnEvent(now.Add(-5 * time.Second).UnixMilli())
			controllerReconciler := &DatabaseSensorReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Lag:    lt,
				Health: health.NewCheckerWithClock(k8sClient, lt, func() time.Time { return now }),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Fabricating a running pod status on the child via the status subresource")
			child := &unstructured.Unstructured{}
			child.SetGroupVersionKind(deployer.DebeziumServerGVK)
			Expect(k8sClient.Get(ctx, typeNamespacedName, child)).To(Succeed())
			Expect(unstructured.SetNestedField(child.Object, "Running", "status", "phase")).To(Succeed())
			Expect(k8sClient.Status().Update(ctx, child)).To(Succeed())

			By("Reconciling again so the checker observes the running pod + recent event")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Deriving Phase=Running with DebeziumAvailable=True and Ready=True")
			Eventually(func(g Gomega) {
				fetched := &sensoriov1.DatabaseSensor{}
				g.Expect(k8sClient.Get(ctx, typeNamespacedName, fetched)).To(Succeed())
				g.Expect(fetched.Status.Phase).To(Equal("Running"))
				dbAvail := meta.FindStatusCondition(fetched.Status.Conditions, "DebeziumAvailable")
				g.Expect(dbAvail).NotTo(BeNil())
				g.Expect(dbAvail.Status).To(Equal(metav1.ConditionTrue))
				ready := meta.FindStatusCondition(fetched.Status.Conditions, "Ready")
				g.Expect(ready).NotTo(BeNil())
				g.Expect(ready.Status).To(Equal(metav1.ConditionTrue))
				g.Expect(fetched.Status.LastEvent).NotTo(BeNil())
			}).Should(Succeed())
		})

		It("should converge the full US-5.5 status surface on a healthy sensor", func() {
			By("Creating the referenced password Secret")
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: resourceNamespace},
				StringData: map[string]string{secretPasswordKey: secretPasswordVal},
			})).To(Succeed())

			By("Wiring an Exporter and LagTracker on a shared fixed clock and feeding synthetic events")
			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			clock := func() time.Time { return now }
			lt := health.NewLagTrackerWithClock(clock)
			exporter := metrics.NewExporterWithClock(prometheus.NewRegistry(), clock)
			// Feed a few recent data-change events for the sensor's db host so the
			// rolling rate is > 0 and the lag is small/healthy. The db label must
			// equal spec.database.host, which the accessor keys off.
			const dbHost = "postgres-svc.default"
			eventTs := now.Add(-5 * time.Second).UnixMilli()
			for range 4 {
				exporter.OnEvent(events.CDCEvent{Op: "c", DB: dbHost, Table: tableOrders, TsMs: eventTs})
			}
			lt.OnEvent(eventTs)

			controllerReconciler := &DatabaseSensorReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Lag:      lt,
				Exporter: exporter,
				Health:   health.NewCheckerWithClock(k8sClient, lt, clock),
			}

			By("Reconciling once to create the child DebeziumServer")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Fabricating a running pod status on the child")
			child := &unstructured.Unstructured{}
			child.SetGroupVersionKind(deployer.DebeziumServerGVK)
			Expect(k8sClient.Get(ctx, typeNamespacedName, child)).To(Succeed())
			Expect(unstructured.SetNestedField(child.Object, "Running", "status", "phase")).To(Succeed())
			Expect(k8sClient.Status().Update(ctx, child)).To(Succeed())

			By("Reconciling again so the health path writes the full status surface")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Asserting every US-5.5 field is populated on a healthy sensor")
			Eventually(func(g Gomega) {
				fetched := &sensoriov1.DatabaseSensor{}
				g.Expect(k8sClient.Get(ctx, typeNamespacedName, fetched)).To(Succeed())
				// phase (derived)
				g.Expect(fetched.Status.Phase).To(Equal("Running"))
				// lag: human-readable duration, non-empty, not a bogus value.
				g.Expect(fetched.Status.Lag).NotTo(BeEmpty())
				g.Expect(fetched.Status.Lag).To(Equal((5 * time.Second).String()))
				// eventsPerMin: reflects the fed events (> 0).
				g.Expect(fetched.Status.EventsPerMin).To(BeNumerically(">", 0))
				g.Expect(fetched.Status.EventsPerMin).To(Equal(4.0))
				// lastEvent: a real timestamp, set.
				g.Expect(fetched.Status.LastEvent).NotTo(BeNil())
				// debeziumServer: populated once the child is ensured.
				g.Expect(fetched.Status.DebeziumServer).To(Equal(resourceName))
				// Ready condition present and True; its message (the Last Error
				// printcolumn source) is the healthy message, not an error.
				ready := meta.FindStatusCondition(fetched.Status.Conditions, "Ready")
				g.Expect(ready).NotTo(BeNil())
				g.Expect(ready.Status).To(Equal(metav1.ConditionTrue))
				g.Expect(ready.Reason).To(Equal("HealthyEndToEnd"))
				g.Expect(ready.Message).To(Equal("Sensor is healthy end-to-end"))
			}).Should(Succeed())
		})

		It("should keep the status stable across a repeated reconcile (idempotency)", func() {
			By("Creating the referenced password Secret")
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: resourceNamespace},
				StringData: map[string]string{secretPasswordKey: secretPasswordVal},
			})).To(Succeed())

			By("Wiring a fixed-clock LagTracker + Checker with a recent event so the sensor converges to a stable phase")
			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			clock := func() time.Time { return now }
			lt := health.NewLagTrackerWithClock(clock)
			lt.OnEvent(now.Add(-5 * time.Second).UnixMilli())
			controllerReconciler := &DatabaseSensorReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Lag:    lt,
				Health: health.NewCheckerWithClock(k8sClient, lt, clock),
			}

			By("Reconciling once to create the child DebeziumServer")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Fabricating a running pod status so the sensor converges to a steady, non-Pending phase")
			child := &unstructured.Unstructured{}
			child.SetGroupVersionKind(deployer.DebeziumServerGVK)
			Expect(k8sClient.Get(ctx, typeNamespacedName, child)).To(Succeed())
			Expect(unstructured.SetNestedField(child.Object, "Running", "status", "phase")).To(Succeed())
			Expect(k8sClient.Status().Update(ctx, child)).To(Succeed())

			By("Reconciling a second time so the health path writes the steady-state status")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Capturing the steady-state phase and each condition's LastTransitionTime")
			steady := &sensoriov1.DatabaseSensor{}
			var steadyPhase string
			type condSnap struct {
				status     metav1.ConditionStatus
				reason     string
				transition metav1.Time
			}
			snaps := map[string]condSnap{}
			condTypes := []string{"DebeziumAvailable", "ReceivingEvents", "Ready"}
			Eventually(func(g Gomega) {
				g.Expect(k8sClient.Get(ctx, typeNamespacedName, steady)).To(Succeed())
				// The sensor must have reached a stable non-Pending phase before
				// we snapshot it, otherwise the "no churn" assertion is vacuous.
				g.Expect(steady.Status.Phase).To(Equal("Running"))
				for _, ct := range condTypes {
					c := meta.FindStatusCondition(steady.Status.Conditions, ct)
					g.Expect(c).NotTo(BeNil())
				}
			}).Should(Succeed())
			steadyPhase = steady.Status.Phase
			for _, ct := range condTypes {
				c := meta.FindStatusCondition(steady.Status.Conditions, ct)
				snaps[ct] = condSnap{status: c.Status, reason: c.Reason, transition: c.LastTransitionTime}
			}

			By("Reconciling a third time with identical inputs")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Asserting the phase is unchanged and no condition churned (same Status/Reason/LastTransitionTime)")
			// meta.SetStatusCondition only bumps LastTransitionTime when a
			// condition's Status actually changes, so a second reconcile with
			// unchanged inputs must leave every condition's transition time
			// steady — this is the DESIGN.MD "second reconcile → same result,
			// no status thrash" contract.
			after := &sensoriov1.DatabaseSensor{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, after)).To(Succeed())
			Expect(after.Status.Phase).To(Equal(steadyPhase))
			for _, ct := range condTypes {
				c := meta.FindStatusCondition(after.Status.Conditions, ct)
				Expect(c).NotTo(BeNil())
				prev := snaps[ct]
				Expect(c.Status).To(Equal(prev.status), "condition %s Status churned", ct)
				Expect(c.Reason).To(Equal(prev.reason), "condition %s Reason churned", ct)
				Expect(c.LastTransitionTime).To(Equal(prev.transition), "condition %s LastTransitionTime churned", ct)
			}

			By("Asserting the DebeziumServer child was not duplicated")
			list := &unstructured.UnstructuredList{}
			list.SetGroupVersionKind(deployer.DebeziumServerGVK)
			Expect(k8sClient.List(ctx, list, client.InNamespace(resourceNamespace))).To(Succeed())
			Expect(list.Items).To(HaveLen(1))
		})

		It("should converge the US-5.5 surface to Degraded on warning-band lag", func() {
			By("Creating the referenced password Secret")
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: resourceNamespace},
				StringData: map[string]string{secretPasswordKey: secretPasswordVal},
			})).To(Succeed())

			By("Wiring an Exporter and LagTracker with a last event ~45s ago (warning band)")
			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			clock := func() time.Time { return now }
			lt := health.NewLagTrackerWithClock(clock)
			exporter := metrics.NewExporterWithClock(prometheus.NewRegistry(), clock)
			const dbHost = "postgres-svc.default"
			eventTs := now.Add(-45 * time.Second).UnixMilli()
			exporter.OnEvent(events.CDCEvent{Op: "c", DB: dbHost, Table: tableOrders, TsMs: eventTs})
			lt.OnEvent(eventTs)

			controllerReconciler := &DatabaseSensorReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Lag:      lt,
				Exporter: exporter,
				Health:   health.NewCheckerWithClock(k8sClient, lt, clock),
			}

			By("Reconciling once to create the child DebeziumServer")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Fabricating a running pod status so lag drives the phase")
			child := &unstructured.Unstructured{}
			child.SetGroupVersionKind(deployer.DebeziumServerGVK)
			Expect(k8sClient.Get(ctx, typeNamespacedName, child)).To(Succeed())
			Expect(unstructured.SetNestedField(child.Object, "Running", "status", "phase")).To(Succeed())
			Expect(k8sClient.Status().Update(ctx, child)).To(Succeed())

			By("Reconciling again so the checker observes the warning-band lag")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Asserting Phase=Degraded with Ready=False/LagAboveWarning and the US-5.5 surface")
			Eventually(func(g Gomega) {
				fetched := &sensoriov1.DatabaseSensor{}
				g.Expect(k8sClient.Get(ctx, typeNamespacedName, fetched)).To(Succeed())
				g.Expect(fetched.Status.Phase).To(Equal("Degraded"))
				ready := meta.FindStatusCondition(fetched.Status.Conditions, "Ready")
				g.Expect(ready).NotTo(BeNil())
				g.Expect(ready.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(ready.Reason).To(Equal("LagAboveWarning"))
				// lag is a non-empty human-readable duration.
				g.Expect(fetched.Status.Lag).NotTo(BeEmpty())
				g.Expect(fetched.Status.Lag).To(Equal((45 * time.Second).String()))
				// eventsPerMin reflects the fed event when an Exporter is wired.
				g.Expect(fetched.Status.EventsPerMin).To(Equal(1.0))
				g.Expect(fetched.Status.LastEvent).NotTo(BeNil())
			}).Should(Succeed())
		})

		It("should honor per-sensor spec.thresholds so a tight lagCrit escalates Degraded to Failed", func() {
			By("Creating the referenced password Secret")
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: resourceNamespace},
				StringData: map[string]string{secretPasswordKey: secretPasswordVal},
			})).To(Succeed())

			By("Tightening spec.thresholds.lagCrit to 10s on the sensor")
			sensor := &sensoriov1.DatabaseSensor{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, sensor)).To(Succeed())
			sensor.Spec.Thresholds = &sensoriov1.ThresholdConfig{LagCrit: "10s"}
			Expect(k8sClient.Update(ctx, sensor)).To(Succeed())

			By("Wiring a LagTracker with a last event ~45s ago (warning band at defaults)")
			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			clock := func() time.Time { return now }
			lt := health.NewLagTrackerWithClock(clock)
			lt.OnEvent(now.Add(-45 * time.Second).UnixMilli())
			controllerReconciler := &DatabaseSensorReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Lag:    lt,
				Health: health.NewCheckerWithClock(k8sClient, lt, clock),
			}

			By("Reconciling once to create the child DebeziumServer")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Fabricating a running pod status so lag drives the phase")
			child := &unstructured.Unstructured{}
			child.SetGroupVersionKind(deployer.DebeziumServerGVK)
			Expect(k8sClient.Get(ctx, typeNamespacedName, child)).To(Succeed())
			Expect(unstructured.SetNestedField(child.Object, "Running", "status", "phase")).To(Succeed())
			Expect(k8sClient.Status().Update(ctx, child)).To(Succeed())

			By("Reconciling again so the checker observes the lag against the tight lagCrit")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Asserting the 45s lag is now Failed/LagAboveCritical (would be Degraded at defaults)")
			Eventually(func(g Gomega) {
				fetched := &sensoriov1.DatabaseSensor{}
				g.Expect(k8sClient.Get(ctx, typeNamespacedName, fetched)).To(Succeed())
				g.Expect(fetched.Status.Phase).To(Equal("Failed"))
				ready := meta.FindStatusCondition(fetched.Status.Conditions, "Ready")
				g.Expect(ready).NotTo(BeNil())
				g.Expect(ready.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(ready.Reason).To(Equal("LagAboveCritical"))
			}).Should(Succeed())
		})

		It("should surface the Last Error source empty when healthy and populated when Failed (US-5.5)", func() {
			By("Reconciling without the Secret so the sensor fails with SecretNotFound")
			controllerReconciler := &DatabaseSensorReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("The Ready condition message (Last Error printcolumn source) names the failure")
			Eventually(func(g Gomega) {
				fetched := &sensoriov1.DatabaseSensor{}
				g.Expect(k8sClient.Get(ctx, typeNamespacedName, fetched)).To(Succeed())
				g.Expect(fetched.Status.Phase).To(Equal("Failed"))
				ready := meta.FindStatusCondition(fetched.Status.Conditions, "Ready")
				g.Expect(ready).NotTo(BeNil())
				g.Expect(ready.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(ready.Reason).To(Equal("SecretNotFound"))
				// Last Error source is populated with the secret name when Failed.
				g.Expect(ready.Message).NotTo(BeEmpty())
				g.Expect(ready.Message).To(ContainSubstring(secretName))
			}).Should(Succeed())

			By("Creating the Secret and fabricating a healthy pod so the sensor recovers")
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: resourceNamespace},
				StringData: map[string]string{secretPasswordKey: secretPasswordVal},
			})).To(Succeed())

			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			clock := func() time.Time { return now }
			lt := health.NewLagTrackerWithClock(clock)
			lt.OnEvent(now.Add(-5 * time.Second).UnixMilli())
			healthy := &DatabaseSensorReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Lag:    lt,
				Health: health.NewCheckerWithClock(k8sClient, lt, clock),
			}
			_, err = healthy.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			child := &unstructured.Unstructured{}
			child.SetGroupVersionKind(deployer.DebeziumServerGVK)
			Expect(k8sClient.Get(ctx, typeNamespacedName, child)).To(Succeed())
			Expect(unstructured.SetNestedField(child.Object, "Running", "status", "phase")).To(Succeed())
			Expect(k8sClient.Status().Update(ctx, child)).To(Succeed())

			_, err = healthy.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("The Ready condition message is the healthy message (no error) once recovered")
			Eventually(func(g Gomega) {
				fetched := &sensoriov1.DatabaseSensor{}
				g.Expect(k8sClient.Get(ctx, typeNamespacedName, fetched)).To(Succeed())
				g.Expect(fetched.Status.Phase).To(Equal("Running"))
				ready := meta.FindStatusCondition(fetched.Status.Conditions, "Ready")
				g.Expect(ready).NotTo(BeNil())
				g.Expect(ready.Status).To(Equal(metav1.ConditionTrue))
				g.Expect(ready.Message).NotTo(ContainSubstring(secretName))
				g.Expect(ready.Message).To(Equal("Sensor is healthy end-to-end"))
			}).Should(Succeed())
		})

		It("should derive Failed with the termination message when the child pod crash-loops (US-7.2)", func() {
			By("Creating the referenced password Secret")
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: resourceNamespace},
				StringData: map[string]string{secretPasswordKey: secretPasswordVal},
			})).To(Succeed())

			controllerReconciler := &DatabaseSensorReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			By("Reconciling once to create the child DebeziumServer")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Fabricating a crash-looping pod status with a termination message")
			child := &unstructured.Unstructured{}
			child.SetGroupVersionKind(deployer.DebeziumServerGVK)
			Expect(k8sClient.Get(ctx, typeNamespacedName, child)).To(Succeed())
			Expect(unstructured.SetNestedField(child.Object, "CrashLoopBackOff", "status", "phase")).To(Succeed())
			Expect(unstructured.SetNestedField(child.Object, "connector failed: auth error", "status", "lastTerminationMessage")).To(Succeed())
			Expect(k8sClient.Status().Update(ctx, child)).To(Succeed())

			By("Reconciling again so the checker observes the crash-loop")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Deriving Phase=Failed with Ready=False/PodCrashLoopBackOff and the termination message")
			Eventually(func(g Gomega) {
				fetched := &sensoriov1.DatabaseSensor{}
				g.Expect(k8sClient.Get(ctx, typeNamespacedName, fetched)).To(Succeed())
				g.Expect(fetched.Status.Phase).To(Equal("Failed"))
				ready := meta.FindStatusCondition(fetched.Status.Conditions, "Ready")
				g.Expect(ready).NotTo(BeNil())
				g.Expect(ready.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(ready.Reason).To(Equal("PodCrashLoopBackOff"))
				dbAvail := meta.FindStatusCondition(fetched.Status.Conditions, "DebeziumAvailable")
				g.Expect(dbAvail).NotTo(BeNil())
				g.Expect(dbAvail.Message).To(ContainSubstring("auth error"))
			}).Should(Succeed())
		})

		It("should derive Failed when no events arrive for over the fail timeout (US-5.4)", func() {
			By("Creating the referenced password Secret")
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: resourceNamespace},
				StringData: map[string]string{secretPasswordKey: secretPasswordVal},
			})).To(Succeed())

			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			lt := health.NewLagTrackerWithClock(func() time.Time { return now })
			// Last event 40 minutes ago -> no-event fail window.
			lt.OnEvent(now.Add(-40 * time.Minute).UnixMilli())
			controllerReconciler := &DatabaseSensorReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Lag:    lt,
				Health: health.NewCheckerWithClock(k8sClient, lt, func() time.Time { return now }),
			}
			By("Reconciling once to create the child DebeziumServer")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Fabricating a running pod status so only the no-event clock drives the phase")
			child := &unstructured.Unstructured{}
			child.SetGroupVersionKind(deployer.DebeziumServerGVK)
			Expect(k8sClient.Get(ctx, typeNamespacedName, child)).To(Succeed())
			Expect(unstructured.SetNestedField(child.Object, "Running", "status", "phase")).To(Succeed())
			Expect(k8sClient.Status().Update(ctx, child)).To(Succeed())

			By("Reconciling again so the checker observes the stale event clock")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Deriving Phase=Failed with Ready=False/NoEventsStale")
			Eventually(func(g Gomega) {
				fetched := &sensoriov1.DatabaseSensor{}
				g.Expect(k8sClient.Get(ctx, typeNamespacedName, fetched)).To(Succeed())
				g.Expect(fetched.Status.Phase).To(Equal("Failed"))
				ready := meta.FindStatusCondition(fetched.Status.Conditions, "Ready")
				g.Expect(ready).NotTo(BeNil())
				g.Expect(ready.Reason).To(Equal("NoEventsStale"))
			}).Should(Succeed())
		})

		It("should derive Failed when the Secret is missing (US-1.3)", func() {
			By("Reconciling without creating the referenced Secret")
			controllerReconciler := &DatabaseSensorReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			By("Not returning an error so the reconcile does not hot-loop")
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(30 * time.Second))

			By("Setting Ready=False/SecretNotFound and Phase=Failed")
			Eventually(func(g Gomega) {
				fetched := &sensoriov1.DatabaseSensor{}
				g.Expect(k8sClient.Get(ctx, typeNamespacedName, fetched)).To(Succeed())
				g.Expect(fetched.Status.Phase).To(Equal("Failed"))
				ready := meta.FindStatusCondition(fetched.Status.Conditions, "Ready")
				g.Expect(ready).NotTo(BeNil())
				g.Expect(ready.Status).To(Equal(metav1.ConditionFalse))
				g.Expect(ready.Reason).To(Equal("SecretNotFound"))
				g.Expect(ready.Message).To(ContainSubstring(secretName))
			}).Should(Succeed())

			By("Not creating a DebeziumServer child")
			child := &unstructured.Unstructured{}
			child.SetGroupVersionKind(deployer.DebeziumServerGVK)
			err = k8sClient.Get(ctx, typeNamespacedName, child)
			Expect(errors.IsNotFound(err)).To(BeTrue())
		})

		It("should create an owned metrics Service always (US-3.3)", func() {
			By("Creating the referenced password Secret")
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: resourceNamespace},
				StringData: map[string]string{secretPasswordKey: secretPasswordVal},
			})).To(Succeed())

			controllerReconciler := &DatabaseSensorReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Finding a <name>-metrics Service on port 8080 owned by the sensor")
			svc := &corev1.Service{}
			metricsKey := types.NamespacedName{Name: resourceName + "-metrics", Namespace: resourceNamespace}
			Expect(k8sClient.Get(ctx, metricsKey, svc)).To(Succeed())
			Expect(svc.Spec.Ports).To(HaveLen(1))
			Expect(svc.Spec.Ports[0].Name).To(Equal("metrics"))
			Expect(svc.Spec.Ports[0].Port).To(Equal(int32(8080)))
			Expect(svc.Spec.Selector).To(HaveKeyWithValue("control-plane", "controller-manager"))
			Expect(svc.Spec.Selector).To(HaveKeyWithValue("app.kubernetes.io/name", "db-sensor-operator"))
			owners := svc.GetOwnerReferences()
			Expect(owners).To(HaveLen(1))
			Expect(owners[0].Name).To(Equal(resourceName))
			Expect(owners[0].Kind).To(Equal("DatabaseSensor"))

			By("Not creating a ServiceMonitor when prometheus.serviceMonitor is absent (opt-in)")
			sm := &monitoringv1.ServiceMonitor{}
			Expect(errors.IsNotFound(k8sClient.Get(ctx, metricsKey, sm))).To(BeTrue())

			By("Reconciling a second time converges without duplicating the Service (idempotency)")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			list := &corev1.ServiceList{}
			Expect(k8sClient.List(ctx, list, client.InNamespace(resourceNamespace))).To(Succeed())
			count := 0
			for _, item := range list.Items {
				if item.Name == resourceName+"-metrics" {
					count++
				}
			}
			Expect(count).To(Equal(1))
		})

		It("should create an owned same-namespace ServiceMonitor when opted in (US-3.2)", func() {
			By("Creating the referenced password Secret")
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: resourceNamespace},
				StringData: map[string]string{secretPasswordKey: secretPasswordVal},
			})).To(Succeed())

			By("Opting into the ServiceMonitor on the sensor")
			fetched := &sensoriov1.DatabaseSensor{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, fetched)).To(Succeed())
			fetched.Spec.Sink.Prometheus = &sensoriov1.PrometheusSink{ServiceMonitor: true}
			Expect(k8sClient.Update(ctx, fetched)).To(Succeed())

			controllerReconciler := &DatabaseSensorReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Finding a <name>-metrics ServiceMonitor in the sensor namespace owned by the sensor")
			sm := &monitoringv1.ServiceMonitor{}
			metricsKey := types.NamespacedName{Name: resourceName + "-metrics", Namespace: resourceNamespace}
			Expect(k8sClient.Get(ctx, metricsKey, sm)).To(Succeed())

			By("Matching the Service labels in the selector and scraping the metrics endpoint on 8080")
			Expect(sm.Spec.Selector.MatchLabels).To(HaveKeyWithValue("app.kubernetes.io/name", "db-sensor-operator"))
			Expect(sm.Spec.Selector.MatchLabels).To(HaveKeyWithValue("sensor.io/sensor-name", resourceName))
			Expect(sm.Spec.Endpoints).To(HaveLen(1))
			Expect(sm.Spec.Endpoints[0].Port).To(Equal("metrics"))
			Expect(sm.Spec.Endpoints[0].Path).To(Equal("/metrics"))

			owners := sm.GetOwnerReferences()
			Expect(owners).To(HaveLen(1))
			Expect(owners[0].Name).To(Equal(resourceName))
			Expect(owners[0].Kind).To(Equal("DatabaseSensor"))

			By("Reconciling a second time converges without duplicating the ServiceMonitor (idempotency)")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			list := &monitoringv1.ServiceMonitorList{}
			Expect(k8sClient.List(ctx, list, client.InNamespace(resourceNamespace))).To(Succeed())
			Expect(list.Items).To(HaveLen(1))
		})

		It("should create a cross-namespace ServiceMonitor with labels and no owner ref", func() {
			const otherNamespace = "monitoring"

			By("Creating the referenced password Secret")
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: resourceNamespace},
				StringData: map[string]string{secretPasswordKey: secretPasswordVal},
			})).To(Succeed())

			By("Creating the target monitoring namespace")
			monNs := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: otherNamespace}}
			if err := k8sClient.Create(ctx, monNs); err != nil {
				Expect(errors.IsAlreadyExists(err)).To(BeTrue())
			}

			By("Opting into a ServiceMonitor in a different namespace")
			fetched := &sensoriov1.DatabaseSensor{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, fetched)).To(Succeed())
			fetched.Spec.Sink.Prometheus = &sensoriov1.PrometheusSink{
				ServiceMonitor: true,
				Namespace:      otherNamespace,
			}
			Expect(k8sClient.Update(ctx, fetched)).To(Succeed())

			controllerReconciler := &DatabaseSensorReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Finding the ServiceMonitor in the configured namespace with traceability labels and no owner ref")
			sm := &monitoringv1.ServiceMonitor{}
			smKey := types.NamespacedName{Name: resourceName + "-metrics", Namespace: otherNamespace}
			Expect(k8sClient.Get(ctx, smKey, sm)).To(Succeed())
			Expect(sm.GetOwnerReferences()).To(BeEmpty())
			Expect(sm.Labels).To(HaveKeyWithValue("sensor.io/sensor-name", resourceName))
			Expect(sm.Labels).To(HaveKeyWithValue("sensor.io/sensor-namespace", resourceNamespace))

			By("Cleaning up the cross-namespace ServiceMonitor")
			Expect(k8sClient.Delete(ctx, sm)).To(Succeed())
		})

		It("should create an owned same-namespace PrometheusRule from alertRules (US-4.1)", func() {
			By("Creating the referenced password Secret")
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: resourceNamespace},
				StringData: map[string]string{secretPasswordKey: secretPasswordVal},
			})).To(Succeed())

			By("Declaring alert rules on the Prometheus sink")
			fetched := &sensoriov1.DatabaseSensor{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, fetched)).To(Succeed())
			fetched.Spec.Sink.Prometheus = &sensoriov1.PrometheusSink{
				AlertRules: []sensoriov1.AlertRule{
					{Name: "HighLag", Expr: "debezium_lag_seconds > 60", For: "5m", Severity: "warning"},
					{Name: "NoEvents", Expr: "rate(debezium_events_total[5m]) == 0", For: "15m", Severity: "critical"},
				},
			}
			Expect(k8sClient.Update(ctx, fetched)).To(Succeed())

			controllerReconciler := &DatabaseSensorReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Finding a db-sensor-<name>-alerts PrometheusRule in the sensor namespace owned by the sensor")
			pr := &monitoringv1.PrometheusRule{}
			alertsKey := types.NamespacedName{Name: "db-sensor-" + resourceName + "-alerts", Namespace: resourceNamespace}
			Expect(k8sClient.Get(ctx, alertsKey, pr)).To(Succeed())

			Expect(pr.Spec.Groups).To(HaveLen(1))
			Expect(pr.Spec.Groups[0].Name).To(Equal("db-sensor.alerts"))
			Expect(pr.Spec.Groups[0].Rules).To(HaveLen(2))
			r0 := pr.Spec.Groups[0].Rules[0]
			Expect(r0.Alert).To(Equal("HighLag"))
			Expect(r0.Expr.String()).To(Equal("debezium_lag_seconds > 60"))
			Expect(r0.For).NotTo(BeNil())
			Expect(string(*r0.For)).To(Equal("5m"))
			Expect(r0.Labels).To(HaveKeyWithValue("severity", "warning"))

			owners := pr.GetOwnerReferences()
			Expect(owners).To(HaveLen(1))
			Expect(owners[0].Name).To(Equal(resourceName))
			Expect(owners[0].Kind).To(Equal("DatabaseSensor"))

			By("Reconciling a second time converges without duplicating the PrometheusRule (idempotency)")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			list := &monitoringv1.PrometheusRuleList{}
			Expect(k8sClient.List(ctx, list, client.InNamespace(resourceNamespace))).To(Succeed())
			Expect(list.Items).To(HaveLen(1))
		})

		It("should POST to the webhook when an alert fires and not fail Phase on a webhook error (US-4.4)", func() {
			By("Creating the referenced password Secret")
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: resourceNamespace},
				StringData: map[string]string{secretPasswordKey: secretPasswordVal},
			})).To(Succeed())

			By("Standing up an httptest webhook that captures the POST")
			var received atomic.Int32
			var gotBody []byte
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				received.Add(1)
				gotBody, _ = io.ReadAll(r.Body)
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			By("Enabling the webhook sink pointing at the test server")
			fetched := &sensoriov1.DatabaseSensor{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, fetched)).To(Succeed())
			fetched.Spec.Sink.Type = "webhook"
			fetched.Spec.Sink.Webhook = &sensoriov1.WebhookSink{Enabled: true, URL: srv.URL}
			Expect(k8sClient.Update(ctx, fetched)).To(Succeed())

			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			lt := health.NewLagTrackerWithClock(func() time.Time { return now })
			// Recent event but lag above critical -> Ready=False/LagAboveCritical
			// -> HighLag alert fires.
			lt.OnEvent(now.Add(-200 * time.Second).UnixMilli())
			controllerReconciler := &DatabaseSensorReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Lag:    lt,
				Health: health.NewCheckerWithClock(k8sClient, lt, func() time.Time { return now }),
			}

			By("Reconciling once to create the child DebeziumServer")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Fabricating a running pod status so lag drives the phase")
			child := &unstructured.Unstructured{}
			child.SetGroupVersionKind(deployer.DebeziumServerGVK)
			Expect(k8sClient.Get(ctx, typeNamespacedName, child)).To(Succeed())
			Expect(unstructured.SetNestedField(child.Object, "Running", "status", "phase")).To(Succeed())
			Expect(k8sClient.Status().Update(ctx, child)).To(Succeed())

			By("Reconciling again so the checker fires the alert and the webhook receives the POST")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			Eventually(func(g Gomega) {
				g.Expect(received.Load()).To(BeNumerically(">=", 1))
			}).Should(Succeed())
			Expect(string(gotBody)).To(ContainSubstring("HighLag"))

			By("Deriving Phase=Failed from the critical lag, not from any webhook result")
			fetchedAfter := &sensoriov1.DatabaseSensor{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, fetchedAfter)).To(Succeed())
			Expect(fetchedAfter.Status.Phase).To(Equal("Failed"))
			ready := meta.FindStatusCondition(fetchedAfter.Status.Conditions, "Ready")
			Expect(ready).NotTo(BeNil())
			Expect(ready.Reason).To(Equal("LagAboveCritical"))
		})

		It("should keep Phase derived from health when the webhook endpoint fails (non-blocking)", func() {
			By("Creating the referenced password Secret")
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: resourceNamespace},
				StringData: map[string]string{secretPasswordKey: secretPasswordVal},
			})).To(Succeed())

			By("Standing up an httptest webhook that always 500s")
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer srv.Close()

			fetched := &sensoriov1.DatabaseSensor{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, fetched)).To(Succeed())
			fetched.Spec.Sink.Type = "webhook"
			fetched.Spec.Sink.Webhook = &sensoriov1.WebhookSink{Enabled: true, URL: srv.URL}
			Expect(k8sClient.Update(ctx, fetched)).To(Succeed())

			now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			lt := health.NewLagTrackerWithClock(func() time.Time { return now })
			// Lag in the warning band -> Ready=False/LagAboveWarning -> Degraded.
			lt.OnEvent(now.Add(-45 * time.Second).UnixMilli())
			controllerReconciler := &DatabaseSensorReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				Lag:    lt,
				Health: health.NewCheckerWithClock(k8sClient, lt, func() time.Time { return now }),
			}

			By("Reconciling once to create the child DebeziumServer")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			child := &unstructured.Unstructured{}
			child.SetGroupVersionKind(deployer.DebeziumServerGVK)
			Expect(k8sClient.Get(ctx, typeNamespacedName, child)).To(Succeed())
			Expect(unstructured.SetNestedField(child.Object, "Running", "status", "phase")).To(Succeed())
			Expect(k8sClient.Status().Update(ctx, child)).To(Succeed())

			By("Reconciling again: the webhook POST fails but the reconcile does not error")
			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(30 * time.Second))

			By("Keeping Phase=Degraded from the warning lag; a failed webhook never flips it to Failed")
			fetchedAfter := &sensoriov1.DatabaseSensor{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, fetchedAfter)).To(Succeed())
			Expect(fetchedAfter.Status.Phase).To(Equal("Degraded"))
			ready := meta.FindStatusCondition(fetchedAfter.Status.Conditions, "Ready")
			Expect(ready).NotTo(BeNil())
			Expect(ready.Reason).To(Equal("LagAboveWarning"))
		})

		It("should not create a PrometheusRule when alertRules are absent (US-4.1 opt-in)", func() {
			By("Creating the referenced password Secret")
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: resourceNamespace},
				StringData: map[string]string{secretPasswordKey: secretPasswordVal},
			})).To(Succeed())

			controllerReconciler := &DatabaseSensorReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			By("Finding no db-sensor-<name>-alerts PrometheusRule")
			pr := &monitoringv1.PrometheusRule{}
			alertsKey := types.NamespacedName{Name: "db-sensor-" + resourceName + "-alerts", Namespace: resourceNamespace}
			Expect(errors.IsNotFound(k8sClient.Get(ctx, alertsKey, pr))).To(BeTrue())
		})
	})
})
