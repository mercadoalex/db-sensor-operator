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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
	"github.com/mercadoalex/db-sensor-operator/internal/deployer"
	"github.com/mercadoalex/db-sensor-operator/internal/inspection"
)

const (
	flowNS     = "default"
	flowName   = "flow-sensor"
	flowSecret = "flow-creds"

	okMessage = `{"factsVersion":1,"tables":[{"name":"public.orders","found":true,"estimatedRows":10,` +
		`"estimateReliable":true,"canWrite":true}],"postgres":{"serverVersion":160015,"walLevel":"logical"}}`
	authFailedMessage = `{"factsVersion":1,"error":{"code":"AuthenticationFailed","message":"password rejected"}}`
)

// These specs drive the inspection state machine against a real API server. envtest has no
// kubelet, so the Pod the Job would have created is fabricated with the termination message the
// kubelet would have copied from /dev/termination-log.
var _ = Describe("DatabaseSensor inspection flow", func() {
	var (
		ctx  context.Context
		key  = types.NamespacedName{Name: flowName, Namespace: flowNS}
		now  = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		jobK = types.NamespacedName{Name: inspection.JobName(flowName), Namespace: flowNS}
		rec  *DatabaseSensorReconciler
	)

	reconcileOnce := func() {
		_, err := rec.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		ExpectWithOffset(1, err).NotTo(HaveOccurred())
	}
	get := func() *sensoriov1.DatabaseSensor {
		out := &sensoriov1.DatabaseSensor{}
		ExpectWithOffset(1, k8sClient.Get(ctx, key, out)).To(Succeed())
		return out
	}
	// finishPod fabricates the pod of the inspection Job as the kubelet would leave it.
	finishPod := func(message string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: flowName + "-inspect-pod", Namespace: flowNS,
				Labels: map[string]string{jobNameLabel: jobK.Name}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: inspection.ContainerName, Image: "img:test"}}},
		}
		ExpectWithOffset(1, k8sClient.Create(ctx, pod)).To(Succeed())
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: inspection.ContainerName,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				Message: message, FinishedAt: metav1.NewTime(now),
			}},
		}}
		ExpectWithOffset(1, k8sClient.Status().Update(ctx, pod)).To(Succeed())
	}
	jobGone := func() bool {
		var job batchv1.Job
		err := k8sClient.Get(ctx, jobK, &job)
		return apierrors.IsNotFound(err) || (err == nil && !job.DeletionTimestamp.IsZero())
	}

	BeforeEach(func() {
		ctx = context.Background()
		rec = &DatabaseSensorReconciler{
			Client: k8sClient, Scheme: k8sClient.Scheme(), InspectorImage: "img:test", Now: func() time.Time { return now },
		}
		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: flowSecret, Namespace: flowNS},
			StringData: map[string]string{"password": "x"},
		})).To(Succeed())
		Expect(k8sClient.Create(ctx, &sensoriov1.DatabaseSensor{
			ObjectMeta: metav1.ObjectMeta{Name: flowName, Namespace: flowNS},
			Spec: sensoriov1.DatabaseSensorSpec{
				Database: sensoriov1.DatabaseConfig{
					Type: "postgresql", Host: "db", Port: 5432, User: "u", PasswordSecret: flowSecret, Name: "orders",
				},
				Tables: []string{"public.orders"},
				Snapshot: &sensoriov1.SnapshotConfig{
					Inspection: &sensoriov1.InspectionConfig{Enabled: true, IntervalSeconds: new(int32)},
				},
			},
		})).To(Succeed())
	})
	AfterEach(func() {
		_ = k8sClient.Delete(ctx, &sensoriov1.DatabaseSensor{ObjectMeta: metav1.ObjectMeta{Name: flowName, Namespace: flowNS}})
		_ = k8sClient.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: flowSecret, Namespace: flowNS}})
		_ = k8sClient.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: flowName + "-inspect-pod", Namespace: flowNS}})
		_ = k8sClient.Delete(ctx, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: jobK.Name, Namespace: flowNS}})
		child := &unstructured.Unstructured{}
		child.SetGroupVersionKind(deployer.DebeziumServerGVK)
		child.SetName(flowName)
		child.SetNamespace(flowNS)
		_ = k8sClient.Delete(ctx, child)
		_ = k8sClient.Delete(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: flowName + "-metrics", Namespace: flowNS}})
	})

	It("starts one owned Job, copies its result into status, and only then deletes the Job", func() {
		reconcileOnce()
		var job batchv1.Job
		Expect(k8sClient.Get(ctx, jobK, &job)).To(Succeed())
		Expect(metav1.IsControlledBy(&job, get())).To(BeTrue(), "the sensor owns the Job")
		c := meta.FindStatusCondition(get().Status.Conditions, inspection.ConditionInspectionAvailable)
		Expect(c).NotTo(BeNil())
		Expect(c.Reason).To(Equal(inspection.ReasonRunning))

		By("Reconciling while the Job runs: still one Job, nothing invented")
		reconcileOnce()
		Expect(k8sClient.Get(ctx, jobK, &job)).To(Succeed())

		By("Finishing the pod and reconciling: facts and plan reach status")
		finishPod(okMessage)
		reconcileOnce()
		snap := get().Status.Snapshot
		Expect(snap.Inspection).NotTo(BeNil())
		Expect(snap.Inspection.Error).To(BeEmpty())
		Expect(snap.Inspection.Tables).To(HaveLen(1))
		Expect(snap.Inspection.ObservedGeneration).To(Equal(get().Generation))
		Expect(snap.Plan).NotTo(BeNil())
		Expect(snap.Plan.Applied).To(BeFalse(), "nothing applies a plan yet")
		c = meta.FindStatusCondition(get().Status.Conditions, inspection.ConditionInspectionAvailable)
		Expect(c.Status).To(Equal(metav1.ConditionTrue))
		Expect(meta.FindStatusCondition(get().Status.Conditions, inspection.ConditionLogicalDecoding)).NotTo(BeNil())
		Expect(jobGone()).To(BeTrue(), "the Job goes after its result is stored")

		By("Reconciling again: interval 0 means once per generation, so no new Job")
		Eventually(jobGone).Should(BeTrue())
		reconcileOnce()
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, jobK, &batchv1.Job{}))).To(BeTrue())
	})

	It("replaces the facts with the error and drops the derived conditions when the inspection fails", func() {
		reconcileOnce()
		finishPod(authFailedMessage)
		reconcileOnce()
		insp := get().Status.Snapshot.Inspection
		Expect(insp.Error).To(ContainSubstring("AuthenticationFailed"))
		Expect(insp.Tables).To(BeEmpty(), "no stale facts beside an error")
		Expect(get().Status.Snapshot.Plan).To(BeNil())
		c := meta.FindStatusCondition(get().Status.Conditions, inspection.ConditionInspectionAvailable)
		Expect(c.Status).To(Equal(metav1.ConditionFalse))
		for _, t := range inspection.DerivedConditions {
			Expect(meta.FindStatusCondition(get().Status.Conditions, t)).To(BeNil())
		}
	})

	It("rejects a result with a different facts version instead of guessing", func() {
		reconcileOnce()
		finishPod(`{"factsVersion":99}`)
		reconcileOnce()
		c := meta.FindStatusCondition(get().Status.Conditions, inspection.ConditionInspectionAvailable)
		Expect(c.Reason).To(Equal(inspection.ReasonUnsupportedFactsVersion))
	})

	It("says so when the operator has no inspector image, and starts no Job", func() {
		rec.InspectorImage = ""
		reconcileOnce()
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, jobK, &batchv1.Job{}))).To(BeTrue())
		c := meta.FindStatusCondition(get().Status.Conditions, inspection.ConditionInspectionAvailable)
		Expect(c).NotTo(BeNil())
		Expect(c.Reason).To(Equal(inspection.ReasonImageNotConfigured))
	})
})

var _ = Describe("DatabaseSensor schema: inspection preconditions", func() {
	const ns = "default"
	var ctx context.Context
	BeforeEach(func() { ctx = context.Background() })

	build := func(name, dbType, dbName string, enabled bool) *sensoriov1.DatabaseSensor {
		return &sensoriov1.DatabaseSensor{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: sensoriov1.DatabaseSensorSpec{
				Database: sensoriov1.DatabaseConfig{Type: dbType, Host: "db", Port: 5432, User: "u", PasswordSecret: "s", Name: dbName},
				Tables:   []string{"public.orders"},
				Snapshot: &sensoriov1.SnapshotConfig{Inspection: &sensoriov1.InspectionConfig{Enabled: enabled}},
			},
		}
	}

	It("accepts inspection on postgresql with a database name", func() {
		s := build("cel-ok", "postgresql", "orders", true)
		Expect(k8sClient.Create(ctx, s)).To(Succeed())
		Expect(k8sClient.Delete(ctx, s)).To(Succeed())
	})
	It("rejects enabled inspection on mysql, which has no inspector yet", func() {
		Expect(k8sClient.Create(ctx, build("cel-mysql", "mysql", "orders", true))).NotTo(Succeed())
	})
	It("rejects enabled inspection without spec.database.name", func() {
		Expect(k8sClient.Create(ctx, build("cel-noname", "postgresql", "", true))).NotTo(Succeed())
	})
	It("does not require them while inspection is off", func() {
		s := build("cel-off", "mysql", "", false)
		Expect(k8sClient.Create(ctx, s)).To(Succeed())
		Expect(k8sClient.Delete(ctx, s)).To(Succeed())
	})
})
