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
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
)

// These specs run against a real API server (envtest) to check what the generated CRD enforces
// for spec.heartbeat (ADR-018).
var _ = Describe("DatabaseSensor schema: heartbeat", func() {
	const ns = "default"
	var ctx context.Context
	var created []string

	BeforeEach(func() { ctx = context.Background(); created = nil })
	AfterEach(func() {
		for _, name := range created {
			_ = k8sClient.Delete(ctx, &sensoriov1.DatabaseSensor{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
		}
	})

	sensor := func(name, dbType string, hb *sensoriov1.HeartbeatConfig) *sensoriov1.DatabaseSensor {
		created = append(created, name)
		return &sensoriov1.DatabaseSensor{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: sensoriov1.DatabaseSensorSpec{
				Database:  sensoriov1.DatabaseConfig{Type: dbType, Host: "db", Port: 5432, User: "u", PasswordSecret: "s"},
				Tables:    []string{"public.payments"},
				Heartbeat: hb,
			},
		}
	}
	every := func(v int32) *sensoriov1.HeartbeatConfig { return &sensoriov1.HeartbeatConfig{IntervalSeconds: &v} }

	It("leaves the heartbeat block absent when the user says nothing", func() {
		Expect(k8sClient.Create(ctx, sensor("hb-none", "postgresql", nil))).To(Succeed())
		got := &sensoriov1.DatabaseSensor{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "hb-none", Namespace: ns}, got)).To(Succeed())
		Expect(got.Spec.Heartbeat).To(BeNil(), "the default (30 s) lives in the operator, not in the stored spec")
	})

	It("accepts 0 (off), 5, 30 and 3600 and keeps an explicit 0", func() {
		for _, ok := range []int32{0, 5, 30, 3600} {
			name := fmt.Sprintf("hb-ok-%d", ok)
			Expect(k8sClient.Create(ctx, sensor(name, "postgresql", every(ok)))).To(Succeed(), "interval %d", ok)
			got := &sensoriov1.DatabaseSensor{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, got)).To(Succeed())
			Expect(got.Spec.Heartbeat).NotTo(BeNil())
			Expect(got.Spec.Heartbeat.IntervalSeconds).NotTo(BeNil())
			Expect(*got.Spec.Heartbeat.IntervalSeconds).To(Equal(ok))
		}
	})

	It("rejects 1 to 4 and anything above an hour or below zero", func() {
		for _, bad := range []int32{1, 2, 4} {
			err := k8sClient.Create(ctx, sensor("hb-bad", "postgresql", every(bad)))
			Expect(err).To(HaveOccurred(), "interval %d", bad)
			Expect(err.Error()).To(ContainSubstring("intervalSeconds must be 0 or at least 5"))
		}
		Expect(k8sClient.Create(ctx, sensor("hb-big", "postgresql", every(3601)))).NotTo(Succeed())
		Expect(k8sClient.Create(ctx, sensor("hb-neg", "postgresql", every(-1)))).NotTo(Succeed())
	})

	It("rejects a heartbeat block on MySQL, where it is not offered yet", func() {
		err := k8sClient.Create(ctx, sensor("hb-mysql", "mysql", every(30)))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.heartbeat is only available for postgresql"))
		Expect(k8sClient.Create(ctx, sensor("hb-mysql-none", "mysql", nil))).To(Succeed())
	})
})
