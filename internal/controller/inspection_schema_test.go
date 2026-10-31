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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
)

// These specs run against a real API server (envtest), so they check what the generated CRD
// actually enforces, which reading the YAML cannot.
var _ = Describe("DatabaseSensor schema: snapshot inspection", func() {
	const ns = "default"
	const schemaTblOrders = "public.orders"
	var ctx context.Context
	var created []string

	BeforeEach(func() { ctx = context.Background(); created = nil })
	AfterEach(func() {
		for _, name := range created {
			_ = k8sClient.Delete(ctx, &sensoriov1.DatabaseSensor{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}})
		}
	})

	sensor := func(name, dbType string, snap *sensoriov1.SnapshotConfig) *sensoriov1.DatabaseSensor {
		created = append(created, name)
		return &sensoriov1.DatabaseSensor{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: sensoriov1.DatabaseSensorSpec{
				Database: sensoriov1.DatabaseConfig{Type: dbType, Host: "db", Port: 5432, User: "u", PasswordSecret: "s"},
				Tables:   []string{schemaTblOrders},
				Snapshot: snap,
			},
		}
	}
	get := func(name string) *sensoriov1.DatabaseSensor {
		out := &sensoriov1.DatabaseSensor{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, out)).To(Succeed())
		return out
	}
	i32 := func(v int32) *int32 { return &v }
	i64 := func(v int64) *int64 { return &v }

	It("leaves the inspection off and absent when the user says nothing", func() {
		Expect(k8sClient.Create(ctx, sensor("insp-none", "postgresql", nil))).To(Succeed())
		Expect(get("insp-none").Spec.Snapshot).To(BeNil())
	})

	It("applies the defaults to an empty inspection block, and enabled defaults to false", func() {
		Expect(k8sClient.Create(ctx, sensor("insp-defaults", "postgresql",
			&sensoriov1.SnapshotConfig{Inspection: &sensoriov1.InspectionConfig{}}))).To(Succeed())
		got := get("insp-defaults").Spec.Snapshot.Inspection
		Expect(got.Enabled).To(BeFalse())
		Expect(got.IntervalSeconds).NotTo(BeNil())
		Expect(*got.IntervalSeconds).To(Equal(int32(600)))
		Expect(got.TimeoutSeconds).To(Equal(int32(30)))
	})

	It("keeps an explicit interval of 0 (once per spec change) instead of defaulting it to 600", func() {
		Expect(k8sClient.Create(ctx, sensor("insp-once", "postgresql",
			&sensoriov1.SnapshotConfig{Inspection: &sensoriov1.InspectionConfig{IntervalSeconds: i32(0)}}))).To(Succeed())
		got := get("insp-once").Spec.Snapshot.Inspection.IntervalSeconds
		Expect(got).NotTo(BeNil())
		Expect(*got).To(Equal(int32(0)))
	})

	It("accepts an interval of exactly 60 and rejects 1 to 59", func() {
		Expect(k8sClient.Create(ctx, sensor("insp-60", "postgresql",
			&sensoriov1.SnapshotConfig{Inspection: &sensoriov1.InspectionConfig{IntervalSeconds: i32(60)}}))).To(Succeed())
		for _, bad := range []int32{1, 30, 59} {
			err := k8sClient.Create(ctx, sensor("insp-bad", "postgresql",
				&sensoriov1.SnapshotConfig{Inspection: &sensoriov1.InspectionConfig{IntervalSeconds: i32(bad)}}))
			Expect(err).To(HaveOccurred(), "interval %d", bad)
			Expect(err.Error()).To(ContainSubstring("intervalSeconds must be 0 or at least 60"))
		}
	})

	It("rejects intervals above a day, and timeouts outside 5 to 300", func() {
		Expect(k8sClient.Create(ctx, sensor("insp-big", "postgresql",
			&sensoriov1.SnapshotConfig{Inspection: &sensoriov1.InspectionConfig{IntervalSeconds: i32(86401)}}))).NotTo(Succeed())
		for _, bad := range []int32{4, 301} {
			Expect(k8sClient.Create(ctx, sensor("insp-timeout", "postgresql",
				&sensoriov1.SnapshotConfig{Inspection: &sensoriov1.InspectionConfig{TimeoutSeconds: bad}}))).NotTo(Succeed(), "timeout %d", bad)
		}
		Expect(k8sClient.Create(ctx, sensor("insp-timeout-ok", "postgresql",
			&sensoriov1.SnapshotConfig{Inspection: &sensoriov1.InspectionConfig{TimeoutSeconds: 5}}))).To(Succeed())
	})

	It("accepts PostgreSQL and MySQL and rejects the engines the operator no longer supports", func() {
		Expect(k8sClient.Create(ctx, sensor("eng-pg", "postgresql", nil))).To(Succeed())
		Expect(k8sClient.Create(ctx, sensor("eng-mysql", "mysql", nil))).To(Succeed())
		for _, bad := range []string{"mongodb", "sqlserver", "oracle", "db2", "cassandra", "sqlite"} {
			Expect(k8sClient.Create(ctx, sensor("eng-bad", bad, nil))).NotTo(Succeed(), bad)
		}
	})

	It("stores inspection facts and tells a real zero from an absent value", func() {
		Expect(k8sClient.Create(ctx, sensor("insp-status", "postgresql", nil))).To(Succeed())
		obj := get("insp-status")
		now := metav1.Now()
		obj.Status.Snapshot = &sensoriov1.SnapshotStatus{
			Inspection: &sensoriov1.InspectionStatus{
				FactsVersion: 1, ObservedGeneration: obj.Generation, ObservedAt: &now,
				Tables: []sensoriov1.TableInspection{
					{Name: schemaTblOrders, Found: true, EstimatedRows: i64(151000), EstimateReliable: true, CanWrite: true},
					{Name: "public.unanalyzed", Found: true, EstimateReliable: false},
				},
				Postgres: &sensoriov1.PostgresInspection{
					ServerVersion: 160015, WALLevel: "logical", MaxSlotWALKeepSize: i64(-1),
					ReplicationSlot: &sensoriov1.SlotInspection{Name: "s1", Active: true, WALStatus: "reserved", RetainedBytes: i64(0)},
				},
			},
		}
		Expect(k8sClient.Status().Update(ctx, obj)).To(Succeed())

		insp := get("insp-status").Status.Snapshot.Inspection
		Expect(insp.FactsVersion).To(Equal(int32(1)))
		Expect(insp.Tables).To(HaveLen(2))
		Expect(insp.Tables[0].EstimatedRows).NotTo(BeNil())
		Expect(*insp.Tables[0].EstimatedRows).To(Equal(int64(151000)))
		Expect(insp.Tables[1].EstimatedRows).To(BeNil(), "unknown stays absent")

		slot := insp.Postgres.ReplicationSlot
		Expect(slot).NotTo(BeNil())
		Expect(slot.RetainedBytes).NotTo(BeNil(), "a retained 0 is stored, not dropped")
		Expect(*slot.RetainedBytes).To(Equal(int64(0)))
		Expect(slot.ConfirmedLagBytes).To(BeNil(), "an unread value stays absent")
		Expect(*insp.Postgres.MaxSlotWALKeepSize).To(Equal(int64(-1)), "-1 means unlimited")
	})

	It("leaves the slot absent when it does not exist, not a zeroed object", func() {
		Expect(k8sClient.Create(ctx, sensor("insp-noslot", "postgresql", nil))).To(Succeed())
		obj := get("insp-noslot")
		obj.Status.Snapshot = &sensoriov1.SnapshotStatus{Inspection: &sensoriov1.InspectionStatus{
			FactsVersion: 1, Postgres: &sensoriov1.PostgresInspection{WALLevel: "logical"}}}
		Expect(k8sClient.Status().Update(ctx, obj)).To(Succeed())
		Expect(get("insp-noslot").Status.Snapshot.Inspection.Postgres.ReplicationSlot).To(BeNil())
	})

	It("accepts a plan per table and rejects an unknown backfill value", func() {
		Expect(k8sClient.Create(ctx, sensor("plan-ok", "postgresql", nil))).To(Succeed())
		obj := get("plan-ok")
		obj.Status.Snapshot = &sensoriov1.SnapshotStatus{Plan: &sensoriov1.SnapshotPlan{
			Applied: false, Tables: []sensoriov1.TablePlan{{Name: schemaTblOrders, Backfill: "signal-table", Reason: "large"}}}}
		Expect(k8sClient.Status().Update(ctx, obj)).To(Succeed())
		Expect(get("plan-ok").Status.Snapshot.Plan.Applied).To(BeFalse())

		obj = get("plan-ok")
		obj.Status.Snapshot.Plan.Tables[0].Backfill = "incremental"
		Expect(k8sClient.Status().Update(ctx, obj)).NotTo(Succeed(), "incremental is not a backfill value; signal-table is")
	})
})
