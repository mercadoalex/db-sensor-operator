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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
	"github.com/mercadoalex/db-sensor-operator/internal/deployer"
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
						Tables: []string{"orders"},
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
		})

		It("should create an owned DebeziumServer when the Secret exists (US-1.1/1.2)", func() {
			By("Creating the referenced password Secret")
			Expect(k8sClient.Create(ctx, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: resourceNamespace},
				StringData: map[string]string{"password": "s3cret"},
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

			By("Deriving a Pending phase from a Ready=Unknown/DebeziumServerEnsured condition")
			Eventually(func(g Gomega) {
				fetched := &sensoriov1.DatabaseSensor{}
				g.Expect(k8sClient.Get(ctx, typeNamespacedName, fetched)).To(Succeed())
				g.Expect(fetched.Status.Phase).To(Equal("Pending"))
				g.Expect(fetched.Status.DebeziumServer).To(Equal(resourceName))
				ready := meta.FindStatusCondition(fetched.Status.Conditions, "Ready")
				g.Expect(ready).NotTo(BeNil())
				g.Expect(ready.Status).To(Equal(metav1.ConditionUnknown))
				g.Expect(ready.Reason).To(Equal("DebeziumServerEnsured"))
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
	})
})
