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

package deployer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
)

func secretCheckScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, sensoriov1.AddToScheme(scheme))
	scheme.AddKnownTypeWithName(DebeziumServerGVK, &unstructured.Unstructured{})
	list := DebeziumServerGVK
	list.Kind += "List"
	scheme.AddKnownTypeWithName(list, &unstructured.UnstructuredList{})
	return scheme
}

// The password Secret is checked for existence only. Asking for the typed Secret would pull its
// data into the operator and, with a caching client, start a cluster-wide informer.
func TestEnsureDebeziumServer_ChecksTheSecretByMetadataOnly(t *testing.T) {
	scheme := secretCheckScheme(t)
	sensor := newSensor("postgresql", "public.orders")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "db-creds", Namespace: "prod"},
		Data:       map[string][]byte{"password": []byte("not-a-real-password")},
	}

	var typedSecretReads, metadataReads int
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret, sensor).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				switch obj.(type) {
				case *corev1.Secret:
					typedSecretReads++
				case *metav1.PartialObjectMetadata:
					if obj.GetObjectKind().GroupVersionKind().Kind == "Secret" {
						metadataReads++
					}
				}
				return cl.Get(ctx, key, obj, opts...)
			},
		}).Build()

	require.NoError(t, EnsureDebeziumServer(context.Background(), c, scheme, sensor))
	assert.Zero(t, typedSecretReads, "the Secret's data must not be fetched")
	assert.Equal(t, 1, metadataReads, "existence is checked with one metadata read")
}

func TestEnsureDebeziumServer_MissingSecretIsReported(t *testing.T) {
	scheme := secretCheckScheme(t)
	sensor := newSensor("postgresql", "public.orders")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sensor).Build()

	err := EnsureDebeziumServer(context.Background(), c, scheme, sensor)
	require.ErrorIs(t, err, ErrSecretNotFound)
}
