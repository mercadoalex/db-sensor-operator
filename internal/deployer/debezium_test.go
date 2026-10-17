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
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
)

func newSensor(dbType string, tables ...string) *sensoriov1.DatabaseSensor {
	return &sensoriov1.DatabaseSensor{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-sensor", Namespace: "prod"},
		Spec: sensoriov1.DatabaseSensorSpec{
			Database: sensoriov1.DatabaseConfig{
				Type:           dbType,
				Host:           "db.prod.svc",
				Port:           5432,
				User:           "debezium",
				PasswordSecret: "db-creds",
			},
			Tables: tables,
		},
	}
}

func sourceConfig(t *testing.T, obj *unstructured.Unstructured) map[string]any {
	t.Helper()
	cfg, found, err := unstructured.NestedMap(obj.Object, "spec", "source", "config")
	require.NoError(t, err)
	require.True(t, found, "spec.source.config missing")
	return cfg
}

func TestBuildDebeziumServer_IdentityAndGVK(t *testing.T) {
	obj, err := BuildDebeziumServer(newSensor("postgresql", "orders"))
	require.NoError(t, err)

	assert.Equal(t, DebeziumServerGVK, obj.GroupVersionKind())
	assert.Equal(t, "orders-sensor", obj.GetName(), "child name should equal the sensor name")
	assert.Equal(t, "prod", obj.GetNamespace())
}

func TestBuildDebeziumServer_Postgresql(t *testing.T) {
	obj, err := BuildDebeziumServer(newSensor("postgresql", "public.orders", "public.items"))
	require.NoError(t, err)

	cfg := sourceConfig(t, obj)
	assert.Equal(t, "io.debezium.connector.postgresql.PostgresConnector", cfg["connector.class"])

	class, _, _ := unstructured.NestedString(obj.Object, "spec", "source", "class")
	assert.Equal(t, "io.debezium.connector.postgresql.PostgresConnector", class)

	assert.Equal(t, "public.orders,public.items", cfg["table.include.list"])

	slot, ok := cfg["slot.name"].(string)
	require.True(t, ok, "slot.name must be present for postgresql")
	assert.NotEmpty(t, slot)
	assert.Equal(t, strings.ToLower(slot), slot, "slot name must be lowercase")
	assert.LessOrEqual(t, len(slot), 63)

	// NFR-4: the real password is never embedded; only the placeholder appears.
	assert.Equal(t, passwordPlaceholder, cfg["database.password"])
	for _, v := range cfg {
		s, ok := v.(string)
		if ok {
			assert.NotContains(t, s, "s3cret")
		}
	}
}

func TestBuildDebeziumServer_MySQL(t *testing.T) {
	obj, err := BuildDebeziumServer(newSensor("mysql", "app.orders"))
	require.NoError(t, err)

	cfg := sourceConfig(t, obj)
	assert.Equal(t, "io.debezium.connector.mysql.MySqlConnector", cfg["connector.class"])

	id, ok := cfg["database.server.id"].(string)
	require.True(t, ok, "database.server.id must be present for mysql")
	assert.NotEqual(t, "0", id)

	// Deterministic: same input yields the same server id.
	obj2, err := BuildDebeziumServer(newSensor("mysql", "app.orders"))
	require.NoError(t, err)
	cfg2 := sourceConfig(t, obj2)
	assert.Equal(t, id, cfg2["database.server.id"])
}

func TestBuildDebeziumServer_MongoDB(t *testing.T) {
	obj, err := BuildDebeziumServer(newSensor("mongodb", "shop.orders"))
	require.NoError(t, err)

	cfg := sourceConfig(t, obj)
	assert.Equal(t, "io.debezium.connector.mongodb.MongoDbConnector", cfg["connector.class"])
	assert.Equal(t, "shop.orders", cfg["collection.include.list"])
	assert.NotContains(t, cfg, "table.include.list")
	assert.NotContains(t, cfg, "slot.name")
	assert.NotContains(t, cfg, "database.server.id")
}

func TestBuildDebeziumServer_UnsupportedType(t *testing.T) {
	_, err := BuildDebeziumServer(newSensor("sqlite", "t"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnsupportedDBType))
}
