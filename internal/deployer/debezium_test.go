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

func TestBuildDebeziumServer_UnsupportedType(t *testing.T) {
	// sqlite never existed; the others are engines the operator no longer accepts.
	for _, dbType := range []string{"sqlite", "mongodb", "sqlserver", "oracle", "db2", "cassandra"} {
		_, err := BuildDebeziumServer(newSensor(dbType, "t"))
		require.Error(t, err, dbType)
		assert.True(t, errors.Is(err, ErrUnsupportedDBType), dbType)
	}
}

func TestBuildDebeziumServer_HTTPSinkAndSecretInjection(t *testing.T) {
	sensor := newSensor("postgresql", "public.orders")
	sensor.Spec.Database.Name = "bench"
	sensor.Spec.Transport = &sensoriov1.TransportConfig{
		Type: "http",
		HTTP: &sensoriov1.HTTPTransport{URL: "http://op.db-sensor.svc:8080/cdc"},
	}

	obj, err := BuildDebeziumServer(sensor)
	require.NoError(t, err)

	sinkType, _, _ := unstructured.NestedString(obj.Object, "spec", "sink", "type")
	assert.Equal(t, "http", sinkType)
	url, _, _ := unstructured.NestedString(obj.Object, "spec", "sink", "config", "url")
	assert.Equal(t, "http://op.db-sensor.svc:8080/cdc", url)

	assert.Equal(t, "bench", sourceConfig(t, obj)["database.dbname"])
	assert.Equal(t, "pgoutput", sourceConfig(t, obj)["plugin.name"])

	// The Secret is exposed by reference (name + prefix) only; no value is embedded.
	from, found, err := unstructured.NestedSlice(obj.Object, "spec", "runtime", "environment", "from")
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, from, 1)
	entry := from[0].(map[string]any)
	assert.Equal(t, secretEnvPrefix, entry["prefix"])
	assert.Equal(t, "db-creds", entry["secretRef"].(map[string]any)["name"])
}

func TestBuildDebeziumServer_PostgresStartsWithoutBlockingSnapshot(t *testing.T) {
	obj, err := BuildDebeziumServer(newSensor("postgresql", "public.orders"))
	require.NoError(t, err)
	assert.Equal(t, "no_data", sourceConfig(t, obj)["snapshot.mode"])

	// Other engines are untouched: their mode names are not verified.
	mysql, err := BuildDebeziumServer(newSensor("mysql", "app.orders"))
	require.NoError(t, err)
	assert.NotContains(t, sourceConfig(t, mysql), "snapshot.mode")
}

func TestBuildDebeziumServer_NoURLKeepsPlaceholderSink(t *testing.T) {
	obj, err := BuildDebeziumServer(newSensor("postgresql", "public.orders"))
	require.NoError(t, err)

	sinkType, _, _ := unstructured.NestedString(obj.Object, "spec", "sink", "type")
	assert.Equal(t, "none", sinkType)
	_, found, _ := unstructured.NestedMap(obj.Object, "spec", "sink", "config")
	assert.False(t, found, "no sink config without a URL")
}

func TestBuildDebeziumServer_PersistentOffsets(t *testing.T) {
	sensor := newSensor("postgresql", "public.orders")
	sensor.Spec.Snapshot = &sensoriov1.SnapshotConfig{OffsetStore: "persistent"}

	obj, err := BuildDebeziumServer(sensor)
	require.NoError(t, err)

	typ, _, _ := unstructured.NestedString(obj.Object, "spec", "runtime", "storage", "data", "type")
	assert.Equal(t, "persistent", typ)
	// The Debezium Operator rejects persistent storage without a PVC template.
	size, found, _ := unstructured.NestedString(obj.Object, "spec", "runtime", "templates", "volumeClaim", "resources", "requests", "storage")
	require.True(t, found, "volumeClaim template is required")
	assert.Equal(t, "100Mi", size)
	// The offset path must be absolute: a relative one resolves to an unwritable cwd.
	file, _, _ := unstructured.NestedString(obj.Object, "spec", "source", "offset", "file", "fileName")
	assert.Equal(t, "/debezium/data/offsets.dat", file)
	// The Secret environment from the HTTP sink change is still present.
	_, found, _ = unstructured.NestedSlice(obj.Object, "spec", "runtime", "environment", "from")
	assert.True(t, found)
}

func TestBuildDebeziumServer_MemoryOffsetsByDefault(t *testing.T) {
	for _, snap := range []*sensoriov1.SnapshotConfig{nil, {}, {OffsetStore: "memory"}} {
		sensor := newSensor("postgresql", "public.orders")
		sensor.Spec.Snapshot = snap
		obj, err := BuildDebeziumServer(sensor)
		require.NoError(t, err)

		_, found, _ := unstructured.NestedMap(obj.Object, "spec", "runtime", "storage")
		assert.False(t, found, "no storage block for the memory store")
		_, found, _ = unstructured.NestedMap(obj.Object, "spec", "source", "offset")
		assert.False(t, found, "no offset block for the memory store")
	}
}

func TestSnapshotStatusFor(t *testing.T) {
	pg := newSensor("postgresql", "public.orders")
	st := SnapshotStatusFor(pg)
	assert.Equal(t, "no-data", st.Strategy)
	assert.Equal(t, "memory", st.OffsetStore)
	assert.Contains(t, st.Reason, "in memory")

	pg.Spec.Snapshot = &sensoriov1.SnapshotConfig{OffsetStore: "persistent"}
	st = SnapshotStatusFor(pg)
	assert.Equal(t, "persistent", st.OffsetStore)
	assert.Contains(t, st.Reason, "persisted")

	// Engines whose non-blocking mode is unverified report the engine default.
	st = SnapshotStatusFor(newSensor("mysql", "app.orders"))
	assert.Equal(t, "engine-default", st.Strategy)
	assert.Contains(t, st.Reason, "PostgreSQL only")
}

// The status reason must stay consistent with what the deployer generates.
func TestSnapshotStatusMatchesGeneratedServer(t *testing.T) {
	sensor := newSensor("postgresql", "public.orders")
	obj, err := BuildDebeziumServer(sensor)
	require.NoError(t, err)
	assert.Equal(t, "no_data", sourceConfig(t, obj)["snapshot.mode"])
	assert.Equal(t, "no-data", SnapshotStatusFor(sensor).Strategy)
}
