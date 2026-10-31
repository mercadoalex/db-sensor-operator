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

// Package deployer builds and reconciles the child resources a DatabaseSensor
// manages. Task 4 covers the DebeziumServer custom resource.
//
// Step 0 dependency decision (resolved during planning, see
// .agents/tasks/task4-plan.md): there is NO importable Go module for the
// DebeziumServer API types. The official Debezium Operator is Java/Quarkus and
// publishes no Go API module (the path in tech.md does not resolve on the proxy).
// Therefore we model the DebeziumServer as an *unstructured.Unstructured carrying
// the correct GVK. controllerutil.SetControllerReference and CreateOrUpdate both
// work on unstructured objects once the owner scheme and the child GVK are known.
// If a typed module is ever published, the unstructured object can be swapped for
// typed structs without changing the reconciler contract.
package deployer

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
)

// DebeziumServerGVK is the GroupVersionKind of the DebeziumServer custom resource
// managed by the official Debezium Operator. Verified against the upstream CRD
// debeziumservers.debezium.io: group debezium.io, the only served+storage version
// is v1alpha1, kind DebeziumServer (plural debeziumservers, scope Namespaced).
var DebeziumServerGVK = schema.GroupVersionKind{
	Group:   "debezium.io",
	Version: "v1alpha1",
	Kind:    "DebeziumServer",
}

// secretEnvPrefix is the prefix the DebeziumServer runtime applies to every key of
// the referenced password Secret when exposing it as environment variables
// (spec.runtime.environment.from[].prefix). The Secret key holding the password
// must be named "password", so the variable the connector config refers to is
// secretEnvPrefix + passwordSecretKey.
const (
	secretEnvPrefix   = "DATABASE_"
	passwordSecretKey = "password"
)

// passwordPlaceholder is written into the DebeziumServer source config instead of
// the real password (NFR-4: credentials are never inlined). Debezium Server
// resolves the ${...} expression from the pod environment, which the deployer
// populates from the referenced Secret via spec.runtime.environment.from.
const passwordPlaceholder = "${" + secretEnvPrefix + passwordSecretKey + "}"

// ErrSecretNotFound is returned by EnsureDebeziumServer when the Secret named by
// spec.database.passwordSecret does not exist (US-1.3). The controller maps it to
// a Ready=False / Reason=SecretNotFound condition and derives Phase=Failed.
var ErrSecretNotFound = errors.New("database password secret not found")

// ErrUnsupportedDBType is returned by BuildDebeziumServer when spec.database.type
// is not a recognised Debezium connector. The CRD enum already guards the API, so
// this is defense-in-depth; the controller surfaces it as a Ready=False condition.
var ErrUnsupportedDBType = errors.New("unsupported database type")

// dbTypePostgres is the spec.database.type value for PostgreSQL.
const dbTypePostgres = "postgresql"

// connectorClasses maps spec.database.type to the fully-qualified Debezium
// connector Java class (US-8).
var connectorClasses = map[string]string{
	dbTypePostgres: "io.debezium.connector.postgresql.PostgresConnector",
	"mysql":        "io.debezium.connector.mysql.MySqlConnector",
}

// BuildDebeziumServer maps a DatabaseSensor spec onto a DebeziumServer object.
// It is a pure function (no cluster I/O) so the mapping is unit-testable without
// envtest.
//
// Child naming: metadata.name == sensor.Name (no suffix) for a 1:1 mental model
// between a DatabaseSensor and its DebeziumServer. Derived identifiers
// (topic.prefix, slot.name, database.server.id) are computed deterministically
// from namespace+name so repeated reconciles converge without churn (idempotency)
// and distinct sensors do not collide.
func BuildDebeziumServer(sensor *sensoriov1.DatabaseSensor) (*unstructured.Unstructured, error) {
	class, ok := connectorClasses[sensor.Spec.Database.Type]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedDBType, sensor.Spec.Database.Type)
	}

	db := sensor.Spec.Database
	config := map[string]any{
		"connector.class":   class,
		"database.hostname": db.Host,
		"database.port":     strconv.Itoa(int(db.Port)),
		"database.user":     db.User,
		// NFR-4: reference, never the real value. The concrete credential is
		// injected from the referenced Secret by a later task.
		"database.password": passwordPlaceholder,
		"topic.prefix":      topicPrefix(sensor),
	}

	// Table filter (US-1.1).
	if len(sensor.Spec.Tables) > 0 {
		config["table.include.list"] = strings.Join(sensor.Spec.Tables, ",")
	}

	// Per-type extras (US-8).
	switch sensor.Spec.Database.Type {
	case dbTypePostgres:
		config["slot.name"] = SlotName(sensor)
		// Do not read existing rows: the sensor derives everything from live
		// changes, so a blocking snapshot is a pure cost that leaves it blind
		// until it ends. "no_data" is the non-blocking mode accepted by Debezium
		// 3.6.3 (verified; "never" is rejected there). ADR-014.
		config["snapshot.mode"] = "no_data"
		// Debezium defaults to the decoderbufs output plugin, which stock
		// PostgreSQL does not ship. pgoutput is built in since PG 10 and is what
		// managed services support, so it is the only portable choice.
		config["plugin.name"] = "pgoutput"
		// The PostgreSQL connector requires the database to capture and cannot
		// start without it.
		if db.Name != "" {
			config["database.dbname"] = db.Name
		}
	case "mysql":
		config["database.server.id"] = strconv.FormatUint(uint64(serverID(sensor)), 10)
	}

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(DebeziumServerGVK)
	obj.SetName(sensor.Name)
	obj.SetNamespace(sensor.Namespace)
	if err := unstructured.SetNestedMap(obj.Object, config, "spec", "source", "config"); err != nil {
		return nil, fmt.Errorf("setting source config: %w", err)
	}
	if err := unstructured.SetNestedField(obj.Object, class, "spec", "source", "class"); err != nil {
		return nil, fmt.Errorf("setting source class: %w", err)
	}
	if persistentOffsets(sensor) {
		if err := applyPersistentOffsets(obj); err != nil {
			return nil, err
		}
	}

	// Expose the referenced Secret to the Debezium Server pod so the
	// ${DATABASE_password} expression in the source config resolves at runtime.
	// Only the Secret's name is recorded here, never its data (NFR-4).
	envFrom := []any{
		map[string]any{
			"prefix":    secretEnvPrefix,
			"secretRef": map[string]any{"name": db.PasswordSecret},
		},
	}
	if err := unstructured.SetNestedSlice(obj.Object, envFrom, "spec", "runtime", "environment", "from"); err != nil {
		return nil, fmt.Errorf("setting runtime environment: %w", err)
	}

	sinkType, sinkConfig := sinkFor(sensor)
	if err := unstructured.SetNestedField(obj.Object, sinkType, "spec", "sink", "type"); err != nil {
		return nil, fmt.Errorf("setting sink type: %w", err)
	}
	if sinkConfig != nil {
		if err := unstructured.SetNestedMap(obj.Object, sinkConfig, "spec", "sink", "config"); err != nil {
			return nil, fmt.Errorf("setting sink config: %w", err)
		}
	}

	return obj, nil
}

// Persistent offset store (ADR-014). All three values were verified against
// Debezium Operator 3.6.3 (bench/experiments/offset-persistence):
//   - the Operator rejects storage type "persistent" without a PVC template
//     ("Missing PVC template"), so the volumeClaim template is required;
//   - a relative offset fileName resolves against the process working directory,
//     which is not writable (AccessDeniedException), not the data volume, so the
//     path must be absolute.
const (
	offsetFilePath        = "/debezium/data/offsets.dat"
	offsetVolumeSize      = "100Mi" // an offset file is a few hundred bytes
	offsetStorePersistent = "persistent"
)

// SnapshotStatusFor explains, in one line, how the sensor's connector starts and
// where it keeps its offset. It is a pure function of the spec (Requirement 4),
// so it matches what BuildDebeziumServer generates and carries no secret values.
func SnapshotStatusFor(sensor *sensoriov1.DatabaseSensor) sensoriov1.SnapshotStatus {
	st := sensoriov1.SnapshotStatus{OffsetStore: "memory"}
	if persistentOffsets(sensor) {
		st.OffsetStore = offsetStorePersistent
	}

	if sensor.Spec.Database.Type != dbTypePostgres {
		st.Strategy = "engine-default"
		st.Reason = "Debezium's default snapshot applies; the non-blocking start is verified for PostgreSQL only"
		return st
	}

	st.Strategy = "no-data"
	if st.OffsetStore == offsetStorePersistent {
		st.Reason = "Existing rows are not read; the offset is persisted, so a Debezium restart resumes where it stopped"
	} else {
		st.Reason = "Existing rows are not read; the offset is in memory, so a Debezium restart can re-deliver recent events"
	}
	return st
}

func persistentOffsets(sensor *sensoriov1.DatabaseSensor) bool {
	return sensor.Spec.Snapshot != nil && sensor.Spec.Snapshot.OffsetStore == offsetStorePersistent
}

func applyPersistentOffsets(obj *unstructured.Unstructured) error {
	if err := unstructured.SetNestedField(obj.Object, "persistent", "spec", "runtime", "storage", "data", "type"); err != nil {
		return fmt.Errorf("setting runtime storage: %w", err)
	}
	claim := map[string]any{
		"accessModes": []any{"ReadWriteOnce"},
		"resources":   map[string]any{"requests": map[string]any{"storage": offsetVolumeSize}},
	}
	if err := unstructured.SetNestedMap(obj.Object, claim, "spec", "runtime", "templates", "volumeClaim"); err != nil {
		return fmt.Errorf("setting volume claim template: %w", err)
	}
	if err := unstructured.SetNestedField(obj.Object, offsetFilePath, "spec", "source", "offset", "file", "fileName"); err != nil {
		return fmt.Errorf("setting offset file: %w", err)
	}
	return nil
}

// sinkFor chooses the Debezium Server sink (ADR-012). With the HTTP transport
// and a configured spec.transport.http.url, Debezium POSTs every event to the
// operator's /cdc endpoint; the operator's HTTP sink maps "url" to
// debezium.sink.http.url. Without a URL (the synthetic demo, which POSTs events
// to /cdc itself) or with the Kafka transport, the sink stays the "none"
// placeholder: delivering to Kafka is not wired by the deployer.
func sinkFor(sensor *sensoriov1.DatabaseSensor) (string, map[string]any) {
	t := sensor.Spec.Transport
	if t == nil || t.Type == "" || t.Type == "http" {
		if t != nil && t.HTTP != nil && t.HTTP.URL != "" {
			return "http", map[string]any{"url": t.HTTP.URL}
		}
	}
	return "none", nil
}

// EnsureDebeziumServer reconciles the DebeziumServer for a DatabaseSensor:
// it verifies the referenced password Secret exists (US-1.3), then creates or
// updates the DebeziumServer with an owner reference back to the sensor
// (US-1.2 → enables US-1.4 cascade delete). It is idempotent via CreateOrUpdate.
func EnsureDebeziumServer(ctx context.Context, c client.Client, scheme *runtime.Scheme, sensor *sensoriov1.DatabaseSensor) error {
	// US-1.3: the Secret must exist. We only check presence, so we ask for its metadata:
	// the data never reaches the operator (NFR-4). The manager's client does not cache
	// Secrets (see cmd/main.go), so this is one direct GET of one named object and needs
	// no list/watch permission.
	secretKey := types.NamespacedName{Name: sensor.Spec.Database.PasswordSecret, Namespace: sensor.Namespace}
	secret := &metav1.PartialObjectMetadata{}
	secret.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	if err := c.Get(ctx, secretKey, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("%w: %q", ErrSecretNotFound, sensor.Spec.Database.PasswordSecret)
		}
		return fmt.Errorf("getting password secret %q: %w", sensor.Spec.Database.PasswordSecret, err)
	}

	desired, err := BuildDebeziumServer(sensor)
	if err != nil {
		return err
	}

	// CreateOrUpdate needs an object carrying only the identity (GVK + name +
	// namespace); the mutate closure applies the desired spec and owner ref so the
	// operation is idempotent and conflict-safe.
	target := &unstructured.Unstructured{}
	target.SetGroupVersionKind(DebeziumServerGVK)
	target.SetName(sensor.Name)
	target.SetNamespace(sensor.Namespace)

	_, err = controllerutil.CreateOrUpdate(ctx, c, target, func() error {
		spec, _, _ := unstructured.NestedMap(desired.Object, "spec")
		if err := unstructured.SetNestedMap(target.Object, spec, "spec"); err != nil {
			return fmt.Errorf("setting desired spec: %w", err)
		}
		return controllerutil.SetControllerReference(sensor, target, scheme)
	})
	if err != nil {
		return fmt.Errorf("ensuring DebeziumServer %q: %w", sensor.Name, err)
	}
	return nil
}

// topicPrefix derives a stable, DNS-safe Debezium topic prefix from the sensor's
// namespace and name.
func topicPrefix(sensor *sensoriov1.DatabaseSensor) string {
	return sanitize(sensor.Namespace + "-" + sensor.Name)
}

var invalidSlotChars = regexp.MustCompile(`[^a-z0-9_]`)

// SlotName derives a stable Postgres replication slot name from the sensor name.
// Postgres slot names must be lowercase alphanumerics or underscores and at most
// 63 characters.
func SlotName(sensor *sensoriov1.DatabaseSensor) string {
	name := strings.ToLower(sensor.Namespace + "_" + sensor.Name)
	name = invalidSlotChars.ReplaceAllString(name, "_")
	if len(name) > 63 {
		name = name[:63]
	}
	return name
}

var invalidDNSChars = regexp.MustCompile(`[^a-z0-9-]`)

// sanitize lowercases and replaces characters outside the DNS-safe set.
func sanitize(s string) string {
	return invalidDNSChars.ReplaceAllString(strings.ToLower(s), "-")
}

// serverID derives a stable, non-zero MySQL server id in the range
// [1, 4294967295] from the sensor's namespace and name. The same input always
// yields the same id so reconciles do not churn.
func serverID(sensor *sensoriov1.DatabaseSensor) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(sensor.Namespace + "/" + sensor.Name))
	id := h.Sum32()
	if id == 0 {
		id = 1
	}
	return id
}
