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

// Package inspection is the operator's side of the database inspection (ADR-014): it builds the
// Job that runs the inspector, reads the result the Job leaves in its termination message, and
// turns it into the facts, the plan and the conditions of a sensor's status.
//
// Everything here is a pure function. The reconciler owns the I/O and the clock.
package inspection

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sensoriov1 "github.com/mercadoalex/db-sensor-operator/api/v1"
	"github.com/mercadoalex/db-sensor-operator/internal/deployer"
)

const (
	// ComponentLabel and ComponentValue label every inspection Job and pod, so the operator's cache
	// can be limited to them instead of holding every pod of the cluster.
	ComponentLabel = "app.kubernetes.io/name"
	ComponentValue = "db-sensor-inspector"
	// SensorLabel names the DatabaseSensor a Job belongs to.
	SensorLabel = "sensor.io/sensor"
	// GenerationAnnotation records the spec generation the Job inspects. The result can be read
	// after the spec has moved on, and it must be attributed to the generation it was run for.
	GenerationAnnotation = "sensor.io/observed-generation"

	// ContainerName is the name of the inspector container.
	ContainerName = "inspector"

	// DefaultTimeoutSeconds applies when spec.snapshot.inspection.timeoutSeconds is not set.
	DefaultTimeoutSeconds = 30

	// finishedJobTTLSeconds is a safety net: the operator deletes a Job as soon as it has read its
	// result, and this removes one it never got to.
	finishedJobTTLSeconds = 600

	maxJobNameLength = 63
	inspectorUser    = int64(65532) // the distroless "nonroot" user
)

// Errors that stop a Job from being built. Each maps to a condition reason.
var (
	ErrNoImage        = errors.New("the operator does not know its own image, so it cannot start the inspector")
	ErrNoDatabaseName = errors.New("spec.database.name is required to inspect a PostgreSQL database")
	ErrEngine         = errors.New("inspection is only available for PostgreSQL")
)

// JobParams are the operator-wide settings of an inspection Job.
type JobParams struct {
	// Image is the operator's own image, which carries /inspector.
	Image string
	// PullPolicy is the pull policy to give the Job's container; empty uses Kubernetes' default.
	PullPolicy corev1.PullPolicy
}

// JobName returns the name of the inspection Job of a sensor. It is stable, so at most one Job
// exists per sensor, and it fits the 63 characters a Job name can have as a pod label.
func JobName(sensorName string) string {
	base := sensorName + "-inspect"
	if len(base) <= maxJobNameLength {
		return base
	}
	sum := sha256.Sum256([]byte(sensorName))
	return base[:maxJobNameLength-9] + "-" + hex.EncodeToString(sum[:])[:8]
}

// TimeoutSeconds is the deadline of one inspection for this sensor.
func TimeoutSeconds(sensor *sensoriov1.DatabaseSensor) int32 {
	if c := config(sensor); c != nil && c.TimeoutSeconds > 0 {
		return c.TimeoutSeconds
	}
	return DefaultTimeoutSeconds
}

func config(sensor *sensoriov1.DatabaseSensor) *sensoriov1.InspectionConfig {
	if sensor.Spec.Snapshot == nil {
		return nil
	}
	return sensor.Spec.Snapshot.Inspection
}

// Enabled reports whether the user asked for the inspection.
func Enabled(sensor *sensoriov1.DatabaseSensor) bool {
	c := config(sensor)
	return c != nil && c.Enabled
}

// BuildJob builds the Job that inspects a sensor's database. It has no owner reference; the caller
// sets it. The pod is hardened the way the experiment verified: non-root, read-only root filesystem,
// no capabilities and no Kubernetes API token, because the inspector needs no API access, only the
// database password the Secret gives it.
func BuildJob(sensor *sensoriov1.DatabaseSensor, p JobParams) (*batchv1.Job, error) {
	if sensor.Spec.Database.Type != "postgresql" {
		return nil, ErrEngine
	}
	if sensor.Spec.Database.Name == "" {
		return nil, ErrNoDatabaseName
	}
	if p.Image == "" {
		return nil, ErrNoImage
	}

	timeout := TimeoutSeconds(sensor)
	// The inspector's own deadline is shorter than the Job's, so it can write its failure reason
	// before the Job's deadline kills it.
	inner := max(timeout*4/5, 1)

	labels := map[string]string{ComponentLabel: ComponentValue, SensorLabel: sensor.Name}
	db := sensor.Spec.Database
	env := []corev1.EnvVar{
		{Name: "INSPECT_ENGINE", Value: "postgresql"},
		{Name: "INSPECT_HOST", Value: db.Host},
		{Name: "INSPECT_PORT", Value: strconv.Itoa(int(db.Port))},
		{Name: "INSPECT_USER", Value: db.User},
		{Name: "INSPECT_DATABASE", Value: db.Name},
		{Name: "INSPECT_TABLES", Value: strings.Join(sensor.Spec.Tables, ",")},
		{Name: "INSPECT_SLOT", Value: deployer.SlotName(sensor)},
		{Name: "INSPECT_TIMEOUT_SECONDS", Value: strconv.Itoa(int(inner))},
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: JobName(sensor.Name), Namespace: sensor.Namespace, Labels: labels,
			Annotations: map[string]string{GenerationAnnotation: strconv.FormatInt(sensor.Generation, 10)},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            new(int32(0)),
			ActiveDeadlineSeconds:   new(int64(timeout)),
			TTLSecondsAfterFinished: new(int32(finishedJobTTLSeconds)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: new(false),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:   new(true),
						RunAsUser:      new(inspectorUser),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name:                     ContainerName,
						Image:                    p.Image,
						ImagePullPolicy:          p.PullPolicy,
						Command:                  []string{"/inspector"},
						TerminationMessagePolicy: corev1.TerminationMessageReadFile,
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: new(false),
							ReadOnlyRootFilesystem:   new(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("10m"),
								corev1.ResourceMemory: resource.MustParse("32Mi"),
							},
							Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")},
						},
						// The Secret is exposed with the same DATABASE_ prefix Debezium uses, so one
						// Secret serves both and the password stays out of the Job's own spec.
						EnvFrom: []corev1.EnvFromSource{{
							Prefix: "DATABASE_",
							SecretRef: &corev1.SecretEnvSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: db.PasswordSecret},
							},
						}},
						Env: env,
					}},
				},
			},
		},
	}, nil
}

// Describe returns a short text for logs.
func Describe(j *batchv1.Job) string { return fmt.Sprintf("%s/%s", j.Namespace, j.Name) }
