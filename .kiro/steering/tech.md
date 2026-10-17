# Technology Stack

## Language & Framework
- **Go 1.22+**
- **Kubebuilder** (scaffolding)
- **controller-runtime** (reconciler loop, client, manager)
- **client-go** (Kubernetes API client)

## Key Dependencies
- `sigs.k8s.io/controller-runtime` — framework de operators
- `github.com/debezium/debezium-operator/api/v1` — tipos Go del DebeziumServer CR (para crear CRs del oficial)
- `github.com/prometheus/client_golang` — métricas Prometheus
- `github.com/prometheus/common` — modelo de métricas
- `github.com/robfig/cron/v3` — parsing de cron expressions (si se implementa scheduling)
- `github.com/tidwall/gjson` — parsing de eventos CDC (JSON de Debezium)
- `github.com/segmentio/kafka-go` — Kafka consumer (si sink es Kafka)
- `github.com/DataDog/datadog-api-client-go` — Datadog API (push de métricas)

## External Dependencies (instaladas por separado)
- **Debezium Operator oficial** (Apache 2.0, Java) — instalado vía Helm, gestiona el lifecycle de Debezium Server
- **Prometheus Operator** — para ServiceMonitor y PrometheusRule CRDs
- **Kafka / Redpanda** (opcional) — si el sink de Debezium es Kafka; si es HTTP, no se necesita

## Build & Deploy
- **Docker** (multi-stage: golang:1.22 builder → gcr.io/distroless/static:nonroot)
- **Helm chart** para desplegar el operator
- **Makefile** con targets: build, test, docker-build, deploy, dev, manifests, generate

## Testing
- **envtest** (sigs.k8s.io/controller-runtime/pkg/envtest) — integration tests con API server real
- **testify** (assert, require) — unit tests
- **ginkgo/gomega** (opcional) — e2e tests

## Conventions
- Go standard project layout (cmd/, internal/, api/)
- Error wrapping con `fmt.Errorf("context: %w", err)`
- Context propagation en todas las funciones que hacen I/O
- No globals: inyectar dependencias vía constructor o struct fields   