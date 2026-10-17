# Project Structure

db-sensor-operator/
├── .kiro/
│ ├── steering/
│ │ ├── product.md
│ │ ├── tech.md
│ │ └── structure.md
│ └── specs/
│ └── db-sensor/
│ ├── requirements.md
│ ├── design.md
│ └── tasks.md
├── api/
│ └── v1/
│ ├── databasesensor_types.go # Spec/Status structs + kubebuilder markers
│ ├── groupversion_info.go # SchemeBuilder, GroupVersion
│ └── zz_generated.deepcopy.go # Generado por controller-gen
├── internal/
│ ├── controller/
│ │ └── databasesensor_controller.go # Reconciler principal
│ ├── deployer/
│ │ └── debezium.go # Crea/actualiza DebeziumServer CR + RBAC
│ ├── metrics/
│ │ ├── exporter.go # /metrics endpoint + registro de métricas
│ │ ├── servicemonitor.go # Crea/actualiza ServiceMonitor
│ │ └── alertrules.go # Crea/actualiza PrometheusRule
│ ├── sink/
│ │ ├── sink.go # Interface Sink + CompositeSink
│ │ ├── prometheus.go # PrometheusSink (pasivo, expone /metrics)
│ │ ├── datadog.go # DatadogSink (push vía API)
│ │ └── webhook.go # WebhookSink (POST JSON)
│ ├── health/
│ │ ├── checker.go # Health check del DebeziumServer + pod
│ │ └── lag.go # LagTracker (ts_ms → lag calculation)
│ └── events/
│ ├── consumer.go # Kafka consumer / HTTP listener
│ └── parser.go # Parsea eventos CDC → métricas
├── cmd/
│ └── main.go # Entry point: manager + reconciler + HTTP server
├── config/
│ ├── crd/
│ │ └── bases/
│ │ └── sensor.io_databasesensors.yaml # Generado por controller-gen
│ ├── rbac/
│ │ ├── role.yaml
│ │ ├── role_binding.yaml
│ │ ├── service_account.yaml
│ │ └── leader_election_role.yaml
│ ├── manager/
│ │ └── manager.yaml # Deployment del operator
│ └── samples/
│ └── v1_databasesensor.yaml # CR de ejemplo
├── charts/
│ └── db-sensor-operator/
│ ├── Chart.yaml
│ ├── values.yaml
│ └── templates/
├── Dockerfile
├── Makefile
├── go.mod
├── go.sum
└── README.md


## Naming Conventions
- CRD kind: `DatabaseSensor`
- Grupo: `sensor.io`
- Versión: `v1`
- Namespace del operator: configurable (default: `db-sensor`)
- ServiceAccount: `db-sensor-operator`
- Labels: `app.kubernetes.io/name=db-sensor-operator`, `app.kubernetes.io/managed-by=helm`   
   