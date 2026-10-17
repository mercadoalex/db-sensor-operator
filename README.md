<p align="center">
  <img src="assets/logo_horizontal_bco.jpeg" alt="Lactiflluus — DatabaseSensor Operator" width="520">
</p>

<h1 align="center">Lactifluus — DatabaseSensor Operator</h1>

<p align="center">
  Un operator de Kubernetes basado en Debezium (CDC) como sensor de observabilidad de bases de datos.
</p>

---

## Qué es

**Lactiflluus** es un operator de Kubernetes escrito en Go que orquesta observabilidad de bases de datos a partir de Change Data Capture (CDC). Declaras en un YAML "monitorea estas tablas y avísame si X", y el operator orquesta automáticamente Debezium + Prometheus + alertas multi-sink.

Se implementa como el custom resource `DatabaseSensor` y actúa como una **capa de observabilidad** sobre el Debezium Operator oficial: deriva métricas, crea `ServiceMonitor` / `PrometheusRule`, hace fan-out de alertas a múltiples backends y expone un status enriquecido en el CR.

### Problema que resuelve

- **Sin operator:** script cron + `kubectl` + configuración imperativa de Debezium, Prometheus y Alertmanager.
- **Con operator:** un solo YAML declarativo que despliega, monitorea y alerta.

### Diferencia con el Debezium Operator oficial

El oficial (Apache 2.0, Java) solo gestiona el lifecycle de Debezium Server (deploy, scale, config). **No** deriva métricas de observabilidad, no crea `ServiceMonitor` / `AlertRules`, no alerta por patrones de mutación ni expone un status rico. Lactiflluus cubre ese gap.

## Capacidades clave

- Crear y gestionar un `DebeziumServer` (vía el Debezium Operator oficial) desde un solo CR.
- Derivar métricas: `insert_rate`, `update_rate`, `delete_rate`, `lag`, `schema_changes`.
- Exponer `/metrics` (formato Prometheus) **siempre**, independientemente del sink configurado.
- Fan-out de alertas a múltiples backends: Prometheus, Datadog, Webhook/Slack.
- Dashboard embebido con gráficos en vivo.
- Status enriquecido visible con `kubectl get`: `Phase`, `Lag`, `EventsPerMin`, `LastError`.
- Soporte multi-BD: PostgreSQL, MySQL/MariaDB, MongoDB, SQL Server, Oracle, Db2, Cassandra.

## Arquitectura

```
┌─────────────────────────────────────────────────────────────┐
│ User aplica DatabaseSensor CR                                │
└──────────────────────────┬──────────────────────────────────┘
                           │ Reconcile (cada 30s)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ DatabaseSensor Reconciler (controller-runtime)               │
│   1. EnsureDebeziumServer() → crea el DebeziumServer CR      │
│   2. EnsureServiceMonitor() → Prometheus scrapea /metrics   │
│   3. EnsureAlertRules()     → PrometheusRule (alertas)      │
│   4. EnsureService()        → expone /metrics + /dashboard  │
│   5. HealthCheck()          → pod status + lag              │
│   6. UpdateStatus()         → Phase, Lag, EventsPerMin...   │
└───────────┬───────────────────┬───────────────┬─────────────┘
            ▼                   ▼               ▼
   ┌─────────────────┐  ┌──────────────┐  ┌──────────────┐
   │ Debezium Op.    │  │ Prometheus   │  │ Datadog /    │
   │ (oficial, Java) │  │ Operator     │  │ Webhook      │
   └─────────────────┘  └──────────────┘  └──────────────┘
```

El operator **no** redespliega Debezium desde cero: crea un `DebeziumServer` CR como recurso hijo (con `ownerReference`), y el Debezium Operator oficial maneja el lifecycle. Todos los recursos hijos usan owner references, por lo que borrar el `DatabaseSensor` limpia todo en cascada.

## Ejemplo de uso

```yaml
apiVersion: sensor.io/v1
kind: DatabaseSensor
metadata:
  name: pg-orders
  namespace: prod
spec:
  database:
    type: postgresql        # postgresql | mysql | mongodb | ...
    host: postgres-svc.prod
    port: 5432
    user: debezium
    passwordSecret: db-creds
  tables: ["orders", "users", "payments"]

  sink:
    type: all               # prometheus | datadog | webhook | all
    prometheus:
      namespace: monitoring
      serviceMonitor: true
      alertRules:
        - name: HighLag
          expr: 'debezium_lag_seconds > 30'
          for: 5m
          severity: warning
        - name: NoEvents
          expr: 'rate(debezium_events_total[5m]) == 0'
          for: 15m
          severity: critical
    datadog:
      enabled: true
      apiKeySecret: dd-api-key
      appKeySecret: dd-app-key
      tags: ["env:prod", "db:postgres"]
    webhook:
      enabled: true
      url: https://hooks.slack.com/services/XXX
      triggerOn: [HighLag, NoEvents]

  dashboard:
    enabled: true
    retention: 72h
```

## Métricas derivadas

| Métrica | Cálculo | Alerta |
|---|---|---|
| `debezium_events_total{op="c"}` (insert) | Count de `op=c` por ventana | `< N/min` → ¿se cayó el tráfico? |
| `debezium_events_total{op="u"}` (update) | Count de `op=u` | `> N/min` → ¿bulk update? |
| `debezium_events_total{op="d"}` (delete) | Count de `op=d` | `> N/min` → ¿borrado masivo? |
| `debezium_schema_changes_total` | Evento DDL detectado | Cualquier DDL → ¿migración sin aviso? |
| `debezium_lag_seconds` | `now() - ts_ms` del evento | `> 30s` → Debezium se queda atrás |
| `debezium_events_per_minute` | Rate de eventos | — |

## Status (`kubectl get`)

El CR expone un status enriquecido con el subresource de status:

| Campo | Descripción |
|---|---|
| `phase` | `Pending` → `Running` → `Degraded` → `Failed` |
| `lag` | Duración legible (p. ej. `42s`) |
| `eventsPerMin` | Float con la tasa de eventos |
| `lastEvent` | Tiempo relativo del último evento |
| `lastError` | Mensaje de error (vacío si no hay) |

## Bases de datos soportadas

| BD | Mecanismo |
|---|---|
| PostgreSQL | Logical decoding (pgoutput, wal2json) |
| MySQL / MariaDB | Binary log (binlog) |
| MongoDB | Change Streams / Oplog |
| SQL Server | CDC Tables / LSN |
| Oracle | LogMiner / XStream |
| Db2 | ASN Capture |
| Cassandra | CommitLog (cdc_raw) |

## Stack tecnológico

- **Go 1.22+**, **Kubebuilder**, **controller-runtime**, **client-go**
- `github.com/prometheus/client_golang` — métricas Prometheus
- `github.com/tidwall/gjson` — parsing de eventos CDC
- `github.com/segmentio/kafka-go` — Kafka consumer (sink Kafka)
- `github.com/DataDog/datadog-api-client-go` — Datadog API
- Docker multi-stage (`golang:1.22` → `distroless/static`) + Helm chart

## Instalación

```bash
# 1. Instalar el Debezium Operator oficial
helm repo add debezium https://charts.debezium.io
helm install debezium-operator debezium/debezium-operator -n debezium

# 2. Instalar Lactiflluus
helm install db-sensor ./charts/db-sensor-operator -n db-sensor

# 3. Crear tu CR
kubectl apply -f config/samples/v1_databasesensor.yaml
```

### Prerequisitos

- Un cluster de Kubernetes.
- **Prometheus Operator** (para los CRDs `ServiceMonitor` y `PrometheusRule`).
- **Debezium Operator oficial** (gestiona el lifecycle de Debezium Server).
- Kafka / Redpanda opcional (si el sink de Debezium es Kafka; si es HTTP, no se necesita).

## Desarrollo

```bash
make build        # compilar
make test         # unit + integration (envtest)
make manifests    # regenerar CRD YAML
make generate     # regenerar deepcopy
make docker-build # construir imagen
make deploy       # desplegar en el cluster
make dev          # correr local con envtest
```

### Estrategia de testing

- **Unit (testify):** lógica pura — `LagTracker`, parser de eventos CDC, implementaciones de `Sink`.
- **Integration (envtest):** reconciler contra un API server real — convergencia de estado, owner references, transiciones de status.
- **E2E (kind):** flujo completo — RBAC real, image pull, leader election.

El principio: testear **convergencia**, no acciones ("el estado final es `Running`", no "se llamó a `Create()`"), y verificar idempotencia con el *second reconcile test*.

No hace falta un cluster, una BD, Kafka ni Datadog para la mayoría de los tests: envtest corre un API server en memoria y el pipeline de eventos se prueba con eventos CDC sintéticos y `httptest`. Guía práctica: [docs/TESTING.md](docs/TESTING.md).

## Estructura del proyecto

```
db-sensor-operator/
├── api/v1/                 # Spec/Status structs + kubebuilder markers
├── internal/
│   ├── controller/         # Reconciler principal
│   ├── deployer/           # Crea/actualiza DebeziumServer CR
│   ├── metrics/            # /metrics, ServiceMonitor, AlertRules
│   ├── sink/               # Fan-out: Prometheus, Datadog, Webhook
│   ├── health/             # Health check + LagTracker
│   └── events/             # Consumer (Kafka/HTTP) + parser CDC
├── cmd/main.go             # Entry point: manager + reconciler + HTTP server
├── config/                 # CRD, RBAC, manager, samples
├── charts/                 # Helm chart
├── Dockerfile
├── Makefile
└── DESIGN.MD               # Documento de diseño detallado
```

## Licencia

- Debezium y el Debezium Operator oficial: Apache 2.0.
- Lactiflluus se construye sobre Go, Kubebuilder y controller-runtime; no hereda copyleft de Debezium.

---

<p align="center">
  Para el diseño completo (ADRs, diagramas de secuencia, manejo de errores y plan de desarrollo), consulta <a href="DESIGN.MD">DESIGN.MD</a>.
</p>
