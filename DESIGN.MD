# Lactifluus (DatabaseSensor Operator) — Documento de Diseño

## 1. Visión General

**Lactifluus** es un operator de Kubernetes basado en Debezium que orquesta la observabilidad de métricas, el fan-out de alertas multi-sink y la detección de DDL (cambios de esquema), implementado como el custom resource `DatabaseSensor`.

Un **operator de Kubernetes** en Go que usa **Debezium** (CDC) como sensor de observabilidad de bases de datos. Permite declarar en un YAML "monitorea estas tablas y avísame si X", y el operator orquesta Debezium + Prometheus + Alertmanager automáticamente.

### Problema que resuelve

- Sin operator: script cron + `kubectl` + oraciones para monitorear cambios en BD
- Con operator: un YAML declarativo que despliega, monitorea y alerta

### Diferencia con el Debezium Operator oficial

El oficial (Apache 2.0, Java) solo gestiona el lifecycle de Debezium Server (deploy, scale, config). **No** hace:

- Derivar métricas de observabilidad (insert/update/delete rate, lag, schema changes)
- Crear `ServiceMonitor` / `AlertRules`
- Alertar por patrones de mutación
- Exponer un status rico en el CR

Este operator cubre ese gap como **capa de observabilidad** encima del oficial.

---

## 2. Arquitectura
   ┌─────────────────────────────────────────────────────────────────┐
│ Debezium Server (lo gestiona el Debezium Operator oficial) │
│ Emite CDC events → Kafka / HTTP sink │
└──────────────────────────┬──────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────────┐
│ DatabaseSensor Operator (el nuestro) │
│ │
│ 1. Crea un DebeziumServer CR (el oficial lo despliega) │
│ 2. Consume eventos (Kafka consumer o HTTP listener) │
│ 3. Deriva métricas: insert_rate, lag, schema_changes... │
│ 4. Expone SIEMPRE en /metrics (formato Prometheus) │
│ 5. Según spec.sink → fan-out a múltiples backends │
└──────────────────────────┬──────────────────────────────────────┘
│
┌────────────┼────────────────┐
▼ ▼ ▼
┌──────────────┐ ┌──────────┐ ┌──────────────┐
│ Prometheus │ │ Datadog │ │ Webhook / │
│ (scrape) │ │ (push) │ │ Dashboard │
└──────────────┘ └──────────┘ └──────────────┘

### Diagrama actualizado 

┌─────────────────────────────────────────────────────────────────┐
│  Tu CR: DatabaseSensor                                           │
│  spec:                                                           │
│    database: { host, port, user, passwordSecret }                │
│    tables: ["orders", "users", "payments"]                       │
│    sink:                                                         │
│      type: all                                                   │
│      prometheus: { namespace, alertRules[] }                     │
│      datadog:    { enabled, apiKeySecret, tags }                 │
│      webhook:    { url, triggerOn[] }                            │
│    dashboard: { enabled, retention }                             │
└──────────────────────────┬──────────────────────────────────────┘
                           │ Reconcile (cada 30s)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  El operator crea / asegura:                                     │
│                                                                 │
│  1. DebeziumServer CR  → el Debezium Operator oficial lo         │
│                          despliega (Deployment+ConfigMap+Secret) │
│  2. ServiceMonitor     → Prometheus scrapea /metrics             │
│  3. PrometheusRule     → alertas (lag, no-events, error-rate)   │
│  4. Service            → expone /metrics + /dashboard + /api     │
│                                                                 │
│  El operator TAMBIÉN:                                            │
│  • Consume eventos CDC (Kafka consumer / HTTP)                   │
│  • Deriva métricas (insert_rate, lag, schema_changes)           │
│  • Fan-out de alertas → Prometheus + Datadog + Webhook          │
│  • Actualiza Status: Phase, Lag, EventsPerMin, LastError        │
└─────────────────────────────────────────────────────────────────┘

Dependencia externa:
  helm install debezium-operator debezium/debezium-operator   


### Decisión de diseño clave

- **No** redesploegamos Debezium desde cero
- El **Debezium Operator oficial** (instalado vía Helm) gestiona el lifecycle
- Nuestro operator crea un `DebeziumServer` CR como recurso hijo
- Nuestra lógica extra: ServiceMonitor, AlertRules, métricas derivadas, status enriquecido

---
### Sequence Diagram

User          K8s API       Reconciler      Debezium Op     Prometheus    Alertmanager
 │               │               │               │               │               │
 │──apply CR──→  │               │               │               │               │
 │               │──watch──→     │               │               │               │
 │               │               │──create DebeziumServer CR──→  │               │
 │               │               │               │──deploy Pod──→│               │
 │               │               │               │               │               │
 │               │               │──create ServiceMonitor────────→│               │
 │               │               │──create PrometheusRule─────────→│               │
 │               │               │──create Service────────────────→│               │
 │               │               │               │               │               │
 │               │               │  [Debezium emits CDC events]   │               │
 │               │               │←──consume events───────────────│               │
 │               │               │──update metrics────────────────→│               │
 │               │               │               │               │──scrape───────│
 │               │               │               │               │               │
 │               │               │  [lag > threshold]             │               │
 │               │               │──update status (Degraded)──→   │               │
 │               │               │──POST webhook──────────────────────────────────│
 │               │               │               │               │──alert───────→│
 │               │               │               │               │               │──notify user
 │               │               │               │               │               │
 │──kubectl get──│               │               │               │               │
 │←──status: Degraded, lag: 42s──│               │               │               │   


---
### Decisiones de Arquitectura (ADRs resumidas)

| # | Decisión | Alternativa rechazada | Razon |
|---|---|---|---|
| **ADR-01** | Crear `DebeziumServer` CR (el oficial lo despliega) | Crear Deployment+ConfigMap+Secret manualmente | No duplicar lógica de lifecycle; el oficial ya maneja upgrades, restarts, config reload |
| **ADR-02** | Go + Kubebuilder + `controller-runtime` | Java (Operator SDK), Python (kopf) | Ecosistema nativo de K8s, librerías optimizadas, binario estático sin runtime |
| **ADR-03** | `Sink` como interface + `CompositeSink` (fan-out) | Hardcodear cada backend en el Reconcile | Open/closed: agregar Datadog, Grafana Cloud, o un sink nuevo = nueva implementación, cero cambios en el core |
| **ADR-04** | `/metrics` **siempre** expuesto (formato Prometheus) | Solo exponer cuando `sink.type=prometheus` | Prometheus es el puente universal; el operator debe ser scrapeable sin configuración extra. Los otros sinks son *aditivos* |
| **ADR-05** | `RequeueAfter: 30s` para health checks | `CronJob` + Pod efímero | El controller tiene estado en memoria (`LagTracker`); CronJob no lo tiene. Además es nativo del reconciler loop, cero recursos extra |
| **ADR-06** | Owner references en todos los recursos hijos | Finalizers + limpieza manual | Cascade deletion automática por el GC de K8s; si borras el `DatabaseSensor`, todo se limpia solo |
| **ADR-07** | Status subresource (no en el mismo objeto) | Status como campo del spec | Separa lo que controla el usuario (spec) de lo que controla el operator (status). Evita conflictos de write |
| **ADR-08** | **No** guardar estado en el proceso del controller | In-memory cache de "qué ya hice" | El controller puede reiniciarse, escalar, o fallar. :inlineCitations{data="&#91;&#123;&quot;url&quot;&#58;&quot;https&#58;//dev.to/piyushjajoo/kubernetes-operators-a-deep-dive-into-the-internals-221m&quot;,&quot;favicon&quot;&#58;&quot;https&#58;//imgs.search.brave.com/UUpF8I85K--vkQzrhameM7iRPRU5VCTXyWbS4sk6G2g/rs&#58;fit&#58;32&#58;32&#58;1&#58;0/g&#58;ce/aHR0cDovL2Zhdmlj/b25zLnNlYXJjaC5i/cmF2ZS5jb20vaWNv/bnMvNTg5MTcxNDBh/N2U4ODc4MDg0NWQ4/MzU0YWJkNDY3ZWRm/MjUxY2U1YjY5MzJm/YWRlOTI4YjAyZDI1/ZjU0MmJjNi9kZXYu/dG8v&quot;,&quot;title&quot;&#58;&quot;Kubernetes&#32;Operators&#58;&#32;A&#32;Deep&#32;Dive&#32;into&#32;the&#32;Internals&#32;-&#32;DEV&#32;Community&quot;,&quot;snippet&quot;&#58;&quot;###&#32;Anti-Patterns&#32;to&#32;Avoid&#92;n**Don't&#32;store&#32;state&#32;in&#32;the&#32;controller&#32;process.**&#32;Your&#32;controller&#32;can&#32;be&#32;restarted,&#32;scaled,&#32;or&#32;fail&#32;over&#32;at&#32;any&#32;moment.&#32;The&#32;only&#32;source&#32;of&#32;truth&#32;is&#32;the&#32;Kubernetes&#32;API.&#32;If&#32;you&#32;need&#32;to&#32;persist&#32;computed…&quot;&#125;&#93;"} La única source of truth es la API de K8s. Si necesitas persistir algo, va en `status` o un `ConfigMap` |
| **ADR-09** | Server-side Apply (SSA) para create-or-update | `Create` + fallback `Update` | Declarativo, idempotente, maneja field ownership correctamente. :inlineCitations{data="&#91;&#123;&quot;url&quot;&#58;&quot;https&#58;//dev.to/piyushjajoo/kubernetes-operators-a-deep-dive-into-the-internals-221m&quot;,&quot;favicon&quot;&#58;&quot;https&#58;//imgs.search.brave.com/UUpF8I85K--vkQzrhameM7iRPRU5VCTXyWbS4sk6G2g/rs&#58;fit&#58;32&#58;32&#58;1&#58;0/g&#58;ce/aHR0cDovL2Zhdmlj/b25zLnNlYXJjaC5i/cmF2ZS5jb20vaWNv/bnMvNTg5MTcxNDBh/N2U4ODc4MDg0NWQ4/MzU0YWJkNDY3ZWRm/MjUxY2U1YjY5MzJm/YWRlOTI4YjAyZDI1/ZjU0MmJjNi9kZXYu/dG8v&quot;,&quot;title&quot;&#58;&quot;Kubernetes&#32;Operators&#58;&#32;A&#32;Deep&#32;Dive&#32;into&#32;the&#32;Internals&#32;-&#32;DEV&#32;Community&quot;,&quot;snippet&quot;&#58;&quot;#&#32;Kubernetes&#32;Operators&#58;&#32;A&#32;Deep&#32;Dive&#32;into&#32;the&#32;Internals&#92;n…and&#32;Anti-Patterns&#92;n###&#32;Patterns&#32;to&#32;Follow&#92;n**Adopt&#32;Whenever&#32;Possible**&#58;&#32;Use&#32;server-side&#32;apply&#32;(client.Apply)&#32;instead&#32;of&#32;create-or-update.&#32;It's&#32;declarative,&#32;handles&#32;field&#32;ownership&#32;correctly,&#32;and&#32;is&#32;idempotent&#32;by&#32;design.&#32;One&#32;critical&#32;caveat&#58;&#32;if&#32;you&#32;adopt&#32;S…&quot;&#125;&#93;"} **Caveat:** usar SSA *consistentemente* en todos los recursos que manages; mezclar SSA y Update causa conflictos de `managedFields` |
| **ADR-10** | Un solo CRD (`DatabaseSensor`), un solo controller | Múltiples CRDs (uno por BD type, uno por sink) | Single responsibility: un Kind = un reconciler. La variación (postgres vs mysql, prometheus vs datadog) se maneja con campos en el spec, no con Kinds distintos |
| **ADR-11** | Status híbrido: `[]metav1.Condition` (fuente de verdad) + campos de medición planos (`lag`, `eventsPerMin`, `lastEvent`) + `phase` derivado | Solo strings planos (diseño original) **o** solo conditions | Conditions dan `observedGeneration`, `lastTransitionTime`, `reason`/`message` y multi-dimensionalidad; las mediciones siguen planas para printcolumns. Ver [docs/adr/ADR-011-hybrid-status-design.md](docs/adr/ADR-011-hybrid-status-design.md) |
| **ADR-12** | Transporte de eventos: HTTP por defecto, Kafka opcional, ambos detrás de la interface `Consumer`, seleccionado por `spec.transport.type` (separado de `spec.sink`) | Kafka por defecto **o** diferir Kafka por completo **o** meter transport dentro de `spec.sink` | HTTP no necesita broker (test/e2e sin infra); el cambio a Kafka es un solo campo del CR aplicado con `kubectl`, sin refactor (downstream solo ve `CDCEvent`). Ver [docs/adr/ADR-012-event-transport-http-default-kafka-optional.md](docs/adr/ADR-012-event-transport-http-default-kafka-optional.md) |

> **Nota:** el `DatabaseSensorStatus` plano mostrado más abajo (§6) queda reemplazado por el diseño híbrido de ADR-11. US-5 se sigue cumpliendo (`phase`, `lag`, `eventsPerMin`, `lastEvent` como campos/printcolumns; `lastError` vía el `message` de una condition).

### Decisiones finales / estado de implementación

Resumen de lo que **realmente se construyó** (el resto del documento recoge el diseño original; esta
nota lo reconcilia con el código final). Las secciones de arriba siguen siendo válidas salvo donde esta
nota las anota.

- **Status híbrido (ADR-11):** `[]metav1.Condition` como fuente de verdad + mediciones planas
  (`lag`, `eventsPerMin`, `lastEvent`) + `phase` derivado en cada reconcile. `lastError` se transporta
  vía el `message`/`reason` de una condition, no como campo propio. Ver
  [docs/adr/ADR-011-hybrid-status-design.md](docs/adr/ADR-011-hybrid-status-design.md).
- **Transporte HTTP por defecto, Kafka opcional (ADR-12):** ambos detrás de la interface `Consumer`,
  seleccionado por `spec.transport.type` (ortogonal a `spec.sink`). HTTP no requiere broker. Ver
  [docs/adr/ADR-012-event-transport-http-default-kafka-optional.md](docs/adr/ADR-012-event-transport-http-default-kafka-optional.md).
- **DebeziumServer como objeto `unstructured`:** el `DebeziumServer` del operator oficial se crea/actualiza
  como `unstructured.Unstructured` (sin importar sus tipos Go), con owner reference al `DatabaseSensor`.
- **Servidor HTTP unificado del operator en `:8080`:** sirve `/metrics` (siempre), `/dashboard` +
  `/api/events` (gated por `dashboard.enabled`, 404 cuando está off), `/cdc`, `/healthz`, `/readyz`.
  Mapa de puertos: `:8080` HTTP del operator, `:8081` health probes del manager, `:8443` métricas del
  manager protegidas por kube-RBAC.
- **Cliente Datadog thin-HTTP:** el sink Datadog hace POST directo a la API HTTP (sin SDK pesado),
  leyendo las keys desde Secrets de K8s.
- **ServiceMonitor/PrometheusRule cross-namespace vía owner refs:** cada sensor obtiene un `Service`
  propio y los `ServiceMonitor`/`PrometheusRule` se enlazan por owner reference para cascade deletion.
- **NetworkPolicy opt-in (default off):** el chart puede desplegar una `NetworkPolicy` que restringe el
  ingress al puerto de métricas; se activa con `--set networkPolicy.enabled=true` y requiere un CNI que
  haga enforcement (Calico, Cilium, Antrea, Azure CNI).

---
### TDD para un Operator
## La piramide de tests

        ┌──────────┐
        │  kind    │  ← E2E: RBAC real, image pull, leader election
        │  (e2e)   │     (1-3 tests, lentos, solo en CI)
        └────┬─────┘
     ┌───────┴────────┐
     │   envtest      │  ← Integration: API server real + etcd,
     │  (integration) │     CRD instalado, reconciler corre de verdad
     └───────┬────────┘     (la mayoría de tus tests viven aquí)
  ┌──────────┴──────────────┐
  │  Unit (testify)         │  ← Puro: LagTracker, parser, sinks
  │  (rápido, sin K8s)      │     (miles de tests, <1s total)
  └─────────────────────────┘   

---
### El principio TDD aplicado al reconciler
## No testees acciones. Testees el estado deseado.

// ❌ MAL: testear que "se creó un Deployment"
It("should create a DebeziumServer", func() {
    // aplicar CR
    // assert que existe el DebeziumServer
})

// ✅ BUE: testear que el estado converge (idempotencia)
It("should converge to Running state", func() {
    // aplicar CR
    Eventually(func() string {
        var sensor v1.DatabaseSensor
        k8sClient.Get(ctx, name, &sensor)
        return sensor.Status.Phase
    }, timeout).Should(Equal("Running"))
})   

### El "Second Reconcile Test" (test de idempotencia)
## La regla de oro de un operator: ejecutar Reconcile N veces debe dar el mismo resultado.

It("should be idempotent across multiple reconciles", func() {
    // 1. Aplicar CR → esperar Running
    // 2. Forzar reconcile 5 veces más (patch el resourceVersion)
    // 3. Assert: no se duplican recursos, status no cambia, no hay errores
})   
---
### Orden TDD por feature

| Feature | Test primero (envtest) | Luego implementas |
|---|---|---|
| CRD + Reconciler básico | "Al aplicar un CR, el status pasa a `Pending`" | Reconcile skeleton + status update |
| Crear DebeziumServer | "Al aplicar un CR, existe un `DebeziumServer` con ownerRef correcto" | `EnsureDebeziumServer` |
| Delete cascade | "Al borrar el CR, el `DebeziumServer` desaparece" | Owner references (ya lo hace K8s, solo testear) |
| Missing Secret | "Si el Secret no existe, status → `Failed` con mensaje" | Error handling en deployer |
| Metrics | "Tras N eventos, `/metrics` expone `debezium_events_total == N`" | Exporter + consumer |
| Lag | "Tras un evento con `ts_ms` viejo, `debezium_lag_seconds > 30`" | LagTracker |
| Degraded | "Si no hay eventos por 5min, status → `Degraded`" | HealthChecker + RequeueAfter |
| Webhook alert | "Si lag > threshold, se recibe un POST en el webhook" | WebhookSink (con `httptest.Server`) |
| ServiceMonitor | "Si `serviceMonitor: true`, existe un ServiceMonitor CR" | `EnsureServiceMonitor` |

--- 
### Setup de envtest (el 80% de tus tests)

// internal/controller/suite_test.go
var testEnv *envtest.Environment

var _ = BeforeSuite(func() {
    testEnv = &envtest.Environment{
        CRDDirectoryPaths: []string{filepath.Join("..", "..", "config", "crd", "bases")},
    }
    cfg, _ = testEnv.Start()  // levanta kube-apiserver + etcd en memoria
    // registrar schemes, crear client, arrancar manager
})

var _ = AfterSuite(func() {
    testEnv.Stop()
})   

# Instalar los binarios de envtest (una vez)
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
setup-envtest use 1.31.0 -p path
# → /home/you/.local/share/kubebuilder-envtest/k8s/1.31.0-linux-amd64

# Correr tests
export KUBEBUILDER_ASSETS=$(setup-envtest use 1.31.0 -p path)
go test ./internal/controller/... -v -coverprofile cover.out   

### Lo que envtest NO testea (y por qué kind entra)
| Problema | envtest | kind |
|---|---|---|
| RBAC incorrecto | ❌ No enforcea RBAC | ✅ Sí |
| Image pull falla | ❌ No hay kubelet | ✅ Sí |
| Leader election | ❌ Solo 1 replica | ✅ Sí |
| Webhook TLS certs | ❌ No hay networking real | ✅ Sí |
| Deployment YAML roto | ❌ No hay scheduler | ✅ Sí |
---

### Resumen: las 3 reglas TDD que importan aquí

Testea convergencia, no acciones. "El estado final es X", no "se llamó a Create()".
Idempotencia es un test. Si tu reconciler no pasa el "second reconcile test", está roto.
El 80% de tus tests son envtest. Unit tests para lógica pura (LagTracker, parser, sinks). Envtest para todo lo que toca la API de K8s. Kind solo para lo que envtest no puede (RBAC, networking, imágenes)

---
## 3. El CRD — `DatabaseSensor`

### Ejemplo de uso

```yaml
apiVersion: sensor.io/v1
kind: DatabaseSensor
metadata:
  name: pg-orders
  namespace: prod
spec:
  database:
    host: postgres-svc.prod
    port: 5432
    user: debezium
    passwordSecret: db-creds
  tables: ["orders", "users", "payments"]

  sink:
    type: all  # prometheus | datadog | webhook | all
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
---

## Métricas derivadas
### Métrica	Cálculo	Alerta

| Métrica | Cálculo | Alerta |
|---|---|---|
| `insert_rate` | Count de `op=c` por ventana | `< N/min` → "¿se cayó el tráfico?" |
| `update_rate` | Count de `op=u` | `> N/min` → "¿bulk update?" |
| `delete_rate` | Count de `op=d` | `> N/min` → "¿borrado masivo?" |
| `schema_change` | Evento de DDL detectado | Cualquier DDL → "¿migración sin aviso?" |
| `lag` | `ts_ms` del evento vs. `now()` | `> 30s` → "Debezium se queda atrás" |

---
## 4. BDs soportadas

| BD | Mecanismo | Madurez |
|---|---|---|
| PostgreSQL | Logical decoding (pgoutput, wal2json) | Estable |
| MySQL / MariaDB | Binary log (binlog) | Estable |
| MongoDB | Change Streams / Oplog | Estable |
| SQL Server | CDC Tables / LSN | Estable |
| Oracle | LogMiner / XStream | Estable |
| Db2 | ASN Capture | Estable |
| Cassandra | CommitLog (cdc_raw) | Estable |
| Vitess | VStream gRPC | Incubating |
| Spanner | Change Streams | Incubating |
| CockroachDB | — | Incubating |
| Informix | Change Streams API | Incubating |
| Milvus | — | Incubating |


---
## 5. Estructura del proyecto

```estructura
db-sensor-operator/
├── DESIGN.md                        ← Este documento
├── api/v1/
│   └── databasesensor_types.go      # Spec/Status structs
├── internal/
│   ├── controller/
│   │   └── databasesensor_controller.go  # Reconcile
│   ├── deployer/                    # Crea DebeziumServer CR + RBAC
│   ├── metrics/                     # ServiceMonitor + AlertRules
│   ├── sink/                        # Fan-out: Prometheus, Datadog, Webhook
│   └── health/                      # Probes + lag tracking + status
├── config/
│   ├── crd/bases/
│   ├── rbac/
│   ├── manager/
│   └── samples/
├── cmd/
│   └── main.go
├── Dockerfile
├── Makefile
└── go.mod   
```
---
## 6. Código clave
### Spec/Status

type DatabaseSensorSpec struct {
    Database DatabaseConfig `json:"database"`
    Tables   []string       `json:"tables"`
    Sink     SinkConfig     `json:"sink"`
    Dashboard *DashboardConfig `json:"dashboard,omitempty"`
}

type DatabaseConfig struct {
    Host           string `json:"host"`
    Port           int32  `json:"port"`
    User           string `json:"user"`
    PasswordSecret string `json:"passwordSecret"`
}

type SinkConfig struct {
    Type       string          `json:"type"` // prometheus | datadog | webhook | all
    Prometheus *PrometheusSink `json:"prometheus,omitempty"`
    Datadog    *DatadogSink    `json:"datadog,omitempty"`
    Webhook    *WebhookSink    `json:"webhook,omitempty"`
}

type DatabaseSensorStatus struct {
    Phase        string `json:"phase,omitempty"`        // Pending | Running | Degraded | Failed
    Lag          string `json:"lag,omitempty"`
    EventsPerMin float64 `json:"eventsPerMin,omitempty"`
    LastEvent    string `json:"lastEvent,omitempty"`
    LastError    string `json:"lastError,omitempty"`
}   

### Scheduling

El operator no usa `CronJob`. El bucle de reconciliación se reencucola
cada 30 segundos (`RequeueAfter`), lo que actúa como health check
periódico. Los eventos CDC en sí son **event-driven** (Debezium los
emite en tiempo real), así que no hay polling: el `LagTracker` solo
se actualiza cuando llega un evento. El `RequeueAfter` sirve para
detectar el caso inverso: **ausencia** de eventos (lag creciente).   

### Reconcile (esqueleto)

func (r *DatabaseSensorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    var sensor v1.DatabaseSensor
    r.Get(ctx, req.NamespacedName, &sensor)

    // 1. Crear/actualizar DebeziumServer CR (el operator oficial lo despliega)
    r.deployer.EnsureDebeziumServer(ctx, &sensor)

    // 2. Crear ServiceMonitor para Prometheus
    r.metrics.EnsureServiceMonitor(ctx, &sensor)

    // 3. Crear Alertmanager rules
    r.metrics.EnsureAlertRules(ctx, &sensor)

    // 4. Health check: ¿el pod está up? ¿cuál es el lag?
    phase, lag := r.health.Check(ctx, &sensor)
    sensor.Status.Phase = phase
    sensor.Status.Lag = lag.String()
    r.Status().Update(ctx, &sensor)

    // 5. Requeue para health check periódico
    return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}   

---
## Lag Tracker


type LagTracker struct {
    mu     sync.Mutex
    lastTs time.Time
    gauge  prometheus.Gauge
}

func (t *LagTracker) OnEvent(tsMs int64) {
    t.mu.Lock()
    defer t.mu.Unlock()
    eventTime := time.UnixMilli(int64(tsMs))
    t.lastTs = eventTime
    t.gauge.Set(time.Since(eventTime).Seconds())
}   
---
### Sink (fan-out)

type Sink interface {
    Push(ctx context.Context, metric Metric) error
    Alert(ctx context.Context, alert Alert) error
}

// Implementaciones:
type PrometheusSink struct { ... }  // Pasivo: expone /metrics
type DatadogSink struct { ... }     // Activo: push vía Datadog API
type WebhookSink struct { ... }     // POST JSON al URL
type CompositeSink struct { ... }   // Fan-out a múltiples   


---
### 7. Alertas de alta latencia (lag)
## Cálculo

lag = now() - ts_ms_del_último_evento   

### Flujo

Debezium emite evento con ts_ms
    → Operator lo consume
    → Calcula lag
    → Expone: debezium_lag_seconds{db="pg-orders", table="orders"} 42.3
    → Prometheus scrapea /metrics cada 15s
    → PrometheusRule evalúa: debezium_lag_seconds > 30 for 5m
    → Alertmanager → Slack / PagerDuty / Datadog / Dashboard   

---
### Error Handling
Error	Handling

Secret not found	Set status to Failed, log error, requeue in 30s
DebeziumServer CR creation fails	Return error, controller-runtime retries with backoff
Kafka consumer disconnects	Reconnect with exponential backoff (max 60s)
Webhook POST fails	Log warning, do NOT fail the reconcile (non-blocking)
Datadog API returns 429	Respect Retry-After header, retry next cycle
Pod in CrashLoopBackOff	Set status to Failed with pod termination message
No events for 5 min	Set status to Degraded, log warning
No events for 30 min	Set status to Failed, log error
---
### Testing Strategy

Layer	Tool	What to test
Unit	testify	LagTracker, CDCEvent parser, Sink implementations, phase transitions
Integration	envtest	Reconciler creates correct CRs, status transitions, owner references
E2E	kind cluster	Full flow: apply CR → Debezium deploys → events flow → metrics appear → alert fires
Property-based	gopter (optional)	Lag calculation with random event sequences
---
### Key Design Decisions
| Decision | Rationale |
|---|---|
| Create DebeziumServer CR instead of Deployment | Reuse the official operator's lifecycle management; avoid duplicating logic |
| Always expose /metrics | Prometheus format is the universal bridge; other sinks are additive |
| RequeueAfter 30s instead of CronJob | Simpler, stateful (LagTracker in memory), no extra resources |
| Sink as interface + CompositeSink | Open for extension; user can add new sinks without modifying core |
| Owner references everywhere | Cascade deletion; no orphaned resources |
| Status subresource | Separates spec (user-managed) from status (controller-managed) |

---
### PrometheusRule generada por el operator


apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: db-sensor-pg-orders-alerts
  namespace: monitoring
spec:
  groups:
    - name: db-sensor.lag
      rules:
        - alert: DebeziumHighLag
          expr: debezium_lag_seconds > 30
          for: 5m
          labels:
            severity: warning
          annotations:
            summary: "Debezium lag alto en {{ $labels.db }}"
            description: "Lag de {{ $value | humanizeDuration }} (umbral: 30s)"
        - alert: DebeziumNoEvents
          expr: rate(debezium_events_total[5m]) == 0
          for: 15m
          labels:
            severity: critical
          annotations:
            summary: "Sin eventos CDC"
            description: "No hay eventos desde hace 15min. ¿Debezium cayó?"   
---
### Niveles de severidad

| Métrica | Warning | Critical | Acción |
|---|---|---|---|
| `lag` | `> 30s` por 5min | `> 120s` por 2min | Reintentar / escalar |
| `events_rate` | `< 10/min` por 15min | `== 0` por 30min | Verificar pod de Debezium |
| `error_rate` | `> 1%` | `> 10%` | Reiniciar connector |

---
### 8. Dashboard propio

| Opción | Stack | Esfuerzo |
|---|---|---|
| A. Embebido | Operator expone `/dashboard` con HTML+JS que lee `/metrics` vía fetch | 1 día |
| B. Grafana auto-provisioning | Operator crea ConfigMap con JSON de dashboard | 2 días |
| C. Mini-UI | Operator + SQLite + React/HTMX en Pod separado | 1 semana |

Recomendado para arrancar: Opción A.

---
### 9. Setup del stack

# 1. Instalar el Debezium Operator oficial
helm repo add debezium https://charts.debezium.io
helm install debezium-operator debezium/debezium-operator -n debezium

# 2. Instalar tu operator
helm install db-sensor ./charts/db-sensor-operator -n debezium

# 3. Crear tu CR
kubectl apply -f samples/databasesensor.yaml   

---
### Stack mínimo

K8s cluster
├── PostgreSQL (o MySQL) → la base a monitorear
├── Debezium Server (lo despliega el operator oficial)
├── Kafka o Redpanda (sink) → o HTTP sink directo
├── Prometheus + Alertmanager → consume las métricas
└── Tu operator → orquesta todo   

---
### 10. Casos de uso de CDC (más allá del monitoreo)


| Caso de uso | Descripción |
|---|---|
| **Observabilidad / Alertas** | ← Lo que construimos. Detectar anomalías en el tráfico de datos |
| **Replicación en tiempo real** | Sincronizar dos BDs (master-slave, multi-región) |
| **Data warehouse en tiempo real** | Alimentar Snowflake/BigQuery con cambios incrementales |
| **Cache invalidation** | Borrar/actualizar Redis o Elasticsearch cuando cambia la BD |
| **Event-driven microservices** | Emitir eventos a Kafka para que otros servicios reaccionen |
| **Audit / Compliance** | Trazar quién cambió qué y cuándo (GDPR, SOX, HIPAA) |
| **Migración zero-downtime** | Migrar de una BD a otra sin parar la app |
| **CQRS** | Mantener el read-model sincronizado con el write-model |
| **Fraud detection** | Detectar patrones sospechosos en transacciones en tiempo real |

---
### ¿Por qué NO un CronJob?

| | CronJob | RequeueAfter |
|---|---|---|
| Granularidad | Mínimo 1 min | Puedes poner 10s, 30s, lo que quieras |
| Contexto | Pod nuevo, no tiene estado | Mismo process, tiene `LagTracker` en memoria |
| Sincronización | No sabe si ya corrió | El controller-runtime garantiza no solaparse |
| Dependencia | Extra (Job + Pod efímero) | Cero, es nativo del reconciler loop |

---
### 11. Plan de desarrollo

| Semana | Hito |
|---|---|
| 1 | CRD se aplica + reconciler loguea "vi un DatabaseSensor" + status `Pending` |
| 2 | Crea `DebeziumServer` CR + Debezium Server corre + eventos fluyen |
| 3 | Métricas derivadas + `/metrics` expuesto + ServiceMonitor |
| 4 | Alertas (PrometheusRule + Webhook) + status enriquecido (`Degraded`, `Failed`) |
| 5 | Dashboard embebido + Datadog sink + docs |

---
### Comandos de arranque
kubebuilder init --domain sensor.io --repo github.com/tuuser/db-sensor-operator
kubebuilder create api --group sensor --version v1 --kind DatabaseSensor
make dev   # corre local con envtest   

---
## Tabla resumen de como majerar errores (aun en definicion) 

| Situación | Return | Status |
|---|---|---|
| CR borrado (NotFound en Get) | `ctrl.Result{}, nil` | — |
| Spec inválido | `ctrl.Result{}, nil` | `Failed` + mensaje |
| Secret no existe | `ctrl.Result{RequeueAfter: 30s}, nil` | `Failed` + "Secret not found" |
| DebeziumServer CRD no existe | `ctrl.Result{}, nil` | `Failed` + "CRD not found" |
| API server timeout / 503 | `ctrl.Result{}, err` | No cambiar (backoff) |
| Conflict en status update | `retry.RetryOnConflict` interno | — |
| Pod de Debezium en CrashLoop | `ctrl.Result{RequeueAfter: 30s}, nil` | `Failed` + termination message |
| No eventos por 5min | `ctrl.Result{RequeueAfter: 30s}, nil` | `Degraded` |
| Webhook POST falla | `ctrl.Result{}, nil` (log + seguir) | `LastError` parcial |
| Datadog 429 | `ctrl.Result{RequeueAfter: RetryAfter}, nil` | No cambiar |
| Todo OK | `ctrl.Result{RequeueAfter: 30s}, nil` | `Running` |


### 12. Licencia y dependencias

Debezium: Apache 2.0 → sin restricciones para uso, modificación, redistribución
Debezium Operator oficial: Apache 2.0, Java (Operator SDK for Java)
Nuestro operator: Go, Kubebuilder, controller-runtime
No hay copyleft: nuestro operator no hereda ninguna licencia de Debezium

###  Nadie ha publicado un operator open source (hasta donde sabemos)

- Reciba un CR declarativo
- Despliegue Debezium (vía el oficial)
- Consuma los eventos CDC y derive métricas por operación/tabla
- Cree automáticamente ServiceMonitor + PrometheusRule
- Haga fan-out a múltiples sinks (Prometheus + Datadog + Webhook)
- Exponga un status enriquecido en el CR 
- Eso es nuestro  espacio.

### El mejor aspecto del proyecto

La abstracción de "un YAML = un sensor completo".

Hoy, para lograr lo que tu operator hace en un kubectl apply, necesitas:

1. Desplegar Debezium (Helm + config manual)
2. Configurar JMX Prometheus Exporter (ConfigMap)
3. Crear ServiceMonitor (YAML)
4. Crear PrometheusRule con alertas (YAML)
5. Crear Grafana dashboard (JSON)
6. Configurar Alertmanager routing (YAML)
7. Escribir un consumer para eventos CDC si quieres métricas por tabla (código)
8. Mantener todo sincronizado cuando cambies la BD o las tablas   

El operator colapsa todo eso en:

spec:
  database: { host, port, type }
  tables: ["orders", "users"]
  sink: { type: all, prometheus: {...}, datadog: {...}, webhook: {...} }   

## Pitch. 

"Un solo recurso declarativo que colapsa el pipeline completo de observabilidad CDC: despliegue de Debezium, consumo de eventos, derivación de métricas por tabla, alertas multi-sink y detección de schema changes — en un kubectl apply. Lo que hoy son 7+ YAMLs, un JMX exporter, un consumer de Kafka escrito a mano y un dashboard de Grafana manual, se convierte en un CR con status visible."

## killer feature ,la metrica que nadie tiene 

schema_change_detected — Alerta instantánea cuando alguien ejecuta un ALTER TABLE sin aviso.


| BD | Qué hace tu operator que nadie más hace |
|---|---|
| **PostgreSQL** | Detecta DDL por schema diffing activo (nobody does this) |
| **MySQL/Oracle** | Consume el schema change topic, clasifica el DDL (additive vs breaking), y alerta (nobody packages this as a K8s operator) |
| **Todas** | Lo hace declarativo (un CR), multi-sink (Slack + Prometheus + Datadog), con status en el CR |

"Logical decoding does not support DDL changes. This means that the connector is unable to report DDL change events back to consumers."
— Stack Overflow, respuesta aceptada 


Para Postgres, tu operator tendría que:

Polling de information_schema.columns / pg_catalog periódicamente
O usar un trigger de DDL (si la BD lo permite)
O comparar snapshots de schema cada N segundos
Eso es algo que Debezium no hace, que el JMX no expone, y que no existe como tool. Ahí sí eres el único.


---
### Referencias: 
[Current Operator on Debezium]https://debezium.io/documentation/reference/stable/operations/debezium-operator.html
[Medium Article]]https://medium.com/@alexmarket/la-captura-de-cambios-de-datos-con-debezium-el-lactarius-deceptivus-de-tu-stack-de-datos-0871aafa06cb?sk=e90546cb65ceba9b313405d0f9d10c48

---
Documento generado el 16 de octubre de 2026.
Autor: Alejandro Mercado Peña
"Debezium es el oído. Lactifluus es el cerebro que decide cuándo gritar."

"Un operator K8s que toma un DatabaseSensor CR y orquesta Debezium + métricas derivadas por evento + alertas multi-sink + detección de DDL. Sin scripts, sin consumers manuales, sin 7 YAMLs."

---
Si alguien te dice "pero Debezium ya emite eventos DDL a Kafka"
Tu respuesta (preparada):

"Sí, para MySQL/Oracle. Pero ¿quién los consume, clasifica (additive vs breaking), y alerta a Slack + Prometheus + Datadog desde un CR? Nadie. Y para PostgreSQL, ni siquiera los emite — hay que hacer schema diffing activo. Eso lo hacemos nosotros."


---
### POR DEFINIR

La conexión es perfecta: Debezium viene de deceit (engaño, en francés). El hongo se llama deceptivus porque se disfraza de lo que no es. Tu operator hace exactamente eso: se disfraza de "un helper inocente de Debezium" pero en realidad es el que ve lo que nadie más ve.

1. Lactifluus (El Conceptual)
Es el nombre taxonómicamente exacto hoy en día para la especie de tu historia, lo que hace que el pitch del README cierre de forma brillante.

La ventaja: La terminación fluus (flujo) es un guiño poético y técnico al concepto central de CDC: el flujo constante de eventos y métricas.

En la terminal: Como CRD (Custom Resource Definition) en Kubernetes, los usuarios teclearían algo como kubectl get lactifluus o podrías darle un alias corto de dos letras como lf (kubectl get lf).

Identidad: Queda perfecto para un logo tipográfico dibujado a mano, con letras que parezcan fluir o derretirse ligeramente, manteniendo esa estética cruda de boceto.


