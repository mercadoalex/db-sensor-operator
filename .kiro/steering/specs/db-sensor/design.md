# DatabaseSensor Operator — Technical Design

## Overview

The DatabaseSensor Operator is a Kubernetes operator written in Go that creates and manages a DebeziumServer (via the official Debezium Operator) and adds an observability layer on top: metric derivation, multi-sink alerting, and a built-in dashboard.

## Architecture
   ┌─────────────────────────────────────────────────────────────────────┐
│ User applies DatabaseSensor CR │
└──────────────────────────┬──────────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────────────┐
│ DatabaseSensor Reconciler (controller-runtime) │
│ │
│ 1. EnsureDebeziumServer() → Creates DebeziumServer CR │
│ 2. EnsureServiceMonitor() → Creates ServiceMonitor CR │
│ 3. EnsureAlertRules() → Creates PrometheusRule CR │
│ 4. EnsureService() → Creates Service for /metrics │
│ 5. HealthCheck() → Checks pod status + lag │
│ 6. UpdateStatus() → Phase, Lag, EventsPerMin, LastError │
│ │
│ RequeueAfter: 30s (health check loop) │
└──────────────────────────┬──────────────────────────────────────────┘
│
┌────────────┼────────────────┐
▼ ▼ ▼
┌──────────────────┐ ┌──────────────┐ ┌──────────────────┐
│ Debezium Operator│ │ Prometheus │ │ Datadog / │
│ (oficial, Java) │ │ Operator │ │ Webhook │
│ Despliega: │ │ Scrapea: │ │ Recibe: │
│ - Deployment │ │ - /metrics │ │ - POST alertas │
│ - ConfigMap │ │ - ServiceMon │ │ - Push métricas │
│ - Secret │ │ - PromRule │ │ │
└──────────────────┘ └──────────────┘ └──────────────────┘


## Components

### 1. Reconciler (`internal/controller/databasesensor_controller.go`)

The main reconciliation loop. Called on:
- CR created/updated/deleted (event-driven)
- Every 30 seconds (RequeueAfter for health checks)
- Owned resources change (via `Owns()`)

```go
type DatabaseSensorReconciler struct {
    client.Client
    Scheme   *runtime.Scheme
    Recorder record.EventRecorder
    Deployer *deployer.DebeziumDeployer
    Metrics  *metrics.Exporter
    Health   *health.Checker
    Sinks    *sink.Registry
}

func (r *DatabaseSensorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    // 1. Get the CR
    // 2. Handle deletion (finalizer)
    // 3. Ensure DebeziumServer CR
    // 4. Ensure Service + ServiceMonitor
    // 5. Ensure PrometheusRule (alert rules)
    // 6. Health check (pod status + lag)
    // 7. Update status
    // 8. Return RequeueAfter: 30s
}   