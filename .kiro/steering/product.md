# Product: Lactiflluus (DatabaseSensor Operator)

## Purpose
**Lactiflluus** es un operator de Kubernetes basado en Debezium que orquesta la observabilidad de métricas de BD, el fan-out de alertas multi-sink y la detección de DDL (cambios de esquema), implementado como el custom resource `DatabaseSensor`. Un operator de Kubernetes que usa Change Data Capture (CDC) vía Debezium como sensor de observabilidad de bases de datos. Permite declarar en un YAML "monitorea estas tablas y avísame si X", y el operator orquesta Debezium + Prometheus + Alertmanager automáticamente.

## Target Users
- SREs / Platform Engineers que necesitan observabilidad de BD sin tocar el código de la app
- Data Engineers que quieren alertas por patrones de mutación (bulk updates, borrados masivos, schema changes)
- DevOps que quieren un pipeline de CDC + observabilidad como infraestructura declarativa

## Business Objectives
1. Reducir el tiempo de detección de anomalías en BD de horas a segundos
2. Eliminar scripts manuales de monitoreo (cron + kubectl + oraciones)
3. Proveer un CRD declarativo que reemplace la configuración imperativa de Debezium + Prometheus + Alertmanager
4. Soportar múltiples sinks de observabilidad (Prometheus, Datadog, Webhook) simultáneamente

## Key Capabilities
- Crear y gestionar un DebeziumServer (vía el Debezium Operator oficial) desde un solo CR
- Derivar métricas de observabilidad: insert_rate, update_rate, delete_rate, lag, schema_changes
- Expone /metrics (formato Prometheus) siempre, independientemente del sink configurado
- Fan-out de alertas a múltiples backends (Prometheus, Datadog, Webhook/Slack)
- Dashboard embebido con gráficos en vivo
- Status enriquecido visible con kubectl (Phase, Lag, EventsPerMin, LastError)
- Soporta PostgreSQL, MySQL/MariaDB, MongoDB, SQL Server, Oracle, Db2, Cassandra   