# Estado del proyecto, en una página

Última actualización: 2026-11-11. Este documento se actualiza a mano cuando una sub-pregunta cambia de estado. No reemplaza `FINDINGS.md` ni los `RESULTS.md` de cada experimento; es el resumen para no perder el hilo.

## La pregunta central

Para cada clase de incidente de datos, ¿cuánto mejora detectar, limitar y reparar una reacción basada en CDC + Kubernetes, frente a las restricciones y triggers de la base, las alertas por contadores y la restauración desde un respaldo?

## Criterio de salida (sigue vigente, no se ha cumplido ni refutado)

Si no aparece más de un caso real donde CDC + reacción supere claramente a la base y a los contadores, se reconsidera la dirección. Hoy no hay ese caso ganador. Hay una ventaja medida (P3), un costo medido que puede ser alto (P4) y una fragilidad medida (P5). Ninguna cierra la pregunta.

## Las 7 sub-preguntas: dónde está cada una

| | Pregunta | Estado | Resultado en una frase |
|---|---|---|---|
| P0 | ¿Qué clases de incidente aparecen en postmortems públicos? | **Sin hacer** | — |
| P1 | ¿Qué clases de incidente ve cada fuente? | **Parcial** | Trigger evadible con `session_replication_role`; una sonda eBPF detecta 7 de 11 formas de evadirlo y nombra el pod, pero no previene nada; lo que cierra el hueco es quitar el privilegio. La misma sonda también lee el login (usuario en claro, nunca la contraseña). |
| P2 | ¿Cuántos commits malos pasan antes de que la reacción actúe? | **Una medición** | 0,72 s para un Job. Una sola corrida. |
| P3 | ¿Se puede reparar desde el flujo, y cómo se compara con PITR? | **Probado contra PITR en un escenario** | El flujo repara exacto (0 filas distintas, 3 corridas) con 3 condiciones caras: `REPLICA IDENTITY FULL`, un almacén de eventos que no existe, y los IDs de las transacciones malas dados de antemano. PITR solo pierde escrituras buenas posteriores al incidente. Falta el híbrido (PITR + reaplicar lo bueno), que es el competidor real. |
| P4 | ¿Cuánto cuesta ver el contenido (`REPLICA IDENTITY FULL`)? | **Medido en filas sintéticas** | De 1,4x a 1,9x más WAL por UPDATE en filas normales; **36x** si la fila tiene una columna TOAST que no cambió. El costo depende de la forma de la tabla, no es un número fijo. Falta la forma de tablas reales. |
| P5 | ¿Cuánto falla la propia reacción? | **Mecanismo confirmado** | En la cadena Argo Events, un pod de Sensor muerto sigue contando como miembro activo del bus (NATS); el evento se le entrega a él y queda atascado ~5 min hasta que el bus lo nota. Si llega un evento más nuevo antes, el viejo se pierde de verdad. Ningún ajuste de configuración lo arregla del todo en ese bus. No sabemos si el mecanismo propio del proyecto tendría la misma falla: no se ha construido. |
| P6 | ¿Sobre qué workload se actúa? | **Sin probar** | — |

## Lo que esto significa para el rumbo

La condición que el proyecto se puso por escrito ("si la reacción pierde eventos por diseño, hay que construir entrega garantizada antes de cualquier política de contenido") ya tiene evidencia real detrás, al menos para una cadena de referencia (Argo Events). El tipo de falla a cubrir ya se conoce: un consumidor que parece vivo y está muerto, y un evento viejo que se pierde si uno nuevo dispara primero.

## Qué sigue, en orden

1. Matriz del diente del heartbeat (5 a 120 s por volcado de 10 a 60 s).
2. Clasificar invariantes de proyectos abiertos (P0 y P1, cierran la pregunta completa).
3. Híbrido de P3 (PITR + reaplicar lo bueno).
4. Fase 2 del heartbeat (tabla quieta vs. conector atascado).

## Dónde está cada cosa

- Hallazgos consolidados: `bench/experiments/FINDINGS.md`
- Resultado de cada experimento: `bench/experiments/<nombre>/RESULTS.md`
- Pendientes detallados: `docs/HANDOFF.md`
- Deck y guion para el equipo: `docs/overview/checkup/`
