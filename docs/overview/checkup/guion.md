# Guion: Checkup del proyecto, para el equipo

Una presentación estratégica de 4 láminas (`deck.html`, en esta carpeta). Dura entre 13 y 15 minutos, más preguntas.
El guion está escrito para decirse en voz alta, en primera persona. Los datos de respaldo y las respuestas a preguntas
probables van aparte de lo que se habla, para que se puedan consultar sin leerlos.

**La idea en una frase.** Llevamos varias semanas probando una dirección. Podemos separar lo que ya está medido de lo
que todavía es una hipótesis, y por eso sabemos qué hacer primero.

**El tono.** Es un balance, no una defensa. Se dice "medido, una corrida" cuando es una corrida, y "no lo sabemos" cuando
no se sabe. Lo que da credibilidad a este checkup es que también cuenta lo que salió mal.

| Parte | Lámina | Minutos |
|---|---|---|
| Apertura | | 1 |
| Lo que sabemos | 1 | 4 |
| Hipótesis | 2 | 3 a 4 |
| Lo que falta | 3 | 3 |
| Próximos pasos y cierre | 4 | 3 |

---

## Apertura (1 minuto)

> Quiero que hagamos un alto en el camino. Llevamos un tiempo construyendo y midiendo, y en ese tiempo cambió lo que
> creíamos al principio. Prefiero que lo veamos juntos ahora, con los números, y que decidamos qué sigue.
>
> Voy a hacer tres cosas. Primero, qué sabemos y con cuánto respaldo. Segundo, qué hipótesis aguantaron y cuáles no.
> Tercero, qué nos falta demostrar y en qué orden lo atacaría. Una advertencia desde ahora: casi todo lo que voy a
> mostrar salió de una sola corrida, en un clúster local. Sirve para decidir qué hacer, no para afirmar cosas sobre
> producción.

**Si el equipo no conoce el proyecto**, agregar una frase de contexto: Lactifluus es un operador de Kubernetes que usa
Debezium para leer los cambios de una base PostgreSQL y reaccionar a ellos. Debezium es el motor que lee el registro
interno de la base (el WAL) y entrega cada cambio como un evento. A eso se le llama CDC, captura de cambios.

---

## Lámina 1: Lo que sabemos (4 minutos)

*Se muestran ocho hallazgos. Una franja verde significa que se confirmó con más de una medición; la ámbar, una sola corrida.*

### Lo que se dice

> Esta tabla es lo que está medido. La leo en tres bloques, porque cuentan una historia.
>
> **El primer bloque es donde CDC no aporta.** Probamos si contar cambios justificaba usar CDC. No. Un contador de
> `pg_stat`, que ya trae PostgreSQL, y la API de Kubernetes ven la misma ráfaga que CDC. Si el producto fuera "métricas
> de lo que pasa en la base", perdería contra un exporter común. Esa hipótesis la dimos por cerrada.
>
> **El segundo bloque es donde CDC sí aporta.** Hay dos cosas que solo vimos desde el flujo de cambios. Una es el
> contenido de un cambio: que un pago pasó de liquidado a pendiente, no solo que hubo una actualización. La otra es el
> orden: que la factura 1003 fue seguida por la 1007, o que un número se repitió. Un contador no ve ninguna de las
> dos. Con una condición que hay que decir: para ver el valor anterior, la tabla necesita `REPLICA IDENTITY FULL`,
> que hace que PostgreSQL escriba la fila completa en su registro en cada actualización. Ese costo todavía no lo
> medimos.
>
> Dentro de este bloque está el hallazgo que más me interesa. Un trigger de la base, que es la defensa que muchos
> equipos ya tienen, no se dispara si la sesión usa un ajuste que usan las cargas masivas y las restauraciones. El cambio
> pasa. CDC igual lo ve, porque lee el registro, que se escribe en ambos casos. Lo medimos: 11 comprobaciones de 11.
> Y tengo que decir la otra mitad con la misma claridad: si el trigger se declara `ENABLE ALWAYS`, se dispara también
> en esa sesión y bloquea el cambio. O sea, la propia base puede cerrar ese hueco sin CDC. CDC sirve como testigo
> independiente donde no se hizo eso. No medimos qué tan seguido ocurre.
>
> También medimos que la reacción se puede armar con piezas que ya existen. Debezium, Kafka y Argo Events suspendieron
> un Job en menos de un segundo, sin escribir código nuestro. Eso significa que la reacción, por sí sola, no es lo que
> nos diferencia.
>
> **El tercer bloque son las trampas y los defectos.** El "quién" hizo el cambio no se recupera por la sesión ni por
> eBPF. Por el identificador de transacción sí se puede, pero solo si el servidor registra cada sentencia, y eso es
> caro. Con Cilium decimos "sin probar", porque nuestro banco no lo corre.
>
> Luego tres cosas del producto, que son nuestras. Un consumidor lento hace crecer el slot de replicación, que es el
> registro que PostgreSQL guarda hasta que el consumidor confirma, y nuestro diagnóstico seguía diciendo "sano". Cuando
> se filtra todo, el slot retiene todo; un heartbeat, un latido que Debezium envía periódicamente, lo acota. Esa es la
> única que está en verde por más de una medición, y ya la construimos. Y una tabla sin escrituras deja a nuestro
> sensor en `Failed` a los dos minutos. Es una falsa alarma, y la medimos y la confirmamos en el código.

### Datos para tener a mano

- Contar cambios: el experimento fue una ráfaga de ejecuciones atrasadas de un scheduler. Contadores y API de Kubernetes
  ven lo mismo. Archivo: `bench/experiments/scheduler-catchup/RESULTS.md`.
- Contenido: con la identidad por defecto, el evento trae `before = null`. Con `FULL` trae `settled` y `pending`.
  Archivo: `row-content/RESULTS.md`.
- Orden: se detectó el salto (1005 a 1009) y el repetido (1009). Límite medido: al reiniciar el consumidor se pierde su
  estado, y un offset en memoria reentregó el 1010 y produjo un falso duplicado. Archivo: `sequence-gap/`.
- Trigger: `bench/experiments/trigger-bypass/RESULTS.md`. Requiere superusuario o el privilegio `SET`.
- Reacción: 0,72 s es una cota superior e incluye la propia llamada. Ocho pods y tres manifiestos de 136 líneas en
  total. Dos límites: no sabe qué Job es (el objetivo es fijo) y, en 2 de 2 pruebas, perdió el evento cuando se
  reemplazó el pod del Sensor, sin diagnosticar. Archivo: `argo-events-chain/RESULTS.md`.
- Atribución por registro: requiere `application_name`, un prefijo con `%a` y `%x`, y `log_min_duration_statement = 0`.
  Con `log_statement = mod` el identificador sale en 0. Archivo: `attribution-inband/RESULTS.md`.
- Slot: de 64 kB a 1953 kB en 6 pasos con el consumidor lento. Archivo: `backpressure/RESULTS.md`.
- Heartbeat: sin él, el desfase subió de 183 a 1121 KiB en 81 s y seguía subiendo. Con 30 s se mantuvo entre unos 60 y
  310 KiB. Archivo: `heartbeat/RESULTS.md`.
- Tabla quieta: `Degraded` a los 35 s; `Failed` entre 96 y 221 s (el código dice 120 s). Archivo: `quiet-table/RESULTS.md`.

### Si preguntan aquí

- *¿Por qué una sola corrida?* Para decidir qué probar después alcanza; para afirmar, no. Por eso cada fila dice cuántas hay.
- *¿Y en producción?* No lo sabemos. Todo salió de un clúster local con PostgreSQL 16 y Debezium 3.6.3.

**Transición.** "Con esto armamos siete hipótesis al principio. Veamos cómo les fue."

---

## Lámina 2: Hipótesis (3 a 4 minutos)

*Siete hipótesis con su estado en un color. Abajo, cuatro afirmaciones nuestras que estaban mal.*

### Lo que se dice

> Cuatro se sostuvieron, una se debilitó, una cayó y una está sin probar.
>
> **H1 y H2 se sostienen y están cerradas.** El valor único de CDC es el contenido y el orden, no el conteo. Un producto
> de métricas pierde contra un exporter.
>
> **H3 se debilitó.** Creíamos que la reacción en Kubernetes era la parte novedosa. La armamos con piezas que ya existen.
> Eso no la vuelve inútil, pero ya no es nuestro argumento.
>
> **H4 cayó.** Creíamos que el "quién" se podía resolver barato. No: ni por la sesión ni por eBPF. Solo vive un camino
> caro. Con Cilium, no lo probamos.
>
> **H5 se sostiene.** Hace falta una capa de confianza alrededor de CDC en Kubernetes. Cada pieza que construimos tiene
> una medición detrás: el slot sin aviso, la tabla quieta en `Failed`, la retención y el heartbeat.
>
> **H6 se sostiene en un caso.** CDC como testigo independiente de dónde se aplican las reglas. Lo vimos con el trigger.
> Pero depende de cómo esté configurado ese trigger, y por eso solo decimos "un caso".
>
> **H7 no se ha probado.** La idea de proponer reglas a partir de patrones no está construida ni medida.
>
> Ahora lo que me importa más que la tabla. Abajo hay cuatro afirmaciones **nuestras** que estaban mal. La primera:
> escribimos que una tabla quieta pasaba a `Degraded`, y pasa a `Failed`. La segunda: el diagnóstico decía sano mientras
> el WAL crecía. La tercera: habíamos explicado el largo del "diente" del desfase por el volcado de offsets de Debezium,
> y con otro valor de heartbeat esa explicación no se sostuvo; la causa no la conocemos. La cuarta: "lo único que CDC
> aporta" debe leerse como "lo único de lo que medimos". Las corregimos en la documentación. Lo cuento porque esa
> corrección es el proceso funcionando, y porque quiero que, si algo de lo que digo hoy está mal, lo digan.

### Qué es el "diente" (si alguien lo pregunta)

El desfase del slot es el registro que PostgreSQL guarda desde la última posición que Debezium confirmó. Al escribir, sube;
al confirmar, baja de golpe. Con heartbeat la gráfica sube y baja como una sierra, y un diente es un ciclo. Su altura es el
máximo que se guarda en el peor momento. En las corridas: diente de unos 30 s con heartbeat de 30 s, y de unos 60 s con
heartbeat de 10 s. Por qué el de 10 s es más largo no lo sabemos. Está en la lista de lo que falta.

### Datos para tener a mano

- Qué tan firme es cada estado: H1, H2, H5 y H6 se apoyan en las filas de la lámina 1. H3 en `argo-events-chain`. H4 en
  `attribution`, `attribution-ebpf` y `attribution-inband`. H7 en nada.
- La corrección sobre `Degraded` y `Failed` está en `quiet-table/RESULTS.md` y contradice el HANDOFF y el brief.

**Transición.** "Entonces, ¿qué nos falta? Se lo puse en una sola pregunta."

---

## Lámina 3: Lo que falta (3 minutos)

*Arriba, la pregunta central. Debajo, siete sub-preguntas con su métrica y el estado de hoy. Abajo, una que no se resuelve en el laboratorio.*

### Lo que se dice

> Esta es la pregunta central, y la reescribí varias veces porque la primera versión no se podía responder. Dice: para
> cada clase de incidente de datos, ¿cuánto mejora el resultado de detectar, limitar y reparar una reacción basada en el
> flujo de cambios más Kubernetes, frente a las restricciones y triggers de la base, las alertas por contadores y la
> restauración desde un respaldo?
>
> Tres detalles. Dice detectar, limitar y reparar, y **no** prevenir, porque CDC ve el cambio cuando ya está confirmado:
> solo la base puede impedirlo. Compara contra tres cosas, incluida la restauración desde respaldo, que es el competidor
> más serio y que habíamos dejado fuera. Y habla de **clases** de incidente, porque la respuesta puede ser distinta
> para una transición prohibida, para un lote masivo o para un valor fuera de rango.
>
> De ahí salen siete sub-preguntas. Cada una tiene una métrica, y el estado de hoy es poco alentador, y está bien así.
>
> Dos no tienen medición: qué clases aparecen en postmortems públicos y sobre qué workload se actúa. Tres tienen una medición débil: qué ve cada fuente, cuántos commits malos pasan antes de que la
> reacción actúe (una medición, 0,72 s, para un Job) y cuánto falla la propia reacción, donde perdimos el evento 2 de 2
> veces al reemplazar el Sensor. Y dos están a medias: la reparación, y el costo de ver el contenido, que medimos en filas sintéticas.
>
> Quiero detenerme en la reparación. El respaldo con restauración a un punto en el tiempo restaura todo y de golpe. Si
> el flujo de cambios guarda el valor anterior de cada fila, quizá se podría reparar solo lo afectado. La primera mitad
> ya la probamos: una tabla de 5000 filas, un trabajo malo con 13 transacciones (actualizaciones, borrados e inserciones
> basura) mezclado con escrituras buenas. Reparamos desde el flujo y el resultado quedó idéntico al esperado, con 0
> filas distintas, en tres corridas. Eso es lo medido, en un escenario.
>
> Tiene tres condiciones. Una, la tabla necesita `REPLICA IDENTITY FULL`. Dos, hace falta un almacén durable de
> eventos, y el operador hoy no guarda ninguno. Tres, los identificadores de las transacciones malas se los dimos
> nosotros. Y la comparación con un respaldo con PITR la hicimos en el mismo escenario. Cambiar la base por el clon recuperado deja 1224 filas distintas de lo esperado, porque le faltan las escrituras buenas posteriores. Copiar de vuelta solo las filas que tocó el trabajo malo deja 1024, porque pisa las escrituras buenas hechas sobre esas filas. La reparación desde el flujo dejó 0. El PITR tardó 0,5 s, y 7,9 s con 1 GB de WAL extra antes del incidente. Eso no dice que el flujo sea mejor en general: el PITR no necesita `REPLICA IDENTITY FULL` ni almacén de eventos, y recupera también cambios de esquema. Falta el híbrido, restaurar y reaplicar las transacciones buenas.
>
> Y la caja de abajo. Hay una pregunta que no se responde en el laboratorio: si alguien necesita esto. Sigue abierta, y
> tenemos un criterio de salida escrito desde antes: si no aparece más de un caso real donde CDC más reacción supere a
> la prevención en la base y a los contadores, la dirección se reconsidera.

### Datos para tener a mano

- Las siete sub-preguntas, con su falsador, están en esta conversación de trabajo y se pueden pasar a `docs/research/`.
- El límite estructural de P2: un lote en una sola transacción ya está confirmado cuando CDC lo ve. La reacción limita el
  daño de trabajos largos y por lotes, no el de una sentencia única.

**Transición.** "¿Y qué hacemos con esto? Lo ordené por costo y por lo que desbloquea."

---

## Lámina 4: Próximos pasos (3 minutos)

*A la izquierda, siete pasos numerados. A la derecha, lo que cambiaría el rumbo y lo que queda fuera. Abajo, una frase que resume.*

### Lo que se dice

> El orden no es por importancia, es por costo y por lo que desbloquea.
>
> **Primero, la matriz del diente.** Barrer el heartbeat de 5 a 120 segundos contra el volcado de offsets de 10 a 60.
> Es barato, cabe en una tarde, y cierra la corrección que dejamos abierta. Además fija con fundamento la ventana de la
> alerta de slot estancado.
>
> **Segundo, el costo de `REPLICA IDENTITY FULL`.** Toda la línea de contenido se apoya en eso, y ya tiene número en cuatro filas sintéticas. Un UPDATE de una columna chica escribe 1,4 veces el WAL en una fila angosta, unas 1,9 veces en filas de 1 a 1,8 KB, y 36 veces en una fila de 6,4 KB con una columna ancha en TOAST que no cambió, porque con la identidad por defecto ese valor no se escribe nunca. Un DELETE de una fila ancha cuesta de 15 a 24 veces más. Lo que falta es saber qué forma tienen las tablas reales.
>
> **Tercero, clasificar las invariantes de proyectos abiertos.** Tomar modelos de datos grandes y públicos, listar las
> reglas que tienen y marcar cuáles caben en restricciones o triggers y cuáles solo se vigilan desde el flujo. Es lo que
> acota con datos el espacio real de CDC frente a la base. Tiene un límite, y es que la clasificación es subjetiva.
>
> **Cuarto, entender por qué la reacción perdió eventos.** Si es un defecto de configuración, es trabajo corto. Si es un
> diseño que puede perder, hay que construir entrega garantizada antes de cualquier política. Eso cambia el tamaño del
> proyecto.
>
> **Quinto, la fase 2 del heartbeat.** Que el sensor use los latidos para distinguir una tabla quieta de un conector
> estancado. Quita la falsa alarma que medimos y habilita la alerta de slot que no avanza.
>
> **Sexto, la variante híbrida de la reparación.** El flujo salió exacto y el PITR solo perdió escrituras buenas, en un escenario; falta restaurar y reaplicar las buenas, y un PITR gestionado. **Séptimo**, el caso de una base monitoreada quieta
> mientras otra escribe, donde quizá el heartbeat solo no alcance.
>
> A la derecha pongo qué cambiaría el rumbo. Si `FULL` sale caro en las tablas reales, que hoy sabemos que depende de la forma de la fila, y casi todas las invariantes caben en la base, el
> proyecto se queda como capa de confianza para CDC. Si la reparación resiste la comparación con un respaldo real, aparece una línea distinta de detectar.
> Si la reacción pierde eventos por diseño, se construye entrega garantizada antes de cualquier política. Y el criterio
> de salida que ya mencioné.
>
> Fuera por ahora: la inteligencia artificial, MySQL, una interfaz para escribir reglas y las reglas sugeridas desde
> patrones. Ninguna tiene evidencia que la sostenga todavía, y prefiero no construirlas antes de saber si la base
> aguanta.
>
> Si me preguntan qué es lo firme y qué es lo abierto: lo firme es una capa de confianza alrededor de CDC, medida.
> Lo abierto es si las políticas de contenido valen lo que cuestan.

### Cierre y lo que se pide al equipo (1 minuto)

> Para cerrar, tres cosas que necesito de ustedes. Que revisen el **orden** de los pasos y me digan si cambiarían algo.
> Que quien quiera tomar uno lo diga ahora, sobre todo el tercero y el cuarto. Y que me digan si el criterio de salida
> les parece razonable, porque prefiero acordarlo hoy que discutirlo cuando llegue.

---

## Preguntas probables y cómo contestarlas

1. **¿Por qué no usar solo triggers y restricciones?**
   Cuando se puede, es mejor: impiden el cambio, y CDC solo lo ve después. CDC aporta donde el trigger está en modo por
   defecto y se salta, donde la regla es de orden o de varias filas, o donde no se puede tocar la base. Lo medimos en un
   caso. No medimos con qué frecuencia importa.

2. **¿Y pgaudit o el registro de auditoría?**
   No lo comparamos. Es una alternativa que no probamos.

3. **¿Debezium ya hace alertas?**
   No leímos qué hace el "alerting" de la Debezium Platform 3.7. Es un pendiente nuestro y lo decimos así.

4. **¿Por qué solo PostgreSQL?**
   Es lo único que podemos verificar hoy. MySQL está mapeado, no verificado. El heartbeat, por ejemplo, se rechaza en MySQL
   en la API hasta que se pueda probar.

5. **¿Y con bases gestionadas, como RDS o Cloud SQL?**
   Por análisis, no por prueba: la reacción y las políticas de contenido deberían funcionar, y se pierde parte del
   diagnóstico del servidor. No comprobamos si un proveedor deja usar el ajuste de replicación, ni la red.

6. **¿Cuánto cuesta?**
   Hay un dato y un vacío. Dato: la cadena con Argo Events usa ocho pods. El costo en WAL de `FULL` se midió en cuatro filas sintéticas: de 1,4 a 1,9 veces por UPDATE si la fila es inline, y 36 veces con una columna TOAST sin cambios. Vacío: no conocemos la forma de las tablas reales, ni el almacenamiento del almacén de eventos (unos 12 KB por evento para una fila de 6,4 KB, de la reparación).

7. **¿Cuándo dejamos de intentarlo?**
   El criterio de salida está escrito: sin más de un caso real donde CDC más reacción supere a la prevención en la base y a
   los contadores, se reconsidera. Uno de los resultados posibles es quedarse como capa de confianza, y es un resultado
   válido.

8. **¿Y la IA?**
   Fuera por ahora. Solo entraría si aparece una clase de incidente que no se pueda escribir como regla de antemano, y la
   condición sería que un detector estadístico dé menos falsos positivos que una regla fija. Los datos y el contenido no
   saldrían del clúster.

9. **¿Qué hacemos hoy con el sensor en `Failed` por la tabla quieta?**
   Existe `spec.thresholds` para subir los umbrales. No probamos si basta. La solución de fondo es la fase 2 del heartbeat.

10. **¿Es esto un producto o una investigación?**
    Hoy es una capa de confianza construida y medida, con una hipótesis de producto encima que no está validada. No conviene
    llamarla producto de políticas todavía.

11. **¿Se puede reparar automáticamente?**
    No todavía. En un escenario, reparar desde el flujo dejó 0 filas distintas del esperado en tres corridas, pero los
    identificadores de las transacciones malas se los dimos nosotros, hace falta `REPLICA IDENTITY FULL` y un almacén
    durable de eventos que el operador no tiene. Frente a un PITR solo ganó en ese escenario; el híbrido (PITR más reaplicar las buenas) no se midió.

---

## Lo que conviene no decir

- "CDC es la única fuente". Decir "la única de las que probamos".
- "Funciona". Decir "lo medimos en una corrida".
- "Reemplaza a los triggers". Con `ENABLE ALWAYS` la base se protege sola.
- "Resolvimos la atribución". Existe un camino caro y condicional.
- "El heartbeat lo arregla todo". Acota el slot; no arregla al consumidor lento ni la falsa alarma de la tabla quieta (fase 2).
- "El flujo repara mejor que un respaldo". Solo se midió en un escenario, contra un PITR solo; falta el híbrido y uno gestionado.
- "FULL duplica el WAL". Decir "depende de la forma de la fila": 1,4x a 1,9x inline, 36x con TOAST sin cambios, en filas sintéticas.
- "El PITR no sirve". Sirve para volver al pasado; solo no repara una base que siguió recibiendo escrituras buenas.
- "La reparación es automática". Necesita la lista de transacciones malas.
- Cualquier número de producción. No hay ninguno.

---

## Mapa de evidencia

| Hallazgo | Dónde está |
|---|---|
| Contar no justifica CDC | `bench/experiments/scheduler-catchup/RESULTS.md` |
| Contenido (`settled` a `pending`) | `bench/experiments/row-content/RESULTS.md` |
| Orden de secuencias | `bench/experiments/sequence-gap/RESULTS.md` y `RESULTS-redelivery.md` |
| Trigger saltado | `bench/experiments/trigger-bypass/RESULTS.md` |
| Reacción con piezas existentes | `bench/experiments/argo-events-chain/RESULTS.md` |
| Atribución | `bench/experiments/attribution/`, `attribution-ebpf/`, `attribution-inband/` |
| Slot que crece, diagnóstico sano | `bench/experiments/backpressure/RESULTS.md` |
| Heartbeat | `bench/experiments/heartbeat/RESULTS.md` y `docs/adr/ADR-018-heartbeat.md` |
| Tabla quieta | `bench/experiments/quiet-table/RESULTS.md` |
| Reparación desde el flujo (primera mitad) | `bench/experiments/repair-from-stream/RESULTS.md` |
| Costo de `REPLICA IDENTITY FULL` | `bench/experiments/replica-identity-full-cost/RESULTS.md` |
| Reparación frente a PITR | `bench/experiments/repair-vs-pitr/RESULTS.md` |
| Resumen consolidado | `bench/experiments/FINDINGS.md` |
| Requisitos de un consumidor con estado | `docs/adr/ADR-016-stateful-stream-consumer-requirements.md` |

## Glosario de una línea

- **CDC:** captura de cambios; leer los cambios de la base desde su registro interno.
- **WAL:** el registro de escritura anticipada de PostgreSQL, donde queda cada cambio.
- **Slot de replicación:** el marcador con el que PostgreSQL recuerda hasta dónde leyó un consumidor y guarda el WAL desde ahí.
- **`REPLICA IDENTITY FULL`:** hace que el WAL guarde la fila completa anterior en cada `UPDATE` o `DELETE`, lo que permite ver el valor viejo.
- **Heartbeat:** un mensaje periódico de Debezium que prueba que está vivo y avanza el marcador.
- **PITR:** restaurar la base a un punto exacto en el tiempo desde un respaldo.
- **Trigger en modo `origin` o `always`:** `origin` no se dispara en sesiones de réplica; `always` sí.
- **`txId`:** el identificador de la transacción que hizo el cambio.
