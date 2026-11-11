# Estado del proyecto

**Fecha de este resumen: 11 de noviembre de 2026.**

Este documento se actualiza a mano cada vez que algo cambia de estado. No reemplaza `bench/experiments/FINDINGS.md` (los hallazgos técnicos consolidados) ni los `RESULTS.md` de cada experimento (el detalle de cada prueba); es el resumen para explicarle el proyecto a cualquiera, técnico o no, sin tener que leer todo lo demás.

## La pregunta que estamos investigando

Para cada clase de incidente de datos (un job que borra filas por error, una migración que corrompe un valor, un reintento que duplica registros), **¿cuánto mejora reaccionar con CDC + Kubernetes, frente a lo que ya existe hoy sin construir nada nuevo?**

**¿Qué es CDC?** Change Data Capture: una técnica que lee el registro interno de cambios de la base de datos (en PostgreSQL se llama WAL, el "write-ahead log") y convierte cada cambio en un evento: qué fila cambió, cuál era su valor antes, cuál es su valor después, y en qué momento exacto. Es la pieza central de este proyecto (usamos Debezium para hacerlo).

### ¿Frente a qué lo comparamos, y por qué justo esas cuatro cosas?

Elegimos estas cuatro porque son las defensas que cualquier equipo con una base de datos en Kubernetes ya tiene disponibles hoy, sin instalar nada nuevo. Si CDC no les gana a estas cuatro, no se justifica el costo y la complejidad de agregar una pieza más al sistema.

1. **Constraints** (restricciones de la base de datos): reglas que la propia base impone y que ninguna fila puede violar. Por ejemplo, "la columna `status` no puede ser nula" o "el monto no puede ser negativo". Las evalúa la base en el instante en que se intenta escribir, y rechaza la escritura si no cumple.
2. **Triggers** (disparadores): código que la base ejecuta automáticamente cuando algo cambia (antes o después de un INSERT, UPDATE o DELETE). Por ejemplo, un trigger que impide que un pago pase de "liquidado" a "pendiente".
3. **Contadores**: las métricas que la base ya expone sola (cuántas filas cambiaron, cuántas conexiones hay, qué tan rápido crece una tabla). Es lo que la mayoría de los paneles de monitoreo (dashboards) usan hoy.
4. **Restaurar desde un respaldo** (también llamado PITR, de *Point-In-Time Recovery*, "recuperación a un punto en el tiempo"): cuando algo salió mal, se restaura una copia de seguridad tomada antes del incidente y se recupera el estado de la base hasta justo antes de que ocurriera el problema.

No comparamos contra, por ejemplo, herramientas de observabilidad de red o soluciones comerciales de terceros, porque esas no son gratis ni están ya instaladas: agregan su propio costo y su propia complejidad, así que no son el punto de partida "sin construir nada nuevo" que buscamos superar.

## El criterio de salida: cuándo diríamos "esto no vale la pena"

Este criterio (lo llamamos internamente "R9", por ser la novena línea de nuestro plan de investigación) se escribió **antes** de hacer ningún experimento, precisamente para no engañarnos después moviendo la meta según nos convenga.

**La regla, en palabras simples:** si no logramos nombrar más de un caso real (no inventado, no de laboratorio) donde CDC + una reacción automática en Kubernetes le gane claramente a prevenir el problema desde la base de datos (constraints/triggers) y a las alertas por contadores, dejamos de insistir en esta dirección.

**¿Qué significa "le gane claramente"?** No basta con que CDC también detecte el problema. Tiene que hacer algo que las otras dos opciones no puedan hacer, o hacerlo de forma sustancialmente mejor (por ejemplo: ver el contenido exacto de un cambio cuando un contador solo ve "algo cambió"; o reparar con precisión en vez de perder trabajo bueno al restaurar un respaldo completo). Si CDC solo iguala lo que ya hacían los contadores, para efectos de este criterio cuenta como que no ganó.

**¿Por qué "constraints y contadores" y no PITR en esta frase puntual?** Porque la pregunta de R9 nació pensando en *prevenir y detectar* (constraints/triggers evitan el problema antes de que ocurra; los contadores avisan que algo pasó). PITR entra más bien como el competidor a la hora de *reparar* después del incidente, y por eso aparece comparado en detalle en la sub-pregunta 3 (ver abajo) en vez de en la frase general del criterio de salida.

**Estado de este criterio hoy: no se ha cumplido ni se ha refutado.** No tenemos todavía ese caso real ganador. Lo que sí tenemos es evidencia medida en las dos direcciones: una ventaja real de CDC en un escenario (sub-pregunta 4), un costo real que puede ser alto (sub-pregunta 5), y una fragilidad real de la reacción (sub-pregunta 6). Nada de esto cierra la pregunta todavía.

**¿Cómo se consigue ese "caso real"?** No sale de experimentos técnicos como los de abajo, que son de laboratorio. Sale de hablar con personas que de verdad administran bases de datos en producción y preguntarles si alguna vez vivieron este tipo de incidente, y qué hubieran querido que pasara automáticamente. Esa parte de la investigación (conversaciones con usuarios reales, documentada en `docs/research/user-conversations.md`) todavía no se ha hecho a fondo, y es la que de verdad puede cerrar esta pregunta en cualquier dirección.

## Las 7 sub-preguntas

Partimos la pregunta central en 7 preguntas más chicas y concretas, cada una con su propio experimento medible. (Nota: en los archivos técnicos del repositorio estas preguntas están numeradas de P0 a P6; en este documento las numeramos de 1 a 7, como contamos las personas, para que sea más fácil de leer. Es solo una diferencia de etiqueta, las preguntas son las mismas.)

### 1. ¿Qué clases de incidente aparecen en reportes públicos de fallas (*postmortems*)?

**¿Qué es un "postmortem"?** Es el reporte que una empresa escribe después de una falla grave, explicando qué pasó, por qué, y qué van a cambiar para que no vuelva a pasar. La palabra viene de "autopsia" (examinar después de la muerte) y se usa así en toda la industria del software.

**¿Por qué "públicos"? ¿Existen postmortems privados?** Sí, existen, y de hecho son la mayoría: casi toda empresa escribe estos reportes internamente, pero casi nunca los publica, porque pueden exponer debilidades de seguridad o errores vergonzosos frente a clientes. Nosotros solo podemos leer los que alguien decidió hacer públicos (empresas como GitLab, Cloudflare o muchas otras publican los suyos). Es una limitación real de este método: lo que encontremos ahí es una muestra sesgada, no el panorama completo de lo que pasa en la industria. Para ver el panorama completo haría falta acceso a postmortems privados, que no tenemos, o preguntarle directamente a la gente (que es justo lo que busca la sub-pregunta de las conversaciones con usuarios, mencionada arriba).

**Estado: sin hacer.** No se ha empezado a leer y catalogar postmortems todavía.

### 2. ¿Qué tipo de incidente puede ver cada fuente de información?

La idea es simple: cuando algo sale mal en una base de datos, distintas herramientas "ven" cosas distintas. Un contador ve que hubo cambios, pero no qué cambió. Un trigger ve el cambio en el instante, pero se puede evadir. CDC ve el contenido exacto, pero también se puede engañar o evadir de formas específicas. Esta sub-pregunta mapea, fuente por fuente, qué puede detectar y qué se le escapa.

**Lo que ya probamos:**

- **Un trigger se puede evadir.** PostgreSQL permite que una sesión le diga a la base "ignora los triggers por esta vez" con el comando `SET session_replication_role = replica`. Si alguien (una persona, un script, un job) hace eso antes de escribir, el trigger simplemente no se dispara, como si no existiera. Lo confirmamos con una prueba directa: 11 de 11 intentos lo lograron.
- **CDC no se deja engañar por ese truco.** Aunque el trigger no se dispare, CDC de todas formas ve el cambio en la base, porque lee directamente del registro de escritura (el WAL), no depende de que el trigger avise nada.
- **Probamos detectar ese "truco" con una sonda.** ¿**Qué es una sonda en este contexto?** Es un programa pequeño que se instala en la máquina (el nodo) donde corre el cliente que se conecta a la base de datos, y observa el tráfico de red que sale de ahí, sin modificar nada ni bloquear nada, solo mirando. La tecnología que usamos para esto se llama eBPF (una forma de ejecutar código de observación dentro del sistema operativo Linux, de forma segura y eficiente). Construimos una sonda que mira el tráfico hacia la base de datos y trata de detectar cuándo alguien manda ese comando de "ignora los triggers". Resultado: la detectó en 7 de 11 formas distintas de escribir el mismo comando (por ejemplo, escrito con mayúsculas distintas, o metido dentro de una función). Las 4 formas que se le escaparon incluyen, por ejemplo, un cliente que construye el texto del comando en el momento (en vez de escribirlo tal cual) o que usa una conexión cifrada (TLS), donde la sonda ya no puede leer el contenido porque está encriptado.
- **Un hallazgo colateral importante de seguridad:** esa misma sonda, sin querer buscarlo, también puede leer el proceso de inicio de sesión (login) completo: ve el nombre de usuario en texto claro, y ve la estructura del intercambio de autenticación (el protocolo se llama SCRAM). Lo que nunca pudo ver, en ninguna prueba, fue la contraseña, porque SCRAM está diseñado justamente para que la contraseña nunca viaje por la red, ni cifrada de forma reversible.
- **Lo que de verdad cierra el hueco del trigger no es ninguna sonda, es un permiso.** Si a la cuenta de base de datos que usa una aplicación se le quita el privilegio para ejecutar ese comando de "ignora los triggers", la base simplemente rechaza el intento. Eso es más efectivo que cualquier forma de detectarlo después.

**Estado: parcial.** Falta todavía clasificar, de forma sistemática, qué reglas de negocio reales (de proyectos de código abierto grandes) caben dentro de un constraint o trigger normal, y cuáles solo se pueden vigilar mirando el contenido de los cambios con CDC.

### 3. ¿Cuántos cambios "malos" alcanzan a pasar antes de que la reacción automática actúe?

Si algo sale mal y la respuesta automática tarda en reaccionar, durante ese tiempo pueden seguir entrando más cambios malos. Esta pregunta mide ese tiempo.

**Lo que ya probamos:** en una sola medición, el tiempo desde que ocurrió el cambio malo hasta que un Job de Kubernetes fue frenado automáticamente fue de 0,72 segundos.

**Estado: una sola medición.** No es un número confiable todavía porque solo se corrió una vez; falta repetirlo varias veces para saber si es consistente.

### 4. ¿Se puede reparar un daño usando el propio flujo de cambios de CDC, y cómo se compara con restaurar un respaldo (PITR)?

Esta es la pregunta donde ya tenemos la comparación más completa.

**La idea de "reparar desde el flujo":** como CDC guarda el valor de cada fila antes y después de cada cambio, en teoría se podría usar esa información para "deshacer" exactamente los cambios malos, sin tocar los cambios buenos que pasaron al mismo tiempo. Es como tener un historial detallado de ediciones y poder revertir solo las líneas que un usuario específico escribió mal, sin perder lo que otros usuarios escribieron bien mientras tanto.

**Lo que probamos:** simulamos un incidente (un trabajo que hizo cambios incorrectos en miles de filas) mientras otros trabajos seguían escribiendo datos buenos al mismo tiempo. Reparamos usando solo la información de CDC, y el resultado fue exacto: 0 filas distintas de lo esperado, repetido en 3 corridas.

**Pero tiene tres condiciones caras que hay que cumplir primero:**
1. La tabla necesita tener activado `REPLICA IDENTITY FULL` (ver la siguiente sub-pregunta: esto tiene un costo real).
2. Hace falta guardar todos los eventos de CDC en algún lugar durable por un tiempo (un "almacén de eventos"), y el operador que estamos construyendo hoy no guarda ninguno; simplemente los cuenta y los descarta.
3. Alguien tiene que decirnos de antemano cuáles fueron exactamente las transacciones malas que hay que deshacer. No lo averiguamos automáticamente.

**Lo comparamos contra PITR (restaurar un respaldo):** si en vez de reparar desde el flujo se restaura un respaldo completo a un punto antes del incidente, se pierde todo el trabajo bueno que se hizo después de ese punto, porque PITR regresa toda la base a como estaba, sin distinguir cambios buenos de malos. Esa es la ventaja real y medida de reparar desde el flujo.

**Lo que todavía falta:** el competidor más realista no es PITR solo, sino un "híbrido": restaurar el respaldo y después reaplicar manualmente los cambios buenos que se perdieron. Ese híbrido todavía no lo hemos medido, y es el próximo paso de esta sub-pregunta.

**Estado: probado contra PITR en un escenario**, falta el híbrido.

### 5. ¿Cuánto cuesta activar la capacidad de ver el contenido completo de un cambio?

**¿Por qué nos importa esto?** Porque `REPLICA IDENTITY FULL` es la configuración que hace posible todo lo interesante de las sub-preguntas 2 y 4: sin ella, cuando una fila se actualiza o se borra, CDC solo ve el valor *nuevo*, no el valor que tenía *antes*. Sin el valor anterior, no se puede detectar una transición prohibida (por ejemplo "de liquidado a pendiente") ni reparar nada, porque no sabemos a qué había que regresar la fila. Es decir: `REPLICA IDENTITY FULL` no es un detalle técnico menor, es el interruptor que activa o desactiva buena parte del valor que este proyecto promete. Por eso necesitábamos saber qué cuesta encenderlo.

**Lo que probamos:** medimos cuánto más espacio ocupa en el registro de escritura (el WAL) cada operación de actualizar o borrar una fila, con esta opción prendida contra apagada, usando distintos tamaños de fila sintéticos (de prueba, no datos reales).

**Resultado, y es la sorpresa más clara de todo el trabajo hasta ahora:** el costo no es un número fijo, depende muchísimo de cómo es la fila. En filas normales, cuesta entre 1,4 y 1,9 veces más espacio por cada actualización. Pero si la fila tiene una columna ancha de texto que PostgreSQL guarda aparte (se llama "TOAST", una técnica para columnas grandes) y esa columna en particular no cambió en esa operación, el costo llega a ser **36 veces más**. Eso es mucho.

**Estado: medido, pero solo en filas sintéticas.** Falta todavía averiguar cómo son las tablas reales de un proyecto real, para saber si ese escenario de 36 veces es raro o común en la práctica.

### 6. ¿Cuánto puede fallar la propia reacción automática?

Esta es la que más puede cambiar el rumbo del proyecto, y es la que acabamos de terminar de investigar a fondo.

**El contexto:** armamos una cadena completa de piezas ya existentes (sin código nuestro) para ver si podían hacer lo que proponemos: Debezium (que lee los cambios de PostgreSQL) envía los eventos a Kafka (un sistema de mensajería), y de ahí a Argo Events (una herramienta que puede reaccionar a eventos ejecutando acciones en Kubernetes, como frenar un Job).

**El problema que encontramos:** si justo en el momento en que llega un evento, el pod de Argo Events que debía procesarlo (llamado "Sensor") está siendo reemplazado por uno nuevo (algo normal y constante en Kubernetes), el evento se podía perder.

**Por qué pasa, explicado desde cero:**

- **¿Qué es un "bus" en este contexto?** Es el sistema intermediario de mensajería que recibe los eventos de un lado y los entrega al otro, parecido a una oficina de correos: los mensajes pasan por ahí antes de llegar a su destino final. En nuestro caso, el bus es un sistema llamado **NATS** (que significa *Neural Autonomic Transport System*, aunque en la práctica el nombre no importa tanto como lo que hace: mover mensajes de forma confiable entre partes de un sistema distribuido). Específicamente usamos una variante suya llamada "NATS Streaming".
- **Lo que descubrimos:** cuando el pod viejo del Sensor muere, a veces no logra avisarle correctamente al bus "ya no estoy, no me mandes más nada". El bus entonces sigue pensando que ese pod muerto está activo, y le sigue entregando mensajes a él, como si fuera un buzón de correo al que alguien sigue mandando cartas aunque la persona ya no vive ahí. El mensaje se queda "atascado" esperando a que ese buzón fantasma lo recoja, cosa que nunca va a pasar.
- **¿Se pierde para siempre?** No necesariamente. El bus tiene su propio mecanismo de salud (llamado "heartbeat" o latido, como el pulso) que cada cierto tiempo revisa si sus miembros siguen vivos. Cuando por fin se da cuenta de que ese pod ya no existe (algo que tardó entre 5 y 6 minutos en nuestras pruebas), libera el mensaje atascado y se lo entrega a un pod que sí está vivo. Entonces el mensaje llega, pero muy tarde.
- **El caso realmente grave:** si durante esos 5 o 6 minutos de atraso llega un mensaje *más nuevo* y ese sí se procesa normalmente, el sistema puede descartar el mensaje viejo cuando por fin se libera, pensando que ya quedó obsoleto. En ese caso el mensaje original, el que reportaba el incidente real, se pierde de verdad, no solo se atrasa. Confirmamos esto en pruebas controladas: pasó en 6 de 6 intentos.
- **¿Se puede arreglar cambiando una configuración?** Probamos varias cosas: acortar el tiempo del "latido" (ayuda a que se note más rápido, pero no evita el descarte del mensaje viejo), y usar un mecanismo distinto de coordinación de Kubernetes (ayudó bastante a que el pod se despida correctamente, pero no cubre todos los casos, como cuando el pod muere de golpe sin aviso, por ejemplo por quedarse sin memoria). Ningún ajuste que probamos cierra el problema del todo en este sistema de mensajería en particular.

**Lo más importante de este hallazgo para el rumbo del proyecto:** esto se probó en una cadena hecha con piezas externas (Argo Events y NATS), no en el mecanismo propio que este proyecto construiría. No sabemos todavía si nuestro propio operador tendría exactamente esta misma falla, porque esa pieza propia nunca se ha construido. Pero ya sabemos, con evidencia real, qué tipo de falla hay que cuidar: un pod que parece estar vivo para el sistema de mensajería pero en realidad ya murió, y un mensaje viejo que puede perderse si uno nuevo lo "adelanta".

**Estado: mecanismo confirmado**, con evidencia medida, no es una suposición.

**Nota de diseño para cuando se construya la pieza propia (entrega garantizada):** revisamos un proyecto externo, `cilium-explain`, que diagnostica fallas de red en Kubernetes. No vamos a usar su código, pero su patrón de diseño nos parece útil: en vez de encadenar condicionales (si pasa A, revisa B, si no, revisa C), ellos corren siempre *todas* las reglas de diagnóstico de forma independiente, cada una produce su propio hallazgo con su propia evidencia, y un componente aparte ("el correlacionador") ordena esos hallazgos en una causa principal y causas secundarias. La ventaja: un hallazgo nunca tapa a otro. Si alguna vez construimos la lógica de "entrega garantizada" que esta sub-pregunta dice que falta, vale la pena diseñarla así desde el principio. Queda anotado en `ROADMAP.md`, en la fila de "Content policies and a Kubernetes reaction".

### 7. ¿Sobre qué trabajo (workload) se está actuando realmente?

Cuando la reacción automática frena o modifica algo en Kubernetes, ¿estamos seguros de que actuamos sobre el trabajo correcto, el que de verdad causó el problema, y no sobre otro que coincide por casualidad en tiempo o en nombre?

**Estado: sin probar.** No se ha hecho ningún experimento sobre esto todavía.

## Qué significa todo esto para hacia dónde va el proyecto

Desde el principio, el proyecto se puso por escrito una condición: *"si la reacción automática puede perder eventos por su propio diseño, hay que construir un mecanismo de entrega garantizada antes de construir ninguna regla sobre el contenido de los datos."*

La sub-pregunta 6 (arriba) le dio a esa condición evidencia real por primera vez, al menos para una cadena de referencia armada con piezas existentes. Ya no es una preocupación teórica: sabemos exactamente qué tipo de falla puede ocurrir y por qué.

Esto no significa que el proyecto esté condenado, ni que esté confirmado. Significa que, si seguimos adelante construyendo nuestro propio mecanismo de reacción, ese mecanismo tiene que estar diseñado desde el principio para resistir este tipo de falla (un "miembro fantasma" que parece vivo), y eso probablemente haya que resolverlo antes de construir cualquier política sobre el contenido de los cambios.

## Lo que sigue, en orden

1. **La "matriz del diente" del heartbeat.** Entender cómo interactúan dos configuraciones del latido de salud (entre 5 y 120 segundos) con la frecuencia de guardado de posición (entre 10 y 60 segundos), para dejar de adivinar por qué el patrón de alertas se ve como un diente de sierra.
2. **Clasificar las reglas de negocio de proyectos reales de código abierto** (sub-preguntas 1 y 2): cuáles caben en un constraint o trigger normal, y cuáles de verdad necesitan ver el contenido con CDC. Esto es lo que termina de cerrar la pregunta central del proyecto.
3. **El híbrido de la sub-pregunta 4:** restaurar un respaldo y después reaplicar encima los cambios buenos que se perdieron, para compararlo de verdad contra reparar desde el flujo.
4. **La fase 2 del heartbeat:** enseñarle al sensor a distinguir una tabla que simplemente no tiene actividad (normal) de un conector que de verdad se quedó atascado (un problema real).

## Dónde está cada cosa, si quieres profundizar

- **Hallazgos técnicos consolidados, con más detalle:** `bench/experiments/FINDINGS.md`
- **El resultado completo de cada experimento, con los números crudos:** `bench/experiments/<nombre-del-experimento>/RESULTS.md`
- **Lista detallada de pendientes y decisiones abiertas:** `docs/HANDOFF.md`
- **La presentación y el guion para explicarle esto al equipo:** `docs/overview/checkup/`
- **El criterio de salida (R9) y el plan de conversaciones con usuarios reales:** `docs/research/user-conversations.md`
