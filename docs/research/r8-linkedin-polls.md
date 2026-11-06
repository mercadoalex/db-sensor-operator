# R8 on LinkedIn: four polls and an invitation message

Free on a personal account: a poll in a post (2 to 4 options, 1 day to 2 weeks) and direct
messages to 1st-degree connections. The limits below (question 140 characters, option 30) come
from third-party guides, not LinkedIn's own pages; every text here was checked against them with a
script on 2026-11-06.

None of the polls names CDC, Debezium or Lactifluus, as the R8 guide asks
([`user-conversations.md`](user-conversations.md)): they start from the problem.

## How to run them

- One poll per post. Publish one every 2 or 3 days, poll 1 first.
- Duration: **1 week** (LinkedIn's default). Use 2 weeks only if a poll has almost no votes after
  3 days.
- Votes find people; they do not decide R9. Message anyone who comments with a story, or who
  votes "a user or customer told us" or "by chance, later" in poll 1, and ask for 10 minutes.
- Record where each answer came from in the capture sheet ([`r8-capture-sheet.md`](r8-capture-sheet.md)).
  Your network knows you and leans towards yes.
- Send messages by hand, not with automation tools.

## Poll 1 — the pain (publish first)

**Published 2026-11-06 (English, 1 week)** with the shorter question below. The first attempt
said the text was too long; the likely cause was the post text pasted into the question field (the
post text is 240 characters, the question 73). Hashtags used: #PostgreSQL #SRE #DevOps
#PlatformEngineering #Kubernetes (no #CDC or #Debezium, to avoid priming).

| Published (EN) | |
|---|---|
| Question | Has a job or deploy ever changed data unexpectedly? How did you find out? |
| 1 | An alert caught it |
| 2 | A customer told us |
| 3 | By chance, later |
| 4 | Never happened to us |

The longer versions below were the drafts.

**Post (ES)**
> Pregunta rápida para quienes operan bases de datos: ¿alguna vez un job, una migración o un deploy cambió datos de una forma que no esperaban? Si les pasó, me encantaría leer la historia en los comentarios: qué pasó y cuánto tardaron en darse cuenta.

**Post (EN)**
> Quick question for anyone who runs databases: has a job, a migration or a deploy ever changed data in a way you didn't expect? If it happened to you, I'd love to read the story in the comments: what happened, and how long it took to notice.

| | ES | EN |
|---|---|---|
| Question | ¿Un job, migración o deploy cambió datos de forma inesperada alguna vez? ¿Cómo se enteraron? | Has a job, migration or deploy ever changed data in a way you didn't expect? How did you find out? |
| 1 | Una alerta lo detectó | An alert caught it |
| 2 | Nos avisó un usuario/cliente | A user or customer told us |
| 3 | Por casualidad, después | By chance, later |
| 4 | Nunca nos ha pasado | Never happened to us |

Reading: "an alert" means their tools already cover it. "A user or customer" or "by chance" means
today's monitoring does not see it, which is the gap.

## Poll 2 — the reaction

| | ES | EN |
|---|---|---|
| Question | Un job rompe una regla de datos (un pedido pagado vuelve a pendiente). ¿Qué debería pasar? | A job breaks a data rule (a paid order goes back to pending). What should happen? |
| 1 | Solo alertar | Just alert |
| 2 | Pausar y pedir aprobación | Pause it, ask for approval |
| 3 | Detener el job solo | Stop the job automatically |
| 4 | Prevenirlo en la base | Prevent it in the database |

Reading: the open "brake" decision. Option 4 is there on purpose: if it wins, that is the R9
counterargument (database-side prevention beats detection).

## Poll 3 — where the database runs

| | ES | EN |
|---|---|---|
| Question | ¿Dónde corre el PostgreSQL de tus apps en Kubernetes? | Where does the PostgreSQL behind your Kubernetes apps run? |
| 1 | En el cluster (operador) | In the cluster (operator) |
| 2 | Gestionado (RDS, Cloud SQL) | Managed (RDS, Cloud SQL...) |
| 3 | En VMs o bare metal | VMs or bare metal |
| 4 | No usamos Kubernetes | Our apps aren't on K8s |

Reading: the three deployment scenarios (in cluster, managed, outside the cluster), and a filter
for people it does not apply to.

## Poll 4 — what versus who

| | ES | EN |
|---|---|---|
| Question | Unos datos cambiaron como no debían. ¿Qué necesitas saber primero? | Data changed in a way it shouldn't. What do you need to know first? |
| 1 | Qué filas y valores | Which rows and values |
| 2 | Qué job o servicio fue | Which job or service did it |
| 3 | Ambas por igual | Both, equally |
| 4 | Cuántas filas cambiaron | How many rows changed |

Reading: "rows and values" is content (only CDC sees it). "Which job" is the "who", the most
expensive part in our experiments. "How many rows" is volume, which a counter covers.

## Invitation message (1st-degree connections, sent by hand)

Personalise the first line with what they voted or commented. Ask for a conversation, not for
feedback on a product.

**ES**
> Hola [nombre], vi tu [voto / comentario] en la encuesta sobre cambios inesperados en los datos. Estoy investigando cómo los equipos se enteran de esos incidentes y qué les habría gustado que pasara. ¿Tendrías 10 minutos esta semana o la próxima para contarme tu caso? No te voy a vender nada; me interesa tu experiencia, incluso si la respuesta es "nuestras herramientas ya lo cubren". Lo que me cuentes lo anoto sin tu nombre ni el de tu empresa, salvo que me digas que sí.

**EN**
> Hi [name], I saw your [vote / comment] on the poll about unexpected data changes. I'm researching how teams find out about those incidents and what they wish had happened. Would you have 10 minutes this week or next to tell me about your case? I'm not selling anything; I'm interested in your experience, even if the answer is "our tools already cover it". I'll note what you tell me without your name or your company's, unless you say it's fine.
