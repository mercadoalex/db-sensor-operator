# Competitive landscape: Debezium on Kubernetes, Strimzi, and where we differ (R7)

What the existing Kubernetes projects around Debezium do, so we know we are not rebuilding
them, and where (if anywhere) our angle is actually new. Checked against the sources below on
2026-11-05; claims are paraphrased for licensing and link to the source.

## The three ways to run Debezium (this matters for every comparison)

Debezium has three runtimes ([Debezium install docs](https://debezium.io/installation/), and a
good third-party breakdown at [sequinstream](https://blog.sequinstream.com/the-debezium-trio-comparing-kafka-connect-server-and-engine-run-times/)):

1. **Kafka Connect** — the common, best-supported way. Connectors run as source connectors inside
   a Kafka Connect cluster and write change events to Kafka topics. Needs a Kafka broker.
2. **Debezium Server** — a standalone process: the same connectors, handing each event to a
   configured sink, one process and one properties file, no Kafka cluster to stand up. The
   process is the unit of scale and of failure ([Debezium Server](https://debezium.io/installation/server/)).
3. **Embedded engine** — a library inside your own app. No Kafka.

**We use #2, Debezium Server, over HTTP** (ADR-012: no broker). That is a deliberate difference
from the mainstream #1 path, and it is why the official Kubernetes docs below do not describe us.

## What the official "Debezium on Kubernetes" doc actually sets up

The [3.7 Kubernetes doc](https://debezium.io/documentation/reference/3.7/operations/kubernetes.html)
is the **Kafka Connect** path. It deploys, in order: the **Strimzi operator**, a 3-node Kafka
cluster, a MySQL data source, a Kafka Connect cluster (built with the connector plugin), and
finally a `KafkaConnector` custom resource. The change events land on a Kafka topic you consume
with `kcat`.

Takeaways for us:
- The official K8s story is Kafka-centric and Strimzi-centric. It is heavy: a broker, Connect
  workers, topics. That is the weight our HTTP Debezium Server path avoids.
- The event payload shown carries `before` and `after` for an update (MySQL example). This
  matches our `row-content` experiment: MySQL gives `before` out of the box, while PostgreSQL
  needs `REPLICA IDENTITY FULL` (our measured caveat).
- Secret handling there uses a `Role` restricted by `resourceNames` to a single secret, read by
  Kafka Connect via a secret config provider. Worth noting against our own Secret-cache fix: they
  scope the read to one named secret; we recently moved to `get`-only with no list/watch.

## What Strimzi is (and is not, for us)

[Strimzi](https://strimzi.io/) runs **Apache Kafka on Kubernetes**: the broker, Kafka Connect,
topics, users, MirrorMaker, all as custom resources, managed with `kubectl`. It is infrastructure
for Kafka.

For our purposes: Strimzi is **not a competitor and not useful to adopt**. It solves "run Kafka
and Connect on K8s". We deliberately avoid Kafka (Debezium Server + HTTP). The only point of
contact is that if a user already runs Strimzi + Kafka Connect + Debezium, they already have the
change stream on a topic — and our value would then have to be the *reaction layer* on top, not
the capture.

## What the official Debezium Operator is (the one we orchestrate)

Separate from the Strimzi/Connect path, there is an official **Debezium Operator** that manages
**Debezium Server** instances on Kubernetes (Helm `debezium/debezium-operator`, on OperatorHub).
This is the one Lactifluus creates `DebeziumServer` CRs for. It handles the lifecycle of the
Server process. It does **not**: derive observability signals, evaluate policies on change
content, diagnose replication-slot risk, or react in the cluster. That gap is the whole reason
Lactifluus exists.

## Where we are, and are not, doing something new

| Capability | Strimzi | Debezium (Connect) | Debezium Server operator | Lactifluus |
|---|---|---|---|---|
| Run Kafka / Connect on K8s | yes | via Strimzi | no | no (by design) |
| Run Debezium Server on K8s | no | no | **yes** | orchestrates it |
| Emit change events | no | yes (to Kafka) | yes (to a sink) | consumes them |
| Derive metrics from the stream | no | JMX counters exist | no | yes |
| Diagnose replication-slot / WAL risk | no | no | no | yes (with a known gap, see backpressure) |
| Evaluate a policy on change *content* | no | no | no | prototyped in experiments, unbuilt |
| React in Kubernetes to the data | no | no | no | the proposed direction, unbuilt |

## Honest conclusions

- **We are not rebuilding Strimzi or the Debezium operators.** They run Kafka / the connector /
  the Server. We sit above a running Debezium Server and add observability, slot diagnosis, and
  (proposed) content policies with a Kubernetes reaction. No overlap on capture.
- **The capture layer is a solved commodity.** Three runtimes, two operators, an official K8s
  guide. Anyone framing a new product around "capture changes on K8s" loses. This is the same
  lesson as the experiments: counting/capturing is not where we add value.
- **Our only defensible ground is the reaction layer** (policy on content/sequence, acting in the
  cluster), which none of the above does. That is exactly what the user conversations (R8) must
  validate before we build it.
- **One thing to watch:** Debezium 3.7's platform is adding alerting
  (noted earlier from the 3.7.0.Beta2 release blog). If upstream moves into alerting, "metrics and
  alerts" differentiates us even less, reinforcing that content-policy + reaction is the only
  angle worth pursuing.

## Hypothesis: in-cluster database vs managed database

A Kubernetes install usually has the database either **in the cluster** (self-hosted, a
StatefulSet) or **external / managed** (RDS, Cloud SQL, Azure DB). Not dogmatic — it depends on
many factors. The question: is there a scenario where Lactifluus is clearly better suited to one?

Hypothesis (grounded in the experiments, not market-validated): **Lactifluus delivers its full
value when the database is in the cluster; with a managed database it still gives the reaction
layer and content policies, but loses the deep slot diagnosis and kernel-level attribution.** So
it is "full vs partial", not "works vs does not".

| Capability | DB in cluster (StatefulSet) | Managed DB (RDS, Cloud SQL) |
|---|---|---|
| Observe the slot, diagnose WAL | Yes | Partial: the slot is visible over SQL, but free disk and some server settings are not |
| `REPLICA IDENTITY FULL` for content policies | You control the DB, you set it | Depends on provider permissions; often possible |
| Attribution via eBPF / `txId` (node access) | Possible | **Impossible** — no access to the DB node (measured, `attribution-ebpf/UPROBE-NOTES.md`) |
| Content policy (value transition, sequence) | Yes | Yes — only needs Debezium to connect and the right replica identity |
| React in Kubernetes (stop a Job, alert) | Yes | **Yes** — the reaction is on the K8s side, independent of the DB |
| "Same cluster, one YAML" story | Fits naturally | The DB lives outside; the story splits |

What does NOT change with a managed DB (do not overstate the in-cluster advantage):
- The **reaction** is a Kubernetes action (fail a Job, alert). A Job writing to an RDS is still
  stoppable. Managed does not block this.
- **Content policies** work against any database Debezium can reach, with the right replica
  identity. They do not need node access.

What is lost with a managed DB: the deep slot/disk diagnosis, and attribution by kernel/node
(eBPF, uprobe) — which `attribution-ebpf/UPROBE-NOTES.md` already found is impossible on managed
databases. So our single most distinctive capability (per-change attribution) serves only
in-cluster databases.

Honest caveat: this is technical analysis, not market evidence. Many teams run managed Postgres
precisely to avoid operating the database. If the market is mostly managed, our strongest
differentiator (attribution) serves few, and the value narrows to content policies + reaction,
which do work on managed. This is a real bet to validate, not a fact.

Connects to R8: the conversation guide already asks "is your Postgres self-operated or managed?"
(`docs/research/user-conversations.md`, Part 3). That answer, across conversations, tells us
whether the in-cluster full-value case is common enough to lead with, or whether we should pitch
only the managed-safe subset (content policy + reaction). Decide the positioning from that, not
from taste.

## Open follow-ups

- Confirm the exact scope of Debezium 3.7 "alerting" (read the release notes in full; only the
  blog summary was seen).
- If pursuing the existing-Strimzi-user case: can Lactifluus consume an existing Kafka topic (not
  only our HTTP Debezium Server)? Unverified; relates to the "attach to an existing Debezium" gap
  already in the handoff.
