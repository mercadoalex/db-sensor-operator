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
Server process. The Server it runs does expose native connector metrics (JMX counters; see
ADR-013 for how our derived counts relate to them). It does **not**: evaluate policies on change
content, diagnose replication-slot risk, or react in the cluster. That gap is the whole reason
Lactifluus exists.

## Where we are, and are not, doing something new

| Capability | Strimzi | Debezium (Connect) | Debezium Server operator | Lactifluus |
|---|---|---|---|---|
| Run Kafka / Connect on K8s | yes | via Strimzi | no | no (by design) |
| Run Debezium Server on K8s | no | no | **yes** | orchestrates it |
| Emit change events | no | yes (to Kafka) | yes (to a sink) | consumes them |
| Metrics about the stream | no | JMX counters exist | JMX counters exist (Debezium Server's connectors expose them too) | yes, derived from the events (ADR-013) |
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
layer and content policies, but loses the deep slot diagnosis and server-side probes.** So
it is "full vs partial", not "works vs does not".

| Capability | DB in cluster (StatefulSet) | Managed DB (RDS, Cloud SQL) |
|---|---|---|
| Observe the slot, diagnose WAL | Yes | Partial: the slot is visible over SQL, but free disk and some server settings are not |
| `REPLICA IDENTITY FULL` for content policies | You control the DB, you set it | Depends on provider permissions; often possible |
| Server-side attribution (a uprobe for the `txId`; needs access to the DB node) | Possible | **Impossible** — the server runs on the provider's machine (`attribution-ebpf/UPROBE-NOTES.md`) |
| Client-side attribution (which pod opened the connection) | Possible | **Possible** — the client pod runs on a node we control (see ADR-017) |
| Content policy (value transition, sequence) | Yes | Yes — only needs Debezium to connect and the right replica identity |
| React in Kubernetes (stop a Job, alert) | Yes | **Yes** — the reaction is on the K8s side, independent of the DB |
| "Same cluster, one YAML" story | Fits naturally | The DB lives outside; the story splits |

What does NOT change with a managed DB (do not overstate the in-cluster advantage):
- The **reaction** is a Kubernetes action (fail a Job, alert). A Job writing to an RDS is still
  stoppable. Managed does not block this.
- **Content policies** work against any database Debezium can reach, with the right replica
  identity. They do not need node access.

What is lost with a managed DB: the deep slot/disk diagnosis, and every server-side probe (a
uprobe on the PostgreSQL process is impossible there, per `attribution-ebpf/UPROBE-NOTES.md`).
Client-side attribution (which pod connected) is not lost: it works for any database.

The capability we have actually demonstrated is the content policy (`row-content`,
`sequence-gap`), and it works on managed databases. Per-change attribution is not demonstrated
(see `attribution/RESULTS.md`, ADR-017); it is the add-on, not the core.

Honest caveat: this is technical analysis, not market evidence. Many teams run managed Postgres
precisely to avoid operating the database. If the market is mostly managed, the full-value
in-cluster scenario serves fewer teams, and the value rests on content policies + reaction, which
do work on managed. This is a real bet to validate, not a fact.

### What the industry surveys say (and what they do not)

No source gives the exact split we want (self-hosted StatefulSet vs managed, in production). It
does not seem to be published; hyperscalers cannot see what runs inside your cluster, and
Postgres.org does not survey this. The closest signals, all pointing the same way:

- **Data on Kubernetes Community Report 2024** (cited by [Percona](https://experience.percona.com/reports/state-of-open-source-db-management-2025/cloud-native-computing-continues-to-reshape-the-space),
  [CNCF](https://www.cncf.io/blog/2026/03/05/the-great-migration-why-every-ai-platform-is-converging-on-kubernetes/)):
  databases are the #1 workload on Kubernetes for the third year running; nearly half of
  organizations run 50%+ of their database workloads in production on Kubernetes, the most
  advanced over 75%.
- **Voice of Kubernetes Experts 2025** (Portworx/Dimensional Research, 500+ practitioners):
  ~98% run data-intensive workloads on cloud-native platforms; databases at ~69%.
- **OperatorHub** grew from 7 database operators to ~50 ([Percona](https://www.percona.com/blog/the-evolution-of-stateful-applications-in-kubernetes/)),
  which only makes sense if people run databases inside the cluster.

Two cautions before leaning on these:
1. **"Database on Kubernetes" is not the same as "self-hosted StatefulSet".** A survey may count
   a workload managed by an operator, or data workloads in general. The exact in-cluster vs
   managed cut is not in these numbers.
2. **The sources are cloud-native vendors** (Percona, Portworx, AppsCode) with an interest in
   "yes, run your DB on K8s". Counter-voices exist (e.g. ["Why I haven't run my databases on
   Kubernetes"](https://substack.com/redirect/8f8ee623-2601-4e68-a003-082c342891ec)). Treat the
   numbers as a directional signal, not neutral truth.

### "Database on Kubernetes" hides three different cases

The survey phrase "runs its database on Kubernetes" blends three situations, and only two are
our full-value case:

1. **Managed DB, outside the cluster.** The app runs on K8s, the database is an RDS / Cloud SQL
   outside it. Some surveys still count this as "uses K8s with a database". Here Lactifluus loses
   server-side probes and deep slot/disk diagnosis.
2. **In-cluster DB via an operator** (CloudNativePG, Percona, Crunchy, Zalando). Self-hosted, but
   an operator handles backups, failover, upgrades. **Lactifluus has its full value here.**
3. **In-cluster DB as a bare StatefulSet** (no operator, like our bench). Also full value, but
   rare for a critical database.

A single survey number merges all three, so it cannot tell us the managed (1) vs in-cluster (2+3)
split. That is why the earlier numbers do not answer the question directly. For us, case 2 is the
realistic full-value target: almost nobody runs a critical database as a bare StatefulSet.

### The two trends, both true at once

- **Toward managed / cloud.** Red Gate's State of the Database Landscape 2024: hosting databases
  mostly or fully in the cloud rose 18% (2020) to 36% (2023), on-premises fell 53% to 31%
  ([Red Gate](https://www.red-gate.com/solutions/state-of-database-landscape/2024/cloud/)).
  Startup Database Trends 2025: most teams run databases on managed cloud platforms
  ([Springtail](https://www.springtail.io/research/startup-database-trends-2025)). AWS RDS leads
  the managed market.
- **Toward in-cluster self-hosted.** Databases are the #1 K8s workload (DoK 2024), OperatorHub
  DB operators grew 7 to ~50, and **CloudNativePG** is now the de facto standard PostgreSQL
  operator: a CNCF project (from EDB), 5,000+ GitHub stars and ~58M downloads
  ([dev.to](https://dev.to/x4nent/complete-guide-to-cloudnativepg-129-postgresql-18-production-kubernetes-database-operator-setup-4kle),
  [EDB](https://www.enterprisedb.com/blog/4300-github-stars-and-counting-cloudnativepg-now-worlds-1-postgresql-operator-kubernetes)).

These do not contradict: "cloud" includes running the DB inside a K8s cluster, which is also
self-hosted. Cloud grows overall, and within it, in-cluster PostgreSQL grows too.

### The tooling of the self-hosted-on-K8s crowd (who our full value serves)

People who run PostgreSQL self-hosted on Kubernetes mostly use an operator, not a bare
StatefulSet. The field, by adoption:
- **CloudNativePG** — the de facto standard, CNCF, the default for new deployments.
- **Percona Operator for PostgreSQL**, **Crunchy Data**, **Zalando** — all built on Patroni,
  pgBackRest, PgBouncer ([Percona comparison](https://www.percona.com/blog/run-postgresql-in-kubernetes-solutions-pros-and-cons/)).

Strategic implication: our full-value audience is not "whoever hand-rolls a StatefulSet", it is
**whoever runs CloudNativePG** (and peers). A concrete next question: should Lactifluus be tested
and positioned alongside CloudNativePG? That is where the users who can use our entire value
actually are. Unverified whether our `DatabaseSensor` cleanly targets a CloudNativePG-managed
instance; worth a focused check.

### What CloudNativePG (CNPG) is, and why it is our closest neighbor

CNPG is a Kubernetes operator that runs PostgreSQL **inside** the cluster: you declare a Postgres
cluster in YAML and it handles deployment, replication, failover, backups and upgrades, the work
a DBA would do by hand ([docs](https://cloudnative-pg.io/documentation/1.24/),
[architecture](https://cloudnative-pg.io/documentation/1.20/architecture/)).

- **Open source, yes.** Apache 2.0, a CNCF project (originated at EDB), vendor-neutral. The de
  facto standard PostgreSQL operator.
- **What it is based on.** Unlike Percona/Crunchy/Zalando, it does **not** use Patroni, repmgr or
  Stolon. It integrates directly with the Kubernetes API and uses PostgreSQL's **native physical
  replication** (WAL shipping / streaming), with its own **instance manager** beside each Postgres
  ([README](https://github.com/cloudnative-pg/cloudnative-pg/blob/main/README.md),
  [instance manager](https://cloudnative-pg.io/docs/1.26/instance_manager)).

Why it matters to us:
- **Not a competitor, a complement.** CNPG manages the database's *lifecycle* (bring it up,
  replicate, heal). It does not observe change content or react to it; it does not do CDC.
  Lactifluus observes data *changes* and would react to them; it does not manage the database. Two
  different layers on the same database.
- **Same philosophy.** Both are genuinely Kubernetes-native (reconciliation, declarative), so they
  speak the same language. The ideal full-value Lactifluus user is someone already running CNPG:
  Postgres in the cluster, operated declaratively, missing exactly the layer we propose.
- **A pitch-grade coincidence:** CNPG uses PostgreSQL's WAL to replicate; Debezium/Lactifluus use
  the same WAL to capture changes. Same underlying mechanism, two purposes. "Lactifluus on top of
  CNPG" is a technically coherent story, not a forced one.

Honest limits: "complementary" is our technical reading, not something the CNPG community has
said. And it is unverified that our `DatabaseSensor` targets a CNPG-managed instance cleanly (it
should, since it is a Postgres with logical WAL, but it needs a test).

Reading for us: the trend favors managed overall, but in-cluster self-hosted (mostly via
CloudNativePG) is a large and growing slice, and it is our full-value home. The precise managed
vs in-cluster split is not published, so R8 (asking each team directly, plus "which operator?")
remains the way to size it for our audience.
Content was rephrased for compliance with licensing restrictions.

Connects to R8: the conversation guide already asks "is your Postgres self-operated or managed?"
(`docs/research/user-conversations.md`, Part 3). That answer, across conversations, tells us
whether the in-cluster full-value case is common enough to lead with, or whether we should pitch
only the managed-safe subset (content policy + reaction). Decide the positioning from that, not
from taste.

## How database monitoring tools get their data (read 2026-11-05, docs only)

Question: when a PostgreSQL runs in Kubernetes, how do the common monitoring tools obtain data
from it, and what do they carry? Read in each tool's public docs. None was installed or tested,
and the list is not exhaustive.

| Tool | How it gets the data | What it carries | Source |
|---|---|---|---|
| CloudNativePG | Built-in Prometheus exporter on each instance (port 9187), with predefined metrics plus SQL you define in a ConfigMap or Secret | Aggregated metrics | [EDB docs](https://www.enterprisedb.com/docs/postgres_for_kubernetes/latest/monitoring/) |
| postgres_exporter | Translates PostgreSQL statistics views into Prometheus metrics | Aggregated metrics | [JusDB guide](https://www.jusdb.com/blog/postgresql-monitoring-prometheus-postgres-exporter) |
| OpenTelemetry PostgreSQL receiver | Periodically queries statistics views such as `pg_stat_database` | Aggregated metrics | [Uptrace guide](https://uptrace.dev/guides/opentelemetry-postgresql) |
| Datadog Database Monitoring | The Agent runs SQL on an interval (15 s default for custom metrics) | Query metrics, query samples, explain plans, wait events, blocking | [Datadog docs](https://docs.datadoghq.com/database_monitoring/setup_postgres/aurora/) |
| Dynatrace | A remote ActiveGate extension queries the database; sampled every minute | Top 200 queries by cost, normalized | [Dynatrace docs](https://docs.dynatrace.com/docs/observe/infrastructure-observability/databases/database-app/data-collected) |
| Lactifluus | Reads the write-ahead log through Debezium (CDC) | The change itself: content and order | this repo |

What this supports (a reading of documentation, not a measurement):
- The common path is **SQL on an interval over statistics views**. That yields totals, rates and
  query shapes. The content and the order of a change are not on that path. This agrees with our
  own experiments, where counting did not need CDC (`bench/experiments/FINDINGS.md`).
- Dynatrace replaces literal values with placeholders on purpose, to remove personal data. So it
  is a design choice, not only a limit. It is also a warning for us: reading content raises a
  privacy question. ADR-017 therefore records a reference to the change, not a copy of the row.
- CDC is not free. Our backpressure experiment measured retained WAL growing when the consumer is
  slow. Its cost against polling is **not measured**, so do not claim CDC is more efficient.

What this does NOT support:
- That no monitoring tool uses the WAL or CDC. Only five were read.
- That any of these cannot react inside Kubernetes. Not checked.

### A closer neighbour to check: db-query-operator

[konnektr-io/db-query-operator](https://github.com/konnektr-io/db-query-operator) polls a database
(PostgreSQL), runs a user-defined SQL query and renders Kubernetes manifests from each returned
row. That is "Kubernetes reacts to data" by **polling**, which is close to our hypothesis. From
the repository description only; nothing else read yet. See the follow-ups below.

## Open follow-ups

- **Review db-query-operator** (and other database monitoring tools) properly: what triggers it,
  what it can react to, how it scales, whether it sees order or only current state. This closes
  the gap left by Appendix A1, which only covers the Debezium and Strimzi ecosystem.

- Confirm the exact scope of Debezium 3.7 "alerting" (read the release notes in full; only the
  blog summary was seen).
- If pursuing the existing-Strimzi-user case: can Lactifluus consume an existing Kafka topic (not
  only our HTTP Debezium Server)? Unverified; relates to the "attach to an existing Debezium" gap
  already in the handoff.
