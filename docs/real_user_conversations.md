Haripriya commented, Feb 9
Hi All,

We’re running Debezium Platform on Kubernetes (Kafka via Strimzi, Postgres source) and are evaluating blue/green deployment strategies where zero CDC downtime is critical (upgrades, config changes).

Looking for community input on a few points:

• Can blue & green Debezium deployments coexist in the same K8s cluster against the same source DB?
• Is it ever safe for blue & green to share metadata (offsets, schema history, replication slots), or must these always be isolated?
• For true zero downtime, is using separate DB read replicas per deployment the only safe pattern?

In blue/green scenarios:

• Is using the same Kafka cluster recommended?
• Is using the same Kafka topics supported?
• When (if ever) is separate Kafka required?

Are there officially recommended patterns for zero-downtime:

• Debezium platform upgrades
• Connector upgrades
• Schema/config evolution

Main concerns:
• Duplicate events
• WAL/binlog contention
• Offset or schema corruption

If anyone has implemented zero-downtime blue/green with Debezium, I’d really appreciate hearing about:

• Architecture
• Cutover strategy
• Lessons learned

Thanks!

Mario Fiore Vitale
3:28 AM
Hi @Haripriya what do you want to achieve with zero-downtime?

Do you intend to avoid missing change events or reduce the consumer lag between the db event and when it is processed?

Haripriya
6:00 AM
@Mario Fiore Vitale
My blue-green strategy is mostly for upgrade scenarios. Blue app is active and running with version v1. Green is installed with version v2. Before switching, I want to run green to see everything works as intended.

We also spin up inactive clusters, ensure apps are running in the new inactive clusters, while the active clusters are running.

I want to understand what i should be careful of in terms of such scenarios.

Chris Cranford 7:32 AM
@Haripriya on the green ones, you won't be able to access the same resources the blue one is accessing. You can't share schema history topics, replication slots, or offsets. You could have a standalone independent, temporary connector to verify it works with one table or something like that however that you throw away after your initial tests are complete.

These isolated resources are irrespective of the runtime, whether its DS standalone, DS on the Platform, Kafka Connect, or even the Embedded Engine etc.

Haripriya 12:47 AM
Hi @Chris Cranford and @Mario Fiore Vitale
I was revisiting on the upgrade strategy for Debezium Management Platofrm.
What is the recommended strategy for upgrade, if blue-green is not recommended?

Mario Fiore Vitale: Hi @Haripriya If you specifically refers to the Debezium Platform components: stage and conductor pods the current update strategy is the k8s default one: RollingUpdate. This guarantees to you the zero-downtime of those components.Mario Fiore Vitale: The pipelines, so the relative DS instances, will not be impacted by the update of the Debezium Platform.Mario Fiore Vitale: they will continue to run independently

Haripriya: So what about the operator component?
Mario Fiore Vitale: also the operator has a default RollingUpdate strategy

---------------------------------------------

We nned to taake a look of this 
An open changelog format : What the gap costs
Every producer has its own dialect, so every consumer keeps a translator for each.
https://open-cdc-hq.github.io/

-------------------------------------------------

Protobuf in Kafka Connect: Building a Converter for Apicurio Registry
maybe eBPF is not the answer, what about protobuf?


--------------------------------------------------------
- The question: can Kubernetes react to the behavior of the data?
here is another point to the roadmap
https://github.com/debezium/debezium-ai-python/
Introducing PyDebeziumAI: Real-Time CDC for LangChain and LangGraph
https://debezium.io/blog/2026/10/19/introducing-pydebeziumai/

----------------------------------------------------------------

Charllyson Souza
4:31 PM
Hi everyone!

I've been trying to enable distributed tracing in Debezium for a while now, but I'm failing. In my development environment using Docker, everything works perfectly. I followed the same example from the Debezium example repository and it worked very well; all the tracing is displayed correctly. The problem is when I try to apply this same behavior to my staging and production environments. To deploy KafkaConnect, I'm using strimzi.

-------------------------------------------------------------------------


Thành Văn: Hi Chris,Thành Văn: I would like to as code my connectors configuration, does debezium support read config from k8s configmap source and apply connectors config? I am seperated dbz deployment and connectors config as two part. When dbz run ok I curl PUT http request to create. Now I wanna automate connector creating step, can you advise a solution?

Chris Cranford

2:58 AM
Hi @Thành Văn if you're using Kafka Connect, which it sounds like you most likely are. In this use case, Debezium isn't responsible for any of the configuration management, that's all delegated and managed by the Kafka Connect runtime itself.

For K8s specifically, with Kafka Connect, users will use Strimzi to automate this for you. You deploy the CRDs to Kubernetes and then the Strimzi Operator manages the Kafka Connect cluster and KafkaConnector deployments on the cluster, moving the configuration from the ConfigMap into the Kafka Connect pod automatically.

----------------------------------------------------------------------
Hi @Chris Cranford
We are implementing a heartbeat mechanism in our Debezium connector to track replication health. The primary goal is to capture the latest heartbeat timestamp and confirm that data is flowing through to Kafka.

Currently, the heartbeat query runs across multiple schemas (FIN, HRM, ADM, COM, PB). For each schema, it:

Performs a MERGE (upsert) to insert/update rows with the current timestamp
Inserts two records (id = 1 and 2)
Deletes the record with id = 2 immediately after 
Partial heartbeat query shown below:

"heartbeat.action.query" BEGIN \n MERGE INTO FIN_DBZM.REPLICATION_HEARTBEAT dst USING (SELECT 1 AS id,'FIN_HEARTBEAT' AS connector_name,CURRENT_TIMESTAMP AS last_heartbeat FROM dual UNION ALL SELECT 2 as id, 'FIN_HEARTBEAT',CURRENT_TIMESTAMP FROM dual) src ON (dst.id = src.id) WHEN MATCHED THEN UPDATE SET dst.connector_name = src.connector_name,dst.last_heartbeat = src.last_heartbeat WHEN NOT MATCHED THEN INSERT (id, connector_name, last_heartbeat) VALUES (src.id, src.connector_name, src.last_heartbeat); \n DELETE FROM FIN_DBZM.REPLICATION_HEARTBEAT WHERE id = 2;\n MERGE INTO HRM_DBZM.REPLICATION_HEARTBEAT dst ...............(for all 5 schemas); \n END;"

This DML operations are performed to monitor if the CDC pipeline continuously emits events. The heartbeat interval is currently set to 15 minutes.

Given this setup, we wanted to get some perspectives on the following:
How does this pattern typically behave in environments with higher transaction volumes and multiple tables under CDC?
Are there any metrics to monitor or best practices when designing heartbeat mechanisms to ensure the data has reached Kafka?
Is this approach commonly used, or are there alternative patterns that are preferred in real-world implementations?
Our intent is to ensure the heartbeat mechanism:

Reliably reflects data flow to Kafka
Works efficiently alongside existing connector workloads
Would appreciate any insights or experiences from similar implementations.

Chris Cranford 8:42 AM
Hi @Vidyashree Raju.

Heartbeats are triggered on the event processing loop, so if you have any situation where something in the loop causes a delay, that can impact the frequency of heartbeats. For example, lets say that someone updates 100M rows and it takes LogMiner 20 minutes to churn through that data, you can expect the heartbeat to be delayed by at least 5 minutes from your normal 15 minute interval, because during the time we are waiting for Oracle to return the data, the processing loop is blocked.

You can use heartbeats to identify that data "is flowing to Kafka", but just understanding that it only reflects data from the heartbeat is flowing, not necessarily that there aren't other issues. Heartbeat's goal above all is to guarantee offsets in the Kafka topic do not become stale, nothing more.

As for the approach, I will say your heartbeat action query is quite complex. I don't quite understand your reasons why you need to have 2 rows the way you are, but if it works for you, that's perfectly fine. Oracle LogMiner records change vectors for tables, even if the column value is the same as it was before, so a simple upsert of a timestamp column would be more than adequate.

Vidyashree Raju
1:11 AM
Hi @Chris Cranford Thanks for your response, we have now simplified the action query to only use upsert statement but for all five schemas, FIN, HRM, ADM, COM and PB. The idea here is to monitor the pipeline health rather than the connector health alone, so we need to check if the data is flowing from Oracle --> debezium--> kafka --> snowflake (table updated every 15 mins which timestamp). It can be simple as one row getting updated every 15 mins, to achieve this we are using heartbeat property. The challenge we face now is the connector getting stalled and the pod crashes after 3-4 days due to space issue.
Could you please let us know if there are any specific Kafka Connect or Debezium properties should we tune to prevent these stalls and the subsequent disk space exhaustion?

Chris Cranford

1:30 AM
Hi @Vidyashree Raju I don't quite get the reason for the disk exhaustion. Can you share your full connector configuration?

-------------------------------------------------------------

rb
3:50 AM
Hello,

As a user, I think that the architecture documentation portion doesn't address how debezium behaves in distributed-mode. Points that I fail to understand are:

Is debezium a statefull or stateless application? What are the persistence requirements for it to keep track of offsets in distributed mode?
How a debezium operator deployment in k8s / openshift distribute load across different instances of the application? Can the traffic be split across different pods? Does the application run on a single pod? If multi-pod, how does each replica knows about traffic-partitioning?
If you could help me understand those attributes I would be thankful.

Best regards,

------------------------------------------------------------------------


Mauricio Zambrano: Hi, I was looking for a sink connector example with the operator. There is just a source connector example that works. Are you supposed to use something else than the operator to replicate from Kafka to PostgreSQL?Jiri Pechanec: @Mauricio Zambrano Hi, Debezium Server does not support sink connectors yet so there is no such example. If you are willing to use Kafka COnnect then you can take a look at Strimzi projet

--------------------------------------------------------------------

Debezium Operator 2.4.0 has been released to OperatorHub https://operatorhub.io/operator/debezium-operator

The operator is still in an incubation phase, however this is the next step towards full support.

---------------------------------------------------------------------------------
Aneesh Makala
5:06 AM
Hey folks, we seem to have hit a silent event loss with the PostgreSQL connector on restart in our of our tests. Sharing the repro steps.

Note:

we hit this originally on 2.5.2, but was able to repro on 3.6.0 as well. AI (claude) reproduced it, but i also manually verified it with 2.5.2.
It seems to ONLY reproduce if debezium.source.provide.transaction.metadata=true
Repro steps: (debezium-server, postgres:13 with wal_level=logical, we used the http sink):

Config:

debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector
debezium.source.plugin.name=pgoutput
debezium.source.snapshot.mode=never            # 3.x: no_data
debezium.source.table.include.list=public.t_long,public.t_short
debezium.source.offset.storage.file.filename=data/offsets.dat
debezium.source.offset.flush.interval.ms=0
debezium.source.provide.transaction.metadata=true
Tables:

CREATE TABLE t_long(id int primary key);
CREATE TABLE t_short(id int primary key);

Chris Cranford

6:08 AM
Hi @Aneesh Makala is the above your only debezium.source.* configuration?



Shishir Sharma
10:37 PM
Hi all, looking for guidance on the recommended way to keep confirmed_flush_lsn advancing when the connector filters out nearly all events.

Since DBZ-7816 the PG connector advances the slot only from committed Kafka offsets. So on a high-write DB where almost everything is filtered whether at the connector (skipped.operations, table.include.list) or via a Filter SMT, no records are produced, the slot stops moving, and WAL accumulates.

In testing, heartbeat.interval.ms does fix this: the heartbeat record carries the current LSN and its offset gets committed, so the slot advances. It works for connector-level filtering and for SMT filtering as long as the SMT passes the heartbeat records through. But if an SMT also drops the heartbeats (e.g. a broad Filter SMT), the slot stalls again which looks like DBZ-9490.

A couple of questions:

Is heartbeat.interval.ms the intended/recommended way to handle "everything filtered" WAL growth, or is the guidance to keep filtering in connector config rather than in SMTs?
Since the heartbeat record itself passes through the SMT chain, relying on it to advance the slot feels a bit fragile, is there a recommended pattern to shield heartbeats from SMTs, or any plan to advance offsets for processed-but-filtered positions independently of produced records?

Chris Cranford

11:00 PM
Hi @Shishir Sharma, regarding your questions:
(1) Yes, this is the goto solution, often times paired with heartbeat.action.query, too.
(2) Offsets advance based on the offset metadata that exists per event written to Kafka. So if the event gets filtered out, its offset metadata is discarded too. What I recommend is for the transformations, add a predicate that excludes the transformation from running on events that are destined for the heartbeat topic, and apply that predicate to each transformation in the SMT chain.


arujit17
6:48 AM
Problem summary
We see an issue exclusively on our high-throughput Debezium PostgreSQL connectors where streaming throughput drops dramatically (in the 3x to 100+x range) while the connector remains in RUNNING state. The drop has no log indication and only resolves when the affected connector or task is restarted. Lag accumulates on the replication slot during the incident and continues growing until restart.



DHRUV MEVADA
4:07 AM
Hi Debezium Community,

We experienced an issue on 15th October 2025, where our Debezium connector for PostgreSQL with Iceberg sink stopped for some hours.
After restarting, the connector ran successfully, but some changes from the source database were missing in downstream systems.
Environment Details:
Database: PostgreSQL

Sink: Iceberg (via Debezium Server)
Investigation Summary:

Questions for the Community:
Recommended best practices to prevent automatic slot drops due to inactivity?
How to ensure data recovery when a replication slot is dropped?
Recommended retry/restart policies for temporary network disruptions?
Ways to automate slot recreation and resume from the last offset?
How to handle cases where delayed CDC records appear as "u" in __op rather than original operation type?

Chris Cranford

4:35 AM
Hi @DHRUV MEVADA

Recommended best practices to prevent automatic slot drops due to inactivity?

I would recommend real time monitoring of the postgres database with alerts when the slot becomes inactive and the confirmed LSN isn't being flushed. This would be similar monitoring one would do to identify when a slot is lagging behind causing unbounded WAL growth.

But if the connector is stopped for an extended period of time and the environment hosting PG has a job that cleans up idle replication slots, then you'll need to make sure that you address the monitoring concerns above and react within the provider's idle activity window before the slot is dropped.

How to ensure data recovery when a replication slot is dropped?

Initial or adhoc incremental/blocking snapshots are the go-to solution here.

Recommended retry/restart policies for temporary network disruptions?

If you're on Kubernetes, I would recommend the Operator to make sure that if the Debezium Server pod stops that it is restarted. If you're not on Kubernetes, you'll need a cron job that monitors the process and restarts it if the process crashes for any reason.

Ways to automate slot recreation and resume from the last offset?

Unfortunately once the slot is removed, the WAL segments are cleaned up. The only solution in this case is to treat the connector like it's a new deployment and rely on initial/incremental snapshot options to guarantee that your data is synchronized.

How to handle cases where delayed CDC records appear as "u" in __op rather than original operation type?

It most likely was an update in the source and you missed the create due to the slot being dropped. In a situation where you have a data loss gap like you described, this is expected.