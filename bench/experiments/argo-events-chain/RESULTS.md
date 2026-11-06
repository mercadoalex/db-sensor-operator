# Debezium to Kafka to Argo Events to a Job: can existing pieces do what we propose?

Reproduce with `make -C bench setup && bench/collectors/cdc/up.sh && bench/experiments/argo-events-chain/up.sh`, then `python3 bench/experiments/argo-events-chain/test.py shape legal forbidden restart-sensor restart-eventsource`, then `down.sh`.

- **Cluster:** kind, PostgreSQL 16.15, Debezium Operator 3.6.3 (a second `DebeziumServer` with a Kafka sink), Apache Kafka 3.9.0 single node (KRaft), Argo Events v1.9.11 with a 3-replica NATS EventBus.
- **Question:** can Debezium to Kafka to Argo Events detect a forbidden value transition (`payments.status` going from `settled` to `pending`) and act on a Kubernetes Job, with no code of ours?
- **Method:** a Sensor with two data filters (`body.before.status = settled`, `body.after.status = pending`) and a Kubernetes trigger that patches the Job `payments-writer` to `suspend: true`. `public.payments` has `REPLICA IDENTITY FULL`, as in `row-content`. The Debezium message value is JSON without schemas.
- **Run date:** 2026-11-06, one run of each phase unless stated.

## What we saw

**Detection: works.** Three legal updates (`pending` to `settled`, and a no-op touch) did not trigger. The forbidden update (`settled` to `pending`) did. The Kafka message is the plain envelope (`before`, `after`, `source`, `op`, `ts_ms`), so a data filter addresses `body.before.status` directly.

**Action and latency: works.** The Job was `suspend: true` and its pod `Terminating` within 0.72 s of the start of the `kubectl exec psql` call that made the update. That figure includes the call itself (0.10 s) and a polling interval of about 0.3 s, so it is an upper bound. One run.

**Restart behaviour: mixed.**

| Case | Result |
|---|---|
| EventSource pod deleted, forbidden update made while it was down | Job suspended after 9.4 s (1 run) |
| Sensor pod deleted while the stream was flowing, forbidden update made right after | **Not suspended within 120 s, in 2 of 2 runs.** The EventSource logged publishing the event 0.8 s before the new Sensor pod subscribed. |
| Sensor scaled to 0 and fully gone, forbidden update made, 15 s later scaled back to 1 | Job suspended 1.8 s after scale-up (1 run). The event was kept and delivered. |

So the loss happens when the Sensor pod is being replaced at the moment the event is published, not when it is simply down. **The mechanism was not diagnosed.** My guess is that the terminating pod took the event and the trigger never ran; that is a guess.

**Kafka outage: the slot stalls and nothing in the chain reports it.** With Kafka scaled to 0 and about 3000 non-forbidden updates, `argo_test_slot` stayed at a confirmed lag of about 530 kB while the sensor's own slot stayed near 20 kB. The `argo-cdc` Debezium pod restarted twice. After Kafka came back the slot caught up within 60 s. The volume was small (about 0.5 MB of WAL), so this shows the mechanism, not a danger. No Argo Events component exposes slot state, and no `DatabaseSensor` was watching that slot.

An earlier attempt measured `restart_lsn` retention instead and could not tell the stalled slot from the healthy one (both showed about 512 kB); `confirmed_flush_lsn` was the signal that separated them.

## What it cannot do (by construction, observed or read)

- **It does not know which Job.** The Sensor's target is a fixed Job name. The event has no workload identity (see `attribution/RESULTS.md`), so the chain solves the "what" and not the "who", as in our own findings. With one writer per table it is enough; with several it is not.
- **Sequence checks: not tested.** Per the Argo Events docs, data filters evaluate one event, and a multi-dependency sensor keeps the latest event per dependency; neither is a comparison with the previous value of a sequence. This is a reading of the docs, not a test. It is the case ADR-016 calls a stateful consumer.

## Cost

Eight pods on top of PostgreSQL (Kafka 1, Debezium 1, Argo Events controller 1, NATS 3, EventSource 1, Sensor 1) and three manifests of 136 lines in total, plus a ServiceAccount, Role and RoleBinding. The Lactifluus path in the same bench is the operator pod and one Debezium pod. Memory and CPU were not measured.

## Verdict

**Yes, existing pieces detect a content rule and act on a Job, quickly, in this setup.** The proposal cannot claim that nobody can do a content-triggered reaction. What the chain did not give us, in this run:
- a way to know which workload did it,
- any view of the slot or of the stalled path,
- sequence and order checks (read, not tested),
- the `REPLICA IDENTITY FULL` check (done by hand here),
- reliable delivery while the Sensor pod is replaced (2 of 2 events lost; cause unknown).

Whether anyone prefers to assemble this from five components instead of using one is a question for the user conversations (R8).

## Not verified

- One run per phase, one local cluster, PostgreSQL 16, tiny data volumes.
- The loss mechanism when the Sensor is replaced.
- Other Argo Events versions, a Kafka cluster with replication, authentication, TLS.
- Whether Argo Rollouts or an HTTP trigger behave the same (the docs say any Kubernetes object, including custom resources, can be triggered; only a Job patch was run).
- Resource use.
- Cleanup: `down.sh` was run once and checked afterwards (no Argo CRDs, no leftover slot, the added column dropped, `payments` replica identity back to default).
