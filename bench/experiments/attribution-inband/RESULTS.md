# Attribution, approach 4 (in-band identity through the server log): results

Reproduce with `make -C bench setup && bench/collectors/cdc/up.sh && bench/experiments/attribution-inband/run.sh`.

- **Cluster:** kind, PostgreSQL 16.15, Debezium Operator 3.6.3, Lactifluus from this repo.
- **Question:** if a workload declares who it is (`application_name` and a SQL comment tag) and the server log prints the transaction id next to it, can a CDC event be joined to the workload exactly by `txId`, with no time window?
- **Method:** `log_line_prefix = '%m [%p] app=%a xid=%x '`. Two sessions with different `application_name` write at the same time. Two logging variants, run once each: A) `log_statement = 'mod'`, B) `log_min_duration_statement = 0`. A receiver prints each CDC event's `txId` and the customer value written by each session.
- **Run date:** 2026-11-06, one run per variant.

## What we saw

**Variant A, `log_statement = 'mod'`: FAIL.** The INSERT is logged before it runs, so the transaction id does not exist yet:

```
app=job/inband-b xid=0 LOG:  statement: insert into public.orders (customer, amount) values (812, 1.00) /* workload=job-b */;
app=job/inband-a xid=0 LOG:  statement: insert into public.orders (customer, amount) values (811, 1.00) /* workload=job-a */;
```
CDC events: `txId=795 customer=812`, `txId=796 customer=811`. The log lines carry `xid=0`, so there is nothing to join on.

**Variant B, `log_min_duration_statement = 0`: PASS for the join.** Statements are logged after they run, so the id exists:

```
app=job/inband-b xid=797 LOG:  duration: 0.306 ms  statement: insert ... values (822, 1.00) /* workload=job-b */;
app=job/inband-a xid=798 LOG:  duration: 0.364 ms  statement: insert ... values (821, 1.00) /* workload=job-a */;
```
CDC events: `txId=797 customer=822`, `txId=798 customer=821`. Each session's xid in the log equals the `txId` of its own CDC event, and equals the `txid_current()` the session printed as ground truth (797 and 798). The comment tag appears in the logged statement text.

In both variants the two sessions committed in the same millisecond (the CDC `ts_ms` was identical for both events). That is partly my script (both sessions sleep one second, then commit together), so it is a constructed worst case, not a typical rate. In that case a time window cannot separate the two sessions and the `txId` can.

The `BEGIN` and `COMMIT` lines carry `xid=0`: the id exists only after the first write and is cleared at commit. The join uses the line of the first write statement or any later one before the commit.

## Verdict

**The join mechanism works, conditionally.** An exact `txId` to workload link exists on PostgreSQL if (1) the workload sets `application_name`, (2) the log prefix includes `%a` and `%x`, and (3) statements are logged **after** they run. This changes a statement in ADR-017, which said in-band identity does not apply to PostgreSQL as it does to HTTP: it can be joined, by the server log.

## Cost and what is NOT verified

- **Logging every statement is expensive and sensitive.** The log line contains the statement text with literal values (`values (822, 1.00)`). `log_min_duration_statement = 0` writes every statement, including reads. Volume and overhead were not measured. A threshold above 0 would skip fast statements, so most short transactions would be left without a line; not tested.
- **The workload must cooperate** by setting `application_name`. Here it came from the `PGAPPNAME` environment variable.
- **Poolers.** Whether PgBouncer passes `application_name` through was not tested.
- **The identity is a declared name**, not a pod identity. The sessions ran inside the PostgreSQL pod over the local socket, not from separate client pods.
- **Transaction id width.** `%x` is a 32-bit id and `txid_current()` is 64-bit. They matched at small values; wraparound and epoch were not tested.
- **Managed databases.** Whether a provider lets you set `log_line_prefix` and `log_min_duration_statement` was not checked.
- **Getting the log out** (collecting, parsing, ordering, lag) was done with `kubectl logs`. Not measured as a pipeline.
- One run per variant, PostgreSQL 16 only.

## Cleanup note

The first run's cleanup left the receiver pod, service and configmap in the cluster, because of an invalid `kubectl delete` argument mix. I removed them by hand and corrected the command in `run.sh`; the corrected cleanup was not re-run. Server settings, the sensor's sink URL and the test rows were restored by the first run.
