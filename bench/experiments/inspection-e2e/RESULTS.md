# Inspection end to end in kind: results

The real operator creates the inspector Job, the kubelet keeps the termination message, the
operator copies it into `status.snapshot` and only then deletes the Job. Reproduce with
`make -C bench setup && bench/collectors/cdc/up.sh && bench/experiments/inspection-e2e/run.sh`.

- **Cluster:** kind (one node, default CNI), PostgreSQL 16.15 (`wal_level=logical`), Debezium Operator 3.6.3
- **Operator:** built from this repository, installed with the Helm chart; `OPERATOR_IMAGE` and
  `OPERATOR_IMAGE_PULL_POLICY=Never` set by the chart, so the Job uses the same image
- **Run date:** 2026-10-31, one run of the script (12 checks) after one manual pass

## Result

All 12 checks passed.

| # | Check | Result |
|---|---|---|
| 1 | First inspection reaches `InspectionAvailable=True` with real facts (`wal_level=logical`, 3 tables found) | pass |
| 2 | Plan reported with `applied: false` | pass |
| 3 | Job deleted after its result was stored | pass |
| 4 | Exactly one Job for the run; none during the next 70 s (`intervalSeconds: 0`) | pass |
| 5 | A spec change (new generation) triggers a new inspection; slot reported `SlotHealthy` (`reserved`) | pass |
| 6 | Wrong password: `AuthenticationFailed`, no facts, no plan, no derived conditions, password not in status | pass |
| 7 | The Job is admitted and finishes in a namespace enforcing Pod Security `restricted` | pass |

The inspector finished in under 5 seconds. Jobs are gone by the time a 4-second poll looks, so
the checks read the Job events and the stored status, not the live Job.

## Findings

- **A once-per-generation inspection can freeze a transient answer.** With `intervalSeconds: 0`
  the first run happened before Debezium created its slot (Debezium started about 45 s later),
  so `status` kept `SlotRetentionHealthy=Unknown / SlotMissing` while the slot was in fact
  healthy. A spec change or the default interval (600 s) corrects it. Slot facts are the kind
  of fact that wants a periodic interval; `0` suits table facts. Worth saying in the docs.
- **The Debezium Server pods are not `restricted`-compatible.** In the Pod Security namespace
  the inspector Job ran, but the ReplicaSet of the DebeziumServer was rejected
  (`violates PodSecurity "restricted:latest"`). Those pods come from the Debezium Operator, not
  from Lactifluus. Adopting the product in a `restricted` namespace needs that solved
  separately. Not investigated how.
- **The failure message carries the server address and the user name.** It never held the
  password, but it does include the pod IP and the database user. It is truncated by the
  inspector. Acceptable for a status field; worth knowing before publishing statuses.

## Not covered

- **NetworkPolicy.** kind's default CNI was used and no policy was applied, so a policy
  blocking the Job's egress was not tested.
- **A real kubelet under load, other CNIs, other clouds.** One local node only.
- **PostgreSQL 12 and 17, replicas, MySQL.** Still untested.
- **A Job killed mid-run, an operator restart between Job end and status write.** The
  design (Job is deleted only after the write; TTL 600 s) predicts recovery, but it was not
  exercised here.
- **Timing.** One run of the script. No claim about variance.
