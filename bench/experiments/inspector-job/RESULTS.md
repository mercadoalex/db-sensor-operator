# Inspector Job experiment: results

Verifies the mechanism that carries an inspection result from a Job to the operator: the
container's termination message. Reproduce with
`make -C bench setup && bench/experiments/inspector-job/run.sh`.

- **Cluster:** kind (Kubernetes v1.32.2), PostgreSQL 16.15, one run, 2026-10-31
- **Image:** the operator image, which now also carries `/inspector` (13 MB, against 79 MB for the manager)
- **Pod hardening used:** non-root (65532), read-only root filesystem, all capabilities dropped,
  no privilege escalation, seccomp RuntimeDefault, no service account token, `backoffLimit: 0`,
  `activeDeadlineSeconds: 60`

## Results

| Scenario | Exit code | Message | Checks |
|---|---|---|---|
| Success, with a replication slot and one missing table | 0 | JSON with `factsVersion: 1`, tables, server and slot facts (527 bytes) | passed |
| Wrong password | 4 | `AuthenticationFailed`, no password in it | passed |
| Unreachable host | 3 | `ConnectionFailed` (DNS failure) | passed |
| 150 tables | 0 | 3918 bytes, valid JSON, `truncated: true`, fewer than 150 tables | passed |
| Unsupported engine | 2 | `InvalidConfiguration` | passed |

All 15 checks passed.

## What this settles

- The termination message works as the result channel. The container is non-root on a read-only
  root filesystem and could still write `/dev/termination-log`, and a **failed** Job also leaves its
  message readable, so a failure carries its reason.
- The Job needs no Kubernetes API access and no write permission of any kind, which is the point of
  choosing this over a ConfigMap written by the Job.
- The size limit is real and handled: the inspector cuts below 4000 bytes, so the kubelet's
  4096-byte truncation never produces a half-written JSON document. Measured with 24-character
  table names: 33 tables with data fit (40 when they are missing). A sensor monitoring more tables than
  that gets a truncated list and `truncated: true`; the limit would be hit sooner with longer names.

## Not covered

- The operator side: nothing yet creates the Job, reads the pod or writes the status. This only
  proves the Job half.
- The reported message for an authentication failure includes the host and database name, which
  are not secret but are not nothing; they are visible in the Pod's status to anyone who can read it.
- A NetworkPolicy that blocks the Job, a namespace enforcing the `restricted` Pod Security
  Standard (the pod above is written to satisfy it, but that was not enforced in this cluster),
  and an image that cannot be pulled.
- MySQL: the inspector rejects it with `InvalidConfiguration`.
- Timing: the Job's start-up latency was not measured.
