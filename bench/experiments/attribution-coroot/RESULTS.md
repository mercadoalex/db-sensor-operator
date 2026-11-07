# Attribution, approach 5 (statement-level client-side eBPF with coroot-node-agent): INCONCLUSIVE

- **Cluster:** kind on Docker Desktop for macOS (kernel `6.10.14-linuxkit`), PostgreSQL 16.15, coroot-node-agent 1.38.1, an OpenTelemetry Collector 0.116.1 with only a `debug` exporter. No Coroot server.
- **Question:** does the agent export a span per PostgreSQL statement with the client pod, and does the end of the COMMIT span line up with the CDC commit time?
- **Run date:** 2026-11-06.

## What we know from the code (read, not run)

From `coroot/coroot-node-agent` at the time of reading:
- `tracing/tracing.go`, `PostgresQuery`: a Postgres span carries `db.system=postgresql` and `db.statement` (the statement text), plus the peer host and port. Its resource has `container.id` and a `service.name` derived from the Kubernetes owner (Deployment, DaemonSet, StatefulSet or CronJob).
- `ebpftracer/l7/postgres.go`: it parses simple queries (`Q`), and for prepared statements it keeps the text from `Parse` and reuses it on `Bind`. A statement it did not see prepared is reported as `EXECUTE <name> /* unknown */`.
- So, on paper, the span answers "this container sent this statement". Whether the literal values appear depends on the client (a simple query carries them in the text; a prepared statement does not).

## What happened

Zero spans reached the collector, in four attempts:
1. Two short `psql` clients at the same second and one alone: no spans. The client pods started before the agent's minimum container age; fixed with `--min-container-age 0` for the next run.
2. Same, with the age fixed and a 60 s lead time: no spans.
3. A client that opens a new `psql` every 2 s: the agent listed the container (`container_info`, CPU and memory) but no TCP connection and no Postgres metric for it.
4. A single long-lived `psql` session (checked in `pg_stat_activity`: connected, idle, last query the probe), and an agent restart while it was open: nothing for that container, and after the restart no connection to port 5432 from any container.

## Probable cause (evidence, not proof)

The kind node is itself a container inside the Docker Desktop VM. In the first run the agent logged `TCP listen open from unknown container {... pid 3269772 ... [::]:4318 ...}`: port 4318 is our collector, so eBPF events do arrive, but with a PID the agent could not map. The highest PID inside the kind node was 515605. The event PIDs come from the VM's root PID namespace, while the agent (`hostPID: true`) sees the kind node's. If that is right, the agent cannot tie kernel events to containers in kind on Docker Desktop, which would explain all four runs. Not confirmed: it would need the agent on a node that is not nested.

## Verdict

**Inconclusive.** Nothing was learned about Coroot's statement spans by measurement. The test bench is the likely blocker, not necessarily the agent.

## To make it conclusive

Run `run.sh` on a node that is not a container inside a VM: for example a single-node k3s or kubeadm on a Linux VM, or a managed cluster node. `parse.py` was written for the collector's `debug` output and was not exercised on real spans.

## Leftovers

The scripts restore the bench on exit. Checked after the last run: namespace `coroot` deleted, the sensor's sink URL restored, no test rows.
