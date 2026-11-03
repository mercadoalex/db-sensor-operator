# Handoff: snapshot policy, phase B3 (inspection)

Merged to `main`. The kind end-to-end passed on 2026-10-31 (15 checks, `bench/experiments/inspection-e2e`). The inspection is off by default, so nothing changes for anyone who does not enable it.

## Pending when the session ended (2026-10-31)

Next experiment (told to Mario Fiore Vitale on 2026-10-31): simulate a scheduler catching up after downtime, and compare what the database's own counters, Kubernetes, and CDC each see, to find where CDC adds something the others don't. Full definition, hypothesis and pass/fail criterion: R11 below.

Do first:
- **PR #5** (Secrets fix + READMEs): wait for CI, merge, then delete the branch (local and remote).
- **kind cluster:** gone (no cluster, container or kubectl context). To run the e2e again: `make -C bench setup && bench/collectors/cdc/up.sh && bench/experiments/inspection-e2e/run.sh`.
- **Two untracked PNGs in `assets/`** (`horizontal_fondo_blanco.png` 3 MB, `logo_navy.png` 2.5 MB): decide to optimize and version them, or leave them out. The diagrams embed their own small copies.

Product and messaging:
- The logo's own tagline says "DDL detection", which the brief contradicts (PostgreSQL does not show DDL). Regenerate the logo without that line, or with a true one.
- Decide the "brake": should the inspection block start-up, or only report? Ask in the first user conversations (brief, section 7).
- Run the first 5 to 10 user conversations before any code for ADR-015 (adoption ladder, draft).
- Idle table reads `Degraded`; decide the behaviour. Rewrite thesis section 13.4 (DDL no longer differentiates CDC).
- AI features are "under constant evaluation" (ROADMAP): no external LLM; rules decide, AI at most suggests.
- OpenTelemetry export is roadmap only.

Inspector gaps (asked of Mario Fiore Vitale, Debezium maintainer, on 2026-10-31: which of these do people trip on most? Wait for his answer to order them):
- Today the inspector reads: `wal_level`, server version, recovery state, `max_slot_wal_keep_size`; per table: exists, estimated rows (and whether reliable), can create in the schema; the replication slot: active, `wal_status`, retained WAL, lag, safe margin.
- It does NOT check: the `REPLICATION` privilege of the database user; the publication (exists, includes the listed tables); each table's replica identity (a table without a primary key can reject updates and deletes once logical replication publishes them); `max_replication_slots` and `max_wal_senders`; `SELECT` privilege on the tables.
- Each new check needs: a query in `internal/snapshot/inspector.go`, a field in the wire contract (`factsVersion` stays 1 only if the field is optional), a condition in `internal/inspection/evaluate.go`, and a test against a real PostgreSQL.

Strategic open question (Mario Fiore Vitale, 2026-10-31):
- His objection, still unanswered: "why start a full CDC just to get metrics?" It was the reason he did not pursue the idea. His reply to the pivot was polite ("I think it is ok") but did not say which pre-flight checks matter most.
- The pivot assumes the user wants CDC; it does not answer why someone who only wants metrics should run it.
- Two audiences to separate: Kubernetes teams that ALREADY run Debezium, and teams that do NOT. The product needs a reason for each.
- Product gap to check: today the operator creates its own `DebeziumServer`. It is NOT verified whether it can attach to a Debezium that already exists (same for pointing the inspector at an existing slot). Check this before pitching to teams that already run Debezium.
- Do not word it as "Lactifluus makes Debezium safe": that implies Debezium is unsafe. Find a framing about what the operator adds, not what Debezium lacks.
- To do: write down what Debezium already gives for operations and monitoring (JMX metrics, heartbeat, signals, the official operator), so the claims about our added value are checked against it.

## Research plan: Kubernetes reacting to data behavior (started 2026-10-31)

Direction (an idea, not a product yet): metrics are not the product; the product is that Kubernetes reacts to the behavior of the data. Workflow:
1. **Sense:** CDC reports what changed in the data (row level, ordered, with commit time).
2. **Decide:** a policy evaluates those signals.
3. **Act:** Kubernetes acts (existing controllers or Events and conditions the operator publishes). The operator does not have to act by itself.

Why CDC and not the database itself (claims to verify in R1 and by the benchmark, not facts yet):
- Database counters (`pg_stat_*`, polled): how many rows changed per table, in aggregate. No exact time, no transaction, no row content.
- CDC: which rows, before and after values, in order, with commit time, pushed in near real time. It does not say who did it, and on PostgreSQL it does not show DDL (measured).
- Kubernetes: who (pod, Job, Deployment) and when it was deployed.

Example used with Mario Fiore Vitale on 2026-10-31 (illustrative, not built): a Job deletes rows it should never touch, say settled payments. CDC sees what was deleted as it commits, a policy flags it, and the cluster stops the Job and alerts. Purpose: ask where it would break in practice. Planned reply to Mario (not yet sent when written): uses the scheduler catch-up example and says a small PoC (R11) will compare what database counters, Kubernetes and CDC each see, and that the result will be shared even if CDC turns out not to be needed.

Viability condition (owner's conclusion, 2026-10-31): the last example sounds realistic to the owner: a Job starts rewriting far more rows than it normally does; the database cannot tell that this is unusual and cannot stop a Kubernetes Job; CDC sees the change stream as it commits, a policy compares it with the Job's usual behavior, and the cluster pauses the Job and alerts. But one example is not enough. The direction only holds if more REAL, recurring use cases exist, and finding them is the main cost and the main risk of this research. Notes on the examples so far: a plain mass delete is detectable with the database's own counters; a rule such as "never delete settled payments" can be prevented inside the database (permissions or a trigger), which beats detecting it afterwards; the pattern example needs a baseline of normal behavior that does not exist today.

Rule: any experiment, demo or PoC must state its hypothesis and its pass/fail criterion before it is built, and is then measured against them. No product code for this direction (policy engine, actions) until R1 to R4 are written and some conversations (R8) have happened.

Research tasks, in order:
- **R1. What each source knows.** For `pg_stat_*` counters, `pgaudit`, logical decoding and Kubernetes, write what each can and cannot tell, and verify every claim in the table above against the official docs (for example, are rolled-back rows counted in `pg_stat`?).
- **R2. Who already reacts to workloads.** Argo Rollouts and Flagger (analysis), KEDA, Kyverno and OPA/Gatekeeper (policy), change-tracking in observability tools. Does anything key on data-mutation behavior? What would be new? Counter-argument to check: exporters that publish PostgreSQL's own `pg_stat` counters to Prometheus can alert on a delete-rate spike without CDC, so detecting volume alone does not justify CDC. What only CDC can give is row content. Check in the Debezium docs: with the default replica identity an UPDATE or DELETE carries only the key, so row-content policies need `REPLICA IDENTITY FULL` (more WAL). That makes the inspector's replica-identity check important.
- **R3. How a policy is expressed.** Own CRD, CEL, or an existing policy engine (Kyverno was suggested). Can a policy engine consume an external signal? What would we gain from reusing it?
- **R4. Attribution (who changed it).** Postgres logical decoding does not say which pod wrote. List the options and what each really gives: time window against Job runs, `application_name` and session information, Debezium transaction metadata, Cilium/Hubble flow logs, eBPF. Note which need the Job to be the only writer. Also: a retention Job legitimately deletes millions of rows in batches, so a fixed threshold gives false positives; a policy needs a baseline per job or table, which does not exist today.
- **R5. Acting after the commit.** CDC sees changes already committed, so it limits damage and does not prevent the first write. List the actions Kubernetes offers (Events, conditions, failing a Job, suspending a CronJob, aborting a rollout), their risk, and an approval model (report, recommend, act with approval, act). Two limits: stopping a migration midway can leave data half-migrated and be worse than letting it finish (prefer alert or approval); and one large DELETE commits before CDC can see it, so the value is limiting damage in batched work.
- **R6. Existing Debezium.** Check in the code whether the operator can attach to a Debezium that already exists, and whether the inspector can read an existing slot (also asked in the strategic section).
- **R7. Competitors.** Read what "alerting" in Debezium Platform 3.7 is (blog: 3.7.0.Beta2), and compare Debezium's own WAL guidance (docs and Gunnar Morling's post on replication slots) with what Lactifluus does and does not do (we set no heartbeat today).
- **R8. Conversations (5 to 10).** Questions that do not name CDC: Has a job, migration or deploy ever mass-deleted or corrupted data? How did you find out and how long did it take? What did you do, and what would you have wanted to happen automatically? Would you accept the cluster acting on its own, or only being told?
- **R9. Kill criteria.** Before investing, write down what findings would make us drop this direction (for example: nobody has had the problem, or attribution cannot be done honestly). Explicit criterion: if after the conversations (R8) we cannot name more than this one real case where CDC plus a Kubernetes reaction beats database-side prevention and counter-based alerts, the product does not hold and the direction must be reconsidered.
- **R10. ADR-016 draft.** Only after R1 to R4: sense/decide/act, the action ladder, the first narrow case, what is verified and what is not.
- **R11. PoC: scheduler catch-up burst (experiment with a hypothesis).** Reuse the `bench/` kind cluster and PostgreSQL. Hypothesis: when a time-based scheduler (CronJob and similar) comes back after downtime and fires every missed run at once, CDC shows something that the database's own counters, the database load metrics and the Kubernetes API do not (row content, exact commit time, attribution to a Job). Setup: simulate the burst by creating N Jobs at once that write to the same table; compare what each source shows and how soon, and record it in `bench/experiments/<name>/RESULTS.md`. Success: we find at least one thing CDC shows earlier or in more detail that a Kubernetes reaction could use. Failure: the other sources show the same, as fast, so CDC adds nothing in this case (a valid result). The goal is to find where CDC does add value for Kubernetes. It cannot prove that real teams hit this problem; only R8 can. Also try the data variant: the same Job run several times writing duplicate rows. Note: the native CronJob probably runs only the latest missed schedule, while some other schedulers have a catch-up mode (to verify in each one's docs).
- **R12. Kyverno integration (explore).** Kyverno is a policy engine for Kubernetes (policies as resources). Idea to test: Lactifluus already publishes conditions in `status` (`SlotRetentionHealthy`, `LogicalDecodingReady`, phase). Could a Kyverno policy read the `DatabaseSensor` status and decide something, for example refuse to admit a new Job that targets a database whose slot is at risk? To verify in the Kyverno docs: how a policy reads other resources (context and API calls, CEL), whether it can act on an already running Job or only at admission and on background scans (reports), and what it can consume as an external signal. Compare with OPA/Gatekeeper, Argo Rollouts and a custom controller, and decide what stays in Lactifluus and what is delegated. This would make "policy-gated" literal using an existing ecosystem component.

First narrow case to test with R8 (hypothesis): Jobs, CronJobs and migrations, because they run alone and have a clear start and end, so the changes in that window can be attributed to them, if they are the only writer. Example: "this migration deleted 2 million rows and the policy allows 100 thousand: fail the Job and alert".

Engineering follow-ups:
- Secrets: the operator can still `get` a Secret by name in any namespace. Try the chart's `rbac.namespaced` and check what breaks. The Datadog sink's Secret read is unit-tested only.
- e2e not covered: NetworkPolicy blocking the Job, operator restart or Job kill between Job end and status write.
- Debezium Server pods (from the Debezium Operator) are rejected by Pod Security `restricted`; the inspector Job is not. Investigate if `restricted` namespaces matter.
- Verify PostgreSQL 12 and 17, and MySQL, before promising them.
- Design-token colour pairs have no measured contrast.

## State

Done and verified (unit, envtest, lint, helm lint/template):
- Inspector binary, wire contract, `DiagnoseSlot`, partitioned-table fix.
- Inspection schema and CEL rules (needs `type: postgresql` and `database.name`).
- Controller flow: Job creation, result read from the Pod termination message, status write
  (operator is the only writer), Job deleted after the write. 8 envtest specs with a fabricated Pod.
- Chart: CRD resynced, `jobs` and `pods` RBAC added, `OPERATOR_IMAGE` env rendered.

Verified in kind with the real operator (PostgreSQL 16, one local cluster): Job creation, result via termination message, status write, Job deletion, no second Job, re-inspection on a spec change, wrong password, Pod Security `restricted` for the inspector Job.

Not verified:
- NetworkPolicy blocking the Job (kind's default CNI, no policy applied).
- Operator restart or Job kill between Job end and status write.
- PostgreSQL 12 (no `wal_status` / `safe_wal_size`).
- MySQL (mapped in the deployer, never run).

## To do, in order

1. ~~Kind e2e~~ Done. Note: a kustomize deploy has no `OPERATOR_IMAGE`, so it shows
   `InspectorImageNotConfigured`. Follow-ups from its findings: document that slot facts want a
   periodic `intervalSeconds` (with `0` the first run can freeze `SlotMissing` before Debezium
   creates its slot); the Debezium Server pods are not Pod Security `restricted`-compatible
   (Debezium Operator, not ours); test a NetworkPolicy and an operator restart mid-run.
2. ~~Secret cache~~ Done. The manager no longer caches Secrets (`cmd/main.go`, `DisableFor`), the
   existence check asks for metadata only, and the ClusterRole has `secrets: get` (no list or watch).
   Reproduced first: with only `get` the old build failed with `failed to list *v1.Secret`.
   Verified in kind (e2e check 8) and by a unit test that fails on the old code. Remaining: the
   operator can still `get` a Secret by name in any namespace; limiting it needs a namespaced Role
   (`rbac.namespaced` exists in the chart, untested). The Datadog sink also reads Secrets (by name,
   with `get`); that path is covered by unit tests only, not by the kind run.
3. **Server capability.** Encode PG version/capability so "absent" and "not measured" differ on
   PG<13. Test on a PG12 container.
4. **Heartbeat.** Deployer sets none. Stall detection (confirmedLagBytes growing across probes)
   needs it. Heartbeat events would reach `LagTracker` via `Pipeline.Handle` and mask the
   no-events check. Decide the order before adding it.
5. **PG17 / standby.** `invalidation_reason`, `inRecovery` behaviour: recorded in ADR-014 Pending, not built.
6. **Staleness thresholds.** `estimate()` assumes autovacuum defaults and does not read server
   settings. Documented limitation.

## Open decisions (need the owner)

- `captureStrategy: auto` in the spec: recommendation is not to add it until something acts on it
  (alternative: a `CaptureStrategyHonored=False` condition).
- Quiet table (no events for the configured timeout): stays `Degraded`. Decided: conservative is correct; without a heartbeat there is no honest way to distinguish a quiet table from a stalled Debezium. Revisit when heartbeat is implemented.
- Rewrite thesis section 13.4 with the verified facts (`no_data` works, `never` is rejected).
- PRs for the inspection are merged (#2, #3).

## Review checklist before the PR

- `git log --oneline origin/main..HEAD` reads as logical commits.
- CRD in `config/crd/bases` and the chart copy are identical apart from the helm annotations.
- `make lint`, `go test $(go list ./... | grep -v /test/e2e)`, `helm lint charts/db-sensor-operator`.
- CI: Lint, Tests, E2E green.
- Schema must not outrun its producer: `plan` is written but never applied (`applied: false`).

## Deferred on purpose (ADR-014)

Backfill (no consumer yet), time-based budget and calibration, WAL-headroom gate.

## Gotchas

- zsh does not expand `$K` strings; use bash scripts or direct kubectl.
- macOS sed lacks `\b`; use perl.
- The port-forward for integration tests often dies on first start; restart and wait for `nc -z 127.0.0.1 15432`.
- Lint rules: goconst, lll(120), modernize (`new(x)`, `max`, `slices`), depguard forbids `sort`.
