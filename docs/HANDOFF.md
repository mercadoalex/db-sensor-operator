# Handoff: snapshot policy, phase B3 (inspection)

Merged to `main`. The kind end-to-end passed on 2026-10-31 (15 checks, `bench/experiments/inspection-e2e`). The inspection is off by default, so nothing changes for anyone who does not enable it.

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
- Quiet table reads Degraded (no events is not always a fault).
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
