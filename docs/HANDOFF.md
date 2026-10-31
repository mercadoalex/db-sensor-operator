# Handoff: snapshot policy, phase B3 (inspection)

Branch: `feat/snapshot-policy-phase-b` (pushed, NOT merged). Do not merge until item 1 below is done.

## State

Done and verified (unit, envtest, lint, helm lint/template):
- Inspector binary, wire contract, `DiagnoseSlot`, partitioned-table fix.
- Inspection schema and CEL rules (needs `type: postgresql` and `database.name`).
- Controller flow: Job creation, result read from the Pod termination message, status write
  (operator is the only writer), Job deleted after the write. 8 envtest specs with a fabricated Pod.
- Chart: CRD resynced, `jobs` and `pods` RBAC added, `OPERATOR_IMAGE` env rendered.

Not verified:
- A real operator creating the Job in kind (image pull, Pod Security `restricted`, NetworkPolicy).
- PostgreSQL 12 (no `wal_status` / `safe_wal_size`).
- MySQL (mapped in the deployer, never run).

## To do, in order

1. **Kind e2e.** `make -C bench setup`, helm install the operator, apply a sensor with
   `snapshot.inspection.enabled: true` and `database.name: bench`. Check: Job runs, facts reach
   status, Job is deleted, no new Job with `intervalSeconds: 0`. Add the result to `bench/` RESULTS.
   Note: a kustomize deploy has no `OPERATOR_IMAGE`, so it shows `InspectorImageNotConfigured`.
2. **Secret cache.** The cached client reads every Secret cluster-wide (`EnsureDebeziumServer`
   does a `Get`; Datadog sink reads too). Make the existence check metadata-only, or the Job's
   password isolation is weaker than documented.
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
- Open the PR after item 1.

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
