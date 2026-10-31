# ADR-011 — Hybrid status design: conditions + measurement fields + derived phase

- **Status:** Accepted
- **Date:** 2026-10-16
- **Context:** Task 2 (Define Spec/Status structs) of the db-sensor implementation plan
- **Supersedes:** the plain-string `DatabaseSensorStatus` sketched in `DESIGN.MD` §6 and the status field list in US-5 (`requirements.md`). The user-facing data from US-5 is still fully delivered; only its internal representation changes.

## Context

The original design (`DESIGN.MD`) modeled `DatabaseSensorStatus` as a flat set of
string fields:

```go
type DatabaseSensorStatus struct {
    Phase        string  `json:"phase,omitempty"`     // Pending | Running | Degraded | Failed
    Lag          string  `json:"lag,omitempty"`
    EventsPerMin float64 `json:"eventsPerMin,omitempty"`
    LastEvent    string  `json:"lastEvent,omitempty"`
    LastError    string  `json:"lastError,omitempty"`
}
```

`AGENTS.md` recommends using `metav1.Condition` for status instead of custom string
fields, in line with Kubernetes API conventions and the Kubebuilder book. This ADR
records the decision to adopt a **hybrid** model rather than choosing one approach
wholesale.

## Decision

Use a hybrid `DatabaseSensorStatus`:

1. **`Conditions []metav1.Condition`** is the source of truth for health and
   readiness state. It carries `observedGeneration`, `lastTransitionTime`,
   `reason`, and `message` per condition. Error state (formerly `LastError`) is
   expressed as the `reason`/`message` of a `False` condition.
2. **Measurement fields stay plain**: `Lag`, `EventsPerMin`, `LastEvent`,
   `DebeziumServer`. These are point-in-time values, not boolean states, so they do
   not belong in conditions.
3. **`Phase`** remains a plain string but is **derived** from the conditions on each
   reconcile (not set independently). It exists purely as a friendly `kubectl get`
   printcolumn.
4. **`LastEvent`** becomes `*metav1.Time` (a real timestamp) instead of a
   pre-formatted relative string. Display formatting happens in a printcolumn, not
   in stored state.

Resulting shape:

```go
type DatabaseSensorStatus struct {
    // Source of truth for health/readiness.
    // +optional
    // +listType=map
    // +listMapKey=type
    Conditions []metav1.Condition `json:"conditions,omitempty"`

    // Derived from Conditions; convenience column for `kubectl get`.
    // +optional
    Phase string `json:"phase,omitempty"` // Pending | Running | Degraded | Failed

    // Measurements (not states).
    // +optional
    Lag string `json:"lag,omitempty"`
    // +optional
    EventsPerMin float64 `json:"eventsPerMin,omitempty"`
    // +optional
    LastEvent *metav1.Time `json:"lastEvent,omitempty"`
    // +optional
    DebeziumServer string `json:"debeziumServer,omitempty"`
}
```

### Condition types (initial set)

| Type                | Meaning when `True`                                   |
|---------------------|-------------------------------------------------------|
| `Ready`             | Sensor is healthy end-to-end (aggregate)              |
| `DebeziumAvailable` | The child `DebeziumServer` pod is running             |
| `ReceivingEvents`   | CDC events have arrived within the no-event timeout   |

`reason` values are machine-readable CamelCase (e.g. `SecretNotFound`,
`PodCrashLoopBackOff`, `NoEventsTimeout`); `message` is the human sentence.

### Phase derivation

`Phase` is computed from conditions each reconcile:

- `DebeziumAvailable=False` and reconcile cannot proceed → `Failed`
- `DebeziumServer` pod not yet running → `Pending`
- `Ready=True` → `Running`
- `Ready=False` due to lag over the warning threshold → `Degraded`
- lag over the critical threshold, or no events for 30m → `Failed`

## Rationale

- **Multi-dimensional truth.** A sensor can be `DebeziumAvailable=True` while
  `ReceivingEvents=False`. A single `phase` string collapses independent facts into
  one lossy label; conditions keep them separate.
- **`observedGeneration`.** Tells clients whether the status reflects the current
  spec or a stale one — `kubectl get` won't falsely show "Running" against an
  unreconciled edit. A raw string status cannot express this.
- **`lastTransitionTime` for free.** Enables "state X for N minutes" alerting
  without a hand-rolled timestamp field.
- **`reason` vs `message` separation.** `reason` is stable and machine-branchable;
  `message` is human detail. A single `lastError` string conflates the two.
- **Tooling and convention.** `meta.SetStatusCondition` / `meta.IsStatusConditionTrue`
  manage the slice (dedup, transition times); `kubectl wait --for=condition=Ready`
  and other controllers understand conditions out of the box.
- **Measurements stay queryable.** `Lag`/`EventsPerMin`/`LastEvent` remain plain
  fields so they render cleanly in printcolumns and don't churn
  `lastTransitionTime` on every reconcile.

## Consequences

- Slightly more machinery than raw strings (condition helpers, phase-derivation
  logic). Accepted for the correctness and interoperability it buys.
- The persisted status shape differs from `DESIGN.MD` §6. **US-5 is still satisfied**:
  `phase`, `lag`, `eventsPerMin`, and `lastEvent` are plain fields/printcolumns;
  `lastError` is surfaced via a condition `message` (and, if the exact spec shape is
  desired, can be mirrored to a `LASTERROR` printcolumn via JSONPath into the
  conditions).
- `kubectl get databasesensor` keeps a human-friendly `PHASE` column plus `LAG`,
  `EVENTS/MIN`, and `AGE`-style columns.

## Alternatives considered

- **Plain strings only (original DESIGN.MD):** simplest, but loses
  `observedGeneration`, transition times, and multi-dimensional health; diverges
  from K8s convention and AGENTS.md guidance.
- **Conditions only:** correct for states but forces measurements (`lag`,
  `eventsPerMin`) into condition messages, which can't be put in printcolumns or
  queried and churn transition times every reconcile.
- **Hybrid (chosen):** conditions for states, plain fields for measurements, derived
  phase for the `kubectl` summary.
