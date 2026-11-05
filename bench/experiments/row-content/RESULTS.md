# Row-content policy: results (R11 follow-up)

Reproduce with `make -C bench setup && bench/collectors/cdc/up.sh && bench/experiments/row-content/run.sh`.

- **Cluster:** kind (one node), PostgreSQL 16, Debezium Operator 3.6.3, Lactifluus from this repo.
- **Scenario:** a policy says `payments.status` must never go from `settled` back to `pending`. A Job does exactly that on one row. Every source sees "1 update on payments". The question: which source can tell this particular update broke the rule?
- **Method:** a throwaway receiver logs each event's `before`/`after`, pointed at by the Debezium sink, so we read the raw CDC stream without touching the operator pipeline. Run twice: default `REPLICA IDENTITY`, then `FULL`.
- **Run date:** 2026-11-05, one run.

## What each source could tell

| Source | Can it tell the update was `settled -> pending`? |
|---|---|
| `pg_stat` counters | No. It shows `n_tup_upd` went up; one update looks like any other. |
| Kubernetes API | No. It knows a Job ran; nothing about the value changed. |
| CDC, default REPLICA IDENTITY | **No.** The event carried `after` but `before=null` — only the key is kept for the old row. The policy needs the old value, so it cannot be evaluated. |
| CDC, REPLICA IDENTITY FULL | **Yes.** The event carried `before={... "status":"settled"}` and `after={... "status":"pending"}`. The transition is visible. |

Observed events (verbatim from the receiver):
- Default: `op=u table=payments before=null after={... "status": "pending"}`
- FULL: `op=u table=payments before={... "status": "settled"} after={... "status": "pending"}`

## Verdict

**PASS.** This is the case where CDC is the only source. A policy on the content of a change (a forbidden value transition, a field that must not change, a value out of range) can be evaluated only from the change stream; `pg_stat` and the Kubernetes API never carry row values. The earlier count-based experiments failed precisely because counting does not need CDC; this one succeeds because the value does.

## The cost, measured here, not hand-waved

- The policy needs the **old** value, which means `REPLICA IDENTITY FULL` on the table. With the default identity, `before` is null and the policy is blind. This is exactly the inspector's replica-identity gap, now concrete.
- `FULL` makes PostgreSQL write the entire old row into the WAL on every UPDATE and DELETE of that table, not just the key. More WAL means more to retain, stream and store. Not quantified here.
- `amount` came through base64-encoded (`"AMg="`) because it is a `numeric` decoded by the JSON converter; a real content policy must decode types, not just read strings.

## What this does and does not establish

- Establishes: CDC has a real, unique capability (row content) that the other sources lack, and a content policy is technically evaluable from it.
- Does NOT establish: that anyone needs it (user conversations, R8), nor attribution — this shows *what* changed, still not *which workload* did it. Attribution stays unexplored (session info, Cilium/Hubble, eBPF).
- Not quantified: the WAL cost of `REPLICA IDENTITY FULL`; one run, no variance.

## Next

- Attribution experiment: can the change be tied to the Job that made it, honestly, beyond a time window? Options to try: `application_name`/session info on the connection, Cilium/Hubble flow logs, eBPF.
- Only after R8 (does anyone want a content policy?) decide whether to expose `before`/`after` in the operator pipeline and to have the inspector flag tables that need `REPLICA IDENTITY FULL`.
