# Scheduler catch-up burst: results (R11)

Reproduce with `make -C bench setup && bench/collectors/cdc/up.sh && bench/experiments/scheduler-catchup/run.sh`.

- **Cluster:** kind (one node), PostgreSQL 16 (`wal_level=logical`), Debezium Operator 3.6.3, Lactifluus from this repo.
- **Scenario modelled:** a time-based scheduler comes back after downtime and fires every missed run at once. Modelled as 6 Kubernetes Jobs created at the same timestamp, all writing to `public.orders`; one of them deletes 300 rows (the "should not" run), five insert 50 rows each.
- **Run date:** 2026-11-05, one run.

## Hypothesis

For a Kubernetes reaction to the data, CDC shows something the database's own counters and the Kubernetes API do not: the content of each change (which rows), the exact commit order, and a link to the Job that caused it.

## What each source showed

| Source | Saw the burst | Detail it gave | What it could NOT give |
|---|---|---|---|
| `pg_stat_user_tables` counters | yes | aggregate totals: `ins 250, upd 0, del 300`, live rows dropped 1001 → 951 | which rows, which transaction, which Job, the order |
| Kubernetes API | yes, immediately | 6 Jobs created at the same timestamp (who and when) | anything about the data they changed |
| CDC (our exporter today) | yes, ~0.5 s lag | per-operation counts: `op=c 250, op=d 300, op=u 0`, split by op and table | **as exported today, still only counts** — not row content, not the Job link |

Numbers from the run: counters went `1001|1|0|1001` → `1251|1|300|951` (ins|upd|del|live); CDC ended at `op=c 251, op=d 300, op=u 1` (the extra c/u are the two marker changes made while bringing CDC up).

## Reading the result (honest, partly negative)

- **As delivered today, CDC does not beat the counters for this scenario.** Both say "300 deletes and 250 inserts happened on orders". CDC split them by operation a bit more cleanly and with ~0.5 s lag, but `pg_stat` showed the same totals. A delete-rate alarm here does **not** need CDC.
- **The Kubernetes API already answers "who and when"** on its own: 6 Jobs, same timestamp. No CDC needed for that either.
- **The two things that would justify CDC were NOT produced by the current pipeline:**
  1. **Row content** — which 300 rows were deleted. CDC carries this in the change stream, but our exporter keeps only counts (and on PostgreSQL a DELETE carries only the key unless `REPLICA IDENTITY FULL` is set — the inspector's replica-identity gap, R-items).
  2. **Attribution** — tying the 300 deletes to `catchup-1-delete`. Nothing joins the CDC stream to the Job. Same timestamp is not proof when other writers exist.

**Verdict against the pass/fail criterion:** FAIL as the pipeline stands today. CDC added nothing over counters + the Kubernetes API for the burst-detection case. It only *could* win on row content and attribution, and neither is built.

## What this tells the direction

- Counting mutations is not a reason to run CDC; `pg_stat` plus a Prometheus exporter covers it. This matches the Debezium maintainer's objection.
- The only defensible edge is **per-row content and per-Job attribution**, and both are unbuilt and non-trivial (attribution needs the Job to be the sole writer, or a correlation signal we do not have).
- Next, before any product code (still under the research rule): decide whether exposing row content + attribution is worth it, and confirm with user conversations (R8) that anyone needs it. If attribution cannot be done honestly, this is a kill-criterion hit (R9).

## Variant: duplicate rows (`variant-duplicates.sh`, run 2026-11-05)

Scenario: the scheduler reran the same daily-summary job 5 times, each inserting the identical row `(customer=999, amount=42.00)`. Question: can any source tell these are duplicates, not 5 legitimate new rows?

| Source | Could it tell they are duplicates? |
|---|---|
| `pg_stat` counters | No. `n_tup_ins` went up by 5; it cannot say they are the same logical row. |
| Kubernetes API | **Partly, and better than CDC here.** Five Jobs with the same base name (`daily-summary-rerun-*`) is a strong hint of a rerun storm — from job metadata alone, no data needed. |
| CDC (as exported) | No. `op=c` went up by 5; the exported counts carry no row content, so no dedup signal. |
| CDC (raw stream, not exposed) | Yes in principle. Each INSERT carried the full row, so the raw stream contains the repeat `(999, 42.00)`. Our pipeline keeps only counts, so this is unrealised. |
| Ground truth (SQL) | 5 identical rows confirmed. |

**Verdict:** FAIL again for the exported pipeline. And a sharper point: for *detecting* a rerun storm, the Kubernetes API (repeated job names) is the better signal, not CDC. CDC's only possible edge stays the same — row content — and it is still not exposed. Deduplication would also need a business key (what makes two rows "the same"), which is not generic.

## Not covered

- Real schedulers with a catch-up mode (this models the burst with plain Jobs; the native CronJob runs only the latest missed schedule).
- A NetworkPolicy, other CNIs, load under contention, one run only (no variance).
