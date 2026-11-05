# User conversations (R8): guide

Goal: find out whether anyone needs the one thing the experiments showed CDC can uniquely do —
a **policy on the content of a data change**, enforced by a reaction — before building any of
it. This decides whether the direction lives or dies (kill criteria, R9).

Do not pitch. Do not say "CDC", "Debezium" or "operator" first. Start from the person's pain,
listen, and only explain the idea if they ask. A conversation where they talk 80% of the time
is a good one.

## Who to talk to

5 to 10 people who run PostgreSQL behind an application on Kubernetes, in roughly this order of
value:
- Platform / SRE engineers who operate the database for others.
- Backend engineers who write the Jobs, migrations and cron tasks that change data.
- Anyone who has had a data incident (bad migration, runaway job, wrong bulk update).

One is enough to start. Breadth matters less than honesty in the answers.

## Part 1 — the pain (no technology named)

Let them tell stories. Follow what they react to.

1. Has a job, migration or deploy ever changed data in a way you did not expect — mass delete, wrong update, duplicates? What happened?
2. How did you find out, and how long did it take? Who noticed first?
3. What did you do once you knew? Could you undo it?
4. Looking back, what would you have wanted to happen automatically, if anything?
5. What do you watch today for your database, and what do you wish you could see but can't?

Listen for: whether the pain is about **volume** (too many changes — then our experiments say
they don't need us, counters suffice) or **content** (a specific value changed that should not
have — then CDC is the only source).

## Part 2 — probe the content angle (only if Part 1 points there)

Use their own example if they gave one; otherwise a concrete one:

6. Imagine a rule like "a settled payment must never go back to pending", or "this column must never drop below zero". Would a rule like that be useful to you? Which real columns or tables?
7. If something broke such a rule, what should happen: just an alert, a human approval step, or the system stopping the thing that did it?
8. How would you feel about the cluster automatically stopping a job that violated a data rule? Where is the line between helpful and dangerous?

Listen for: do they want **reporting**, or **action**? This is the open product decision (the
"brake"). Do not assume; let them draw the line.

### Content-policy examples (support, not a script)

Do NOT read this list at people. Ask them for their own rule first ("Is there a value or a state
in your database that, if it changed the wrong way, would be a serious incident?"). A rule they
name is worth far more than one we suggest. Use these only to unstick a conversation or to check
feasibility.

Grouped by what each rule needs, because that is what sets the cost and whether it is even
possible. Raw brainstorm; depure later. Nothing here is built.

**A. Only the new value (cheapest; no `REPLICA IDENTITY FULL`):**
- *Out of range* — a `balance` goes negative, a `price` jumps 100x, a `discount` exceeds 100%.
- *Outside a time window* — a payment inserted with `paid_at` in the future or >90 days back.
- If we build one, start here: CDC always carries the new value, no extra setup.

**B. Old value vs new value (needs `REPLICA IDENTITY FULL` → more WAL):**
- *Forbidden transition* — a payment `settled -> pending`, an order `shipped -> pending`. This is the type we proved (`row-content`).
- *Immutable field* — `created_at`, `account_id`, `invoice_number` must never change once set.
- *Forbidden deletion* — a settled payment or an audit record is deleted (default identity gives only the key; its values need FULL).

**C. Accumulated state in the consumer (the strongest "why CDC, not counters" cases):**
- *Per-entity rate* — one `user_id` makes >50 invoices in 10 min; one card authorizes 10 payments in a minute. Counters aggregate per table; CDC can count per key. Best fraud angle.
- *Skipped or duplicate sequence* — `invoice_number` jumps 1003 -> 1007, or two rows share a number.
- *Revert pattern* — a value goes A -> B -> A within N seconds (someone probing limits). Needs a short per-key history; compares consecutive new values, so no FULL.
- These need the operator to keep state over time, which a trigger or a constraint does poorly and a stream consumer does well. Most defensible that it belongs on CDC.

**D. A lookup into another table (needs a read per event; has a race):**
- *Cross-reference* — a transaction with `account_id=42` while account 42 is `closed`; an order for a `banned` customer.
- *Orphan state* — an order marked `shipped` with no row in `shipments`; a settled payment with no ledger entry.
- Cost: one query to the DB per event, and the other table may change between the event and the check. A foreign-key constraint in the DB prevents part of this better than detecting it after.

**E. Metadata CDC does not carry (flag — measured limitation):**
- *Unexpected context* — a role change to admin from an unknown IP or out of hours; a mass DELETE by the app user when normally only admin does it.
- **Blocker:** the CDC event has no connection user, `application_name`, IP or client identity (measured: `attribution` experiment failed on exactly this). Getting it would mean DB-side triggers that record who did what — which breaks the product's promise of not touching the database. Same unsolved problem as attribution. Keep as a "nice if attribution were solved", not a near-term rule.

Honest framing when you discuss these: A is cheap and real; B is "only CDC sees it" but costs
`REPLICA IDENTITY FULL`; C is the strongest CDC-specific value; D needs lookups and overlaps with
DB constraints; E is blocked by the attribution gap we already measured. Do not promise any of it
is built.

## Part 3 — attribution (the hard, unsolved part)

9. Is your PostgreSQL self-operated (you control the node) or managed (RDS, Cloud SQL, Azure)? (This decides whether node-level attribution is even possible.)
10. When data changes unexpectedly, do you need to know which service or job did it? Or is knowing what changed enough?
11. (If they need the "who") How do you figure that out today?

Listen for: whether attribution is a must-have or a nice-to-have. The experiments showed it is
the hard part; if few people need it, we can ship content policies without it.

## What a "yes" and a "no" look like

- **Yes (pursue):** several people describe a real content incident, want a rule on values, and
  would accept at least an alert or an approval step. Bonus if they say counters/dashboards did
  not catch it.
- **No (drop or shrink, R9):** the pain is only volume or load; or they would never let anything
  act on their database; or nobody has had a content incident worth the setup cost
  (`REPLICA IDENTITY FULL`, running this next to production).

## Rules for us

- Record each conversation's facts, not our interpretation. Quote them.
- A polite "sounds interesting" is not a yes. Push for a real past incident.
- We are allowed to hear "no". That is a successful conversation too; it saves the build.
- Nothing gets built from a single conversation.

## What the experiments already tell us to bring (not to lead with)

- CDC is the only source that sees a value transition (row content). Measured: `bench/experiments/row-content`.
- Counting changes does not need CDC. Measured: `bench/experiments/scheduler-catchup`.
- Attribution to a workload is unsolved beyond a sole-writer Job. Measured: `bench/experiments/attribution`.
- Cost of content policies: `REPLICA IDENTITY FULL` (more WAL). See `bench/experiments/FINDINGS.md`.
