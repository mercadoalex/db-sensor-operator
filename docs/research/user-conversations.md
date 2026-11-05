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
