# R8 capture sheet — community talk

Short form of [`user-conversations.md`](user-conversations.md) for the talk and the hallway after
it. One sheet per person. Write what they **said**, not what we think it means. Use a role and
initials, not a full name or company, unless they agree to be named.

**Bias to remember.** At the talk people have already heard "CDC" and our proposal, so their
answers are primed. Treat a "yes" from the audience as weaker than a "yes" from someone who
brought up the problem unprompted. A real past incident counts; "sounds interesting" does not.

---

## Part A — the room (Q&A, 5 minutes)

Count hands or answers for the three questions on slide 05. Write numbers, not impressions.

| Question (slide 05) | Answers |
|---|---|
| 1. Postgres in the cluster (CloudNativePG or similar) or managed (RDS, Cloud SQL)? | in-cluster: ___  managed: ___  both / other: ___ |
| 2. Need to know only **what** changed, or also **which workload** did it? | what: ___  also which: ___ |
| 3. When a rule breaks: only alert, wait for approval, or stop the job? | alert: ___  approval: ___  stop: ___ |

Questions from the room, as asked (quote, and who asked by role):

1. _______________________________________________
2. _______________________________________________
3. _______________________________________________

Did anyone say an existing tool already does this (Argo Events, KEDA, Kyverno, a trigger in the
database, something else)? Which one, and what did they use it for?

_______________________________________________

---

## Part B — one person (hallway, 5 to 10 minutes)

**Who:** role ________  initials ____  date ________
**Database:** in-cluster ☐  managed ☐  both ☐  which: ________
**How the conversation started:** they came to us ☐  we asked ☐  they raised the problem first ☐

### 1. The pain (ask first, do not name a technology)

- Has a job, migration or deploy ever changed data in a way you did not expect? What happened?
  > _______________________________________________
- How did you find out, how long did it take, who noticed?
  > _______________________________________________
- Could you undo it? What would you have wanted to happen automatically?
  > _______________________________________________

Type of pain: **volume** (too many changes) ☐  **content** (a specific value changed wrongly) ☐  neither ☐
*(Volume means counters already serve them; content is where CDC is the only source.)*

### 2. The content rule (only if part 1 points there)

- Is there a value or a state that, if it changed the wrong way, would be a serious incident?
  Their own rule, in their words:
  > _______________________________________________
- Group (see the guide): A new value only ☐  B old vs new ☐  C per key over time ☐  D lookup in another table ☐  E needs who did it ☐
- Could the database prevent it instead (a constraint, a trigger, a permission)? Did they consider it?
  > _______________________________________________

### 3. The reaction

- If the rule broke: alert ☐  approval ☐  stop the job ☐  "never let anything act" ☐
- Where is the line between helpful and dangerous? (quote)
  > _______________________________________________

### 4. The "who"

- Need to know which service or job did it: must-have ☐  nice-to-have ☐  not needed ☐
- How do they find out today?
  > _______________________________________________

### 5. Close

- Would they try it, or talk again? ☐ yes ☐ no  — contact only if they offered: ________
- Strongest quote of the conversation:
  > _______________________________________________

---

## After the talk (same day)

Transcribe each sheet into a dated entry (for example `docs/research/conversations/2026-11-xx-<initials>.md`),
facts and quotes only. Then score against the guide's "What a yes and a no look like":

| | Count |
|---|---|
| People with a **real past content incident** | ___ |
| Of those, would accept at least an alert or an approval | ___ |
| People whose pain was only volume or load | ___ |
| People who would never let anything act on their database | ___ |
| People who said the database could prevent it better | ___ |

R9 reminder: if we cannot name more than one real case where CDC plus a Kubernetes reaction
beats database-side prevention and counter-based alerts, the direction does not hold. Nothing
gets built from a single conversation, and a talk audience is a primed sample.
