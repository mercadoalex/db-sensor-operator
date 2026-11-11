# P0 (sub-question 1): data incidents in public postmortems, and where CDC would or would not have helped

Investigation date: 2026-11-11 (read-only web research, no code changed).
Feeds: R9 kill criterion in [`STATUS.md`](STATUS.md), [`r8-capture-sheet.md`](r8-capture-sheet.md).
Language note: written in English like the other research capture files; `STATUS.md` sub-question 1 (labelled 1 there, P0 in the technical files) can be updated from the plain-language summary below.

## 1. Summary answer

**Question:** what classes of data incident appear in public postmortems, how were they detected and repaired, and for how many would CDC plus a content policy (or CDC-based repair) have clearly beaten a database constraint/trigger or a counter alert?

**Short answer.** I read 14 company-published postmortems in full or in the relevant part, plus 2 incidents known only through press coverage (flagged). Every one is cited with a URL in section 5. The honest result for R9 is weak but not empty:

- **1 clear case** where CDC with a content policy and CDC-based repair would plausibly have beaten both prevention and counters, on both detection and repair: **Bungie / Destiny 2, Feb 2020**. A bug wrote a wrong value (item quantities cut to a wrong cap) on a subset of servers. Players noticed. Bungie then rolled back *every* account to a pre-patch backup, because finding the affected accounts was judged too risky. That is exactly the "backup throws away good work" cost.
- **1 strong-but-mixed case: Matrix.org, Jul 2025.** A database constraint, added after the fact, caught the culprit job within hours of being installed, so for *prevention* the constraint wins. For *repair*, an event log with before-images of the deleted rows would plausibly have given the exact victim list that the team had to reconstruct by diffing a restored backup against production (about a week of scripts). Plausible, not proven.
- **2 plausible but speculative or unverified:** GitHub Sept 2012 (cross-store ID drift, a duplicate/regressing-ID rule on the stream might have flagged it), and Salesforce May 2019 (a permission flag flipped on many rows, but a counter on mass updates and a deploy-time guard would do as well, and I could not read a primary source).
- **The other 12 are neither a CDC win nor even a CDC fit**, or are won by prevention (revoked permissions, soft deletes, environment isolation) or by something other than CDC (counters, the database's own binlog, WAL archiving/PITR, cross-store invariant checks).

**What this means for R9.** R9 asks for "more than one real case where CDC + reaction clearly beats prevention and counters". Public postmortems give **one clear case and one case that is clear only on repair**. By a strict reading R9 is **not met by public postmortems alone**. By a generous reading (Bungie + Matrix-repair) it is barely met, and both rest on my reasoning about what the stream *would* have shown, not on a documented CDC deployment. I would not write "R9 met" in `STATUS.md`. I would write "public evidence: one clear case, one partial; needs R8 conversations".

**Notable pattern that supports the project in a narrower form.** In 8 of 16 incidents, recovery meant restoring a whole database, site set or account population from backup, and the postmortem itself names the good work that was lost or the care it took to avoid losing it (GitLab about 6 hours, Keepthescore about 7 hours, GitHub 2012 "since last backup", Travis about 15 minutes, Turso everything after Dec 1, Bungie all accounts, Atlassian up to 5 minutes, Razorpay/Matrix needed manual forensic reconstruction). But in most of those the bad change was a *total* wipe, and for a total wipe the right repair really is "restore, then replay the log", which PITR with WAL archiving already does. The project's repair advantage over PITR only appears when **the bad change is a subset and legitimate writes kept arriving** (Bungie, Matrix). That matches what the bench measured (`bench/experiments/repair-vs-pitr/RESULTS.md`: PITR returns a past state and drops or overwrites later good writes; stream repair undid only the bad transactions). It also bounds how often the advantage applies: **2 of 16 here**.

**Detection pattern.** The first signal that the *data* was wrong came from: customers or outside people in 5 cases (Atlassian, Matrix, Bungie, Turso, OpenAI); symptom-based automated alerts in 5 (GitHub 2018, Twilio, Travis, Razorpay, Kickstarter); the engineer who caused it or a colleague in 4 (GitLab, GitHub 2012 test, Keepthescore, GitHub Sept 2012); a loud outage in 1 (Azure DevOps); unclear in 1 (Salesforce). **In 0 of 16 did a content-aware alert fire.** That is not evidence a content policy would have worked (these companies may not have had one), but it shows the gap is real: nobody in this sample was watching *what* changed, only *that* something broke.

## 2. Method and honest limits

- Searched for company-published postmortems and read the originals where I could fetch them. For aggregator lists I used the danluu/post-mortems README as an index only, then went to the original. Blog-style retellings (dev.to, Substack, Medium) were found in search but **not used as evidence**.
- "Data incident" test applied: wrong, corrupted, duplicated or wrongly deleted data caused by a bug, a bad deploy/migration, a script/job, a race, or an operator mistake. Excluded pure outages (see section 4).
- **Not verified / not obtained:**
  - Azure DevOps 2023: the Microsoft devblog original returned 404 on my fetch; I used The Register's article quoting Microsoft's engineering manager. Aggregator-level trust.
  - Salesforce 2019: no primary postmortem found; only press coverage. Aggregator-level trust, and I did not read the articles in full.
  - Kickstarter "The Day the Replication Died": the page I read is a Wayback copy and does not state the year; the danluu index also omits it. I do not state a year.
  - Bungie: the postmortem does not name the database engine. I do not know if it is PostgreSQL, so "CDC would have worked" is about the idea, not about Debezium on their stack.
  - Cloudflare 18 Nov 2025: read only the search snippets and index entry, not the full text. Excluded anyway (section 4).
- **Not found within this investigation:** usable data-level incidents from Stripe, Shopify, HashiCorp and Honeycomb. Searches returned mostly SEO articles and availability reports. Honeycomb's incident reviews that surfaced were ingest/availability outages. This is a limit of my search, not proof that those companies have no such incidents.
- **Sample bias (important).** Public postmortems skew to loud, large, embarrassing events and to companies with a blameless-postmortem culture. Silent data corruption that is fixed quietly, and anything that exposes customers legally, is under-represented. Private postmortems (the majority, as noted in `STATUS.md`) are invisible here. The sample of 16 is hand-picked by what I could find, not random.
- **What "public postmortem" is, as a source, and why that matters more than ordinary sample bias.** A postmortem is not a log and not an independent audit; it is a document the company itself wrote and chose to publish. I do not think these are fabricated: the technical ones (GitLab, GitHub, Atlassian) include detail specific enough (exact commands, minute-by-minute timestamps, internal tool names) that a fabrication would be easy to contradict publicly and none has been, as far as I found. But "probably not fabricated" is not the same as "complete". Three separate filters sit between an incident and what I read: (1) whether the company publishes anything about it at all — most data incidents never become a public postmortem, only the ones that were already visible to users or that the company chooses to use as a transparency exercise; (2) what gets included within the published account — the technical cause is usually told well, because it makes the team look competent and honest, but context that is uncomfortable and not part of that incident's narrative (a repeat of a known failure mode, a slow on-call response, understaffing) is rarely volunteered; (3) no raw log, traffic capture or internal chat is ever in the public text, only the narrated summary. The practical consequence for this file is not mainly "a given incident's account may be false", it is "the set of incidents and the completeness of their detail are both shaped by what the company chose to show". This is a harder limit than generic sample bias, and it is the main reason, alongside having no real CDC deployment behind either Bungie or Matrix, that this file's R9 reading stays "not met, not refuted" rather than a settled answer.

## 3. Evidence: the incidents

Trust label: **[P]** = published by the company itself and read in full or in the relevant section. **[A]** = aggregator or press summary, verify against the original.

### 3.1 Categories used (small, recurring set)

| Code | Category | Incidents |
|---|---|---|
| A | **Wrong or total deletion by a script, job, test run or operator** (the intent was different from what ran) | GitLab 2017, Atlassian 2022, Travis 2018, GitHub 2012 (test suite), Keepthescore 2020, Azure DevOps 2023 |
| B | **Wrong value written or wrong rows removed by buggy code or a latent fault** | Bungie 2020, Matrix 2025, Kickstarter |
| C | **Data lost or diverged by failover/replication/config** | GitHub 2018, GitHub Sept 2012, Razorpay 2019, Twilio 2013 |
| D | **Wrong data shown to or tied to the wrong tenant/identity** | OpenAI 2023, Turso 2023, Salesforce 2019 (+ the secondary effect in Travis and GitHub Sept 2012) |

Twilio also has a side effect category (duplicate customer charges), described below. Several incidents sit in two categories; I placed them by the *primary* data harm.

### 3.2 Incident table

Times are as stated in each postmortem. "Detected by" is who or what first showed the *data* problem, not the outage.

| # | Incident | Category | Detected by | Time to detect | Time to repair | How repaired | Postmortem said it wished... | Trust |
|---|---|---|---|---|---|---|---|---|
| 1 | GitLab.com, 31 Jan 2017 | A | The engineer, within a second or two | Seconds | About 18 h of outage; ~6 h of DB writes permanently lost (about 5,000 projects, 5,000 comments, 700 users) | Restored from a 6-hour-old LVM snapshot copied from staging; the `pg_dump`, Azure snapshots and replica were all unusable | Working backups with alerting, point-in-time recovery/WAL archiving, an owner for data durability, clearer host prompts | [P] |
| 2 | Matrix.org (Element), Jun to Jul 2025 | B | Users (bug report "No create event in auth events"), then logs | First log errors 27 Jun; cause found 3 Jul; the underlying index corruption likely existed for a year or more | Majority of rooms repaired by 11 Jul (about 2 weeks from first symptoms) | Rebuilt the 4 TB corrupt index overnight; diffed a 26 Jun backup against production to list victim rows; re-inserted them | Not framed as a wish. They added a Postgres constraint during the incident to reject deleting state still referenced | [P] |
| 3 | Atlassian, 5 to 18 Apr 2022 | A | A customer support ticket 8 minutes after the script started (07:38 to 07:46 UTC). Internal monitoring did not fire because the deletion used a standard workflow | 8 min to first ticket; 39 min to incident declared; 75 min to script confirmed as cause | First sites back 8 Apr; all 883 sites (775 customers) by 18 Apr; no customer lost more than 5 min of data | Site-by-site restore from backups across many products, then increasingly automated | Universal soft deletes; automated multi-site, multi-product restore; support access for customers who lost contact data | [P] |
| 4 | GitHub, 21 Oct 2018 | C | Automated monitoring alerts 2 minutes after a 43-second network partition | 2 min | 24 h 11 min degraded; unreplicated writes (e.g. 954 writes on the busiest cluster) still being reconciled when the post was published | Restored clusters from backup, failed forward; captured the binlogs of the unreplicated writes and analyzed which could be reconciled automatically | Orchestrator should not promote primaries across regions; chaos testing | [P] |
| 5 | GitHub, 10 to 11 Sep 2012 ("availability this week") | C (+ D) | A developer reported wrong query results from the standby the morning after a failover | About 1 day | Partition and out-of-date node elected 08:19 to 08:26 PDT; 16 private repos exposed for 7 min | Removed leaked events; audited misrouted repos; notified owners | Failover of the primary DB should be manual only | [P] |
| 6 | Twilio, 18 to 19 Jul 2013 | C (duplicate charges) | Automated billing monitoring flagged an anomaly | 46 min after the Redis master restart wiped balances (02:42 to 03:28 PDT); billing system offline at 04:10 | Recurrence at 12:36 after bringing it back; refunds done next day; all-clear about 43 h after the restart | Restored balances from a separate double-bookkeeping relational store; refunded; credited 10% of 30-day spend | Auto-recharge must fail safe when balance data is missing; validate balances against the second store in real time | [P] |
| 7 | Bungie, Destiny 2, 28 Jan and 11 Feb 2020 | B | Players reporting lost currency/materials | Hours (hundreds of thousands of players had logged in before shutdown at 10:30 PST) | Same day, but by full rollback | Restored **all** character data from the backup taken just before the patch, "rather than trying to identify each affected account and risk missing something" | Additional safeguards (list truncated in the page I read) | [P] |
| 8 | Travis CI (.com), 13 Mar 2018 | A (+ D) | Alerts, immediately; the root cause took a day to find | Minutes | 5.5 h non-operational; about 15 min of data lost; bad tokens revoked about 26 h later | Restored the full production DB from backup; revoked tokens | Revoked `TRUNCATE` permission; guard in test helpers and shell prompt; read-only follower access; automate failover | [P] |
| 9 | GitHub, Sept 2012 ("Today's outage") | A | The engineer who caused it | Immediate | Hours; events table restored "bit by bit over the next few days" | Restored last backup; skipped the large events table to speed recovery; some data since the last backup lost | Isolate test from production; shrink the events table; begin storing binlogs | [P] |
| 10 | Keepthescore, 17 Oct 2020 | A | The developer | Immediate | 30 min to be back online; scores from 15:47 to 23:21 CET (about 7 h) gone | Restored the once-a-day managed backup | Safer dev tooling; fast backup restore "absolutely essential" | [P] |
| 11 | Razorpay, Dec 2019 | C | Alerts on 5xx errors and replica duplicate-key errors; the **data loss** itself found the next day by comparing application trace logs with the DB | Alerts immediate; data loss about 1 day | Replicas rebuilt in about 6 h; data corrected in an 8-hour session | Dug the exact queries from the binlog and matched them to application trace logs; corrected rows. Root cause: `innodb_flush_log_at_trx_commit=2` | Fix the config (set to 1) | [P] |
| 12 | Turso, 20 Nov to 4 Dec 2023 | D | An outside person reported it | Noticed 4 Dec 08:10 UTC; the faulty change was 20 Nov, databases mis-recreated 1 Dec (3 days) | Fixed 67 min after the report | Re-ran the migration; recreated affected DBs from the 1 Dec backup; discarded writes after 1 Dec (possible data loss) | Backup/ID validation in control plane and data plane; stricter configs | [P] |
| 13 | OpenAI, 20 Mar 2023 | D | Users saw other users' chat titles | Within the day; billing exposure window 9 h (1 to 10 AM PT) for 1.2% of active Plus users | Hours | Patched the client-library bug; restored service except a few hours of chat history | Not framed as a wish in the page I read | [P] |
| 14 | Kickstarter ("The Day the Replication Died", year not stated) | B | A MySQL replication error (duplicate key) | The inconsistent rows were days old, some over a month old | Not stated in the page I read | The page is about the MySQL bug, not the repair | Understand the database better | [P] |
| 15 | Azure DevOps, South Brazil, 24 May 2023 | A | The outage itself (service down) | Immediate | More than 10 h; all data recovered | Restored 17 production databases by on-call engineers, hampered by mixed backup configs | Reported only as "fixes and reconfigurations" | [A] via The Register quoting Microsoft; original not retrieved |
| 16 | Salesforce / Pardot, 17 May 2019 | D | Unclear in what I read | Not established | About 12 days of disruption per press | Salesforce cut access to affected orgs, then restored original permissions | Not available | [A] press only; no primary postmortem read |

Details worth recording for each, in a few lines each:

1. **GitLab 2017** [P]. A tired engineer ran `rm -rf` on the PostgreSQL data directory on the primary, thinking it was the secondary. Detected within seconds, but all five backup mechanisms failed (`pg_dump` version mismatch, failure emails rejected by DMARC, no Azure snapshots on DB hosts). Recovery came from an LVM snapshot taken manually 6 hours earlier. They also bumped all sequences by 100,000 so IDs would not be reused. Source also lists "investigate PITR and continuous archiving" among its fixes.
2. **Matrix 2025** [P]. A corrupt Postgres index made a clean-up job delete rows of *other* rooms (the delete targeted state group X, but the index pointed at heap space later reused by state group Y). The index corruption was probably more than a year old and harmless until the empty heap space got reused. After diagnosis they added a constraint that rejects deleting state for a state group still in use; re-enabling the clean-up job immediately hit that constraint. Repair used a backup restored from before the damage and a victim list found by cross-referencing it to production.
3. **Atlassian 2022** [P]. A script meant to delete a legacy app was given site IDs instead of app IDs and ran in permanent-delete mode. Peer review and staging did not catch it because the IDs did not exist in staging. Their own words: internal monitoring did not detect an issue because the sites were deleted through a standard workflow.
4. **GitHub 2018** [P]. Primarily a failover/partition incident. The data-level part: seconds of writes existed only in the East Coast, 30+ minutes of writes only in the West. They chose data integrity over availability and later analyzed the captured MySQL binlogs to decide which writes could be reconciled automatically.
5. **GitHub Sept 2012** [P]. A schema migration load triggered a failover to a stale node; a later Pacemaker segfault made that stale node primary for about 7 minutes. MySQL auto-increment IDs issued in that window did not line up with IDs in Redis, so some events showed on the wrong dashboards and some repositories were misrouted (16 private ones exposed for 7 minutes).
6. **Twilio 2013** [P]. A Redis master restart read the wrong config, dropped all balance data and booted read-only. With balances at zero, auto-recharge charged cards repeatedly without being able to record the payment (this is the duplicate-charge side effect). 1.1% of customers hit at first, 0.3% more on the recurrence.
7. **Bungie 2020** [P]. A "fix" for quest sorting disabled too much of an inventory clean-up routine, so the cap for stacked items (currency, materials) was computed wrong and items above the wrong cap vanished. A server-config workaround for a crash-on-startup problem let some servers run old code again on the second release. Internal testers had seen the first bug and dismissed it as a tooling failure.
8. **Travis 2018** [P]. A test run in an old tmux shell with `DATABASE_URL` still pointing at production truncated all tables. The API stayed up about 30 minutes on an empty database; users who logged in got *new* records with IDs from the existing sequence. After restore, those IDs collided with real accounts, so some users were logged in as the wrong user and tokens had to be revoked.
9. **GitHub 2012 test run** [P]. CI ran against production and the standard "destroy and recreate the DB" step wiped it. They restored the last backup, skipped the large events table to be faster, and could not recover writes after that backup. Fix list includes storing binlogs.
10. **Keepthescore 2020** [P]. A local-only "drop and recreate tables" function somehow connected to production. Restore from the daily managed backup was fast; the day's writes were lost.
11. **Razorpay 2019** [P]. A Multi-AZ failover lost about 5 seconds of committed transactions because the log was not flushed per commit, while the binlog (flushed per commit) still held them; replicas then failed with duplicate-key errors when the new primary reissued the same auto-increment IDs. The old replicas' snapshots proved the lost data existed.
12. **Turso 2023** [P]. A migration bug left 0.07% of databases with an empty backup identifier, so they pointed at one shared backup location; when some were recreated they received another tenant's data (leak) and the fix discarded recent writes (loss).
13. **OpenAI 2023** [P]. A bug in an open-source Redis client library returned responses to the wrong requester. Not a bad row in a table; a mis-delivered read.
14. **Kickstarter** [P]. A MySQL bug ignored `ORDER BY` in a sequence-assigning job, so primary and replicas assigned different sequence numbers to the same rows. Nothing noticed until a later unsafe statement replicated row-based and hit a unique index on a replica.
15. **Azure DevOps 2023** [A]. A typo in a large package-migration pull request made a snapshot-cleanup job delete the whole Azure SQL Server instead of one snapshot database, taking out all 17 production databases of one scale unit. Loud, immediate; recovery slowed by differing backup configs and a warm-up task.
16. **Salesforce 2019** [A]. A database script deployed for Pardot granted broad "modify all" permissions to users in affected orgs. Salesforce blocked access to affected instances while it restored permissions. I could not read a primary postmortem, so the detection mechanism and timeline are not verified.

## 4. Excluded, and why

- **Cloudflare 18 Nov 2025** (index entry and search snippets only): a database permission change made a metadata query return duplicate rows into a generated bot-management "feature file", which then exceeded a size limit in the proxy. A data-shaped trigger, but the harm was a global availability outage and the bad data was a generated config artifact, not stored application rows. Not a data-correctness incident for this project's purposes.
- **Honeycomb** incident reviews that surfaced (July 2023 outage, Kafka maintenance, an unnoticed 10% event-rejection incident from earlier years): ingestion/availability issues on an observability pipeline, not row-level data corruption, and I did not read them closely.
- **danluu entries I saw but did not read** that may be data-level and worth a second pass: Sentry June 2016 (wrong S3 settings on backups), TravisCI Sept 2016 (VM image cleanup), Stack Exchange (leak of user info via ported code), Heroku Feb 2017 (filesystem corruption).
- The ChatGPT and Turso cases are kept in the main table even though they are not "wrong row in a table", because they are wrong *data attribution* and the project should know CDC does not address them.

## 5. Cross-cutting finding: CDC + content policy vs prevention vs counters

Definitions used, the same bar `STATUS.md` sets for R9: CDC "clearly beats" prevention and counters only if it does something they cannot, or does it substantially better (seeing the exact content of a change when a counter sees only "something changed"; repairing precisely when the alternative is losing good work).

Verdicts are my own reasoning from the postmortem text, **not** evidence from a deployed CDC pipeline. "Plausible" means the information the stream would carry existed; it does not mean the team would have written the right rule.

| # | Incident | Prevention (constraint, trigger, permission, soft delete) | Counter or symptom alert | CDC + content policy (detect) | CDC repair from event log | Verdict |
|---|---|---|---|---|---|---|
| 1 | GitLab 2017 | No (physical `rm -rf`) | No | No: nothing logical to see; the data directory vanished | Only as a retained off-box log, which equals WAL archiving/PITR (GitLab lists exactly that) | **Neither.** CDC offers nothing PITR does not |
| 2 | Matrix 2025 | **Yes.** The constraint they added caught the clean-up job the moment it was re-enabled | Rows deleted per job looked normal; a user bug report was the first signal | Weak: the rule is relational ("deleted row still referenced") which a constraint does better | **Plausible.** Logical decoding emits the rows actually deleted, so a victim list would not need a restored backup diff | **Prevention wins detection; CDC repair plausibly faster and more precise.** Counts as a partial case |
| 3 | Atlassian 2022 | **Yes.** Their own fix: soft deletes everywhere | A count of site deletions per hour (near zero normally, 883 in 23 minutes) would fire | Content adds nothing: a site delete is a legal transition | Their data was across many product stores; restore, not row-undo, was needed | **Counter or soft delete suffices.** CDC no clear gain |
| 4 | GitHub 2018 | Config (no cross-region promotion) | Alerts fired in 2 min | n/a | They used **MySQL binlogs** to reconcile; the same primitive CDC reads, but natively | **Neither beyond native binlog** |
| 5 | GitHub Sept 2012 | Manual failover only | A developer spotted it | **Possible:** a duplicate or regressing auto-increment ID on the stream (the stale node reissued IDs) is what the `sequence-gap` bench experiment targets | Not needed | **Ambiguous.** The harm was a Redis/MySQL ID mismatch, which a MySQL-only stream might not show |
| 6 | Twilio 2013 | No (Redis, external card API) | **Yes**, but 46 min late; it was the one thing that worked | No: CDC on the relational store would not see Redis or card charges | No | **Neither.** The fix is a real-time invariant check against the second store |
| 7 | Bungie 2020 | **No.** The wrong cap was a valid number written by buggy code | A write counter sees nothing wrong; an aggregate (currency in circulation) might | **Yes:** before/after on the currency and item rows, rule "stack quantity dropped without a spend event" (needs domain knowledge to write) | **Yes:** only some servers ran the bad code, so undoing the bad writer's changes would have spared players the full rollback | **Clear CDC win**, both detection and repair, with caveats below |
| 8 | Travis 2018 | **Yes.** `REVOKE TRUNCATE` removes the whole problem | Alerts already fired immediately | Postgres CDC can emit a truncate event, but the team already knew in minutes | A reaction ("scale the API to zero on truncate") might have avoided the 30-minute window that created colliding IDs | **Prevention wins.** CDC reaction is a backstop, and any alert could trigger it |
| 9 | GitHub 2012 test | **Yes.** Isolate test from production | n/a | n/a | Binlog storage (their own fix) is the log-based repair, equal to PITR | **Prevention wins** |
| 10 | Keepthescore 2020 | Maybe (separate credentials, they already believed they had) | n/a | `DROP TABLE` produces no row events, so CDC would not "see" the deletion | A retained stream of earlier inserts could replay the lost 7 h, same as PITR | **Neither**; continuous backup solves it |
| 11 | Razorpay 2019 | Config fix | The duplicate-key error on replicas was the detector, which is itself a **constraint** | n/a | They repaired from the binlog and trace logs by hand; the same data a stream would hold | **Constraint detected; binlog repaired.** CDC no clear gain |
| 12 | Turso 2023 | Control-plane validation (non-empty backup ID) | No | No (restore swaps the whole file) | No | **Neither** |
| 13 | OpenAI 2023 | No | No | No (mis-delivered reads, no bad row) | No | **Neither** |
| 14 | Kickstarter | The unique index on the replica did catch it, days late | No | A stream reads the primary only, so replica divergence is invisible | No | **Neither** (unique constraint caught it late) |
| 15 | Azure DevOps 2023 | Better test coverage; deletion guards | The outage was the alert | No (server gone) | No | **Neither** |
| 16 | Salesforce 2019 (unverified) | Deploy-time guard on the script | A mass-update count on permission rows would fire | **Plausible:** forbidden transition "profile gets modify-all" | Possibly: revert only flipped flags | **Ambiguous, unverified.** Counter would do about as well |

Tally on this sample of 16:
- CDC clearly better than prevention and counters: **1** (Bungie).
- CDC repair plausibly better, prevention better for detection: **1** (Matrix).
- Plausible but speculative or unverified: **2** (GitHub Sept 2012, Salesforce).
- Prevention would have removed it (permissions, soft delete, isolation, config): Atlassian, Travis, GitHub 2012 test, GitHub 2018, Razorpay (5, counting the ones where it is the cheaper answer).
- Neither CDC nor a database constraint: GitLab, Twilio, Keepthescore, Turso, OpenAI, Kickstarter, Azure DevOps (7).

(Some incidents appear in more than one line because several of them have two answers; the verdict column is the one that counts.)

### 5.1 Why Bungie is the strongest case, and what weakens it

**For:** the harm was a *valid-looking but wrong value* written by buggy application code on a subset of servers. No constraint can say a quantity is wrong. A counter on writes does not move. Players were the detector, and the repair chosen was a full rollback of every account, on the stated reasoning that finding the affected accounts risked missing some. A change log with before/after values and a transaction or server identifier is exactly the missing tool for "undo only what the bad servers wrote". It is the same shape as the bench's `repair-from-stream` scenario: a bad writer on part of the data while good writers continue.

**Against:**
- The postmortem does not say which database Bungie used, so I do not know if logical decoding with before-images was available.
- Writing the detection rule needs game-domain knowledge ("quantity dropped without a matching spend"). A false-positive rate in a live game with legitimate spends is unknown.
- Their deeper failure was process: the first bug was seen in testing and dismissed; the config fix silently did not apply. Better release verification would have prevented it before data was ever written.
- Roll-forward repair across accounts that kept playing may have conflicts that need a human (the bench stream repair flagged 50 true conflicts and left them for a person).

### 5.2 Why Matrix is a mixed case

The stream would have carried the deleted rows. But the signal that something was wrong was a user error message, and the *prevention* (a constraint) was the thing that caught the job in the act. The honest comparison is: constraint wins on detection and on stopping further damage; stream-based victim listing would plausibly have shortened the roughly week-long reconstruction, **if** the stream had been running for years with every table, which is an expensive assumption (see `STATUS.md` sub-question 5 on the cost of `REPLICA IDENTITY FULL`).

Another honest note: the corrupt index could also have corrupted what logical decoding reads. Logical decoding reads the WAL, not the index, so the delete events would still name the heap tuples actually removed. I reasoned this from how PostgreSQL works, not from the postmortem, and it is unverified here.

### 5.3 What the pattern says about the project's "reaction" half

The postmortems show reaction speed mattering in two places the content policy does not drive:
- **Travis:** about 30 minutes of an application serving an empty database created the identity collisions that lasted a day. A "stop the writers" reaction triggered within seconds would have avoided that. But any trigger (a row-count drop, a truncate event, an error-rate alert) could fire that reaction. The value is the reaction, not the CDC content.
- **Twilio:** billing stayed on for 88 minutes after the data was lost. A reaction that halts side effects when the invariant breaks is the fix Twilio proposed (fail-safe plus real-time validation against a second store), and it is not CDC-specific.

This suggests the project's second pillar (a Kubernetes reaction) has support in the sample, but it is not obviously tied to CDC. It could weaken the "CDC specifically" story while supporting "a data-aware reaction".

## 6. Conclusions

1. **What kinds of incidents.** Four recurring classes: (A) wrong-target or total deletions by humans, scripts and tests; (B) buggy code or a latent fault writing wrong values or removing wrong rows; (C) failover, replication or config losing or diverging committed data; (D) data tied to the wrong tenant or identity. Class A is the most common in this sample (6 of 16).
2. **How detected.** Mostly by people outside the engineering team or by symptom alerts. No content-aware detection in any case. Silent wrong data lived the longest: Matrix (years latent, weeks of symptoms), Kickstarter (up to over a month), Turso (3 days), Razorpay (a day to notice lost data).
3. **How repaired.** Whole-database or whole-population restore from backup was the default (8 of 16). Repairs that were precise (Matrix victim diff, Razorpay binlog forensics, GitHub 2018 binlog reconciliation, Twilio second-store restore) all needed a second source of truth and days of manual work. That second source (a log or ledger) is exactly what an event stream could provide, but the three binlog-based repairs show the database's native log already served that role.
4. **Where CDC adds something.** Only where (i) the bad value is valid-looking, so no constraint applies, **and** (ii) the bad change touches a subset while good writes continue, so a backup restore wastes work. Public postmortems show this twice (Bungie, Matrix repair), not many times.
5. **R9 status from this source.** Not met under a strict reading, marginally met under a generous one. I recommend "one clear, one partial, both unproven" in `STATUS.md`.
6. **Also worth stating in `STATUS.md`:** the strongest competitors in the sample are not exotic. Revoked permissions, soft deletes, environment isolation, constraints added after the incident, continuous backup with WAL archiving, and counters for mass-deletes beat or matched CDC in most cases.

## 7. Recommendations (nothing implemented)

1. **Update `STATUS.md` sub-question 1** from "sin hacer" to "hecho, resultado: 1 caso claro, 1 parcial", linking this file, and keep R9 listed as "not met, not refuted". Do not name more than the two cases above as "wins".
2. **Add the sample-bias caveat** (public only, loud-event skew, hand-picked) next to any statement derived from this file.
3. **Turn the two cases into bench scenarios** instead of citing them as evidence:
   - Bungie-shaped: a subset writer (identified by `txId` or client label) writes a valid-looking wrong value; compare stream repair, PITR clone swap, and full restore. This is a variant of the existing `bench/experiments/repair-from-stream` and `repair-vs-pitr`.
   - Matrix-shaped: confirm on PostgreSQL that logical decoding delete events list the rows actually removed when a statement's index path is wrong (could be approximated by a deliberately inconsistent plan or by deleting through a corrupted copy). Today this is only my reasoning.
4. **Search more for class B and the "subset-bad, good-writes-continue" shape**, since that is the only shape where the project beats the alternatives. Useful queries: "rolled back all accounts", "restored from backup and lost N hours of", "re-ran the job after fixing", "batch job overwrote", "bad deploy wrote incorrect". Sources I did not get to: Stripe, Shopify, HashiCorp, Honeycomb data-corruption reviews, GitHub availability reports month by month, incident.io write-ups, status-page postmortems, the unread danluu entries listed in section 4.
5. **Use the R8 conversations to ask the discriminating question** that public postmortems cannot answer: "In your last bad data change, was it a whole table or a subset, and did good writes keep arriving while you fixed it? How did you find out?" That separates the Bungie-shaped case from the GitLab-shaped one, and it goes into `r8-capture-sheet.md` Part B if the team wants it.
6. **Reconsider the pitch wording.** The sample supports "a data-aware reaction" and "precise repair of a subset" more than it supports "CDC detects incidents". Detection by content showed up only once (Bungie) and, in several cases, a counter or constraint did the detecting.
7. **Do not count Twilio, GitLab, Turso, OpenAI, Azure DevOps** as support for the project in any talk. They are data incidents but CDC does not address them, and an informed audience will point that out.

## 8. Sources

Company-published [P] (read):
- GitLab, "Postmortem of database outage of January 31": https://about.gitlab.com/blog/postmortem-of-database-outage-of-january-31/
- Matrix.org / Element, "How we discovered, and recovered from, Postgres corruption on the matrix.org homeserver": https://matrix.org/blog/2025/07/postgres-corruption-postmortem/
- Atlassian, "Post-Incident Review on the Atlassian April 2022 outage": https://www.atlassian.com/engineering/post-incident-review-april-2022-outage
- GitHub, "October 21 post-incident analysis": https://github.blog/news-insights/company-news/oct21-post-incident-analysis/
- GitHub, "GitHub availability this week" (Sept 2012): https://github.blog/news-insights/the-library/github-availability-this-week/
- GitHub, "Today's Outage" (CI ran against production): https://github.blog/news-insights/today-s-outage/
- Twilio, "Billing Incident Post-Mortem": https://www.twilio.com/en-us/blog/company/communications/billing-incident-post-mortem-breakdown-analysis-and-root-cause-html
- Bungie, "Destiny 2 Outage and Rollback" (Wayback copy): https://web.archive.org/web/20230504222953/https://www.bungie.net/en/News/Article/48723
- Travis CI, "Major outage on travis-ci.com": https://www.traviscistatus.com/incidents/z2b3lz2kwcfp
- Keepthescore, "We deleted the production database by accident" (Wayback copy): https://web.archive.org/web/20201101133510/https://keepthescore.co/blog/posts/deleting_the_production_database/
- Razorpay, "The Day of the RDS Multi-AZ Failover" (Wayback copy): https://web.archive.org/web/20250207075402/https://razorpay.com/blog/day-of-rds-multi-az-failover/
- Turso, "Incident 2023-12-04: Data leak and loss in some free tier databases": https://turso.tech/blog/incident-2023-12-04-data-leak-and-loss-in-some-free-tier-databases-7cba5bc7
- OpenAI, "March 20 ChatGPT outage: Here's what happened": https://openai.com/index/march-20-chatgpt-outage/
- Kickstarter Engineering, "The Day the Replication Died" (Wayback copy): https://web.archive.org/web/20170728131458/https://kickstarter.engineering/the-day-the-replication-died-e543ba45f262

Aggregator or press [A] (verify against original):
- The Register on Azure DevOps South Brazil (quotes Microsoft's engineering manager; Microsoft original not retrieved): https://www.theregister.com/software/2023/06/03/typo-blamed-for-microsoft-azure-devops-outage-in-brazil/508301
- Salesforce / Pardot 2019, press only: https://www.theregister.com/software/2019/05/17/salesforce-salesfarce-cloud-giant-in-multi-hour-meltdown-after-database-blunder-grants-users-access-to-all-data/591002 and https://www.computerweekly.com/news/252464214/Salesforce-completes-Pardot-fix-after-12-days-disruption
- Index used for discovery only: https://github.com/danluu/post-mortems/blob/master/README.md
- Cloudflare 18 Nov 2025 (snippet only, excluded): https://blog.cloudflare.com/18-november-2025-outage

Project files consulted: `docs/research/STATUS.md` (R9 criterion, sub-question 1), `docs/research/r8-capture-sheet.md`, `bench/experiments/FINDINGS.md`, `bench/experiments/repair-vs-pitr/RESULTS.md`, `bench/experiments/repair-from-stream/RESULTS.md`.

Content was rephrased for compliance with licensing restrictions.
