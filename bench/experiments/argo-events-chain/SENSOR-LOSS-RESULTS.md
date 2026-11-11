# Why the Argo Events reaction lost the event when the Sensor pod was replaced, and whether configuration can fix it (P5)

Follow-up to `RESULTS.md` in this directory, which reported "Sensor pod deleted, forbidden update right after: not suspended within 120 s, 2 of 2" and said the mechanism was not diagnosed.

- **Setup:** the same chain as `RESULTS.md`. kind cluster, Argo Events v1.9.11, EventBus `spec.nats.native` with 3 replicas. Step 0 of this study found that this bus is NATS Streaming (STAN) 0.22.1 in raft cluster mode, not JetStream. The Sensor uses a durable queue subscription (`ack_wait: 1`, `max_inflight: 3`, manual ack).
- **Question 1:** what happens to the event, with evidence from the bus, the Sensor logs and the source?
- **Question 2:** can it be fixed from the Argo Events / NATS side (an ack-wait or redelivery setting), or is it a gap that the reaction itself would have to cover (ADR-016, guaranteed delivery)?
- **Run dates:** session 1 on 2026-11-10 (35 runs), session 2 on 2026-11-11 (K, M, X, L, Q and JS runs, plus 160 ghost-rate trials). Single machine, single-node kind, Kafka and Argo Events pinned as in `up.sh`.
- **Where the numbers come from:** the working reports `findings1.md` and `findings2.md` and the raw evidence live in `.agents/tasks/argo-sensor-loss/` (git-ignored, about 120 run directories). This document is built from them. A compact excerpt is committed in `evidence-sensor-loss/` (index at the end). I re-checked the figures that carry the verdict against the raw evidence while writing this; where a figure is only from the working reports, it says so.

## Answer

**When a Sensor process dies without closing its subscription, the bus hands the next event to that dead process (it still counts as a queue-group member) and holds the event until the bus's heartbeat check removes the member.** With stock settings that took 301 to 326 s after the kill, in 10 of 10 SIGKILL runs on the default bus. On a bus with shortened heartbeats the release came at 12.7 to 14.5 s (5 of 5), and in all 15 of these forced-ghost runs the release came within -0.1 to +0.3 s of the bus's `Timed out on heartbeats` line for the dead client. A check at 120 s, as in `RESULTS.md`, sees this as "lost". Delayed is not the same as erased: a newer event that triggers in the meantime makes it a real loss, because the late event is then acknowledged and dropped without a trigger. That was tested only on the short-heartbeat bus (ghost lasting about 13 s, newer event 6 s after the kill): 6 of 6 dropped, against 5 of 5 controls that triggered. At the stock 5-minute window the same drop is expected from the source but was not run.

**How often this happens on a graceful replacement is a separate, smaller number.** On clean baselines (no ghost carried over from a previous run) a graceful delete followed by an event lost 0 of 5 (X3) and 1 of 10 (X4) on the default bus, and 0 of 8 with lease election (L-X3). The 7 of 14 in session 1 includes ghosts carried over from earlier runs and is not a per-run rate. A graceful stop skips the unsubscribe in 10 of 60 pods (17%, counted from a missing log line); a SIGKILL, OOM kill or node loss leaves the ghost every time. The original "2 of 2 lost" in `RESULTS.md` is not explained by a rate; it is consistent with a ghost carried over from an earlier operation, which I cannot confirm because those runs were not instrumented.

**It is not fixable by an ack-wait or redelivery setting on the native bus.** The Sensor's `AckWait` is a literal in the code (1 s) and did not release the event for 5 minutes. In cluster mode the server redelivers to the same queue member, by the source comment, and the native EventBus is always clustered. Two configuration changes reduce the problem and neither closes it. A JetStream EventBus delivered in the informative protocol (SIGKILL, JS-K1) in 5 of 5 runs; its graceful-delete protocol (JS-S) cannot discriminate, because the STAN equivalent also delivered 5 of 5 (X3, 0 of 5 lost). JetStream is a candidate, not a result.

**Consequence for ADR-016:** on this native-bus path, delivery across a Sensor pod replacement cannot be assumed, and I found no setting on the native bus that restores the guarantee. One untested-where-it-matters alternative exists (JetStream), and no reaction-side guarantee was built or measured. This supports the premise "guaranteed delivery has to exist before any content policy" for this Argo Events chain only. It is evidence about this chain, not a decision about ADR-016.

**Status in one line.** The mechanism chain is confirmed from forced-ghost runs and two interventions. The pre-registered "found" rule (one mechanism supported in at least 4 of 5 S-delete runs) was **not** met, and a pre-registered tripwire fired (see "Against the pre-registered decision rule"). Read the claims below with that in mind.

Mechanism status, split by link, because the links do not have the same strength:

| Link | Status |
|---|---|
| 1. A Sensor process that exits without closing its subscription stays on the bus as a live member | **confirmed** for SIGKILL (observed on the bus, 10 of 10). For graceful stops the ghost is counted from a missing log line (10 of 60 in session 2, 9 of 30 in session 1); the bus-side counts for session 2 are 4, 9, 2, 5 and 0 per group (13 of 60 for default pooled) and are false-positive-prone, so this part is observed in logs |
| 1b. Why a graceful stop sometimes skips the unsubscribe (race at process exit) | **supported**, not observed. Source reading, log shape, exit code 0, and an intervention (0 of 60 with another elector) |
| 2. The dead member is removed only by the heartbeat check, 301 to 326 s by default | **confirmed**, with an intervention: shortening the heartbeat moved the time to 12.7 to 14.5 s (release) / 12.8 to 14.7 s (bus log line) |
| 3. The pending event stays on the dead member while a live member waits with nothing pending | **confirmed** (observed). The reason (clustered redelivery rule) is **supported** by source |
| 4. Which member gets the event (fewest pending, ties to the first) | **supported**, one prediction tested (K2) |
| 5. A late redelivered event is dropped if a newer event already triggered | outcome **confirmed** only on the short-heartbeat bus (ghost about 13 s, newer event 6 s after the kill); not run at stock settings. Reason **supported** (source, debug line not seen) |

## Hypotheses, written before the runs

Copied from `prereg.md` (the original is git-ignored; a verbatim copy with its SHA-256 is now committed as `evidence-sensor-loss/prereg.md` and `prereg.SHA256`). A commit cannot prove the order of writing; "written before the runs" is the author's record, checkable only through the dated section markers inside the file. The text below is a condensed reproduction. Support rule: a mechanism is **supported** if its criterion passes in at least 4 of 5 sensor-replacement runs and the controls behave as predicted; **falsified** if the falsifying condition holds in at least 2 of 5 runs with a clean timeline. The diagnosis is **found** when one mechanism is supported in at least 4 of 5 S-delete runs. If S-delete delivers in 4 or more of 5 runs, the diagnosis becomes "loss is intermittent".

| ID | Hypothesis | Pass | Falsified if |
|---|---|---|---|
| M1 | The old pod received and acked E, then died before the trigger ran | Ack timestamped before container exit and no trigger-success line anywhere | E unacked on the bus after exit, or never delivered to the old subscription, or a trigger line exists |
| M2 | E delivered to the old pod, not acked, redelivery too slow or never | E pending at t0+120 s, or redelivered at a time within 10% of the configured ack-wait or heartbeat timeout | E acked by the old subscription, or never delivered to any subscription, or triggered within 5 s |
| M3 | E never reaches a subscriber (durable removed, start position after E, not retained) | E's sequence is above the subscription's last-sent and the new subscription starts after E | Any Sensor subscription is recorded as receiving E |
| M4 | State lost with the old pod; the new pod receives E and does not fire | New pod log shows E received and not triggered, with a reason | New pod never received E, or received and triggered |
| M5a | Harness problem (order or duplicate messages) | Kafka order or content differs from what the test assumed | Exactly one forbidden message per run, in order |
| M5b | Trigger ran and failed | Trigger attempt with an error for E | No trigger attempt for E |
| M5c | The bus lost E (restart, leader change) | Bus restart or leader change in the window and E missing | E present in the channel |
| M5d | Publish not confirmed | Publish error or missing confirmation | Publish confirmed and E present |
| M5e | A dead member stays in the queue group and the bus routes E to it | Bus lists the old connection as a member after exit and E goes to it | not stated as a separate threshold, same 2-of-5 rule |

Designs and controls, also pre-registered: S-delete (target 8, minimum 5), a timing sweep at delays 0, 1 and 3 s, controls C1 to C6 (event before, event well after, scale 0/1, EventSource delete, no restart, fresh Sensor). A fix counts as **fixed** only with 0 losses and 0 delays above 30 s in at least 5 runs, with a baseline in the same session that lost at least 3 of 5. If the baseline loses fewer than 3 of 5, the fix test is "inconclusive".

**Added after seeing data (disclosed, not pre-registered).** Written into `prereg.md` as appended sections, the second one before the session-2 runs it describes:
- M5f: a Sensor pod exits without unsubscribing, leaving a ghost member (suggested by 9 of 30 pods in session 1).
- M5g: a redelivered E is acked and dropped when a newer event already triggered on that pod.
- M5h: the process exits before `sub.Close()` reaches the bus (the reason behind M5f).
- Session-2 experiments, each with a prediction written before it ran: K1 (SIGKILL, E at 0 s), K2 (E after the ghost missed a heartbeat), X3 (clean graceful baseline), X4 (E in flight at SIGTERM), G ghost-rate trials (default, rollout restart, `preStop`, Kubernetes-lease election), the M5g test (M5G and control M5GC), fix candidates F1 (short heartbeats) and F2 (JetStream).
- Amendments during session 2, all in `prereg.md`: the `--grace-period=0 --force` premise was wrong (it is not a SIGKILL), so X1/X2 became K1/K2 (`kill -9` of the container from the node); one run was invalid; the kubelet restart back-off forced a second M5G attempt.
- **Not in the amendments list, found in review: the short-heartbeat parameters changed.** `prereg.md` fixed F1/M5G at `--hb_interval 2s --hb_timeout 1s --hb_fail_count 2` and predicted "delivered but late, a few seconds to 10 s". The patch actually applied (`fix-bus-heartbeat.patch.json`) is `3s / 2s / 4`, and the release came at 12.7 to 14.5 s, so **the timing prediction was missed**. I do not know from the record why the values changed. The outcome (late delivery tied to the removal of the dead client) does not depend on the choice, but the departure should have been logged when it happened.
- **Other predictions that missed.** G2 predicted a G0 skip rate "near 30%"; observed 17% (10 of 60). M2's "within 10% of the configured timeout" check: see the M2 row below, it fails for the short setting.

## What we saw

### Session 1: natural protocol, 35 runs

| Condition | Delivered | Not delivered in the window | Note |
|---|---|---|---|
| S-delete d=1 (5, main group) | 3 | 2 | later triggers at +260 s and +158 s |
| Timing sweep d=0 / d=1 / d=3 | 0/3, 2/3, 2/3 | 3, 1, 1 | the window alone does not decide |
| C1 event before, event after | 5 of 5 | 0 | |
| C2 wait until Ready, 15 s, E | 2 | 3 | no window, still lost |
| C3 scale 0, E, scale 1 | 0 | 3 | scale to zero also loses |
| C4 EventSource delete | late by the EventSource restart | 0 | 6.9 to 8.5 s |
| C5 no restart, C6 fresh Sensor | 3 of 3, 1 of 1 | 0 | 0.7 to 1.0 s |
| Long wait (35-LONG-1, the pre-registered extra check of M2) | 1 | 0 | delivered in 0.97 s; no ghost was involved, so **it tested nothing** about the long wait |

Rows sum to 35 runs: 22 delivered (counting the 3 EventSource-restart runs, delivered late), 13 not delivered.

In 13 of 13 runs where E was not delivered, E had been handed to a member whose pod was already dead; in the other 22 of 22 it was handed to a live pod (`tables/per-run-classification.txt`). Of those 13, 7 involved the ghost of the pod this run had just replaced, and 6 a ghost left by an earlier run (a ghost lives about 5.5 minutes, longer than a run). Later triggers for E came at +121 to +500 s in 10 of them; in 3 (runs 14, 28, 34) none was observed.

S-delete lost 2 of 5 in the main group and 7 of 14 across the S-delete runs that had a 120 s window (8 of 15 delivered counting the long-wait run; exact 95% interval for 7 of 14 about 23% to 77%). The runs are not independent, because ghosts carried over (disclosed in "Deviations"). Compare the clean-baseline graceful protocols of session 2: X3 0 of 5, X4 1 of 10, L-X3 0 of 8 (1 of 15 lost on the default bus).

### Session 2: protocols that control the ghost

| Code | What | n | Result |
|---|---|---|---|
| K1 | SIGKILL of the Sensor container, E at 0 s, default bus | 5 | 5 of 5 late, 301.1 to 317.1 s |
| K1 + lease | same with Kubernetes-lease leader election | 5 | 5 of 5 late, 301.0 to 326.3 s |
| K2 | SIGKILL, restarted container subscribed, +60 s, then E | 5 | 5 of 5 delivered, 0.42 to 1.01 s |
| X3 | graceful delete, E 1 s later, clean baseline | 5 | 5 of 5 delivered, 0.70 to 1.03 s |
| X4 | E first, graceful delete 0.3 to 0.9 s later | 10 | 9 delivered, 1 lost with a ghost (Q-X4-8) |
| G0/G0b | 60 graceful deletes, no events: did the pod unsubscribe | 60 | 10 did not (17%, exact 95% CI 8 to 29%) |
| G3 | 20 `rollout restart` | 20 | 2 did not |
| G2 | `preStop` sleep 10 + grace 60 | 20 | 3 did not (vs 10 of 60: Fisher p = 1) |
| G1/G1b | Kubernetes-lease election | 60 | 0 did not (CI 0 to 6%; vs 10 of 60: Fisher p = 0.001) |
| M5G | SIGKILL, E at 0 s, newer E2 at +6 s, **3 s / 2 s / 4 heartbeat bus (ghost about 13 s)** | 6 | E never triggered in 60 s: 6 of 6 |
| M5GC | same without E2 (also the F1 fix test) | 5 | E triggered 12.7 to 14.5 s after the kill: 5 of 5 |
| JS-K1 / JS-S / JS-C5 | SIGKILL / graceful delete / no restart on a JetStream bus | 5 / 5 / 3 | 5 of 5 (1.26 to 2.55 s) / 5 of 5 (0.72 to 1.30 s) / 3 of 3. Only JS-K1 discriminates; JS-S has a STAN twin (X3) that also delivered 5 of 5 |

Why the natural protocol is not the fix baseline: the clean graceful protocol (X3) lost 0 of 5 and X4 lost 1 of 10. By the pre-registered rule (baseline must lose at least 3 of 5) it leaves no room to show a fix, so fix candidates are compared on K1 (a certain ghost) and on the ghost rate G. This is a limit of the study: the fix results are for the forced-ghost protocol, not for the natural 17% race.

### The chain, with the key lines (`evidence-sensor-loss/Y01-K1-1/`)

Run Y01-K1-1, default bus, times 2026-11-11 UTC. Bus samples are from bus node 1 (`bus-subscription-states.txt`, `bus-heartbeat-timeouts.txt`), the node whose log carries the heartbeat line:

```
00:57:25.870  SIGKILL of the Sensor container (client -50)
00:57:26.051  E issued (payments id 300, Kafka offset 109, bus seq 110)
00:57:27.196  bus: -50  online, last_sent 110, pending 1        (only member)
00:57:41.931  bus: -50  online, last_sent 110, pending 1
              bus: -68  online, last_sent 0,   pending 0        (restarted container joins)
   ...        no change in the sampled state until:
01:02:37.348  bus log: [ERR] STREAM: [Client:client-4092367313-50] Timed out on heartbeats
01:02:37.756  Job payments-writer observed suspended (E + 311.71 s)
01:02:38.239  bus: -68  last_sent 110, pending 0
```

- Dead member and live member coexisted for about 217 to 313 s in the 10 K1 / K1+lease runs (Y01 alone: about 296 s; range recomputed by the reviewer from the raw bus samples). In 9 of 10 the dead member held `pending 1` and the live one `pending 0` in every sample. In the tenth (L-K1-1) the live member joined 0.2 s before the hand-over, both had `pending 0`, and the event went to the dead member (a tie, first in the list); the reviewer's re-check found `pending 1` / `0` throughout from the hand-over, so "9 of 10" is conservative. That is consistent with the routing rule and is not a separate result.
- The release came with the removal of the dead client. The Job was observed suspended within about 0.5 s of the bus log line in all 15 forced-ghost runs (10 default-heartbeat runs at 301 to 326 s, 5 short-heartbeat runs at 12.7 to 14.5 s; `findings2.md` reports 0.3 s; the Job is polled, and I re-checked Y01-K1-1 only: 0.41 s). For 4 of the 10 default-heartbeat runs (Y01, Y04, Y12, L-K1-5) the evidence for "E triggered" is the Job change plus the bus state, because the log follower had stopped at the kill and did not capture the restarted container; the other 6 have a Sensor `Successfully processed trigger` line for E.
- The heartbeat check is the only thing that released the event. With 3 s / 2 s / 4 misses the same chain ended 12.7 to 14.5 s after the kill (bus log line at 12.8 to 14.7 s; M5GC, 5 of 5). The time moved with the setting.
- Graceful exit, ghost vs clean (`G0-default/exit-path-clean-vs-ghost.txt`): the clean pod logs `exiting, unsubscribing and closing connection...` and the bus drops the member within about 1 s. The ghost pod's log ends at `Shutting down...` and `exiting...`, with container exit code 0, and its member stays online until the heartbeat check.
- Late drop (`M-M5G-1/`, short-heartbeat bus, ghost about 13 s; not run at stock settings): E (offset 215, bus seq 216) goes to the dead member. E2 (offset 216) is issued 5.6 s later and triggers on the live pod. At 04:09:04.355 the bus logs `Timed out on heartbeats` for the dead client and the pending message is cleared. No `Successfully processed trigger` line for offset 215 exists in any Sensor log of the run, and the Job is not suspended again through 04:09:51.9. The same in 6 of 6 runs. The control without E2 (`M-M5GC-1/`) triggers E 13.0 s after the kill.

### Mechanism in source terms (read, not run)

Source lines are from Argo Events v1.9.11 and STAN 0.22.1 as cited in the working reports; I did not re-read the source for this document.

- On SIGTERM the elector goroutine returns and the process exits (`pkg/shared/leaderelection/leaderelection.go:195-197`, `pkg/sensors/listener.go`), while the subscription is closed in another goroutine that nothing waits for (`pkg/eventbus/stan/sensor/trigger_conn.go:144-151`).
- STAN keeps a client until its heartbeat check fails. Defaults are 30 s interval, 10 s timeout, fail count 10 (`server.go:68-73`). The Argo Events config template writes none of them and the EventBus CRD has no field for them.
- `performAckExpirationRedelivery`: in cluster mode the server always redelivers to the same queue member (`server.go:3721-3724`, guard `!isClustered` at 3774). When the member is removed, its pending messages move to the others with an expiry in the past, so redelivery is immediate (`server.go:~1173`).
- `trigger_conn.go:207-213`: a message whose bus timestamp is at or before the last trigger time is acked, cached and dropped at debug level.

## Verdict per hypothesis

| ID | Verdict | Why |
|---|---|---|
| M1 old pod acked then died | **falsified** as the cause of the losses; the in-flight sub-case was not produced | In all 13 non-delivered session-1 runs and the 10+5 forced-ghost runs, E was handed to a member whose process was dead or exiting (the classifier label for runs 06, 14 and 22 is "exiting/exited before delivery", shutdown 3.6 to 0.0 s before; "already dead" would be stronger than that). Falsification rests on the pre-registered criterion: E stayed unacked on the bus after the exit. E in flight at SIGTERM (X4): the six runs where the terminating pod handled E all triggered before the delete command; the ghost in Q-X4-8 came from the exit race, not from an ack |
| M2 delivered, unacked, redelivery too slow | **confirmed**, as the held-by-a-dead-member chain | The pre-registered pass is "E pending at t0+120 s **or** redelivered within 10% of the configured timeout". The first branch holds: `pending 1` on the dead member with a live member at `pending 0` for about 217 to 313 s in the 10 default-heartbeat runs, and release at the heartbeat removal (15 of 15 forced-ghost runs), not at the 1 s ack-wait. The 10% branch is weak and I do not rely on it: defaults imply roughly 300 to 330 s by my arithmetic, observed 301 to 326 s (inside); the short setting implies roughly 15 s, observed 12.7 to 14.5 s, which is **outside 10% in 4 of 5 runs** (only 14.5 s is inside 13.5 to 16.5). The 35-LONG-1 extra check did not test the long wait (no ghost) |
| M3 E never reaches a subscriber | **falsified** | A Sensor subscription received E in 35 of 35 session-1 runs; same durable name in all pods; E retained in the channel |
| M4 new pod skips E because of state | **falsified** for the main loss. M5g is a different effect that does drop E | The new pod never received E in the non-delivered session-1 runs; the fresh-Sensor control delivered |
| M5a harness | **falsified** | One forbidden Kafka message per id in every run |
| M5b trigger failed | **falsified** | No failed trigger attempt in any Sensor log |
| M5c bus lost E | **falsified** | No bus restart or leader change after startup; E present in the channel |
| M5d publish not confirmed | **falsified** | `Succeeded to publish an event` in 35 of 35 session-1 runs |
| M5e dead member gets E | **confirmed** | session 1: 13 of 13 non-delivered runs went to a dead or exiting member, 22 of 22 delivered went to a live one; session 2: 10 of 10 default SIGKILL runs, 5 of 5 on the short bus, 6 of 6 M5G |
| M5f exit without unsubscribe | **supported** | rates above; reason not observed |
| M5g late drop | outcome **confirmed** only on the 3 s / 2 s / 4 bus with E2 issued 6 s after the kill (6 of 6 vs 5 of 5); reason **supported** | The sole difference between the two sets is E2. Stock-settings case not run |
| Replacement window alone | **not a cause** | C2, C3 and K2 show that a ghost present without any window decides the outcome; the window matters only because a ghost can be the sole member at that moment |

**Against the pre-registered decision rule.** "Found" required one mechanism supported in at least 4 of 5 S-delete runs. The natural S-delete group lost only 2 of 5 (and X3 on a clean baseline 0 of 5), so by the letter the rule is not met, and the pre-registered fallback applies: **loss under graceful replacement is intermittent**. What carries the claim is different evidence: the complete separation in session 1 (13 of 13 non-delivered runs went to a dead member, 22 of 22 delivered went to a live one), the forced-ghost protocol (10 of 10, plus 11 more with the short-heartbeat bus), and the two interventions (heartbeat setting moves the time; the elector change removes the graceful ghosts). I call the chain confirmed on that basis and say so here, not on the S-delete count. This is a substitution of evidence, not the pre-registered route to "found"; a reader who holds me to the letter should read the status as "mechanism confirmed under forced ghosts, natural loss intermittent".

**The pre-registered tripwire fired.** `prereg.md` says that if C1, C2, C3 or C5 show any loss, "the chain has a broader problem and the Sensor-replacement result cannot be attributed to the replacement window", and the report must say so. C2 lost 3 of 5 and C3 lost 3 of 3. `findings1.md` explains them (ghosts carried over from earlier runs, so the ghost is what decided the outcome and the replacement window did not), which is why I write "replacement window alone: not a cause". The mechanism work was restarted from there in session 2, as the pre-registration requires. The consequence is that the loss in `RESULTS.md` should not be described as tied to pod replacement: it is tied to a dead process being a group member, and replacement is one way (a common one) of creating that.

The first observation in `RESULTS.md` (2 of 2 lost) is consistent with a carried-over ghost, but its runs were not instrumented, so I cannot say which ghost it was.

## Can it be fixed from the Argo Events / NATS side?

| Candidate | Evidence | Verdict |
|---|---|---|
| Sensor `AckWait` / bus ack-wait / redelivery setting | `AckWait(1s)` is a literal (`trigger_conn.go:95-101`); no CRD field or env var found. Observed: a 1 s ack-wait did not release the event for about 300 s. Cluster mode redelivers to the same member, and the native bus is forced to 3 or more replicas | **Not fixable by a setting on the native bus.** A non-clustered STAN (not a supported shape here) was not tested |
| `preStop` sleep 10 s, grace period 60 s | G2: 3 of 20 ghosts vs 10 of 60 | No effect: the race happens after SIGTERM. Irrelevant to SIGKILL |
| Kubernetes-lease leader election (`LEADER_ELECTION=k8s`) | G1: 0 of 60 ghosts. L-X3: 8 of 8 delivered. **L-K1: 5 of 5 still late 301 to 326 s** | Removes the graceful-stop ghost in this setup. Does not cover SIGKILL, OOM kill or node loss. Side effects: every Sensor stop exits with code 1 (`leader lost`), and Lease RBAC is needed |
| Shorter STAN heartbeats (3 s / 2 s / 4; the pre-registered setting was 2 s / 1 s / 2, see "Added after seeing data") | M5GC: ghost removed in 12.8 to 14.7 s, 5 of 5 delivered late (about 13 s). C5: 0.41 to 0.71 s, one trigger per E | Meets "fixed" for K1 only as "delivers late". **Does not prevent the late drop** (M5G 6 of 6 lost E on the same bus). No CRD field: applied as extra server flags on the StatefulSet, outside the supported spec. A 2 s timeout may evict live clients under load (not measured) |
| JetStream EventBus (`spec.jetstream`, 3 replicas, own namespace) | JS-K1 5 of 5 (informative), JS-S 5 of 5 (non-discriminating: STAN X3 also 0 of 5 lost), JS-C5 3 of 3. No group membership by connection | The only candidate with 0 losses and 0 delays above 30 s in the forced-ghost protocol, on 5 informative runs. **Promising, not proven:** the late-drop case not tested, a pull request in flight at kill time not tested, and changing the bus is a design change |
| `maxUnavailable 0`, readiness gates, `replicas: 2` | not run | No mechanism-based reason to expect an effect: the ghost is not a rollout-overlap problem, and a second NATS-bus Sensor replica is leader-only |

**Answer to question 2.** No ack-wait or redelivery setting fixes it on the native bus. It is partly mitigable by configuration (two changes, neither closes the gap), and the one thing that passed in the informative protocol is a different bus (JetStream, 5 of 5 on JS-K1) whose guarantee I tested only lightly. So on the native bus it is a structural property of this delivery path (group membership tied to connection liveness, same-member redelivery in cluster mode, drop of older events), not a missing knob. Whether JetStream closes the gap is open.

## What this means for the project

- The HANDOFF condition was: "if event loss is by design, guaranteed delivery must be built before any content policy". The finding supports the premise for this path: a dead Sensor process can delay an event by about 5 minutes (10 of 10 forced kills; in natural graceful replacements it happened in roughly 1 of 15 clean runs and in 17% of pod stops as a ghost), and, if a newer event arrives first, erase it (shown with a 13 s ghost only), and no knob on the native bus changes that.
- For a Sensor where each event must produce its own reaction, the drop matters. For this Sensor it did not: the newer event had the same effect (`suspend: true`). The loss is harmless here and not harmless in general.
- This is a finding about the Argo Events chain, which is a comparison point and not our product path. It does not show that a Lactifluus reaction would lose events. What it does show is the kind of failure a guarantee has to cover: a consumer that looks present but is dead, and an older event that is acknowledged and discarded.
- A reaction-side guarantee, for example a level-triggered check of the current state that can be re-driven, or an idempotent reaction keyed on the event, follows from the mechanism. I did not build or test one. Whether to build it before content policies is the owner's call; this document supplies the evidence and not the decision.

## Deviations and invalid runs

- **Non-independent runs in session 1.** The pre-registered "all subscriptions healthy" baseline was implemented as "at least one online subscription". Ghosts survived into later runs (6 of the 13 losses). The loss counts are for that mix, not a clean per-run rate. Session 2 uses a clean baseline.
- **Forced delete is not SIGKILL.** `kubectl delete pod --grace-period=0 --force` ghosted 1 of the 6 X runs, so X1/X2 were replaced by K1/K2 (`kill -9` of the container from the node). I read the kubelet as still sending SIGTERM with a 2 s minimum grace; I did not verify that in its source.
- **Invalid runs.** `X07-X2-3`: host stall (a `psql` call took 60 s, a bus pod restarted). The first M5G/M5GC attempt (8 runs) hit the kubelet restart back-off and was replaced by a second attempt, which is the reported set. The host showed load averages of 26 to 44 at three points and the API server once timed out; runs where the bus or API stalled were excluded, others were not re-examined for host effects.
- **Late-joining live member in K1/K2.** After repeated SIGKILLs the restarted container joined 0.9 to 97 s after E in some runs. The K1 result does not depend on it; link 3 uses the time both were present.
- **Baseline and fix runs were not interleaved:** default-bus runs first, then ghost trials, lease, `preStop`, the heartbeat patch (reverted), and JetStream last.
- **Sensor log capture in K1 runs** stopped at the kill for 4 of 10 default-heartbeat runs (see above). The M5G set captured the restarted container, so the absence of a trigger line for E there is meaningful.
- **Client-id reuse.** The bus-side ghost count ("subscription still online 8 s later") has false positives because client ids are reused. The log-based count is primary.
- The ghost rate of 17% comes from graceful deletes with no events and a quiet bus. It is not the probability that an event is lost during a rollout.

## Not covered, and not claimed

- The late drop at stock heartbeat settings (E2 arriving within the 5-minute ghost window). Only the 13 s ghost was run.
- A clean per-run loss rate for a natural graceful replacement with an event in a realistic position. The clean baselines (1 of 15) are small samples.
- Other Argo Events or STAN versions, a non-clustered STAN, a larger JetStream test, JetStream under the late-redelivery case, JetStream with a pull request in flight at kill time. JetStream is not claimed to be a fix.
- Node loss, OOM kill by the kernel, bus pod loss during the replacement, upgrades by changing the Sensor spec.
- Non-idempotent triggers, and a stream of events during the replacement (one event at a time here).
- An event that reaches a live Sensor pod milliseconds before SIGTERM, mid-trigger (the in-flight case of M1). X4 did not produce it.
- That `sub.Close()`'s request is lost in the client write buffer at exit. It is inferred from the log shape, exit code 0 and the elector intervention, not observed.
- That the debug-level drop line exists in the late-drop runs. It is not logged at info level; the reading rests on the source and on the 6-vs-5 contrast.
- Whether the lease-election result (0 of 60) holds on a slower or more loaded node; the cost of short heartbeats on live clients under load; whether the EventBus controller reverts the StatefulSet patch over a longer time (it did not in about an hour).
- That the ghost is the only cause of loss in session 1. Every non-delivered run had it, but 13 cases are not proof of no other cause. Runs 14, 28 and 34 never showed a trigger for E; the late-drop reading fits them and was not shown for them.
- A reaction-side guarantee (ADR-016) was neither built nor measured. Nothing here says how it should be built.

## Evidence

Compact excerpt in `evidence-sensor-loss/` (not the raw dump of about 120 run directories):

- `Y01-K1-1/`, `M-M5G-1/`, `M-M5GC-1/`, `Q-X4-8/`: `timeline.log`, `bus-subscription-states.txt` (changed samples of one bus node), `bus-heartbeat-timeouts.txt`, `sensor-log-key-lines.txt`, `kafka-messages-for-ids.txt`.
- `G0-default/exit-path-clean-vs-ghost.txt`, `G0-default/result-line.txt`, `JS-K1-1/timeline.log`.
- `prereg.md` and `prereg.SHA256`: verbatim copy of the git-ignored pre-registration, with hash. It shows content, not authorship time.
- `tables/`: `findings2-tables.txt` (rates, K1/K2/M5G latencies, hand-over analysis for all runs), `ghost-table.txt` and `per-run-classification.txt` (session 1).
- `scripts/`: the analysis scripts that produced the tables (`analyze.py`, `analyze2.py`, `ghosts.py`, `summarize2.py`). They read the raw evidence directory, which is not committed.

Scripts that ran the experiments are in this directory: `diagnose.py`, `diagnose2.py`, `jsfix.py`, `argo-fix.yaml`, `argo-fix-jetstream.yaml`, `fix-*.patch.json`, `fix-*.revert.json`. The raw per-run directories and the pre-registration stay under `.agents/tasks/argo-sensor-loss/`.

State left in the cluster: STAN chain restored to defaults; the JetStream chain from F2 still runs in namespace `argo-js` (`kubectl delete -f argo-fix-jetstream.yaml` removes it).
