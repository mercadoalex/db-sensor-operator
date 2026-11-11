# Pre-registration: why the reaction lost the event when the Sensor pod was replaced (P5)

Written before any run of this diagnosis. Nothing here has been run or measured. Edits after the first run go in a dated "Amendments" section at the end, never in place.

## Established facts (from `bench/experiments/argo-events-chain/RESULTS.md`, `up.sh`, `argo.yaml`, `test.py`)

- Chain: Postgres -> Debezium -> Kafka -> Argo Events v1.9.11 EventSource (Kafka, consumer group `argo-events-payments`) -> EventBus -> Sensor `settled-to-pending` -> k8s patch trigger (`suspend: true` on Job `payments-writer`).
- Measured earlier: Sensor pod deleted (`kubectl delete pod -l sensor-name=settled-to-pending --wait=false`), forbidden update right after: Job not suspended within 120 s, 2 of 2. EventSource logged publishing about 0.8 s before the new Sensor pod subscribed. EventSource pod deleted: suspended after 9.4 s (1 run). Sensor scaled to 0 and back: delivered 1.8 s after scale-up (1 run).
- The EventBus is created as `spec.nats.native {replicas: 3, auth: token}` (`up.sh`). **In Argo Events v1.9.x the `nats` bus is NATS Streaming (STAN), and JetStream is the separate `spec.jetstream` bus.** The brief says JetStream; the manifest says otherwise. Step 0 below must settle which one is running before any mechanism is tested. If it is STAN, all JetStream-specific terms (consumer, ack-wait, max-deliver) are replaced by their STAN equivalents (durable subscription, queue group, AckWait, MaxInflight), and the fixability section uses those.
- `kubectl delete pod` on a Deployment-managed pod makes the ReplicaSet create a replacement immediately while the old pod is Terminating, so old and new pods overlap (no gap by construction). This is a reading of Kubernetes behaviour, to be confirmed in M5/M6 by pod timestamps.
- The delete is `--wait=false` and the forbidden update follows within about a second, so the event is published inside the replacement window by design of the test.

## Definitions used by every criterion

- **Event E**: one forbidden update (`settled` -> `pending`) on a distinct `payments.id` (a different id per repetition, so each message is separately identifiable in Kafka, EventSource log, bus and Sensor log).
- **Lost**: Job not `suspend: true` 120 s after E, and no trigger execution for E in any Sensor pod log (old pod's log captured before it is gone, see instrumentation).
- **Delayed**: suspended, but later than 5 s after E. (Baseline no-restart latency was 0.72 s.)
- **Delivered**: suspended within 5 s of E.
- t0 for each repetition is the timestamp of the `psql` call. Every timestamp is wall clock from a single host (kubectl and the test run on the same machine), pod-side timestamps are taken from container logs and Kubernetes events.

## Step 0: instrumentation that must exist before any repetition (no mechanism is judged without it)

1. Identify the bus: `kubectl -n argo-events get eventbus default -o yaml`, the StatefulSet image and args, `kubectl exec` into a bus pod for its monitoring endpoint (`:8222`, `/streaming/channelsz?subs=1` for STAN, `/jsz?consumers=true` for JetStream). Record the result in the report.
2. Stream the old Sensor pod's log to a file for the whole window with `kubectl logs -f` started before the delete (the log is lost with the pod otherwise). Same for the new pod, the EventSource pod and the bus pods. Use `--timestamps`.
3. Poll the bus monitoring endpoint every 100 ms (inside the cluster or via port-forward) from 10 s before the delete to 60 s after E: subscriptions/consumers per channel, their last-sent and last-acked sequence, pending count, redelivered count, queue-group membership and connection ids/client ids.
4. Record a timeline per repetition: delete issued, old pod `deletionTimestamp`, old container SIGTERM/exit (Kubernetes events and pod status), new pod scheduled/Ready/subscribed (Sensor log), E issued, Kafka message offset, EventSource "publishing" log line, bus sequence number of E, first delivery of that sequence to any subscriber, ack of that sequence, trigger start, trigger result, Job `suspend` change time.
5. Capture Sensor pod spec: `terminationGracePeriodSeconds`, probes, `strategy`, `replicas`, `preStop`, and the Sensor container's handling of SIGTERM as visible in its log.
6. Capture the bus-side sequence of E by publishing identifiable ids (`id` in the payload) and grepping, not by guessing the sequence number.

## Candidate mechanisms

Each has a prediction that can fail. A mechanism is **supported** only if its criterion passes in at least 4 of 5 sensor-replacement repetitions and the controls behave as it predicts; it is **falsified** if the falsifying condition holds in at least 2 of 5 repetitions with a clean timeline. More than one can be true; the report states which explain which repetitions.

### M1: old pod received and acked E, then died before the trigger ran

- Prediction: bus shows E delivered to the old pod's subscription and acked; no redelivery; no trigger log line for E in the old pod log; the new pod never receives E.
- Instrumentation: bus delivery/ack state for E (Step 0.3), old Sensor pod log (Step 0.2) showing receipt of E (or last line before exit), exit time of the old container vs ack time.
- Pass: ack of E by the old subscription is timestamped before the old container exit, and no trigger-success log line for E exists anywhere.
- Falsify: E is unacked on the bus after the old pod exit, or E was never delivered to the old subscription, or a trigger-success line for E exists.
- Distinguishing sub-question (reported either way): does the Sensor ack before or after the trigger executes? Read from the v1.9.11 source (`pkg/sensors`, eventbus driver subscribe/ack call order) and confirm against log timestamps. Source reading alone does not count as a pass.
- Repetitions: all 5 sensor-replacement runs plus the 3 timing-sweep runs below.

### M2: delivered to old pod, not acked, but redelivery is too slow or never happens

- Prediction: E is delivered to the old subscription, never acked, stays pending on the bus past 120 s (or past the Job check), the new subscription never receives it, or receives it only after a delay longer than 120 s.
- Instrumentation: pending/redelivered counters (Step 0.3), configured AckWait / redelivery / max-deliver (STAN `AckWait`, `MaxInflight`; JetStream `ack_wait`, `max_deliver`), subscription membership after the old pod exit (does the bus still list the dead connection until a heartbeat timeout).
- Pass: E pending (delivered, unacked) at t0+120 s, or redelivered to the new pod at a time within 10% of the configured ack-wait/heartbeat timeout.
- Falsify: E acked by the old subscription (M1 territory), or never delivered to any subscription (M3 territory), or redelivered and triggered within 5 s.
- Extra check: wait out the configured AckWait plus 30 s in one extra run (no 120 s cap) and record if the Job is eventually suspended. If it is, "lost" in the earlier runs was "delayed beyond 120 s", and the report says so.
- Repetitions: same 5 plus the extra long-wait run.

### M3: E never reaches a Sensor subscriber (durable subscription/consumer removed, recreated, or starting after E)

- Prediction: the bus shows no delivery of E to any Sensor subscription; the subscription's durable name or consumer is recreated or its start position is after E's sequence, or the queue group had no active member when E arrived and the bus did not retain E for it.
- Instrumentation: durable name/consumer name before, during and after the replacement; channel/stream last sequence vs the subscription's last-sent sequence; whether the new pod subscribes with "new only" or "from last received/durable resume"; bus retention config (limits, interest vs limits policy, max age) as it applies to E.
- Pass: E's sequence is greater than the subscription's last-sent sequence before the replacement, and the new subscription's first delivered sequence is greater than E's sequence (or E is not retained for it), with E present in the channel/stream.
- Falsify: any Sensor subscription is recorded as receiving E.
- Interest-based retention check: if the stream/channel uses interest retention and no subscription existed, E would be dropped; record the retention policy explicitly.
- Repetitions: 5 plus the 3 timing-sweep runs.

### M4: in-Sensor state (dependency/filter/trigger/circuit state) lost with the old pod

- Prediction: the new pod receives E but does not fire the trigger because it lacks state the old pod held (for example, a dependency's latest event, a partial multi-dependency group, or a trigger's rate/circuit state), or the new pod treats E as a duplicate.
- Instrumentation: new pod log for receipt of E and the decision (filter result, "dependency not satisfied", dedup). This sensor has one dependency, so partial state is not expected; the config shows no circuit, no `conditions`, no rate limit.
- Pass: new pod log shows E received and not triggered, with a recorded reason.
- Falsify: new pod never received E (then M4 cannot be the cause), or received E and triggered.
- Prior: low, because of the single-dependency sensor. It stays in the list because it is cheap to rule out from the same logs. Also run one non-restart control with the same E but a freshly started Sensor that had no prior events, to see whether "fresh pod" alone changes behaviour.
- Repetitions: same 5.

### M5: something else the evidence suggests (named in advance so they are tested, not appended)

- **M5a: the test harness, not the chain.** `test.py` resets with `update ... status='settled' where id=1` and then forbids on the same row; a reset may itself be mistaken for a transition, or the reset is processed after E. Check: per repetition, list the Kafka messages for the id and compare the order seen by the EventSource with the order of the updates. Pass for M5a: the message order or content differs from what the test assumed. Falsify: Kafka and EventSource logs show exactly one forbidden message per repetition, in order.
- **M5b: trigger ran and failed.** The Sensor tries the patch and gets an error (RBAC, API timeout during replacement, retry exhausted). Pass: trigger attempt logged with an error for E. Falsify: no trigger attempt logged for E.
- **M5c: the bus, not the Sensor, loses E during the replacement.** The bus has 3 replicas; if a bus pod is also affected (it is not deleted by the test, but check restarts and leader changes during the window), E may be dropped by publishing to a node without quorum. Pass: bus pod restart or leader change within the window, and E missing from the channel/stream. Falsify: E present in the channel/stream on the bus after the window.
- **M5d: EventSource publishes but the publish is not confirmed.** The EventSource logs "publishing" before the publish ack. Pass: publish error/timeout or missing publish confirmation for E in the EventSource log. Falsify: publish confirmed and E present in the channel/stream.
- **M5e: old and new Sensor pods both hold the same queue/durable identity and the bus routes E to the dead one.** This is the specific failure mode for STAN queue groups with a durable name: a dead connection stays a group member until heartbeat failure is detected. Pass: bus lists the old connection as a group member after the old container exit, and E goes to it. This overlaps M2; the report keeps them separate by whether E was delivered to the old connection (M5e/M2) or acked (M1).
- Any other cause that appears in the timelines is added as M5f, M5g, ... in an Amendment, with the date and the evidence that suggested it.

## Experimental design

### Test group (sensor replacement)

- **S-delete, 5 repetitions minimum (target 8):** same as `phase_restart("sensor")`: delete the Sensor pod with `--wait=false`, issue E at the same delay as before (about 1 s). Each repetition is on a fresh baseline: Job reset, all bus subscriptions healthy, the previous repetition's pods fully gone. Distinct `id` per repetition. Reported as a count out of N.
- Order of repetitions interleaved with controls (see below) in a fixed pre-written random order, to avoid time-of-day or accumulated-state effects. The order is written to the run log before the first run.

### Timing sweep (3 repetitions each, to locate the window)

Delete the Sensor pod and issue E at fixed delays after the delete: 0 s, 1 s, 3 s. The goal is not statistics but the shape: which delays lose, which deliver. A mechanism that predicts a window (M1, M2, M5e) must predict which of these three lose.

### Controls (each at least 3 repetitions, 5 for the first two)

- **C1: event well before replacement.** E issued, confirmed suspended, Job reset, then the Sensor pod deleted. Then E2 issued 30 s after the new pod is Ready. Expect: both delivered. Fails the experiment if E or E2 is lost (the chain is broken for another reason).
- **C2: event well after replacement.** Delete the Sensor pod, wait until the new pod is Ready and subscribed (per its log and the bus), wait 15 s, then issue E. Expect delivered within 5 s. This is the "no loss outside the window" control. 5 repetitions.
- **C3: clean scale 0/1.** Scale to 0, wait until the pod is gone and the bus shows no subscription, issue E, wait 15 s, scale to 1. Expect delivered. 3 repetitions. The earlier single run delivered at 1.8 s after scale-up.
- **C4: EventSource pod delete with E issued right after.** 3 repetitions. The earlier single run was delayed to 9.4 s; this checks if that was one-off and records its bus timeline as a comparison for M2.
- **C5: no-op baseline.** E with no restart. 3 repetitions. Latency baseline (earlier: 0.72 s, one run).
- **C6: fresh Sensor, no prior events (for M4).** 1 repetition per M4 note above.

If C1, C2, C3 or C5 show any loss, the chain has a broader problem and the Sensor-replacement result cannot be attributed to the replacement window; the report says so and the mechanism work restarts from that.

### Decision rule for the diagnosis

- The diagnosis is **found** when one mechanism (or a stated combination) is supported by its criterion in at least 4 of 5 S-delete runs, no other mechanism is supported in those same runs for the same reason, and the timeline for each run is consistent with it. Each of the 5 runs gets its own verdict row, including the ones that do not fit.
- If no mechanism passes in at least 4 of 5, the report says "not diagnosed" and lists what was ruled out, with the counts.
- A result where S-delete delivers in 4 or more of 5 runs does not match the earlier 2 of 2 loss. That would be reported as a failure to reproduce (loss rate with its count), not discarded, and the diagnosis is changed to "loss is intermittent"; the timing sweep then carries the analysis.
- With N=5, 0 losses has a 95% upper bound of about 45% on the loss rate (rule of three: 3/5 = 60% is the looser bound; the report uses the exact binomial). No claim of "never loses" is made from N=5.

## Fixability question (pre-registered)

Settings considered, each tested alone unless a combination is explicitly motivated by a supported mechanism. Which settings exist depends on Step 0.1 (STAN vs JetStream); the list gives both names.

| Candidate fix | Tied to | What it changes |
|---|---|---|
| Sensor `terminationGracePeriodSeconds` (longer) and a `preStop` hook that delays exit until in-flight triggers finish | M1, M2 | Gives the old pod time to finish the trigger and ack |
| Sensor `replicas: 2` or more, with the same queue group | M1, M2, M3 | One member is always alive; the group redelivers to the survivor |
| Deployment strategy: `maxUnavailable 0` / `maxSurge 1` with readiness gated on a bus subscription | M3, M5e | New pod subscribes before old pod leaves |
| Bus ack wait / redelivery (STAN `AckWait`; JetStream `ack_wait`) lowered | M2, M5e | Faster redelivery of unacked E |
| JetStream `max_deliver` and a backoff, or switch the bus to `spec.jetstream` | M2, M3 | Bounded, repeated redelivery; durable consumer with explicit ack |
| Sensor ack-after-trigger vs ack-before-trigger | M1 | Only if a setting exists in v1.9.11; if it needs a code change, it is reported as "not fixable by configuration" |
| EventBus replicas / persistence settings | M5c | Only if M5c is supported |

Not tested unless a mechanism asks for it: changing Argo Events version (report as "unknown for other versions"), anything that needs forking Argo Events.

### Criterion for "fixed"

- Per candidate fix: with only that fix applied, run the S-delete protocol at least 5 times (target 8, with at least the 0 s and 1 s timing points among them). **Fixed** = 0 losses and 0 delays above 30 s in N >= 5 repetitions, where the baseline (same cluster, same session, no fix) lost in the baseline runs of this study.
- Baseline must be re-measured in the same session: at least 5 S-delete runs without the fix. If the baseline loses fewer than 3 of 5 in this session, there is no room to show a fix, and the fix test is reported as "inconclusive", not as success.
- A fix that gives 0 losses in 5 but whose latency in delivered runs is above 30 s is reported as "delivers late", not "fixed".
- A fix is also checked for side effects with C5 (no-op latency within 2x of baseline, no duplicate trigger executions for one E). Duplicate triggers on a merge patch to `suspend: true` are harmless here, and the report states that this result does not transfer to non-idempotent triggers.
- A fix passes the "fixed" test only for the sensor-replacement case. It is not claimed for node loss, OOM kill, bus pod loss, or forced deletion (`--grace-period=0`). Those are listed under "Not covered" unless run.
- If the supported mechanism cannot be addressed by any configuration listed above, the answer to the fixability question is "not fixable by configuration in this version", with the setting-by-setting evidence.

## What the report must contain (house style)

Follow the other `RESULTS.md` files: question; method; run date; a "What we saw" table with counts per run; mechanism verdicts; the fixability result; a verdict section with the implication for ADR-016 (guaranteed delivery before any policy), kept to what the data support; "Not verified" or "Not covered, and not claimed". The earlier guess in `argo-events-chain/RESULTS.md` ("the terminating pod took the event and the trigger never ran") is M1 and is treated as a hypothesis here, not as a finding.

## Not covered by design (declared now)

- Other Argo Events versions, other bus types unless Step 0.1 shows JetStream is also in use, a Kafka cluster with replication, TLS, bus authentication beyond `auth: token`.
- Node-level failures, OOM kills, `--grace-period=0` deletes, and Sensor upgrades by changing the Sensor spec.
- The Kafka -> EventSource leg (consumer group offsets) except where M5a/M5d show it matters.
- Non-idempotent triggers.
- Scale: one forbidden event at a time, not a stream of them during the replacement.

---

## Added after seeing data (2026-11-10, appended; nothing above was changed)

Appended after the 35-run batch described in `findings1.md`. These were not pre-registered; they are hypotheses that the data suggested and they are labelled as such wherever they are used.

- **M5f (added after seeing data): a Sensor pod exits without unsubscribing its STAN queue member ("ghost member").** Evidence that suggested it: in 9 of 30 Sensor pod shutdowns the log stops at `Shutting down... / exiting...` and never reaches `exiting, unsubscribing and closing connection`; the bus keeps that member listed as online. E, routed to it, stays pending (see `evidence/ghost-table.txt`). This is the mechanism by which M5e's "bus routes E to the dead one" happens; M5e itself was pre-registered.
- **M5g (added after seeing data): a redelivered E is silently dropped by the receiving pod.** `trigger_conn.go` (v1.9.11) acks and drops any message whose bus timestamp is not newer than the pod's last trigger time (`lastResetTime`). Evidence that suggested it: for 3 non-delivered runs (14, 28, 34) no trigger for E was ever observed, although the ghost holding E was reaped. Not tested directly; it is listed as undecided.
- **Baseline deviation (disclosed, not a post-hoc change of criteria):** the pre-registered "fresh baseline: all bus subscriptions healthy" was implemented as "at least one online subscription", so ghost members from earlier runs persisted into later runs. The data show ghosts live about 5.3-5.8 minutes, longer than a run, so runs are not independent. `findings1.md` reports this.

---

## Added after seeing data, second session (findings2). Written before any run of this session.

Nothing above this line was changed. Everything below is not part of the original pre-registration; it was written after `findings1.md`, after reading source (listed in findings2), and before running the experiments it describes. Runs are in `evidence/X*` and `evidence/G*` directories, script `bench/experiments/argo-events-chain/diagnose2.py`.

### Source reading that motivated the experiments (read, not yet tested)

- STAN 0.22.1 `server/server.go` `performAckExpirationRedelivery`: "In cluster mode we will always redeliver to the same queue member" (lines ~3721-3724, 3774, 3797-3806). The EventBus here is clustered (3 raft peers; `native` forces >= 3 replicas in `installer/nats.go:438`). So `ack_wait: 1` should only re-send to the member that already holds the message.
- `subStore.Remove` (`server.go` ~1068-1260): when a queue member is removed and others remain, its pending messages are moved to the remaining members with an expiry in the past (immediate redelivery).
- `findBestQueueSub` (`server.go:3477`): picks the member with the fewest pending (`acksPending`), skipping stalled members and members whose client has a failed heartbeat; ties go to the first in the list, which is then rotated to the end; if all are skipped it takes the first.
- Heartbeats: defaults 30 s interval, 10 s timeout, fail count 10 (`server.go:67-73`; flags `hb_interval`, `hb_timeout`, `hb_fail_count`, `conf.go:655-660`). The Argo Events generated config (`installer/nats.go:503-531`) writes none of them and the EventBus CRD has no field for them.
- Sensor (`pkg/sensors/listener.go`, `pkg/shared/leaderelection/leaderelection.go:195-197`): on SIGTERM the elector goroutine (`natsEventBusElector.RunOrDie`) returns at `<-ctx.Done()` and `Start`/`main` return, while the subscription is closed in a different goroutine (`trigger_conn.go:144-151`, `sub.Close()` is a network round trip). Nothing waits for that goroutine. Hypothesis M5h: the process exits before `sub.Close()` reaches the bus in some fraction of exits.
- `trigger_conn.go` `processEventSourceMsg`: a message whose bus timestamp is <= `lastResetTime` (set to the timestamp of the last message that triggered) is acked, cached and dropped at Debug level only. This is M5g.
- The Sensor `AckWait(1s)` and `MaxInflight` are literals in `trigger_conn.go`; I found no CRD field or env var that changes them.

### Experiments and predictions (each can fail)

Common: every run starts only after a clean baseline: exactly one online Sensor subscription on the bus and the live Sensor pod Ready. (findings1 deviated here.)

- **X1 forced ghost, default bus (n=5).** `kubectl delete pod --grace-period=0 --force` on the Sensor pod (SIGKILL, so no unsubscribe is possible by construction), E issued 0 s later (the ghost is the only group member for about 1 s). Prediction: E is handed to the dead member in 5 of 5; no log line of the dead pod can contain E (it was dead before E existed), so M1 cannot hold; E stays pending on the dead member for at least 120 s while the live member is subscribed with pending 0; E's trigger appears only after the bus removes the dead client, 270-360 s after the kill. Falsified if E is delivered within 30 s in 2 or more runs, or if the pending count moves to the live member before the dead client is removed.
- **X2 routing after the ghost has failed a heartbeat (n=5).** Forced kill, wait for the new pod to subscribe, wait 60 s, then E. Prediction from `findBestQueueSub`: E goes to the live member and is delivered within 5 s in 5 of 5 (the ghost's client has missed a heartbeat by then). Falsified if E is handed to the dead member in 2 or more runs.
- **X3 natural baseline, clean (n=5).** Graceful `kubectl delete pod`, E 1 s later, same as S-delete, but each run starts from a clean baseline. Reported as a rate with its count. Prediction: loss rate well below findings1's 7 of 14 because ghosts from earlier runs are excluded. If fewer than 3 of 5 lose, the pre-registered rule applies: no room to show a fix with this protocol, "inconclusive".
- **G ghost-rate trials (n=20 each).** Graceful delete of the Sensor pod with no E, 20 s apart; per pod record whether the log contains `exiting, unsubscribing and closing connection` and whether the bus removes its subscription within 5 s. Configs: G0 default; G1 Sensor annotation `events.argoproj.io/leader-election: k8s` (changes the elector code path); G2 `preStop: sleep 10` plus `terminationGracePeriodSeconds: 60` on the Deployment (out-of-CRD patch). Prediction: G0 skip rate near 30%; G2 no different from G0 (the exit race happens after SIGTERM, a preStop does not touch it); G1 unknown, reported either way. No fix claim from G unless the skip rate drops to 0 of 20.
- **M5g test (n=5 + control n=5), done on a bus with short heartbeats (see F1) only to make the ghost live seconds instead of minutes; the Sensor-side drop logic does not depend on heartbeat timing.** Forced kill; E at 0 s (goes to the dead member); at about +6 s a newer event E2 is issued and triggers on the live pod (Job reset after); then the bus removes the dead client and redelivers E. Prediction: E produces no trigger (no `triggeredByEvents` containing E's offset) in 5 of 5, and the bus shows E no longer pending. Control: same without E2: E triggers within 5 s of the ghost removal in 5 of 5. Falsified if E triggers in 2 or more of the 5 E2 runs.
- **F1 fix candidate: short STAN client heartbeats.** Not available through the EventBus CRD. Applied out-of-band as additional server flags (`--hb_interval 2s --hb_timeout 1s --hb_fail_count 2`) via a patch on the bus StatefulSet, recorded as `argo-fix.yaml`/patch file in the experiment dir. Protocol: X1 (forced ghost, E at 0 s) n=5 on the patched bus, plus X1 baseline n=5 on the default bus in the same session (X1 above). "Fixed" per the original criterion (0 losses, 0 delays > 30 s). Predicted: E delivered but late (about the reap time, a few seconds to 10 s), i.e. "delivered late", not instant. A side-effect check with C5 (no restart, latency and duplicates) follows.
- **F2 fix candidate: JetStream bus** (`spec.jetstream`), in a separate namespace so the STAN chain is untouched. Run only if time allows; if it is not run, findings2 says so.
- Not testable by configuration (reported from source and from X1, not from a fix run): Sensor `AckWait` (literal), clustered redelivery to the same member (native bus cannot be non-clustered).

### Limits declared now

Forced kill (`--grace-period=0`) is declared a stress protocol: it makes the ghost certain, it is not the natural 30% race. It is used to test links of the chain, not to estimate a rate. Single kind node, one forbidden event at a time, non-idempotent triggers not covered.

### Amendments during session 2 (appended after the runs started; each one is a departure from the text above)

- **X1/X2 premise was wrong.** `kubectl delete pod --grace-period=0 --force` as I read the kubelet it still delivers SIGTERM (2 s minimum grace; not verified in source), so it is not a certain SIGKILL and does not make a ghost certain. Evidence: `X06-X1-2` (forced delete, E at 0 s) delivered in 1.9 s with the old subscription going offline immediately; only `X04-X1-1` ghosted. Those six runs (`X01`-`X06`) are kept in `evidence/` as force-delete/graceful data and are not counted in X1/X2. The real SIGKILL protocol is K1/K2: `kill -9` of the container process from the kind node (`diagnose2.py`, `sigkill_sensor_container`), after which kubelet restarts the container in the same pod. X1 -> K1 (E at 0 s), X2 -> K2 (E 60 s after the restarted container subscribed). The plan for K1/K2/X3 is `evidence/plan3.json` (written before those runs); `plan2.json` is the abandoned first plan.
- **X07-X2-3 invalid** (host stall: `psql` call took 60 s, a bus pod restarted). Moved to `evidence/invalid/`.
- **Kubelet restart back-off.** Repeated SIGKILLs of the same pod delay the next container start (10 s, 20 s, ... up to 5 min). The Y K1/K2 runs were >= 5 min apart so most were unaffected but some restarted containers subscribed 15-120 s late (see timeline logs); this does not change the K1 result (E handed to the dead member, pending until the heartbeat time-out) but it means the live member joined late in those runs. The first M5G/M5GC attempt (8 runs) hit the back-off and is moved to `evidence/invalid/M-first-attempt-restart-backoff/`. From then on `diagnose2.py` replaces the pod before every K/M5G run (fresh restartCount 0). The M5G/M5GC set reported in findings2 is the second attempt.
- **F1 protocol = M5GC.** The F1 fix test (K1 on the short-heartbeat bus, n=5) is the same procedure as M5GC (SIGKILL, E at 0 s, no E2), so the M5GC runs are reported as the F1 runs instead of repeating them.
- **G1 applied with `kubectl set env LEADER_ELECTION=k8s`** on the Sensor Deployment, not only the annotation: the controller does not re-render the Deployment when only an annotation changes (its spec hash covers the spec). The annotation was also set and later removed.
- **G2 uses the Kubernetes `sleep` preStop action** (`lifecycle.preStop.sleep`), because the Sensor image is `FROM scratch` and has no `sleep` binary for an exec hook.
- **G3 (rollout restart) and X4 (E in flight when SIGTERM arrives) were added** to cover "rollout restart vs delete" and M1 directly. G3: `kubectl rollout restart` n=20. X4: E first, graceful delete 0.30-0.90 s later, n=10.
- **Ghost counting in G trials.** The primary measure is "the pod's log has no `exiting, unsubscribing and closing connection` line". The bus check ("subscription of that client id still online 8 s later") has false positives because client ids (`client-4092367313-<n>`) are reused by later pods; both are reported.
- **Heartbeat patch persisted** on the bus StatefulSet for about 1 h with no revert by the EventBus controller; it was reverted by the opposite patch (`fix-bus-heartbeat.revert.json`).
