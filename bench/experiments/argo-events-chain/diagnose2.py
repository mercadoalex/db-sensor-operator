#!/usr/bin/env python3
"""Second-session experiments for the lost-event-on-Sensor-replacement diagnosis (see
.agents/tasks/argo-sensor-loss/prereg.md, section "Added after seeing data, second session").
Additive: reuses the recorder and helpers of diagnose.py, changes neither diagnose.py nor test.py.

Every run starts from a CLEAN baseline: one Ready Sensor pod, one Ready EventSource pod, exactly one
Sensor queue-group subscription on the bus, online, with nothing pending. (diagnose.py only required "at
least one online subscription", so ghosts from earlier runs leaked into later ones.)

Kinds (run <kind>):
  K1     SIGKILL the Sensor container from the node (kill -9), E issued 0 s later
  K2     SIGKILL, wait until the restarted container subscribed, wait 60 s, then E
  (X1/X2 were first tried with `kubectl delete --grace-period=0 --force`; that still sends SIGTERM with a 2 s floor,
   so those runs are not SIGKILL runs. Kept in evidence as FD runs.)
  X4     E first, graceful delete --e2delay seconds later (E in flight when SIGTERM arrives)
  X3     graceful `kubectl delete pod`, E 1 s later (the original S-delete protocol, clean baseline)
  M5G    SIGKILL, E at 0 s, newer event E2 at +e2delay s (triggers on the live pod), Job reset, watch for E
  M5GC   as M5G without E2 (control)
  C5     no restart (latency control)
Other commands:
  ghosts <name> --n N     N graceful deletes of the Sensor pod, no events; per pod: did it unsubscribe,
                          did the bus drop its subscription within 8 s
  batch default           the pre-written interleaved X1/X2/X3 order (plan2.json), default bus
"""
import argparse
import json
import os
import random
import re
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import diagnose as D  # noqa: E402

now, iso, kc = D.now, D.iso, D.kc


def sensor_subs(ch):
    return [s for s in (ch or {}).get("subscriptions", []) if str(s.get("queue_name", "")).startswith("client-")]


def is_clean(pf):
    sp, ep = D.pods(D.SENSOR_SEL), D.pods(D.ES_SEL)
    if not (len(sp) == 1 and len(ep) == 1):
        return False
    if not all(D.pod_state(p)["ready"] and not p["metadata"].get("deletionTimestamp") for p in sp + ep):
        return False
    bp = D.pods("eventbus-name=default") or []
    if bp and not all(D.pod_state(p)["ready"] for p in bp):
        return False
    n, ch, cl = D.bus_summary()
    subs = sensor_subs(ch)
    return len(subs) == 1 and not subs[0].get("is_offline") and subs[0].get("pending_count") == 0


def wait_clean(pf, limit=900):
    end = now() + limit
    t_start = now()
    while now() < end:
        pf.ensure()
        if is_clean(pf):
            return True, now() - t_start
        time.sleep(2)
    return False, now() - t_start


def live_sensor():
    return [D.pod_state(p) for p in D.pods(D.SENSOR_SEL) if not p["metadata"].get("deletionTimestamp")]


def pod_client_id(pod):
    out = kc("-n", D.NS, "logs", pod, timeout=20)
    m = re.findall(r"ClientID:(client-\d+-\d+)", out)
    return m[-1] if m else None


def pod_subscribed(pod):
    return "Subscribed to subject" in kc("-n", D.NS, "logs", pod, timeout=20)


def wait_new_subscribed(old, limit=120):
    end = now() + limit
    while now() < end:
        for s in live_sensor():
            if s["name"] not in old and s["ready"] and pod_subscribed(s["name"]):
                return s["name"], now()
        time.sleep(0.2)
    return None, now()


NODE = "db-sensor-bench-control-plane"


def sigkill_sensor_container():
    """SIGKILL the Sensor container's main process from the kind node (no SIGTERM, no chance to unsubscribe).
    `kubectl delete --grace-period=0 --force` is NOT this: the kubelet still sends SIGTERM and grants a 2 s floor.
    The pod survives; kubelet restarts the container (new process, new STAN client id)."""
    pod = live_sensor()[0]["name"]
    cid = kc("-n", D.NS, "get", "pod", pod, "-o", "jsonpath={.status.containerStatuses[0].containerID}").split("//")[-1]
    out = subprocess.run(["docker", "exec", NODE, "sh", "-c",
                          "PID=$(crictl inspect --output go-template --template '{{.info.pid}}' %s) && kill -9 $PID && echo killed $PID" % cid],
                         capture_output=True, text=True, timeout=30)
    return pod, cid, out.stdout.strip(), out.stderr.strip()


def wait_restarted_subscribed(pod, old_cid, limit=120):
    end = now() + limit
    while now() < end:
        p = [x for x in D.pods(D.SENSOR_SEL) if x["metadata"]["name"] == pod]
        if p:
            st = D.pod_state(p[0])
            if st["ready"] and pod_subscribed(pod):
                c = pod_client_id(pod)
                if c and c != old_cid:
                    return c, now()
        time.sleep(0.2)
    return None, now()


def restart_count(pod):
    p = [x for x in D.pods(D.SENSOR_SEL) if x["metadata"]["name"] == pod]
    return ((p[0]["status"].get("containerStatuses") or [{}])[0].get("restartCount", 0)) if p else 0


def follow_restarted(rec, pod, fname, base_count, limit=60):
    def run():
        end = now() + limit
        while now() < end:
            p = [x for x in D.pods(D.SENSOR_SEL) if x["metadata"]["name"] == pod]
            if p:
                cs = (p[0]["status"].get("containerStatuses") or [{}])[0]
                if cs.get("restartCount", 0) > base_count and "running" in (cs.get("state") or {}):
                    rec.follow(pod, fname)
                    return
            time.sleep(0.1)
    th = threading.Thread(target=run, daemon=True)
    th.start()
    rec.threads.append(th)


def bus_restarts():
    out = kc("-n", D.NS, "get", "pods", "-o", "json")
    try:
        return {p["metadata"]["name"]: sum(c.get("restartCount", 0) for c in p["status"].get("containerStatuses", []))
                for p in json.loads(out)["items"] if p["metadata"]["name"].startswith("eventbus-")}
    except Exception:
        return {}


def run_x(name, kind, row_id, pf, limit, e2delay=6.0, wait_ready=60.0):
    d = os.path.join(D.EVID, name)
    os.makedirs(d, exist_ok=True)
    tl = open(os.path.join(d, "timeline.log"), "a", buffering=1)

    def log(msg):
        tl.write("%s %s\n" % (iso(), msg))
        print("[%s] %s" % (name, msg), flush=True)
    row2 = row_id + 50
    summary = {"name": name, "kind": kind, "row_id": row_id, "row2": row2 if kind == "M5G" else None, "limit": limit, "events": {}}
    if kind in ("K1", "K2", "M5G", "M5GC") and live_sensor() and restart_count(live_sensor()[0]["name"]) > 0:
        # kubelet applies a restart back-off (10 s, 20 s, ... up to 5 min) to a container killed repeatedly;
        # a fresh pod (restartCount 0) restarts immediately. Replace the pod before the run and let the bus settle.
        kc("-n", D.NS, "delete", "pod", "-l", D.SENSOR_SEL, "--wait=true")
        wait_new_subscribed([])
    ok, waited = wait_clean(pf)
    summary["clean_baseline"], summary["clean_wait_s"] = ok, round(waited, 1)
    log("clean baseline=%s after waiting %.1f s" % (ok, waited))
    if not ok:
        summary["outcome"] = "ABORT-no-clean-baseline"
        json.dump(summary, open(os.path.join(d, "summary.json"), "w"), indent=1)
        return summary
    D.reset_job()
    D.set_row(row_id, "settled")
    D.set_row(row2, "settled")
    time.sleep(5)
    D.reset_job()
    time.sleep(1)
    n, ch, cl = D.bus_summary()
    summary["bus_before"] = {"node": n, "channel": ch}
    summary["bus_last_seq_before"] = (ch or {}).get("last_seq")
    old = [p["metadata"]["name"] for p in D.pods(D.SENSOR_SEL)]
    summary["old_sensor_pods"] = old
    summary["old_client_id"] = pod_client_id(old[0]) if old else None
    summary["bus_restarts_before"] = bus_restarts()
    log("baseline: old pod %s client %s bus last_seq=%s" % (old, summary["old_client_id"], summary["bus_last_seq_before"]))
    rec = D.Recorder(d, pf)
    rec.start()
    time.sleep(10)
    ev = summary["events"]
    t0 = None
    if kind in ("K1", "K2", "M5G", "M5GC"):
        ev["kill"] = now()
        base_rc = restart_count(live_sensor()[0]["name"])
        kpod, kcid, kout, kerr = sigkill_sensor_container()
        ev["kill_done"] = now()
        summary["sigkill"] = {"pod": kpod, "container": kcid, "out": kout, "err": kerr}
        log("SIGKILL of container %s in pod %s at %s (done %s): %s %s" % (kcid[:12], kpod, iso(ev["kill"]), iso(ev["kill_done"]), kout, kerr))
        follow_restarted(rec, kpod, "sensor-%s-restart-%d.log" % (kpod, base_rc + 1), base_rc)
        if kind == "K2":
            c, tt = wait_restarted_subscribed(kpod, summary["old_client_id"])
            ev["new_subscribed"] = tt
            log("restarted container subscribed as %s at %s; waiting %.0f s" % (c, iso(tt), wait_ready))
            time.sleep(wait_ready)
        t0, t1 = D.issue_E(row_id, log)
        ev["E_start"], ev["E_end"] = t0, t1
        rec.fast_until = t0 + 90
        if kind == "M5G":
            time.sleep(max(0, ev["kill"] + e2delay - now()))
            ev["E2_start"], _ = D.issue_E(row2, log)
            ts2 = D.poll_suspend(ev["E2_start"], 30, rec, log)
            ev["E2_suspended"] = ts2
            D.reset_job()
            ev["job_reset_after_E2"] = now()
            log("Job reset after E2 at %s; watching for E" % iso())
    elif kind == "X4":
        # E first, graceful delete e2delay seconds later: E is in flight (EventSource -> bus -> Sensor) when SIGTERM arrives
        t0, t1 = D.issue_E(row_id, log)
        ev["E_start"], ev["E_end"] = t0, t1
        rec.fast_until = t0 + 90
        time.sleep(max(0, t0 + e2delay - now()))
        ev["kill"] = now()
        kc("-n", D.NS, "delete", "pod", "-l", D.SENSOR_SEL, "--wait=false")
        log("graceful delete issued %.2f s after E start" % (ev["kill"] - t0))
    elif kind == "X3":
        ev["kill"] = now()
        kc("-n", D.NS, "delete", "pod", "-l", D.SENSOR_SEL, "--wait=false")
        log("graceful delete issued %s" % iso(ev["kill"]))
        time.sleep(1.0)
        t0, t1 = D.issue_E(row_id, log)
        ev["E_start"], ev["E_end"] = t0, t1
        rec.fast_until = t0 + 90
    elif kind == "C5":
        t0, t1 = D.issue_E(row_id, log)
        ev["E_start"], ev["E_end"] = t0, t1
        rec.fast_until = t0 + 60
    else:
        raise SystemExit("unknown kind")
    # watch: Job suspend poll plus the number of Sensor subscriptions on the bus (ghost removal time)
    end = t0 + limit
    sus = None
    nsub_prev = None
    while now() < end:
        if D.suspended():
            sus = now()
            log("Job suspended observed +%.2f s from E" % (sus - t0))
            break
        n_, ch_, _ = D.bus_summary()
        k = len(sensor_subs(ch_))
        if k != nsub_prev:
            log("sensor subscriptions on bus: %s" % [(s.get("client_id"), s.get("is_offline"), s.get("last_sent"), s.get("pending_count")) for s in sensor_subs(ch_)])
            if nsub_prev is not None and k < nsub_prev:
                ev.setdefault("sub_removed", []).append(now())
            nsub_prev = k
        time.sleep(0.25)
    if sus is None:
        log("Job NOT suspended within %.0f s" % limit)
    ev["suspended"] = sus
    summary["suspend_latency_s"] = round(sus - t0, 3) if sus else None
    summary["outcome"] = "lost" if sus is None else ("delivered" if sus - t0 <= 5 else "delayed")
    time.sleep(4)
    n, ch, cl = D.bus_summary()
    summary["bus_after"] = {"node": n, "channel": ch}
    log("final bus subs=%s" % [(s.get("client_id"), s.get("is_offline"), s.get("last_sent"), s.get("pending_count")) for s in sensor_subs(ch)])
    rec.stop()
    summary["bus_restarts_after"] = bus_restarts()
    summary["bus_stable"] = summary["bus_restarts_before"] == summary["bus_restarts_after"]
    kd = D.kafka_dump(os.path.join(d, "kafka-topic.txt"))
    km = [l for l in kd.splitlines() if ('"id":%d,' % row_id) in l or ('"id": %d' % row_id) in l or ('"id":%d,' % row2) in l]
    open(os.path.join(d, "kafka-E-id%d.txt" % row_id), "w").write("\n".join(km[-8:]) + "\n")
    json.dump(D.job_json(managed=True), open(os.path.join(d, "job-after.json"), "w"), indent=1)
    summary["events_iso"] = {k: (iso(v) if isinstance(v, float) else [iso(x) for x in v]) for k, v in ev.items() if v}
    json.dump(summary, open(os.path.join(d, "summary.json"), "w"), indent=1, default=str)
    tl.close()
    return summary


def ghost_trials(name, n, pf, spacing=3.0, mode="delete"):
    d = os.path.join(D.EVID, name)
    os.makedirs(d, exist_ok=True)
    tl = open(os.path.join(d, "timeline.log"), "a", buffering=1)

    def log(msg):
        tl.write("%s %s\n" % (iso(), msg))
        print("[%s] %s" % (name, msg), flush=True)
    ok, waited = wait_clean(pf)
    log("clean baseline=%s after %.1f s" % (ok, waited))
    rec = D.Recorder(d, pf)
    rec.start()
    time.sleep(5)
    rows = []
    for i in range(n):
        live = live_sensor()
        if len(live) != 1 or not pod_subscribed(live[0]["name"]):
            nm, _ = wait_new_subscribed([])
            live = live_sensor()
        pod = live[0]["name"]
        cid = pod_client_id(pod)
        time.sleep(spacing)
        t_del = now()
        if mode == "rollout":
            dep = kc("-n", D.NS, "get", "deploy", "-l", D.SENSOR_SEL, "-o", "jsonpath={.items[0].metadata.name}")
            kc("-n", D.NS, "rollout", "restart", "deploy/" + dep)
        elif mode == "sigkill":
            sigkill_sensor_container()
        else:
            kc("-n", D.NS, "delete", "pod", pod, "--wait=false")
        for _ in range(300):
            if pod not in [p["metadata"]["name"] for p in D.pods(D.SENSOR_SEL)]:
                break
            time.sleep(0.1)
        t_gone = now()
        time.sleep(8)
        n_, ch, _ = D.bus_summary()
        still = [s for s in sensor_subs(ch) if s.get("client_id") == cid and not s.get("is_offline")]
        rows.append({"i": i + 1, "pod": pod, "client": cid, "t_delete": iso(t_del), "t_gone": iso(t_gone), "still_online_8s_after_gone": bool(still)})
        log("trial %d pod %s client %s gone after %.1f s; sub still online 8 s later: %s" % (i + 1, pod, cid, t_gone - t_del, bool(still)))
        nm, _ = wait_new_subscribed([pod])
    time.sleep(3)
    rec.stop()
    for r in rows:
        f = [x for x in os.listdir(d) if x.startswith("sensor-") and x.endswith("-" + r["pod"] + ".log")]
        txt = open(os.path.join(d, f[0]), errors="replace").read() if f else ""
        r["log_has_unsubscribe_line"] = "exiting, unsubscribing and closing connection" in txt
        r["log_has_shutting_down"] = "Shutting down..." in txt
        r["log_has_elector_exiting"] = "leaderelection.go" in txt and '"exiting..."' in txt
    json.dump(rows, open(os.path.join(d, "ghost-trials.json"), "w"), indent=1)
    ghosts = sum(1 for r in rows if r["still_online_8s_after_gone"])
    nounsub = sum(1 for r in rows if not r["log_has_unsubscribe_line"])
    log("RESULT %s: n=%d ghost(sub still online)=%d  no-unsubscribe-line=%d" % (name, n, ghosts, nounsub))
    tl.close()
    return rows


def make_plan2(seed):
    runs = [("K1", 5, 480), ("K2", 5, 120), ("X3", 3, 120)]
    items = []
    for k, c, lim in runs:
        for j in range(c):
            items.append({"kind": k, "limit": lim})
    random.Random(seed).shuffle(items)
    cnt = {}
    for i, it in enumerate(items):
        cnt[it["kind"]] = cnt.get(it["kind"], 0) + 1
        it["name"] = "Y%02d-%s-%d" % (i + 1, it["kind"], cnt[it["kind"]])
        it["row_id"] = 300 + i
    return {"seed": seed, "written": iso(), "runs": items}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run", "ghosts", "batch", "plan3"])
    ap.add_argument("what", nargs="?")
    ap.add_argument("--name", default=None)
    ap.add_argument("--id", type=int, default=300)
    ap.add_argument("--limit", type=float, default=120)
    ap.add_argument("--n", type=int, default=20)
    ap.add_argument("--e2delay", type=float, default=6.0)
    ap.add_argument("--seed", type=int, default=20261111)
    ap.add_argument("--prefix", default="")
    ap.add_argument("--mode", default="delete", choices=["delete", "rollout"])
    a = ap.parse_args()
    os.makedirs(D.EVID, exist_ok=True)
    if a.cmd == "plan3":
        p = os.path.join(D.EVID, "plan3.json")
        if os.path.exists(p):
            raise SystemExit("plan3.json exists")
        plan = make_plan2(a.seed)
        json.dump(plan, open(p, "w"), indent=1)
        for r in plan["runs"]:
            print(r["name"], r["kind"], r["limit"], r["row_id"])
        return
    pf = D.PortForwards()
    pf.start()
    try:
        if a.cmd == "run":
            run_x(a.name or "%s-adhoc-%d" % (a.what, int(now())), a.what, a.id, pf, a.limit, e2delay=a.e2delay)
        elif a.cmd == "ghosts":
            ghost_trials(a.name or a.what, a.n, pf, mode=a.mode)
        elif a.cmd == "batch":
            plan = json.load(open(os.path.join(D.EVID, "plan3.json")))
            for r in plan["runs"]:
                if os.path.exists(os.path.join(D.EVID, r["name"], "summary.json")):
                    continue
                run_x(r["name"], r["kind"], r["row_id"], pf, r["limit"])
    finally:
        pf.stop()


if __name__ == "__main__":
    main()
