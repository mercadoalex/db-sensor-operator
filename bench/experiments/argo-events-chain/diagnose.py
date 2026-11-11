#!/usr/bin/env python3
"""Instrumented diagnosis of the lost-event-on-Sensor-replacement result (see
.agents/tasks/argo-sensor-loss/prereg.md). Additive: test.py is untouched.

Usage:
  diagnose.py plan  [--seed N]          write the fixed run order to <evidence>/plan.json (once, before any run)
  diagnose.py all                       execute every run in plan.json that has no summary.json yet
  diagnose.py run <scenario> [--delay S] [--name NAME] [--limit SECONDS]

Scenarios: S-delete (delete Sensor pod, E after --delay), C1 (E, reset, delete, E2 30 s after Ready),
C2 (delete, wait Ready+subscribed, 15 s, E), C3 (scale 0, E, 15 s, scale 1), C4 (delete EventSource pod,
E after 1 s), C5 (no restart), C6 (delete+recreate the Sensor resource, wait, E).

Per run it records (all timestamps are host wall clock, UTC): pod lifecycle poll, `kubectl logs -f
--timestamps` of every Sensor pod (old pod captured live), the EventSource pod, the 3 bus pods; the NATS
Streaming monitoring endpoints (/streaming/channelsz?subs=1, /streaming/clientsz, /streaming/serverz) of all
3 bus pods polled every 100 ms (changes only, plus a keepalive); Job suspend poll; Kafka messages for E.
"""
import argparse
import json
import os
import random
import re
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone

CTX = "kind-db-sensor-bench"
NS = "argo-events"
HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
EVID = os.path.join(REPO, ".agents", "tasks", "argo-sensor-loss", "evidence")
CHANNEL = "eventbus-argo-events"
SENSOR_SEL = "sensor-name=settled-to-pending"
ES_SEL = "eventsource-name=payments-cdc"
BUS_PODS = ["eventbus-default-stan-0", "eventbus-default-stan-1", "eventbus-default-stan-2"]
PF_PORT = {p: 18220 + i for i, p in enumerate(BUS_PODS)}


def now():
    return time.time()


def iso(t=None):
    t = now() if t is None else t
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def kc(*args, stdin=None, timeout=60):
    try:
        r = subprocess.run(["kubectl", "--context", CTX, *args], input=stdin, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip()
    except subprocess.TimeoutExpired:
        return ""


def sql(stmt):
    return kc("-n", "bench", "exec", "-i", "bench-postgres-0", "--", "psql", "-U", "bench", "-d", "bench", "-At", stdin=stmt)


def job_json(managed=False):
    args = ["-n", "bench", "get", "job", "payments-writer", "-o", "json"]
    if managed:
        args.append("--show-managed-fields")
    out = kc(*args)
    return json.loads(out) if out else None


def suspended():
    return kc("-n", "bench", "get", "job", "payments-writer", "-o", "jsonpath={.spec.suspend}") == "true"


def pods(sel):
    out = kc("-n", NS, "get", "pods", "-l", sel, "-o", "json")
    try:
        return json.loads(out)["items"]
    except Exception:
        return []


def pod_state(p):
    cs = (p["status"].get("containerStatuses") or [{}])[0]
    return {
        "name": p["metadata"]["name"],
        "phase": p["status"].get("phase"),
        "created": p["metadata"].get("creationTimestamp"),
        "deletionTimestamp": p["metadata"].get("deletionTimestamp"),
        "deletionGracePeriodSeconds": p["metadata"].get("deletionGracePeriodSeconds"),
        "ready": cs.get("ready"),
        "state": cs.get("state"),
        "conditions": {c["type"]: c["status"] for c in p["status"].get("conditions", [])},
    }


def http_json(port, path):
    try:
        with urllib.request.urlopen("http://127.0.0.1:%d%s" % (port, path), timeout=2) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)}


class PortForwards:
    def __init__(self):
        self.procs = {}

    def start(self):
        for p, port in PF_PORT.items():
            self.procs[p] = subprocess.Popen(["kubectl", "--context", CTX, "-n", NS, "port-forward", "pod/" + p, "%d:8222" % port],
                                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(50):
            if all("error" not in http_json(port, "/streaming/serverz") for port in PF_PORT.values()):
                return
            time.sleep(0.2)

    def ensure(self):
        for p, port in PF_PORT.items():
            if self.procs[p].poll() is not None or "error" in http_json(port, "/streaming/serverz"):
                if self.procs[p].poll() is None:
                    self.procs[p].kill()
                self.procs[p] = subprocess.Popen(["kubectl", "--context", CTX, "-n", NS, "port-forward", "pod/" + p, "%d:8222" % port],
                                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                time.sleep(1.0)

    def stop(self):
        for pr in self.procs.values():
            pr.kill()


def bus_snapshot(node):
    port = PF_PORT[node]
    ch = http_json(port, "/streaming/channelsz?channel=%s&subs=1" % CHANNEL)
    cl = http_json(port, "/streaming/clientsz?subs=1")
    return ch, cl


def bus_summary():
    """Leader-agnostic: first node that answers with a channel."""
    for n in BUS_PODS:
        ch, cl = bus_snapshot(n)
        if "error" not in ch:
            return n, ch, cl
    return None, None, None


class Recorder:
    """Background threads that stream evidence for one run into <dir>."""

    def __init__(self, d, pf):
        self.d, self.pf = d, pf
        self.stop_ev = threading.Event()
        self.threads, self.procs = [], []
        self.followed = set()
        self.fast_until = float("inf")
        self.lock = threading.Lock()
        self.pod_events = []  # (t, state) changes
        self.bus_t0 = now()

    def _open(self, name):
        return open(os.path.join(self.d, name), "a", buffering=1)

    def follow(self, pod, fname):
        def run():
            f = self._open(fname)
            deadline = now() + 400
            while not self.stop_ev.is_set() and now() < deadline:
                f.write("# follower attach %s\n" % iso())
                pr = subprocess.Popen(["kubectl", "--context", CTX, "-n", NS, "logs", "-f", "--timestamps", pod],
                                      stdout=f, stderr=subprocess.PIPE, text=True)
                with self.lock:
                    self.procs.append(pr)
                _, err = pr.communicate()
                f.write("# follower exit rc=%s %s %s\n" % (pr.returncode, iso(), (err or "").strip()[:200]))
                if pr.returncode == 0:
                    break
                if "NotFound" in (err or "") or "not found" in (err or ""):
                    break
                time.sleep(0.3)
            f.close()
        t = threading.Thread(target=run, daemon=True)
        t.start()
        self.threads.append(t)

    def pod_watcher(self):
        f = self._open("pods.jsonl")
        last = {}
        while not self.stop_ev.is_set():
            t = now()
            for kind, sel in (("sensor", SENSOR_SEL), ("eventsource", ES_SEL)):
                for p in pods(sel):
                    st = pod_state(p)
                    key = (kind, st["name"])
                    if last.get(key) != st:
                        last[key] = st
                        f.write(json.dumps({"t": iso(t), "kind": kind, **st}) + "\n")
                        with self.lock:
                            self.pod_events.append((t, kind, st))
                    if key not in self.followed:
                        self.followed.add(key)
                        self.follow(st["name"], "%s-%s.log" % (kind, st["name"]))
                seen = {k for k in last if k[0] == kind}
            time.sleep(0.15)
        f.close()

    def bus_poller(self):
        fs = {n: self._open("bus-%s.jsonl" % n) for n in BUS_PODS}
        last, last_w = {}, {}
        lock = threading.Lock()

        def one(n):
            while not self.stop_ev.is_set():
                t = now()
                ch, cl = bus_snapshot(n)
                # strip volatile "now"
                cl2 = dict(cl)
                cl2.pop("now", None)
                state = json.dumps({"channel": ch, "clients": cl2}, sort_keys=True)
                if state != last.get(n) or t - last_w.get(n, 0) > 5:
                    fs[n].write(json.dumps({"t": iso(t), "changed": state != last.get(n), "node": n,
                                            "channel": ch, "clients": cl2}) + "\n")
                    last_w[n] = t
                last[n] = state
                time.sleep(0.1 if t < self.fast_until else 1.0)
        for n in BUS_PODS:
            th = threading.Thread(target=one, args=(n,), daemon=True)
            th.start()
            self.threads.append(th)

    def start(self):
        for n in BUS_PODS:
            self.follow(n, "buslog-%s.log" % n)
        th = threading.Thread(target=self.pod_watcher, daemon=True)
        th.start()
        self.threads.append(th)
        self.bus_poller()

    def stop(self):
        self.stop_ev.set()
        for pr in list(self.procs):
            try:
                pr.terminate()
            except Exception:
                pass
        for t in self.threads:
            t.join(timeout=5)


def sensor_pods_live():
    return [pod_state(p) for p in pods(SENSOR_SEL) if not p["metadata"].get("deletionTimestamp")]


def wait_baseline(limit=180):
    """One live Ready Sensor pod, one live Ready EventSource pod, a non-offline Sensor sub on the bus,
    and no leftover Terminating Sensor/EventSource pod."""
    t_end = now() + limit
    while now() < t_end:
        sp, ep = pods(SENSOR_SEL), pods(ES_SEL)
        ok = len(sp) == 1 and len(ep) == 1 and all(pod_state(p)["ready"] and not p["metadata"].get("deletionTimestamp") for p in sp + ep)
        if ok:
            n, ch, cl = bus_summary()
            if ch and any(not s.get("is_offline") for s in ch.get("subscriptions", [])):
                return True
        time.sleep(1)
    return False


def kafka_dump(path, timeout_ms=6000):
    out = kc("-n", "bench", "exec", "deploy/argo-kafka", "--", "/opt/kafka/bin/kafka-console-consumer.sh",
             "--bootstrap-server", "localhost:9092", "--topic", "argotest.public.payments", "--from-beginning",
             "--timeout-ms", str(timeout_ms), "--property", "print.offset=true", "--property", "print.timestamp=true",
             "--property", "print.partition=true", timeout=90)
    with open(path, "w") as f:
        f.write(out)
    return out


def set_row(i, status):
    sql("update public.payments set status='%s' where id=%d;" % (status, i))


def reset_job():
    kc("-n", "bench", "patch", "job", "payments-writer", "--type=merge", "-p", '{"spec":{"suspend":false}}')


def issue_E(i, log):
    t = now()
    sql("update public.payments set status='pending' where id=%d;" % i)
    t1 = now()
    log("E issued id=%d  psql call start=%s end=%s" % (i, iso(t), iso(t1)))
    return t, t1


def poll_suspend(t0, limit, rec, log, step=0.25):
    """Poll Job.spec.suspend; returns first-true wall time or None."""
    while now() - t0 < limit:
        if suspended():
            tt = now()
            log("Job suspended observed at %s (+%.2fs from t0)" % (iso(tt), tt - t0))
            return tt
        time.sleep(step)
    log("Job NOT suspended within %.0f s" % limit)
    return None


def run_one(name, scen, delay, limit, row_id, pf):
    d = os.path.join(EVID, name)
    os.makedirs(d, exist_ok=True)
    tl = open(os.path.join(d, "timeline.log"), "a", buffering=1)

    def log(msg):
        tl.write("%s %s\n" % (iso(), msg))
        print("[%s] %s" % (name, msg), flush=True)
    summary = {"name": name, "scenario": scen, "delay": delay, "limit": limit, "row_id": row_id, "events": {}}
    log("run start scenario=%s delay=%s limit=%s id=%s" % (scen, delay, limit, row_id))
    pf.ensure()
    if not wait_baseline():
        log("WARNING: baseline not reached in time")
        summary["baseline_ok"] = False
    else:
        summary["baseline_ok"] = True
    # fresh baseline: Job unsuspended, row settled via a legal transition, flush it
    reset_job()
    set_row(row_id, "settled")
    time.sleep(5)
    reset_job()
    time.sleep(1)
    n, ch, cl = bus_summary()
    summary["bus_before"] = {"node": n, "channel": ch}
    log("baseline bus state node=%s msgs=%s last_seq=%s subs=%s" % (
        n, ch and ch.get("msgs"), ch and ch.get("last_seq"),
        [(s.get("client_id"), s.get("is_offline"), s.get("last_sent"), s.get("pending_count")) for s in (ch or {}).get("subscriptions", [])]))
    if suspended():
        log("WARNING: Job suspended before E (contaminated baseline)")
        summary["contaminated"] = True
        reset_job()
    # instrumentation: start 10 s before the disturbance
    rec = Recorder(d, pf)
    rec.start()
    json.dump({"sensor_pods": [pod_state(p) for p in pods(SENSOR_SEL)], "eventsource_pods": [pod_state(p) for p in pods(ES_SEL)]},
              open(os.path.join(d, "pods-before.json"), "w"), indent=1)
    time.sleep(10)
    ev = summary["events"]
    old = [p["metadata"]["name"] for p in pods(SENSOR_SEL)]
    old_es = [p["metadata"]["name"] for p in pods(ES_SEL)]
    summary["old_sensor_pods"], summary["old_eventsource_pods"] = old, old_es
    t0 = None
    tE = None

    def new_sensor_info():
        return [s for s in sensor_pods_live() if s["name"] not in old]

    def wait_new_ready_and_subscribed(limit=120):
        t_end = now() + limit
        while now() < t_end:
            ns = new_sensor_info()
            if ns and ns[0]["ready"]:
                name_ = ns[0]["name"]
                p = os.path.join(d, "sensor-%s.log" % name_)
                if os.path.exists(p) and "Subscribed to subject" in open(p).read():
                    return name_, now()
            time.sleep(0.2)
        return None, now()

    if scen == "S-delete":
        ev["delete_issued"] = now()
        kc("-n", NS, "delete", "pod", "-l", SENSOR_SEL, "--wait=false")
        ev["delete_returned"] = now()
        log("sensor pod delete issued %s returned %s" % (iso(ev["delete_issued"]), iso(ev["delete_returned"])))
        time.sleep(delay)
        t0, t1 = issue_E(row_id, log)
        ev["E_start"], ev["E_end"] = t0, t1
        rec.fast_until = t0 + 60
    elif scen == "C5":
        t0, t1 = issue_E(row_id, log)
        ev["E_start"], ev["E_end"] = t0, t1
        rec.fast_until = t0 + 60
    elif scen == "C4":
        ev["delete_issued"] = now()
        kc("-n", NS, "delete", "pod", "-l", ES_SEL, "--wait=false")
        ev["delete_returned"] = now()
        log("eventsource pod delete issued %s returned %s" % (iso(ev["delete_issued"]), iso(ev["delete_returned"])))
        time.sleep(delay)
        t0, t1 = issue_E(row_id, log)
        ev["E_start"], ev["E_end"] = t0, t1
        rec.fast_until = t0 + 60
    elif scen == "C2":
        ev["delete_issued"] = now()
        kc("-n", NS, "delete", "pod", "-l", SENSOR_SEL, "--wait=false")
        log("sensor pod delete issued")
        nm, tt = wait_new_ready_and_subscribed()
        ev["new_ready_subscribed"] = tt
        log("new pod %s ready+subscribed at %s" % (nm, iso(tt)))
        time.sleep(15)
        t0, t1 = issue_E(row_id, log)
        ev["E_start"], ev["E_end"] = t0, t1
        rec.fast_until = t0 + 60
    elif scen == "C1":
        # E1 first, no restart, confirm suspended
        t0a, t1a = issue_E(row_id, log)
        ev["E1_start"] = t0a
        tsa = poll_suspend(t0a, 30, rec, log)
        ev["E1_suspended"] = tsa
        reset_job()
        set_row(row_id, "settled")
        time.sleep(4)
        reset_job()
        ev["delete_issued"] = now()
        kc("-n", NS, "delete", "pod", "-l", SENSOR_SEL, "--wait=false")
        log("sensor pod delete issued (after E1 confirmed)")
        nm, tt = wait_new_ready_and_subscribed()
        ev["new_ready_subscribed"] = tt
        log("new pod %s ready+subscribed at %s; waiting 30 s" % (nm, iso(tt)))
        time.sleep(30)
        rec.fast_until = now() + 60
        t0, t1 = issue_E(row_id, log)
        ev["E_start"], ev["E_end"] = t0, t1
    elif scen == "C3":
        dep = kc("-n", NS, "get", "deploy", "-l", SENSOR_SEL, "-o", "jsonpath={.items[0].metadata.name}")
        ev["scale0_issued"] = now()
        kc("-n", NS, "scale", "deploy", dep, "--replicas=0")
        for _ in range(150):
            if not pods(SENSOR_SEL):
                break
            time.sleep(0.2)
        ev["pod_gone"] = now()
        time.sleep(3)
        n, ch, cl = bus_summary()
        log("scaled to 0, pod gone %s; bus subs=%s" % (iso(ev["pod_gone"]), [(s.get("client_id"), s.get("is_offline")) for s in (ch or {}).get("subscriptions", [])]))
        rec.fast_until = now() + 120
        t0, t1 = issue_E(row_id, log)
        ev["E_start"], ev["E_end"] = t0, t1
        time.sleep(15)
        ev["scale1_issued"] = now()
        kc("-n", NS, "scale", "deploy", dep, "--replicas=1")
        log("scaled back to 1 at %s" % iso(ev["scale1_issued"]))
    elif scen == "C6":
        ev["sensor_delete_issued"] = now()
        kc("-n", NS, "delete", "sensor", "settled-to-pending", "--wait=true")
        log("Sensor resource deleted")
        for _ in range(100):
            if not pods(SENSOR_SEL):
                break
            time.sleep(0.3)
        ev["sensor_recreate"] = now()
        kc("-n", NS, "apply", "-f", os.path.join(HERE, "argo.yaml"))
        log("argo.yaml re-applied (Sensor recreated)")
        old.clear()
        nm, tt = wait_new_ready_and_subscribed()
        ev["new_ready_subscribed"] = tt
        log("fresh sensor pod %s ready+subscribed %s" % (nm, iso(tt)))
        reset_job()
        time.sleep(15)
        t0, t1 = issue_E(row_id, log)
        ev["E_start"], ev["E_end"] = t0, t1
        rec.fast_until = t0 + 60
    else:
        raise SystemExit("unknown scenario")
    ev["t0"] = t0
    if scen == "C3":
        tsus = poll_suspend(t0, limit, rec, log)
        if tsus:
            log("(C3) suspend delay from scale-up: %.2fs" % (tsus - ev["scale1_issued"]))
    else:
        tsus = poll_suspend(t0, limit, rec, log)
    ev["suspended"] = tsus
    summary["suspend_latency_s"] = round(tsus - t0, 3) if tsus else None
    summary["outcome"] = (None if tsus is None else ("delivered" if tsus - t0 <= 5 else "delayed")) or "lost"
    time.sleep(3)
    # post-run state
    n, ch, cl = bus_summary()
    summary["bus_after"] = {"node": n, "channel": ch, "clients": cl}
    log("final bus state node=%s msgs=%s last_seq=%s subs=%s" % (
        n, ch and ch.get("msgs"), ch and ch.get("last_seq"),
        [(s.get("client_id"), s.get("is_offline"), s.get("last_sent"), s.get("pending_count")) for s in (ch or {}).get("subscriptions", [])]))
    serverz = {p: http_json(PF_PORT[p], "/streaming/serverz") for p in BUS_PODS}
    json.dump(serverz, open(os.path.join(d, "serverz-after.json"), "w"), indent=1)
    jj = job_json(managed=True)
    json.dump(jj, open(os.path.join(d, "job-after.json"), "w"), indent=1)
    rec.stop()
    json.dump(json.loads(kc("-n", NS, "get", "events", "-o", "json") or "{}"), open(os.path.join(d, "k8s-events.json"), "w"), indent=1)
    kd = kafka_dump(os.path.join(d, "kafka-topic.txt"))
    km = [l for l in kd.splitlines() if ('"id":%d,' % row_id) in l or ('"id": %d' % row_id) in l]
    open(os.path.join(d, "kafka-E-id%d.txt" % row_id), "w").write("\n".join(km[-6:]) + "\n")
    summary["events_iso"] = {k: iso(v) for k, v in ev.items() if v}
    summary["events"] = {k: v for k, v in ev.items()}
    json.dump(summary, open(os.path.join(d, "summary.json"), "w"), indent=1, default=str)
    tl.close()
    return summary


def capture_static(d):
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "static.txt"), "w") as f:
        for title, args in (
                ("eventbus", ["-n", NS, "get", "eventbus", "default", "-o", "yaml"]),
                ("bus statefulset", ["-n", NS, "get", "sts", "eventbus-default-stan", "-o", "yaml"]),
                ("bus configmap", ["-n", NS, "get", "cm", "eventbus-default-stan-configmap", "-o", "yaml"]),
                ("sensor deployment", ["-n", NS, "get", "deploy", "-l", SENSOR_SEL, "-o", "yaml"]),
                ("sensor resource", ["-n", NS, "get", "sensor", "settled-to-pending", "-o", "yaml"]),
                ("eventsource deployment", ["-n", NS, "get", "deploy", "-l", ES_SEL, "-o", "yaml"]),
                ("bus pods", ["-n", NS, "get", "pods", "-o", "wide"])):
            f.write("\n######## %s\n" % title)
            f.write(kc(*args) + "\n")
        f.write("\n######## serverz\n")


def make_plan(seed):
    conds = ([("S-delete", 1.0, 120)] * 5 + [("S-delete", 0.0, 120)] * 3 + [("S-delete", 1.0, 120)] * 3 + [("S-delete", 3.0, 120)] * 3
             + [("C1", 0, 120)] * 5 + [("C2", 0, 60)] * 5 + [("C3", 0, 60)] * 3 + [("C4", 1.0, 120)] * 3 + [("C5", 0, 60)] * 3
             + [("C6", 0, 60)] + [("S-delete-LONG", 1.0, 300)])
    # labels: S-delete d=1 first 5 are the main group (S), the next 3 d=1 are sweep SW1
    labels = (["S"] * 5 + ["SW0"] * 3 + ["SW1"] * 3 + ["SW3"] * 3 + ["C1"] * 5 + ["C2"] * 5 + ["C3"] * 3 + ["C4"] * 3 + ["C5"] * 3 + ["C6"] + ["LONG"])
    runs = []
    for i, (c, dl, lim) in enumerate(conds):
        runs.append({"label": labels[i], "scenario": "S-delete" if c.startswith("S-delete") else c, "delay": dl, "limit": lim})
    rnd = random.Random(seed)
    long_run = [r for r in runs if r["label"] == "LONG"]
    rest = [r for r in runs if r["label"] != "LONG"]
    rnd.shuffle(rest)
    order = rest + long_run  # the long-wait run goes last so its extra minutes don't shift others
    counts = {}
    for k, r in enumerate(order):
        counts[r["label"]] = counts.get(r["label"], 0) + 1
        r["name"] = "%02d-%s-%d" % (k + 1, r["label"], counts[r["label"]])
        r["row_id"] = 100 + k
    return {"seed": seed, "written": iso(), "runs": order}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["plan", "all", "run", "static"])
    ap.add_argument("scenario", nargs="?")
    ap.add_argument("--delay", type=float, default=1.0)
    ap.add_argument("--limit", type=float, default=120)
    ap.add_argument("--name", default=None)
    ap.add_argument("--id", type=int, default=900)
    ap.add_argument("--seed", type=int, default=20261110)
    ap.add_argument("--evidence", default=None)
    a = ap.parse_args()
    global EVID
    if a.evidence:
        EVID = a.evidence
    os.makedirs(EVID, exist_ok=True)
    if a.cmd == "plan":
        pth = os.path.join(EVID, "plan.json")
        if os.path.exists(pth):
            raise SystemExit("plan.json already exists; refusing to overwrite (pre-registered order)")
        plan = make_plan(a.seed)
        json.dump(plan, open(pth, "w"), indent=1)
        for r in plan["runs"]:
            print(r["name"], r["scenario"], r["delay"], r["limit"], r["row_id"])
        return
    if a.cmd == "static":
        capture_static(os.path.join(EVID, "static"))
        return
    pf = PortForwards()
    pf.start()
    try:
        if a.cmd == "run":
            run_one(a.name or "adhoc-%d" % int(now()), a.scenario, a.delay, a.limit, a.id, pf)
        else:
            plan = json.load(open(os.path.join(EVID, "plan.json")))
            for r in plan["runs"]:
                if os.path.exists(os.path.join(EVID, r["name"], "summary.json")):
                    continue
                run_one(r["name"], r["scenario"], r["delay"], r["limit"], r["row_id"], pf)
                time.sleep(5)
    finally:
        pf.stop()


if __name__ == "__main__":
    main()
