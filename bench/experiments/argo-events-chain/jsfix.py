#!/usr/bin/env python3
"""F2 fix candidate: the same replacement-loss protocols on a JetStream EventBus (argo-fix-jetstream.yaml, namespace
argo-js). Additive to diagnose.py/diagnose2.py; the STAN chain in argo-events is not touched.

  jsfix.py run <K1|S|C5> --name N --id ROW [--limit S]
    K1  SIGKILL the Sensor container from the kind node, E 0 s later (same as diagnose2 K1)
    S   graceful `kubectl delete pod`, E 1 s later (same as diagnose2 X3)
    C5  no restart
Records: Sensor pod logs (incl. restarted container), JetStream consumer state from /jsz (changes only, 100 ms),
Job suspend poll, Kafka messages for E.
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import diagnose as D  # noqa: E402
import diagnose2 as X  # noqa: E402

NS = "argo-js"
SEL = "sensor-name=settled-to-pending-js"
JOB = "payments-writer-js"
JS_PODS = ["eventbus-default-js-0", "eventbus-default-js-1", "eventbus-default-js-2"]
PORTS = {p: 18230 + i for i, p in enumerate(JS_PODS)}
now, iso, kc = D.now, D.iso, D.kc


def suspended():
    return kc("-n", "bench", "get", "job", JOB, "-o", "jsonpath={.spec.suspend}") == "true"


def reset_job():
    kc("-n", "bench", "patch", "job", JOB, "--type=merge", "-p", '{"spec":{"suspend":false}}')


def pods():
    out = kc("-n", NS, "get", "pods", "-l", SEL, "-o", "json")
    try:
        return json.loads(out)["items"]
    except Exception:
        return []


def live():
    return [D.pod_state(p) for p in pods() if not p["metadata"].get("deletionTimestamp")]


def jsz(port):
    try:
        with urllib.request.urlopen("http://127.0.0.1:%d/jsz?consumers=true&streams=true" % port, timeout=2) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)}


def consumers_of(j):
    out = []
    for acc in j.get("account_details", []) or []:
        for st in acc.get("stream_detail", []) or []:
            for c in st.get("consumer_detail", []) or []:
                out.append({"stream": st.get("name"), "stream_last_seq": (st.get("state") or {}).get("last_seq"),
                            "name": c.get("name"), "delivered": (c.get("delivered") or {}).get("stream_seq"),
                            "ack_floor": (c.get("ack_floor") or {}).get("stream_seq"), "num_ack_pending": c.get("num_ack_pending"),
                            "num_redelivered": c.get("num_redelivered"), "num_waiting": c.get("num_waiting"), "num_pending": c.get("num_pending"),
                            "cluster_leader": (c.get("cluster") or {}).get("leader")})
    return out


class Rec:
    def __init__(self, d):
        self.d = d
        self.stop_ev = threading.Event()
        self.pf = {}
        self.procs = []
        self.followed = set()
        self.threads = []

    def start(self):
        for p, port in PORTS.items():
            self.pf[p] = subprocess.Popen(["kubectl", "--context", D.CTX, "-n", NS, "port-forward", "pod/" + p, "%d:8222" % port],
                                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(1.5)
        for target in (self.pods_watch, self.jsz_poll):
            th = threading.Thread(target=target, daemon=True)
            th.start()
            self.threads.append(th)

    def follow(self, pod, fname):
        def run():
            f = open(os.path.join(self.d, fname), "a", buffering=1)
            while not self.stop_ev.is_set():
                f.write("# follower attach %s\n" % iso())
                pr = subprocess.Popen(["kubectl", "--context", D.CTX, "-n", NS, "logs", "-f", "--timestamps", pod], stdout=f, stderr=subprocess.PIPE, text=True)
                self.procs.append(pr)
                _, err = pr.communicate()
                f.write("# follower exit rc=%s %s\n" % (pr.returncode, iso()))
                time.sleep(0.2)
                p = [x for x in pods() if x["metadata"]["name"] == pod]
                if not p:
                    break
                cs = (p[0]["status"].get("containerStatuses") or [{}])[0]
                if "running" not in (cs.get("state") or {}) and p[0]["metadata"].get("deletionTimestamp"):
                    break
            f.close()
        th = threading.Thread(target=run, daemon=True)
        th.start()
        self.threads.append(th)

    def pods_watch(self):
        f = open(os.path.join(self.d, "pods.jsonl"), "a", buffering=1)
        last = {}
        while not self.stop_ev.is_set():
            for p in pods():
                st = D.pod_state(p)
                cs = (p["status"].get("containerStatuses") or [{}])[0]
                st["restartCount"] = cs.get("restartCount")
                if last.get(st["name"]) != st:
                    last[st["name"]] = st
                    f.write(json.dumps({"t": iso(), **st}) + "\n")
                if st["name"] not in self.followed:
                    self.followed.add(st["name"])
                    self.follow(st["name"], "sensor-%s.log" % st["name"])
            time.sleep(0.15)
        f.close()

    def jsz_poll(self):
        fs = {p: open(os.path.join(self.d, "jsz-%s.jsonl" % p), "a", buffering=1) for p in JS_PODS}
        last = {}
        lw = {}

        def one(p):
            while not self.stop_ev.is_set():
                t = now()
                j = jsz(PORTS[p])
                state = json.dumps(consumers_of(j) if "error" not in j else j, sort_keys=True)
                if state != last.get(p) or t - lw.get(p, 0) > 5:
                    fs[p].write(json.dumps({"t": iso(t), "node": p, "consumers": json.loads(state)}) + "\n")
                    lw[p] = t
                last[p] = state
                time.sleep(0.1)
        for p in JS_PODS:
            th = threading.Thread(target=one, args=(p,), daemon=True)
            th.start()
            self.threads.append(th)

    def stop(self):
        self.stop_ev.set()
        for pr in self.procs:
            try:
                pr.terminate()
            except Exception:
                pass
        for pr in self.pf.values():
            pr.kill()
        for t in self.threads:
            t.join(timeout=3)


def ready_subscribed(limit=120):
    end = now() + limit
    while now() < end:
        lv = live()
        if len(lv) == 1 and lv[0]["ready"] and "Subscribing to subject" in kc("-n", NS, "logs", lv[0]["name"], timeout=20):
            return lv[0]["name"]
        time.sleep(0.5)
    return None


def run(name, kind, row, limit):
    d = os.path.join(D.EVID, name)
    os.makedirs(d, exist_ok=True)
    tl = open(os.path.join(d, "timeline.log"), "a", buffering=1)

    def log(m):
        tl.write("%s %s\n" % (iso(), m))
        print("[%s] %s" % (name, m), flush=True)
    S = {"name": name, "kind": kind, "row_id": row, "limit": limit, "events": {}}
    if kind in ("K1", "S"):
        # fresh pod: kubelet restart back-off would otherwise delay repeated SIGKILL restarts
        kc("-n", NS, "delete", "pod", "-l", SEL, "--wait=true")
    pod = ready_subscribed()
    time.sleep(8)
    log("baseline pod %s" % pod)
    reset_job()
    D.sql("update public.payments set status='settled' where id=%d;" % row)
    time.sleep(5)
    reset_job()
    time.sleep(1)
    rec = Rec(d)
    rec.start()
    time.sleep(8)
    ev = S["events"]
    if kind == "K1":
        ev["kill"] = now()
        cid = kc("-n", NS, "get", "pod", pod, "-o", "jsonpath={.status.containerStatuses[0].containerID}").split("//")[-1]
        r = subprocess.run(["docker", "exec", X.NODE, "sh", "-c", "PID=$(crictl inspect --output go-template --template '{{.info.pid}}' %s) && kill -9 $PID && echo killed $PID" % cid],
                           capture_output=True, text=True, timeout=30)
        log("SIGKILL %s: %s %s" % (cid[:12], r.stdout.strip(), r.stderr.strip()))
        S["sigkill"] = r.stdout.strip()
        t0 = now()
        D.sql("update public.payments set status='pending' where id=%d;" % row)
    elif kind == "S":
        ev["kill"] = now()
        kc("-n", NS, "delete", "pod", "-l", SEL, "--wait=false")
        log("graceful delete issued")
        time.sleep(1.0)
        t0 = now()
        D.sql("update public.payments set status='pending' where id=%d;" % row)
    else:
        t0 = now()
        D.sql("update public.payments set status='pending' where id=%d;" % row)
    ev["E_start"] = t0
    log("E issued id=%d at %s" % (row, iso(t0)))
    sus = None
    while now() - t0 < limit:
        if suspended():
            sus = now()
            break
        time.sleep(0.25)
    S["suspend_latency_s"] = round(sus - t0, 3) if sus else None
    S["outcome"] = "lost" if sus is None else ("delivered" if sus - t0 <= 5 else "delayed")
    log("outcome %s latency %s" % (S["outcome"], S["suspend_latency_s"]))
    time.sleep(4)
    rec.stop()
    kd = D.kafka_dump(os.path.join(d, "kafka-topic.txt"))
    open(os.path.join(d, "kafka-E-id%d.txt" % row), "w").write("\n".join([l for l in kd.splitlines() if ('"id":%d,' % row) in l][-6:]) + "\n")
    S["events_iso"] = {k: iso(v) for k, v in ev.items() if v}
    json.dump(S, open(os.path.join(d, "summary.json"), "w"), indent=1)
    tl.close()
    return S


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run"])
    ap.add_argument("kind", choices=["K1", "S", "C5"])
    ap.add_argument("--name", required=True)
    ap.add_argument("--id", type=int, required=True)
    ap.add_argument("--limit", type=float, default=120)
    a = ap.parse_args()
    os.makedirs(D.EVID, exist_ok=True)
    run(a.name, a.kind, a.id, a.limit)


if __name__ == "__main__":
    main()
