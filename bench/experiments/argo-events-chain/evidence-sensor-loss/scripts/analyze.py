#!/usr/bin/env python3
"""Summarise evidence/<run>/ into one timeline per run (read-only on the evidence)."""
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone

EV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "evidence")


def ts(s):
    s = s.replace("Z", "+00:00")
    m = re.match(r"(.*\.\d{6})\d*(\+00:00)", s)
    if m:
        s = m.group(1) + m.group(2)
    return datetime.fromisoformat(s).timestamp()


def fmt(t, t0):
    return "%+.2f" % (t - t0) if t is not None else "-"


def parse_log(path):
    out = []
    for l in open(path, errors="replace"):
        if l.startswith("#") or not l.strip():
            continue
        m = re.match(r"(\S+Z) (.*)", l)
        if not m:
            continue
        try:
            j = json.loads(m.group(2))
        except Exception:
            j = {"msg": m.group(2)}
        out.append((ts(m.group(1)), j))
    return out


def analyze(run):
    d = os.path.join(EV, run)
    s = json.load(open(os.path.join(d, "summary.json")))
    ev = s["events"]
    t0 = ev["t0"]
    r = {"run": run, "scenario": s["scenario"], "delay": s["delay"], "id": s["row_id"], "outcome": s["outcome"],
         "latency": s["suspend_latency_s"], "contaminated": s.get("contaminated", False), "baseline_ok": s["baseline_ok"]}
    # Kafka offset of E
    kf = open(os.path.join(d, "kafka-E-id%d.txt" % s["row_id"])).read().splitlines()
    eoff = None
    for l in kf:
        if '"before":{' in l and '"status":"settled"' in l.split('"after"')[0] and '"status":"pending"' in l.split('"after"')[1].split('"source"')[0]:
            m = re.search(r"Offset:(\d+)", l)
            eoff = int(m.group(1))
    r["kafka_offset"] = eoff
    evid = ":%d" % eoff if eoff is not None else None
    # EventSource publish line for E
    pub = None
    for f in glob.glob(os.path.join(d, "eventsource-*.log")):
        for t, j in parse_log(f):
            if evid and j.get("msg") == "Succeeded to publish an event" and j.get("eventID", "").endswith(":0" + evid):
                pub = t
    r["es_publish"] = pub
    # sensors
    sens = {}
    for f in sorted(glob.glob(os.path.join(d, "sensor-*.log"))):
        name = os.path.basename(f)[len("sensor-"):-4]
        recs = parse_log(f)
        info = {"client": None, "started": None, "subscribed": None, "shutdown": None, "unsub": None, "sub_closed": None, "trig": [], "last": None}
        for t, j in recs:
            msg = j.get("msg", "")
            if j.get("clientID"):
                info["client"] = j["clientID"]
            if msg == "Sensor started.":
                info["started"] = t
            if msg.startswith("Subscribed to subject"):
                info["subscribed"] = t
            if msg == "Shutting down...":
                info["shutdown"] = t
            if "unsubscribing and closing" in msg:
                info["unsub"] = t
            if msg.startswith("subscription on subject") and "closed" in msg:
                info["sub_closed"] = t
            if msg.startswith("Successfully processed trigger"):
                info["trig"].append((t, j.get("triggeredByEvents")))
            info["last"] = t
        sens[name] = info
    r["sensors"] = sens
    # trigger for E
    r["trigger_for_E"] = [(n[-5:], t, ev_) for n, i in sens.items() for t, ev_ in i["trig"] if evid and ev_ and any(e.endswith(":0" + evid) for e in ev_)]
    # bus: find seq of E and who received it
    bus = []
    for l in open(os.path.join(d, "bus-eventbus-default-stan-0.jsonl")):
        j = json.loads(l)
        if j["changed"]:
            bus.append(j)
    r["bus"] = bus
    return r, s


def show(run, verbose=True):
    r, s = analyze(run)
    ev = s["events"]
    t0 = ev["t0"]
    print("=" * 100)
    print("%s  scen=%s delay=%s id=%s kafka_off=%s outcome=%s latency=%s contaminated=%s" % (
        run, r["scenario"], r["delay"], r["id"], r["kafka_offset"], r["outcome"], r["latency"], r["contaminated"]))
    print("  t0=E psql start; times below relative to t0 (s)")
    for k in ("delete_issued", "scale0_issued", "pod_gone", "scale1_issued", "new_ready_subscribed", "E_start", "suspended"):
        if k in ev and ev[k]:
            print("  %-22s %s" % (k, fmt(ev[k], t0)))
    print("  es_publish E          %s" % fmt(r["es_publish"], t0))
    for n, i in r["sensors"].items():
        print("  sensor %-8s client=%-26s started=%s subscribed=%s shutdown=%s unsub=%s sub_closed=%s triggers=%d" % (
            n[-5:], i["client"], fmt(i["started"], t0), fmt(i["subscribed"], t0), fmt(i["shutdown"], t0), fmt(i["unsub"], t0), fmt(i["sub_closed"], t0), len(i["trig"])))
    print("  trigger_for_E:", [(a, fmt(b, t0)) for a, b, c in r["trigger_for_E"]])
    if verbose:
        last = None
        for b in r["bus"]:
            tt = ts(b["t"])
            if tt < t0 - 12:
                continue
            ch = b["channel"]
            subs = [(x["client_id"][-3:], "OFF" if x["is_offline"] else "on", x["last_sent"], x["pending_count"]) for x in ch.get("subscriptions", [])]
            cls = [c["id"][-3:] for c in b["clients"].get("clients", [])]
            key = (ch.get("last_seq"), tuple(subs), tuple(cls))
            if key != last:
                print("   bus %s seq=%s subs=%s clients=%s" % (fmt(tt, t0), ch.get("last_seq"), subs, cls))
                last = key


if __name__ == "__main__":
    runs = sys.argv[1:] or sorted(os.path.basename(p) for p in glob.glob(os.path.join(EV, "[0-9]*")) if os.path.exists(os.path.join(p, "summary.json")))
    for r in runs:
        show(r)


def pod_table():
    """All Sensor pod lifetimes seen in the evidence; client ids repeat across pods, so resolve by time."""
    seen = {}
    for p in glob.glob(os.path.join(EV, "[0-9]*", "sensor-*.log")):
        pod = os.path.basename(p)[len("sensor-settled-to-pending-sensor-"):-4]
        recs = parse_log(p)
        cid = started = shut = sub_closed = last = None
        for t, j in recs:
            msg = str(j.get("msg", ""))
            if j.get("clientID") and cid is None:
                cid = j["clientID"]
            if msg == "Sensor started." and started is None:
                started = t
            if msg == "Shutting down...":
                shut = t
            if msg.startswith("subscription on subject") and "closed" in msg:
                sub_closed = t
            last = t
        if pod in seen:  # same pod captured by two runs: merge
            o = seen[pod]
            cid = cid or o["cid"]; started = started or o["started"]; shut = shut or o["shutdown"]; sub_closed = sub_closed or o["sub_closed"]
            last = max(last or 0, o["last"] or 0)
        seen[pod] = {"pod": pod, "cid": cid, "started": started, "shutdown": shut, "sub_closed": sub_closed, "last": last}
    return seen


def classify(run, table=None):
    """Who received E on the bus, and was that member a pod that was still alive?"""
    table = table or pod_table()
    r, s = analyze(run)
    ev = s["events"]
    t0 = ev["t0"]
    base_seq = None
    for b in r["bus"]:
        if ts(b["t"]) <= t0:
            base_seq = b["channel"].get("last_seq")
    eseq = recv = t_recv = t_pub_bus = None
    pend_hist = []
    for b in r["bus"]:
        tt = ts(b["t"])
        if tt < t0 - 1:
            continue
        ch = b["channel"]
        if eseq is None and ch.get("last_seq", 0) > (base_seq or 0):
            eseq, t_pub_bus = ch["last_seq"], tt
        if eseq:
            for x in ch.get("subscriptions", []):
                if x["last_sent"] >= eseq and recv is None:
                    recv, t_recv = x["client_id"], tt
    final = r["bus"][-1]["channel"] if r["bus"] else {}
    out = {"run": run, "E_seq": eseq, "t_bus_seq": t_pub_bus, "receiver": recv, "t_recv": t_recv, "t0": t0}
    if recv:
        cands = [v for v in table.values() if v["cid"] == recv and v["started"] and v["started"] <= t_recv + 0.5]
        L = max(cands, key=lambda v: v["started"]) if cands else None
        if L:
            dead_at = L["shutdown"]
            out.update(receiver_pod=L["pod"], receiver_started=L["started"], receiver_shutdown=dead_at,
                       receiver_sub_closed_logged=bool(L["sub_closed"]),
                       receiver_state="DEAD (process exiting/exited before delivery)" if dead_at and t_recv > dead_at else "alive")
        else:
            out.update(receiver_pod=None, receiver_state="unknown pod (no log in evidence)")
    out["final_subs"] = [(x["client_id"][-3:], x["last_sent"], x["pending_count"]) for x in final.get("subscriptions", [])]
    out["outcome"] = s["outcome"]
    out["latency"] = s["suspend_latency_s"]
    return out
