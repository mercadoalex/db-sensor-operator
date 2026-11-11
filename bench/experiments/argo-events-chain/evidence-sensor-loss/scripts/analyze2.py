#!/usr/bin/env python3
"""Read-only summary of the second-session runs (evidence/X*, evidence/M*, evidence/F*).
For each run: E's bus sequence, which member E was handed to (first member whose last_sent reaches E's seq),
whether that member belongs to the pod killed in this run, when it was handed, when the pending count on that
member went back to 0 or the member was removed, when the bus removed the killed client (bus log line
'Timed out on heartbeats' and the client list), and when a trigger for E was seen (Job suspend and sensor log).
usage: analyze2.py [prefix ...]   (default: every directory starting with X, M or F)"""
import glob
import json
import os
import re
import sys
from datetime import datetime

EV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "evidence")


def ts(s):
    s = s.replace("Z", "+00:00")
    m = re.match(r"(.*\.\d{3,6})\d*(\+00:00)", s)
    if m:
        s = m.group(1) + m.group(2)
    return datetime.fromisoformat(s).timestamp()


def load_bus(d, nodes=(0,)):
    recs = []
    for node in nodes:
        p = os.path.join(d, "bus-eventbus-default-stan-%d.jsonl" % node)
        if os.path.exists(p):
            for l in open(p):
                try:
                    j = json.loads(l)
                except Exception:
                    continue
                recs.append((ts(j["t"]), node, j))
    recs.sort(key=lambda x: x[0])
    return recs


def subs_of(j):
    return [s for s in (j.get("channel") or {}).get("subscriptions", []) if str(s.get("queue_name", "")).startswith("client-")]


def clients_of(j):
    return [c["id"] for c in (j.get("clients") or {}).get("clients", []) if c["id"].startswith("client-4092367313-")]


def analyse(d):
    sp = os.path.join(d, "summary.json")
    if not os.path.exists(sp):
        return None
    S = json.load(open(sp))
    name = S["name"]
    out = {"name": name, "kind": S.get("kind"), "outcome": S.get("outcome"), "latency": S.get("suspend_latency_s")}
    ev = S.get("events", {})
    if not ev or "E_start" not in ev:
        return out
    seqE = (S.get("bus_last_seq_before") or 0) + 1
    out["seqE"] = seqE
    out["old_client"] = S.get("old_client_id")
    tE = ev["E_start"]
    recs = load_bus(d)
    handed = None
    for t, node, j in recs:
        if t < tE - 1:
            continue
        for s in subs_of(j):
            if (s.get("last_sent") or 0) >= seqE:
                handed = (t, s.get("client_id"), s.get("pending_count"))
                break
        if handed:
            break
    if handed:
        out["handed_t"] = round(handed[0] - tE, 2)
        out["handed_to"] = handed[1]
        out["handed_to_killed_pod"] = handed[1] == S.get("old_client_id")
        # when did the pending stay and when did it leave that member
        cid = handed[1]
        left = None
        stuck_until = None
        for t, node, j in recs:
            if t <= handed[0]:
                continue
            mine = [s for s in subs_of(j) if s.get("client_id") == cid]
            if not mine or mine[0].get("pending_count") == 0:
                left = t
                break
        out["pending_left_member_t"] = round(left - tE, 2) if left else None
        # max live-member pending seen for the other member while E stuck
    if handed:
        samples = {}
        for off in (2, 30, 60, 120, 250):
            best = None
            for t, node, j in recs:
                if t <= tE + off:
                    best = j
            if best:
                samples["+%ds" % off] = [(x.get("client_id", "")[-3:], x.get("last_sent"), x.get("pending_count"), x.get("is_offline")) for x in subs_of(best)]
        out["subs_at"] = samples
    # bus log: heartbeat time-out of the killed client
    hb = None
    t_kill = ev.get("kill") or tE
    for p in glob.glob(os.path.join(d, "buslog-*.log")):
        for l in open(p, errors="replace"):
            if S.get("old_client_id") and S["old_client_id"] in l and "Timed out on heartbeats" in l:
                m = re.match(r"(\S+Z) ", l)
                if m and ts(m.group(1)) >= t_kill:
                    tt = ts(m.group(1))
                    hb = tt if hb is None else min(hb, tt)
    out["hb_timeout_t"] = round(hb - tE, 2) if hb else None
    if ev.get("kill"):
        out["kill_to_E"] = round(tE - ev["kill"], 2)
        out["hb_timeout_after_kill"] = round(hb - ev["kill"], 1) if hb else None
    # trigger lines in sensor logs
    trig = []
    for p in glob.glob(os.path.join(d, "sensor-*.log")):
        for l in open(p, errors="replace"):
            if "Successfully processed trigger" in l:
                m = re.match(r"(\S+Z) ", l)
                if m:
                    trig.append((ts(m.group(1)) - tE, os.path.basename(p)[-9:-4], l))
    out["triggers_rel_E"] = sorted((round(x[0], 2), x[1]) for x in trig)
    return out


def main():
    pre = sys.argv[1:] or ["X", "M", "F"]
    rows = []
    for d in sorted(glob.glob(os.path.join(EV, "*"))):
        b = os.path.basename(d)
        if not any(b.startswith(p) for p in pre) or not os.path.isdir(d):
            continue
        r = analyse(d)
        if r:
            rows.append(r)
    for r in rows:
        print(json.dumps(r))


if __name__ == "__main__":
    main()
