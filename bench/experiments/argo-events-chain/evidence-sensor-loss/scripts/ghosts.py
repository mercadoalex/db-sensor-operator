#!/usr/bin/env python3
"""For each Sensor pod that was shut down, how long did its STAN subscription / client stay listed on the bus (node 0 view)?"""
import glob, json, os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import analyze as A

T = A.pod_table()
recs = []
for p in sorted(glob.glob(os.path.join(A.EV, "[0-9]*", "bus-eventbus-default-stan-0.jsonl"))):
    run = os.path.basename(os.path.dirname(p))
    for l in open(p):
        j = json.loads(l)
        recs.append((A.ts(j["t"]), run, j))
recs.sort(key=lambda x: x[0])
rows = []
pods = sorted([v for v in T.values() if v["cid"] and v["started"]], key=lambda v: v["started"])
for i, v in enumerate(pods):
    if not v["shutdown"]:
        continue
    nxt = [w["started"] for w in pods if w["cid"] == v["cid"] and w["started"] > v["started"]]
    horizon = min(nxt) if nxt else 1e18
    S = v["shutdown"]
    sub_gone = cl_gone = None
    sub_seen_after = cl_seen_after = False
    last_obs = None
    offline_at = None
    for t, run, j in recs:
        if t < S or t >= horizon:
            continue
        last_obs = t
        subs = {x["client_id"]: x for x in j["channel"].get("subscriptions", [])}
        cl = {c["id"] for c in j["clients"].get("clients", [])}
        x = subs.get(v["cid"])
        if x is not None and not x["is_offline"]:
            sub_seen_after = True
        if x is None and sub_gone is None:
            sub_gone = t
        if x is not None and x["is_offline"] and offline_at is None:
            offline_at = t
        if v["cid"] not in cl and cl_gone is None:
            cl_gone = t
    rows.append((v["pod"], v["cid"][-3:], S, sub_gone, offline_at, cl_gone, last_obs, sub_seen_after))
print("pod            cid  | shutdown | sub removed(+s) / offline(+s) | client removed(+s) | last observed(+s) | sub still ONLINE after shutdown")
for pod, cid, S, sg, off, cg, lo, sa in rows:
    f = lambda t: "%7.1f" % (t - S) if t else "      -"
    print("%-14s %-4s | %s | %s / %s | %s | %s | %s" % (pod[-5:], cid, __import__("datetime").datetime.fromtimestamp(S, __import__("datetime").timezone.utc).strftime("%H:%M:%S"), f(sg), f(off), f(cg), f(lo), sa))
