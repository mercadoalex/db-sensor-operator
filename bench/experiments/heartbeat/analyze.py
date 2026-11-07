#!/usr/bin/env python3
"""Analysis for the heartbeat experiment (A1 and E1). See ADR-018.

Inputs (all in one results directory):
  receiver.log  lines "REQ {json}" captured from the hb-receiver pod (raw, untouched)
  samples.csv   variant,epoch_s,current_lsn,confirmed_flush_lsn,restart_lsn,active,lag_bytes
  phases.tsv    variant<TAB>phase<TAB>start_epoch_s<TAB>end_epoch_s   (phase: silent|noise|interleave)
  params.env    INTERVAL_MS, SILENT, NOISE, ...

Clocks: receiver arrival times come from the pod clock, samples and phases from the PostgreSQL
clock. Both run in the same kind node VM, so they share a clock source.
"""
import csv
import json
import os
import statistics
import sys

d = sys.argv[1]


def read_params():
    p = {}
    with open(os.path.join(d, "params.env")) as f:
        for line in f:
            line = line.strip()
            if "=" in line:
                k, v = line.split("=", 1)
                p[k] = v
    return p


def lsn(s):
    hi, lo = s.split("/")
    return (int(hi, 16) << 32) + int(lo, 16)


params = read_params()
interval_s = int(params["INTERVAL_MS"]) / 1000.0

# ---- receiver log --------------------------------------------------------------------------
reqs = []
with open(os.path.join(d, "receiver.log")) as f:
    for line in f:
        if not line.startswith("REQ "):
            continue
        try:
            r = json.loads(line[4:])
        except ValueError:
            continue
        r["variant"] = r["path"].strip("/")
        # keys / op / ts_ms / envelope were computed by the receiver on the full body.
        reqs.append(r)

# ---- phases and samples --------------------------------------------------------------------
phases = {}
with open(os.path.join(d, "phases.tsv")) as f:
    for line in f:
        v, ph, s, e = line.rstrip("\n").split("\t")
        phases[(v, ph)] = (int(s), int(e))

samples = {}
with open(os.path.join(d, "samples.csv")) as f:
    for row in csv.DictReader(f):
        samples.setdefault(row["variant"], []).append(
            {
                "t": int(row["epoch_s"]),
                "cur": lsn(row["current_lsn"]),
                "lag": int(row["lag_bytes"]),
            }
        )

out = []


def p(*a):
    s = " ".join(str(x) for x in a)
    print(s)
    out.append(s)


def in_phase(r, v, ph):
    s, e = phases[(v, ph)]
    return r["variant"] == v and s * 1000 <= r["t"] <= e * 1000


def is_event(r):
    return r["op"] is not None or "source" in r["keys"]


p("== requests received")
for v in ("nohb", "hb"):
    vr = [r for r in reqs if r["variant"] == v]
    p(f"  {v}: {len(vr)} total, {sum(1 for r in vr if is_event(r))} with op/source, "
      f"{sum(1 for r in vr if not is_event(r))} without")

# ---- A1 -------------------------------------------------------------------------------------
p("\n== A1: does the HTTP sink deliver heartbeats, and what do they look like?")
a1 = "INCONCLUSIVE"
for v in ("nohb", "hb"):
    cand = [r for r in reqs if in_phase(r, v, "silent")]
    s, e = phases[(v, "silent")]
    p(f"  [{v}] silent window {e - s}s, no writes at all: {len(cand)} request(s)")
    if v == "nohb":
        control_ok = len(cand) == 0
        p(f"    control (no heartbeat configured, expect 0): {'ok' if control_ok else 'UNEXPECTED'}")
        continue
    expected = (e - s) / interval_s
    p(f"    expected about {expected:.0f} at interval {interval_s:g}s")
    if not cand:
        a1 = "FALSE (zero requests in the silent window)"
        continue
    shapes = {tuple(r["keys"]) for r in cand}
    envs = {r["envelope"] for r in cand}
    clean = all("ts_ms" in r["keys"] and not ({"source", "op", "before", "after", "ddl"} & set(r["keys"]))
                for r in cand)
    arrivals = sorted(r["t"] for r in cand)
    gaps = [(b - a) / 1000.0 for a, b in zip(arrivals, arrivals[1:])]
    delays = [(r["t"] - r["ts_ms"]) / 1000.0 for r in cand if isinstance(r["ts_ms"], int)]
    p(f"    payload key sets: {sorted(shapes)}")
    p(f"    wrapped in schema/payload envelope: {sorted(envs)}")
    if gaps:
        p(f"    gap between heartbeats: median {statistics.median(gaps):.1f}s, max {max(gaps):.1f}s")
    if delays:
        p(f"    arrival minus ts_ms: median {statistics.median(delays):.2f}s, max {max(delays):.2f}s")
    hk = {k.lower() for r in cand for k in r["headers"]}
    p(f"    request headers seen: {sorted(hk)}")
    topic_hdr = [k for k in hk if "topic" in k or "debezium" in k or "key" in k]
    p(f"    headers that could carry a topic or key: {topic_hdr or 'none'}")
    p(f"    sample body: {cand[0]['body'][:300]}")
    if len(cand) >= 0.5 * expected and clean:
        a1 = "PASS (heartbeats arrive; ts_ms present; no source/op/before/after)"
    elif len(cand) >= 0.5 * expected:
        a1 = "OTHER SHAPE (arrive, but the body has fields the classifier rule did not expect)"
    else:
        a1 = f"PARTIAL ({len(cand)} of about {expected:.0f} expected)"

    # interleave: do events and heartbeats coexist?
    if (v, "interleave") in phases:
        il = [r for r in reqs if in_phase(r, v, "interleave")]
        ev = [r for r in il if is_event(r)]
        hbs = [r for r in il if not is_event(r)]
        p(f"    interleave window (writes to the monitored table): {len(ev)} event(s), "
          f"{len(hbs)} heartbeat(s)")
p(f"  A1 verdict: {a1}")

# ---- E1 -------------------------------------------------------------------------------------
p("\n== E1: does the heartbeat keep confirmed_flush_lsn moving when every change is filtered?")
res = {}
for v in ("nohb", "hb"):
    s, e = phases[(v, "noise")]
    ss = [x for x in samples.get(v, []) if s <= x["t"] <= e + 1]
    if len(ss) < 3:
        p(f"  [{v}] not enough samples in the noise window ({len(ss)})")
        continue
    wal = ss[-1]["cur"] - ss[0]["cur"]
    growth = ss[-1]["lag"] - ss[0]["lag"]
    res[v] = {"wal": wal, "start": ss[0]["lag"], "end": ss[-1]["lag"], "max": max(x["lag"] for x in ss),
              "growth": growth}
    p(f"  [{v}] noise {e - s}s: WAL written {wal / 1024:.0f} KiB; "
      f"lag start {ss[0]['lag'] / 1024:.0f} KiB, end {ss[-1]['lag'] / 1024:.0f} KiB, "
      f"max {res[v]['max'] / 1024:.0f} KiB, growth {growth / 1024:.0f} KiB")
    series = " ".join(f"{x['lag'] // 1024}" for x in ss)
    p(f"        lag KiB over time: {series}")

if "nohb" in res and "hb" in res:
    base = res["nohb"]
    hbv = res["hb"]
    demonstrates = base["wal"] >= 1024 * 1024 and base["growth"] >= 0.5 * base["wal"]
    if not demonstrates:
        e1 = ("INCONCLUSIVE (the baseline did not stall: it grew "
              f"{base['growth'] / 1024:.0f} KiB of {base['wal'] / 1024:.0f} KiB written; "
              "the problem was not reproduced, so a pass would mean nothing)")
    # The heartbeat lag is a sawtooth (it drops at every heartbeat), so the value at the last
    # sample depends on where in the tooth we stopped. Compare the MAXIMUM instead: a bounded
    # lag peaks near interval x write rate, while an unbounded one reaches the baseline.
    elif hbv["max"] <= 0.2 * base["growth"]:
        e1 = (f"PASS (with heartbeat the lag peaked at {hbv['max'] / 1024:.0f} KiB vs "
              f"{base['growth'] / 1024:.0f} KiB of growth without)")
    else:
        e1 = (f"FAIL (with heartbeat the lag peaked at {hbv['max'] / 1024:.0f} KiB vs "
              f"{base['growth'] / 1024:.0f} KiB of growth without; expected at most 20%)")
    p(f"  E1 verdict: {e1}")
else:
    p("  E1 verdict: INCONCLUSIVE (missing samples)")

with open(os.path.join(d, "summary.txt"), "w") as f:
    f.write("\n".join(out) + "\n")
