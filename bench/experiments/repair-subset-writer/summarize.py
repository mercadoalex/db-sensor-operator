#!/usr/bin/env python3
"""Judge the pre-registered hypotheses H1 to H6 (see RESULTS.md) over all runs in raw/, and print the tables.

Usage: summarize.py [raw_dir]   (default: raw/ next to this script; runs named run-* )
The criteria below are a literal transcription of the hypotheses in RESULTS.md. Do not edit them after a run.
The padded run is the one whose summary.json has pad_passes > 0.
"""
import glob
import json
import os
import statistics
import sys

BAD_APP = "inventory-patch-v2"
HERE = os.path.dirname(os.path.abspath(__file__))
raw = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "raw")
runs = []
for d in sorted(glob.glob(os.path.join(raw, "run-*"))):
    if not os.path.exists(d + "/metrics.json"):
        print("INCOMPLETE run (no metrics.json), counted as a failed run: " + d)
        continue
    runs.append({"dir": d, "name": os.path.basename(d), "m": json.load(open(d + "/metrics.json")),
                 "s": json.load(open(d + "/summary.json")), "r": json.load(open(d + "/repair-report.json")),
                 "ledger": [l.strip().split(",") for l in open(d + "/bad-ledger.csv") if l.strip()]})
plain = [r for r in runs if r["s"]["pad_passes"] == 0]
padded = [r for r in runs if r["s"]["pad_passes"] > 0]
verdicts = {}


def judge(h, r, ok, why):
    verdicts.setdefault(h, []).append((r["name"], ok, why))


for r in runs:
    m, s, rep = r["m"], r["s"], r["r"]
    a, b, c = m["a"], m["b"], m["c"]
    # H1
    bad_keys = [k for k in rep["source_keys_seen"] if any(w in k.lower() for w in ("application", "client", "user"))]
    ledger_tx = {int(t) % 2 ** 32 for _, t in r["ledger"]}
    h1 = not bad_keys and set(rep["tagged_txids"]) == ledger_tx and rep["tagged_events"] == len(r["ledger"])
    judge("H1", r, h1, "source keys with application/client/user: %s; tagged txids == ledger txids: %s; tagged events %d vs ledger rows %d"
          % (bad_keys, set(rep["tagged_txids"]) == ledger_tx, rep["tagged_events"], len(r["ledger"])))
    # H2
    h2 = (a["rows_wrong_outside_affected"] == 0 and a["legit_writes_lost"] == 0 and a["affected_left_wrong_ids_equal_conflicts"]
          and rep["column_conflicts"] >= 10 and len(rep["column_conflict_ids"]) >= 10)
    judge("H2", r, bool(h2), "outside affected wrong %d, legit lost %d, left-wrong == flagged: %s, conflict rows %d"
          % (a["rows_wrong_outside_affected"], a["legit_writes_lost"], a["affected_left_wrong_ids_equal_conflicts"], len(rep["column_conflict_ids"])))
    # H3
    ratio = c["legit_writes_lost"] / m["w_post"] if m["w_post"] else 0
    h3 = 0.95 <= ratio <= 1.0 and c["rows_wrong_total"] >= 10 * a["rows_wrong_total"] and a["rows_wrong_total"] > 0
    judge("H3", r, h3, "(c) lost/W_post = %d/%d = %.3f; rows wrong (c) %d vs (a) %d"
          % (c["legit_writes_lost"], m["w_post"], ratio, c["rows_wrong_total"], a["rows_wrong_total"]))
    # H4
    h4 = (b["rows_wrong_other_servers"] == 0 and b["legit_writes_lost"] > 0 and b["rows_wrong_total"] > a["rows_wrong_total"]
          and b["rows_wrong_total"] < c["rows_wrong_total"])
    judge("H4", r, h4, "(b) wrong on other servers %d, legit lost %d, rows wrong (a) %d < (b) %d < (c) %d"
          % (b["rows_wrong_other_servers"], b["legit_writes_lost"], a["rows_wrong_total"], b["rows_wrong_total"], c["rows_wrong_total"]))

for r in plain:
    t = r["m"]["time_s"]
    judge("H5", r, max(t.values()) < 10 and t["c"] < t["a"], "time_s a=%.2f b=%.2f c=%.2f" % (t["a"], t["b"], t["c"]))

if plain and padded:
    mean_rec = statistics.mean(r["s"]["pitr_recovery_s"] for r in plain)
    mean_str = statistics.mean(r["s"]["stream_total_s"] for r in plain)
    for r in padded:
        judge("H6", r, r["s"]["pitr_recovery_s"] >= 2 * mean_rec and r["s"]["stream_total_s"] <= 2 * mean_str,
              "recovery %.2f s vs unpadded mean %.2f s (needs >= 2x); stream %.2f s vs unpadded mean %.2f s (needs <= 2x)"
              % (r["s"]["pitr_recovery_s"], mean_rec, r["s"]["stream_total_s"], mean_str))
else:
    verdicts["H6"] = [("-", False, "needs at least one unpadded and one padded run")]

print("\n### Per-run metrics (rows; 'lost' = legitimate writes lost)\n")
print("| run | pad WAL (MB) | affected | W_post | (a) fixed / left wrong / outside / lost | (b) fixed / left wrong / outside / lost | (c) fixed / left wrong / outside / lost | rows wrong a / b / c |")
print("|---|---|---|---|---|---|---|---|")
for r in runs:
    m = r["m"]
    f = lambda x: "%d / %d / %d / %d" % (x["affected_correct"], x["affected_left_wrong"], x["rows_wrong_outside_affected"], x["legit_writes_lost"])
    print("| %s | %.0f | %d | %d | %s | %s | %s | %d / %d / %d |" % (
        r["name"], r["s"]["wal_bytes_since_pre_backup_point"] / 1e6, m["affected_rows"], m["w_post"], f(m["a"]), f(m["b"]), f(m["c"]),
        m["a"]["rows_wrong_total"], m["b"]["rows_wrong_total"], m["c"]["rows_wrong_total"]))
print("\n### Per-run times (seconds)\n")
print("| run | (a) fetch + repair = total | PITR recovery | (b) recovery + row copy | (c) recovery |")
print("|---|---|---|---|---|")
for r in runs:
    s = r["s"]
    print("| %s | %.2f + %.2f = %.2f | %.2f | %.2f | %.2f |" % (r["name"], s["stream_fetch_s"], s["stream_repair_s"], s["stream_total_s"],
                                                           s["pitr_recovery_s"], s["pitr_recovery_s"] + s["rowcopy_s"], s["pitr_recovery_s"]))
print("\n### Hypotheses\n")
for h in sorted(verdicts):
    v = verdicts[h]
    held = sum(1 for _, ok, _ in v if ok)
    print("%s: %s (%d of %d runs)" % (h, "HELD" if held == len(v) else "FAILED", held, len(v)))
    for n, ok, why in v:
        print("   %s %s: %s" % ("ok  " if ok else "FAIL", n, why))
