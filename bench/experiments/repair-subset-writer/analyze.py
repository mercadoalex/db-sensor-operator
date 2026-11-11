#!/usr/bin/env python3
"""Score the three repair strategies against the oracle, from CSV dumps. Part of the Bungie-shaped scenario.

Inputs (all in one run directory): oracle.csv (final oracle), oracle-pre.csv (oracle at the restore point),
bad-ledger.csv (id,tx the bad writer changed), state-a.csv, state-b.csv, state-c.csv, repair-report.json, summary.json.
CSV columns: id,server_id,account,item,qty,note,legit_ver,md5(body)   (writer_app is deliberately left out).
Writes metrics.json and prints it.
"""
import csv
import json
import sys

BAD_SERVERS = {3, 4, 5}


def load(path):
    rows = {}
    with open(path, newline="") as f:
        for r in csv.reader(f):
            rows[int(r[0])] = tuple(r)
    return rows


def ver(row):
    return int(row[6]) if row else 0


def main(d):
    oracle, pre = load(d + "/oracle.csv"), load(d + "/oracle-pre.csv")
    ledger = [l.strip().split(",") for l in open(d + "/bad-ledger.csv") if l.strip()]
    affected = {int(i) for i, _ in ledger}
    rep = json.load(open(d + "/repair-report.json"))
    conflict_ids = set(rep["column_conflict_ids"])
    summ = json.load(open(d + "/summary.json"))

    # W_post: legitimate writes after the restore point, from the oracle snapshots (a floor, see RESULTS.md)
    w_post = sum(ver(r) - ver(pre.get(i)) for i, r in oracle.items()) + sum(1 for i in pre if i not in oracle)

    out = {"affected_rows": len(affected), "w_post": w_post, "conflict_rows_flagged_by_stream": len(conflict_ids)}
    for name in "abc":
        s = load("%s/state-%s.csv" % (d, name))
        ids = set(oracle) | set(s)
        wrong = {i for i in ids if oracle.get(i) != s.get(i)}
        lost_ver = sum(max(ver(r) - ver(s.get(i)), 0) for i, r in oracle.items())
        undone = sum(1 for i in s if i not in oracle)
        wrong_other_servers = {i for i in wrong if int((oracle.get(i) or s.get(i))[1]) not in BAD_SERVERS}
        out[name] = {
            "affected_correct": len(affected - wrong),
            "affected_left_wrong": len(affected & wrong),
            "rows_wrong_total": len(wrong),
            "rows_wrong_outside_affected": len(wrong - affected),
            "rows_wrong_other_servers": len(wrong_other_servers),
            "legit_writes_lost": lost_ver + undone,
            "legit_lost_versions": lost_ver, "legit_deletes_undone": undone,
            "affected_left_wrong_ids_equal_conflicts": (affected & wrong) == conflict_ids if name == "a" else None,
        }
    out["time_s"] = {"a": summ["stream_total_s"], "b": summ["pitr_recovery_s"] + summ["rowcopy_s"], "c": summ["pitr_recovery_s"]}
    json.dump(out, open(d + "/metrics.json", "w"), indent=2)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main(sys.argv[1])
