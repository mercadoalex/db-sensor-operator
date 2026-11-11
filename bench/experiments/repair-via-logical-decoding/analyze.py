#!/usr/bin/env python3
"""Judge H1 to H6 of the Matrix-shaped scenario for ONE run directory, literally as pre-registered in RESULTS.md.
Inputs in the run directory (.gz accepted): config.json, pre-<t>.json / post-<t>.json (seqscan dumps of the heap),
incident-<t>.txt (TX, EXPLAIN, RETURNING * of the DELETE), amcheck-<t>.rc/.err, stats-before.txt, stats-after.txt,
events.jsonl, stream.json, fixed-<t>.json (FULL tables). Writes metrics.json and prints a table.
The events are never used to build the ground truth; the ground truth is the heap dumps and RETURNING.
"""
import gzip
import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal

TABLES = ["sc2_full_bad", "sc2_full_ok", "sc2_pk_bad", "sc2_pk_ok"]
COLS = ["id", "state_group", "event_id", "depth", "amount", "ts", "content"]
NONKEY = [c for c in COLS if c != "id"]
EXPECT = {5: (30, 15, 15), 370: (30, 30, 30)}  # SHIFT -> (removed, foreign, missed) on the corrupted tables


def read(path):
    if os.path.exists(path):
        return open(path).read()
    return gzip.open(path + ".gz", "rt").read()


def jload(path):
    return json.loads(read(path), parse_float=Decimal)


def norm(col, v):
    if v is None:
        return None
    if col == "amount":
        return Decimal(str(v)).quantize(Decimal("0.01"))
    if col == "ts":
        s = str(v).replace("Z", "+00:00").replace(" ", "T")
        d = datetime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc)
    if col in ("id", "state_group", "depth"):
        return int(v)
    return v


def nrow(r):
    return {c: norm(c, r.get(c)) for c in COLS}


def main(d):
    cfg = jload(f"{d}/config.json")
    shift, targets = int(cfg["shift"]), set(cfg["targets"])
    out = {"run": cfg["run"], "shift": shift, "tables": {}}
    # events
    events = [json.loads(l) for l in read(f"{d}/events.jsonl").splitlines() if l.strip()]
    stream = jload(f"{d}/stream.json")
    # stats
    def stats(path):
        s = {}
        for l in read(path).splitlines():
            p = l.split()
            if len(p) == 3:
                s[p[0]] = (int(p[1].split("=")[1]), int(p[2].split("=")[1]))
        return s
    sb, sa = stats(f"{d}/stats-before.txt"), stats(f"{d}/stats-after.txt")
    removed_total = 0
    for t in TABLES:
        bad = t.endswith("_bad")
        full = "_full_" in t
        m = {"corrupted": bad, "identity": "FULL" if full else "default"}
        pre = {int(r["id"]): nrow(r) for r in jload(f"{d}/pre-{t}.json")}
        post = {int(r["id"]) for r in jload(f"{d}/post-{t}.json")}
        txt = read(f"{d}/incident-{t}.txt")
        tx = int(txt.split("TX ")[1].split()[0])
        explain = txt.split("\n", 1)[1].split("REMOVED_BEGIN")[0]
        returned = json.loads(txt.split("REMOVED_BEGIN")[1].split("REMOVED_END")[0].strip(), parse_float=Decimal)
        removed_rows = {int(r["id"]): nrow(r) for r in returned}
        removed = set(removed_rows)
        intended = {i for i, r in pre.items() if r["state_group"] in targets}
        foreign, missed = removed - intended, intended - removed
        removed_total += len(removed)
        m.update(removed=len(removed), intended=len(intended), foreign=len(foreign), missed=len(missed),
                 foreign_heap_groups=sorted({pre[i]["state_group"] for i in foreign}),
                 removed_equals_seqscan_diff=(set(pre) - post) == removed,
                 returning_equals_snapshot=all(removed_rows[i] == pre[i] for i in removed))
        # H1
        ac_rc = int(read(f"{d}/amcheck-{t}.rc").strip())
        ac_err = read(f"{d}/amcheck-{t}.err").strip().splitlines()
        m["amcheck_rc"], m["amcheck_first_line"] = ac_rc, (ac_err[0] if ac_err else "")
        a_ok = (ac_rc != 0) if bad else (ac_rc == 0)
        idx = f"{t}_grp_idx"
        b_ok = (f"Index Scan using {idx}" in explain and "Seq Scan" not in explain and "Bitmap" not in explain
                and sa[t][1] - sb[t][1] >= 1 and sa[t][0] - sb[t][0] == 0)
        m["explain"] = explain.strip().splitlines()
        m["idx_scan_delta"], m["seq_scan_delta"] = sa[t][1] - sb[t][1], sa[t][0] - sb[t][0]
        if bad:
            c_ok = len(foreign) >= 1 and len(missed) >= 1 and (len(removed), len(foreign), len(missed)) == EXPECT.get(shift, (None,) * 3)
        else:
            c_ok = len(removed) == 30 and not foreign and not missed
        m["h1"] = {"a_amcheck": a_ok, "b_index_driven": b_ok, "c_counts": c_ok, "all": a_ok and b_ok and c_ok}
        # events for this table
        ev = [e for e in events if e.get("source", {}).get("table") == t]
        ev_d = [e for e in ev if e.get("op") == "d"]
        ids = [int(e["before"]["id"]) for e in ev_d]
        m["events"], m["events_op_d"] = len(ev), len(ev_d)
        same_set = len(ids) == len(set(ids)) and set(ids) == removed
        m["event_ids_equal_removed"] = same_set
        m["event_foreign_present"] = foreign <= set(ids)
        m["event_missed_absent"] = not (missed & set(ids))
        txs = {e["source"]["txId"] for e in ev}
        m["event_txids"], m["delete_txid"] = sorted(txs), tx
        m["h6_tx"] = txs == {tx}
        mism, nonnull, cells = 0, 0, 0
        for e in ev_d:
            b = e["before"]
            i = int(b["id"])
            if i in pre:
                if full:
                    nb = nrow(b)
                    for c in COLS:
                        cells += 1
                        if nb[c] != pre[i][c]:
                            mism += 1
                else:
                    nonnull += sum(1 for c in NONKEY if b.get(c) is not None)
        if full:
            m["before_cells_compared"], m["before_cells_mismatching"] = cells, mism
            ok = same_set and mism == 0 and m["event_foreign_present"] and m["event_missed_absent"]
        else:
            m["before_nonkey_values_present"] = nonnull
            ok = same_set and nonnull == 0 and m["event_foreign_present"] and m["event_missed_absent"]
        m["identity_criteria"] = ok
        # H5
        if full:
            fixed = {int(r["id"]): nrow(r) for r in jload(f"{d}/fixed-{t}.json")}
            diff = sum(1 for i in set(fixed) | set(pre) if fixed.get(i) != pre.get(i))
            m["h5_rows_differing_after_reinsert"] = diff
            m["h5"] = diff == 0
        out["tables"][t] = m
    T = out["tables"]
    out["stream"] = {"events_received": stream["events_received"], "removed_total": removed_total,
                     "only_op_d": all(e.get("op") == "d" for e in events)}
    h = {}
    h["H1"] = all(T[t]["h1"]["all"] for t in TABLES)
    h["H2"] = T["sc2_full_bad"]["identity_criteria"]
    h["H3"] = T["sc2_pk_bad"]["identity_criteria"]
    h["H4"] = T["sc2_full_ok"]["identity_criteria"] and T["sc2_pk_ok"]["identity_criteria"]
    h["H5"] = T["sc2_full_bad"]["h5"] and T["sc2_full_ok"]["h5"]
    h["H6"] = (all(T[t]["h6_tx"] for t in TABLES) and out["stream"]["events_received"] == removed_total
               and out["stream"]["only_op_d"] and len(events) == removed_total)
    out["hypotheses"] = h
    out["valid_run"] = h["H1"] and out["stream"]["events_received"] == removed_total
    json.dump(out, open(f"{d}/metrics.json", "w"), indent=1, default=str)
    print(f"run {cfg['run']}  SHIFT={shift}  valid(H1 held and stream complete)={out['valid_run']}")
    print(f"{'table':14} {'identity':8} {'corrupt':7} {'removed':>7} {'foreign':>7} {'missed':>6} {'amcheck rc':>10} {'events':>6} {'ids==removed':>12} {'mismatch/nonkey':>15}")
    for t in TABLES:
        m = T[t]
        extra = m.get("before_cells_mismatching", m.get("before_nonkey_values_present"))
        print(f"{t:14} {m['identity']:8} {str(m['corrupted']):7} {m['removed']:7} {m['foreign']:7} {m['missed']:6} {m['amcheck_rc']:10} {m['events_op_d']:6} {str(m['event_ids_equal_removed']):>12} {extra!s:>15}")
    print("hypotheses:", ", ".join(f"{k}={'held' if v else 'FAILED'}" for k, v in h.items()))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1].rstrip("/")))
