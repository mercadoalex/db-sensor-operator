#!/usr/bin/env python3
"""Undo the changes of ONE writer, identified by a tag carried in the event (not by txId), using only the before and
after images of the change stream. Adapted from ../repair-from-stream/repair.py. Not production code.

An event belongs to the bad writer if op == 'u' and after[TAG_COL] == --tag. Every compensating statement is guarded:
it changes a column only if the row still holds the value the bad write left. A row that a later, legitimate write
touched is therefore never overwritten; it is reported as a conflict. Statements run in the reverse of the order the
events arrived, in one transaction.

Usage: repair.py --events events.jsonl[.gz] --tag inventory-patch-v2 --table public.sc_work_a --psql "kubectl ... psql ..."
"""
import argparse
import gzip
import json
import shlex
import subprocess
import sys
import time

PLACEHOLDER = "__debezium_unavailable_value"   # Debezium's marker for an unchanged TOAST column
PK = "id"
TAG_COL = "writer_app"
# column -> type used to build a literal. Hand-written for the experiment's table.
COLS = {"id": "integer", "server_id": "integer", "account": "integer", "item": "text", "qty": "integer",
        "note": "text", "legit_ver": "integer", "writer_app": "text", "body": "text"}
SKIP = {PK, TAG_COL}        # the tag column is bookkeeping, not data to restore


def lit(col, v):
    if v is None:
        return "NULL"
    t = COLS[col]
    if t == "integer":
        return str(int(v))
    s = v if isinstance(v, str) else json.dumps(v)
    return "'%s'::%s" % (s.replace("'", "''"), t)


def read_events(path):
    events, bad_lines = [], 0
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt") as f:
        for n, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except ValueError:
                bad_lines += 1
                continue
            p = d.get("payload", d)
            if not isinstance(p, dict) or "op" not in p or "source" not in p:
                continue                      # a heartbeat or anything that is not a change
            events.append({"n": n, "op": p["op"], "before": p.get("before"), "after": p.get("after"),
                           "tx": int(p["source"]["txId"]) % (2 ** 32), "source_keys": sorted(p["source"].keys()),
                           "bytes": len(line) + 1})
    return events, bad_lines


def is_bad(e, tag):
    return e["op"] == "u" and e["after"] is not None and e["after"].get(TAG_COL) == tag


def build(events, tag, table):
    """Return (statements, meta, placeholders_seen). One guarded statement per changed column."""
    stmts, meta, placeholders = [], [], 0
    for e in reversed([e for e in events if is_bad(e, tag)]):
        b, a = e["before"], e["after"]
        pk = b[PK]
        for col in COLS:
            if col in SKIP:
                continue
            if a[col] == PLACEHOLDER:
                placeholders += 1             # unchanged TOAST column: nothing to undo
                continue
            if b[col] != a[col]:
                stmts.append("UPDATE %s SET %s = %s WHERE %s = %s AND %s IS NOT DISTINCT FROM %s;" % (
                    table, col, lit(col, b[col]), PK, lit(PK, pk), col, lit(col, a[col])))
                meta.append(("restore", pk, col))
    return stmts, meta, placeholders


def run_psql(psql, sql, single_tx=True):
    cmd = shlex.split(psql) + (["-1"] if single_tx else []) + ["-v", "ON_ERROR_STOP=1"]
    r = subprocess.run(cmd, input=sql, capture_output=True, text=True)
    return r.returncode, r.stdout, r.stderr


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--tag", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--psql", required=True)
    ap.add_argument("--report", default="")
    a = ap.parse_args()

    t0 = time.time()
    events, unparsable = read_events(a.events)
    stmts, meta, placeholders = build(events, a.tag, a.table)
    t_build = time.time() - t0

    t1 = time.time()
    rc, out, err = run_psql(a.psql, "\n".join(stmts) + "\n")
    t_apply = time.time() - t1
    if rc != 0:
        print("REPAIR FAILED, transaction rolled back:\n" + err[-1500:], file=sys.stderr)
        sys.exit(2)
    tags = [l for l in out.splitlines() if l.startswith(("UPDATE ", "DELETE ", "INSERT "))]
    if len(tags) != len(stmts):
        print("tag count %d != statement count %d" % (len(tags), len(stmts)), file=sys.stderr)
        sys.exit(3)
    affected = [int(t.split()[-1]) for t in tags]

    # classify the guarded updates that changed nothing: the row is gone, or a later write changed the column
    zero = [(m[1], m[2]) for m, n in zip(meta, affected) if m[0] == "restore" and n == 0]
    existing = set()
    if zero:
        ids = sorted({i for i, _ in zero})
        rc, out, err = run_psql(a.psql, "select %s from %s where %s in (%s);" % (PK, a.table, PK, ",".join(map(str, ids))), False)
        existing = {int(x.strip()) for x in out.splitlines() if x.strip().isdigit()}
    gone = sorted({i for i, _ in zero if i not in existing})
    conflicts = [(i, c) for i, c in zero if i in existing]
    tagged = [e for e in events if is_bad(e, a.tag)]

    rep = {
        "events_in_file": len(events), "unparsable_lines": unparsable,
        "tagged_events": len(tagged), "tagged_txids": sorted({e["tx"] for e in tagged}),
        "source_keys_seen": sorted({k for e in events for k in e["source_keys"]}),
        "file_bytes": sum(e["bytes"] for e in events), "tagged_event_bytes": sum(e["bytes"] for e in tagged),
        "statements": len(stmts),
        "columns_restored": sum(1 for m, n in zip(meta, affected) if m[0] == "restore" and n == 1),
        "column_conflicts": len(conflicts),
        "column_conflict_ids": sorted({i for i, _ in conflicts}),
        "column_conflict_columns": sorted({c for _, c in conflicts}),
        "rows_gone": len(gone), "rows_gone_ids": gone,
        "seconds_build": round(t_build, 2), "seconds_apply": round(t_apply, 2),
    }
    if a.report:
        json.dump(rep, open(a.report, "w"), indent=2)
    print(json.dumps({k: v for k, v in rep.items() if not k.endswith("_ids") and k != "tagged_txids"}, indent=2))


if __name__ == "__main__":
    main()
