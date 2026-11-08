#!/usr/bin/env python3
"""Undo the changes made by a set of bad transactions, using only the before and after images of the
change stream (Debezium events). Experiment P3, see run.sh and RESULTS.md. Not production code.

Every compensating statement is guarded: it changes a value only if the row still holds the value the bad
change wrote. A row that a later, legitimate write touched is therefore never overwritten; it is reported as a
conflict. Statements run in the reverse of the order the changes were committed, in one transaction.

Usage: repair.py --events events.jsonl --bad-txids 101,102 --psql "kubectl ... psql -X -U bench -d bench"
"""
import argparse
import json
import shlex
import subprocess
import sys
import time

PLACEHOLDER = "__debezium_unavailable_value"   # Debezium's marker for an unchanged TOAST column
TABLE = "public.rp_orders"
PK = "id"
# column -> type used to build a literal. Hand-written for the experiment's table.
COLS = {"id": "integer", "status": "text", "amount": "numeric", "qty": "integer", "note": "text",
        "updated_at": "timestamptz", "meta": "jsonb", "active": "boolean", "body": "text"}


def lit(col, v):
    if v is None:
        return "NULL"
    t = COLS[col]
    if t == "integer":
        return str(int(v))
    if t == "boolean":
        return "true" if v in (True, "true") else "false"
    s = v if isinstance(v, str) else json.dumps(v)
    return "'%s'::%s" % (s.replace("'", "''"), t)


def read_events(path):
    events, bad_lines = [], 0
    with open(path) as f:
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
                           "tx": int(p["source"]["txId"]) % (2 ** 32), "lsn": p["source"].get("lsn"),
                           "bytes": len(line) + 1})
    return events, bad_lines


def build(events, bad_tx):
    """Return (statements, meta, placeholders_seen). One statement per column, per reinsertion or per deletion."""
    stmts, meta, placeholders = [], [], 0
    for e in reversed([e for e in events if e["tx"] in bad_tx]):
        if e["op"] == "u":
            b, a = e["before"], e["after"]
            pk = b[PK]
            for col in COLS:
                if col == PK:
                    continue
                if a[col] == PLACEHOLDER:
                    placeholders += 1         # unchanged TOAST column: nothing to undo
                    continue
                if b[col] != a[col]:
                    stmts.append("UPDATE %s SET %s = %s WHERE %s = %s AND %s IS NOT DISTINCT FROM %s;" % (
                        TABLE, col, lit(col, b[col]), PK, lit(PK, pk), col, lit(col, a[col])))
                    meta.append(("restore", pk, col))
        elif e["op"] == "d":
            b = e["before"]
            cols = list(COLS)
            if any(b[c] == PLACEHOLDER for c in cols):
                raise SystemExit("a deleted row came with an unavailable column; it cannot be re-inserted exactly")
            stmts.append("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING;" % (
                TABLE, ", ".join(cols), ", ".join(lit(c, b[c]) for c in cols)))
            meta.append(("reinsert", b[PK], None))
        elif e["op"] == "c":
            a = e["after"]
            cols = list(COLS)
            stmts.append("DELETE FROM %s WHERE %s = %s AND ROW(%s) IS NOT DISTINCT FROM ROW(%s);" % (
                TABLE, PK, lit(PK, a[PK]), ", ".join(cols), ", ".join(lit(c, a[c]) for c in cols)))
            meta.append(("remove", a[PK], None))
    return stmts, meta, placeholders


def run_psql(psql, sql, single_tx=True):
    cmd = shlex.split(psql) + (["-1"] if single_tx else []) + ["-v", "ON_ERROR_STOP=1"]
    r = subprocess.run(cmd, input=sql, capture_output=True, text=True)
    return r.returncode, r.stdout, r.stderr


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True)
    ap.add_argument("--bad-txids", required=True)
    ap.add_argument("--psql", required=True)
    ap.add_argument("--report", default="")
    a = ap.parse_args()
    bad_tx = {int(x) % (2 ** 32) for x in a.bad_txids.split(",") if x}

    t0 = time.time()
    events, unparsable = read_events(a.events)
    stmts, meta, placeholders = build(events, bad_tx)
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
    zero_restores = [(m[1], m[2]) for m, n in zip(meta, affected) if m[0] == "restore" and n == 0]
    existing = set()
    if zero_restores:
        ids = sorted({i for i, _ in zero_restores})
        rc, out, err = run_psql(a.psql, "select %s from %s where %s in (%s);" % (PK, TABLE, PK, ",".join(map(str, ids))), False)
        existing = {int(x.strip()) for x in out.splitlines() if x.strip().isdigit()}
    gone = sorted({i for i, _ in zero_restores if i not in existing})
    col_conflicts = [(i, c) for i, c in zero_restores if i in existing]

    rep = {
        "events_in_file": len(events), "unparsable_lines": unparsable,
        "bad_events": sum(1 for e in events if e["tx"] in bad_tx),
        "bad_ops": {op: sum(1 for e in events if e["tx"] in bad_tx and e["op"] == op) for op in ("c", "u", "d")},
        "file_bytes": sum(e["bytes"] for e in events),
        "bad_event_bytes": sum(e["bytes"] for e in events if e["tx"] in bad_tx),
        "statements": len(stmts),
        "columns_restored": sum(1 for m, n in zip(meta, affected) if m[0] == "restore" and n == 1),
        "column_conflicts": len(col_conflicts),
        "column_conflict_ids": sorted({i for i, _ in col_conflicts}),
        "column_conflict_columns": sorted({c for _, c in col_conflicts}),
        "rows_gone": len(gone), "rows_gone_ids": gone,
        "rows_reinserted": sum(1 for m, n in zip(meta, affected) if m[0] == "reinsert" and n == 1),
        "reinsert_conflicts": sum(1 for m, n in zip(meta, affected) if m[0] == "reinsert" and n == 0),
        "rows_removed": sum(1 for m, n in zip(meta, affected) if m[0] == "remove" and n == 1),
        "remove_conflicts": sum(1 for m, n in zip(meta, affected) if m[0] == "remove" and n == 0),
        "unchanged_toast_placeholders_skipped": placeholders,
        "seconds_build": round(t_build, 2), "seconds_apply": round(t_apply, 2),
    }
    if a.report:
        json.dump(rep, open(a.report, "w"), indent=2)
    print(json.dumps(rep, indent=2))


if __name__ == "__main__":
    main()
