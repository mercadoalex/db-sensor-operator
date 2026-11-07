#!/usr/bin/env python3
"""Parse the OpenTelemetry collector's `debug` exporter output (verbosity: detailed) and the
CDC receiver's EVENT lines, then print the Postgres spans and how each COMMIT span lines up with
the CDC commit time (ts_ms)."""
import re
import sys
from datetime import datetime, timezone


def parse_time(s):
    # e.g. "2026-11-06 15:50:12.123456789 +0000 UTC"
    m = re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)(\.\d+)?", s.strip())
    if not m:
        return None
    base = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    frac = float("0" + (m.group(2) or ".0"))
    return base.timestamp() + frac


def parse_spans(path):
    spans, res, cur, mode = [], {}, None, None
    for raw in open(path, errors="replace"):
        line = raw.rstrip("\n")
        if "ResourceSpans #" in line:
            res, cur, mode = {}, None, "res-wait"
        elif line.startswith("Resource attributes:"):
            mode = "res"
        elif line.startswith("Span #"):
            cur = {"res": dict(res), "attrs": {}}
            spans.append(cur)
            mode = "span"
        elif line.strip().startswith("Name") and cur is not None and mode == "span" and ":" in line:
            cur["name"] = line.split(":", 1)[1].strip()
        elif line.strip().startswith("Start time") and cur is not None:
            cur["start"] = parse_time(line.split(":", 1)[1])
        elif line.strip().startswith("End time") and cur is not None:
            cur["end"] = parse_time(line.split(":", 1)[1])
        elif line.startswith("Attributes:"):
            mode = "attrs"
        elif "->" in line:
            m = re.match(r"\s*-> ([\w.]+): \w+\((.*)\)\s*$", line)
            if not m:
                continue
            key, val = m.groups()
            if mode == "res":
                res[key] = val
            elif mode == "attrs" and cur is not None:
                cur["attrs"][key] = val
    return spans


def parse_cdc(path):
    out = {}
    for line in open(path):
        m = re.search(r"txId=(\d+) lsn=(\d+) customer=(\d+) ts_ms=(\d+)", line)
        if m:
            out[int(m.group(3))] = {"txId": m.group(1), "ts": int(m.group(4)) / 1000.0}
    return out


def main():
    spans = parse_spans(sys.argv[1])
    cdc = parse_cdc(sys.argv[2])
    pg = [s for s in spans if s["attrs"].get("db.system") == "postgresql"]
    print("total spans: %d, postgresql spans: %d" % (len(spans), len(pg)))
    print("resource keys on a Postgres span:", sorted(pg[0]["res"].keys()) if pg else "-")
    print("span attribute keys:", sorted(pg[0]["attrs"].keys()) if pg else "-")
    mine = [s for s in pg if re.search(r"orders|commit|begin|pg_sleep", s["attrs"].get("db.statement", ""), re.I)]
    print("\nPostgres spans from the clients (statement | container | start..end | duration):")
    t0 = min((s["start"] for s in mine if s.get("start")), default=0)
    for s in sorted(mine, key=lambda x: x.get("start") or 0):
        print("  +%7.3fs %7.3fs  %-44s %s  %s" % (
            (s["start"] or 0) - t0, (s["end"] or 0) - (s["start"] or 0),
            s["attrs"].get("db.statement", "")[:44],
            s["res"].get("container.id", "?"), s["res"].get("service.name", "?")))
    print("\nCDC events (customer: txId, commit ts_ms):")
    for c, v in sorted(cdc.items()):
        print("  customer=%d txId=%s ts=%.3f (+%.3fs)" % (c, v["txId"], v["ts"], v["ts"] - t0))
    print("\nJoin: for each CDC event, the COMMIT span (from the container whose INSERT carried that customer) and the gap")
    for c, v in sorted(cdc.items()):
        ins = next((s for s in mine if ("values (%d," % c) in s["attrs"].get("db.statement", "")), None)
        if not ins:
            print("  customer=%d: no INSERT span found" % c)
            continue
        cid = ins["res"].get("container.id")
        commits = [s for s in mine if s["res"].get("container.id") == cid
                   and s["attrs"].get("db.statement", "").strip().lower().startswith("commit")]
        if not commits:
            print("  customer=%d: container %s has no COMMIT span" % (c, cid))
            continue
        cm = min(commits, key=lambda s: abs((s["end"] or 0) - v["ts"]))
        print("  customer=%d txId=%s -> %s | COMMIT span end - CDC ts = %+.1f ms" % (
            c, v["txId"], cid, ((cm["end"] or 0) - v["ts"]) * 1000))


if __name__ == "__main__":
    main()
