#!/usr/bin/env python3
"""Build the SQL for the repair check (H5) of the Matrix-shaped scenario: copy the post-incident table (read with
indexes off, by the caller's session settings) into <table>_fixed, then re-insert the before images of the delete events
received for <table>. The comparison with the pre-incident snapshot is done by analyze.py."""
import argparse
import gzip
import json

ap = argparse.ArgumentParser()
ap.add_argument("--events", required=True)
ap.add_argument("--table", required=True)
a = ap.parse_args()
opener = gzip.open if a.events.endswith(".gz") else open
before = []
with opener(a.events, "rt") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        e = json.loads(line)
        if e.get("op") == "d" and e["source"]["table"] == a.table:
            before.append(e["before"])
doc = json.dumps(before, separators=(",", ":"))
assert "$j$" not in doc
print(f"create table public.{a.table}_fixed as select * from public.{a.table};")
print(f"insert into public.{a.table}_fixed select * from jsonb_populate_recordset(null::public.{a.table}, $j${doc}$j$::jsonb);")
