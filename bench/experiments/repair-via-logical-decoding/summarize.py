#!/usr/bin/env python3
"""Apply the pre-registered criteria of the Matrix-shaped scenario across raw/run-*/metrics.json (H7 and the verdict
rule are applied here; H1 to H6 were judged per run by analyze.py). Prints the tables used in RESULTS.md."""
import glob
import json
import os

here = os.path.dirname(os.path.abspath(__file__))
runs = sorted(glob.glob(f"{here}/raw/run-*/metrics.json"))
M = [json.load(open(p)) for p in runs]
T = ["sc2_full_bad", "sc2_full_ok", "sc2_pk_bad", "sc2_pk_ok"]
print("per run (valid = H1 held and the stream complete):")
print("run | shift | valid | " + " | ".join(f"H{i}" for i in range(1, 7)))
for m in M:
    h = m["hypotheses"]
    print(f"{m['run']} | {m['shift']} | {m['valid_run']} | " + " | ".join("held" if h[f'H{i}'] else "FAILED" for i in range(1, 7)))
print("\ncounts per run: table: removed/foreign/missed, amcheck rc, events, before cells mismatching (FULL) or non-key values present (default)")
for m in M:
    print(m["run"])
    for t in T:
        x = m["tables"][t]
        extra = x.get("before_cells_mismatching", x.get("before_nonkey_values_present"))
        print(f"  {t}: {x['removed']}/{x['foreign']}/{x['missed']}  amcheck_rc={x['amcheck_rc']}  events={x['events_op_d']}  "
              f"idx_scan+{x['idx_scan_delta']} seq_scan+{x['seq_scan_delta']}  extra={extra}  ids_equal_removed={x['event_ids_equal_removed']}")
valid = [m for m in M if m["valid_run"]]
sig = lambda m: json.dumps({t: {k: v for k, v in m["tables"][t].items() if k in ("removed", "foreign", "missed", "events_op_d", "event_ids_equal_removed", "identity_criteria", "before_cells_mismatching", "before_nonkey_values_present", "h5_rows_differing_after_reinsert")} for t in T}, sort_keys=True)
by_shift = {}
for m in valid:
    by_shift.setdefault(m["shift"], set()).add(sig(m))
h7 = len(valid) >= 3 and all(len(v) == 1 for v in by_shift.values())
print(f"\nH7 reproducibility: valid runs = {len(valid)}; distinct outcome signatures per SHIFT = { {k: len(v) for k, v in by_shift.items()} } -> {'held' if h7 else 'FAILED'}")
def judged(h):
    return all(m["hypotheses"][h] for m in valid) and len(valid) >= 3
full_ok = len(valid) >= 3 and all(m["tables"][t]["identity_criteria"] for m in valid for t in ("sc2_full_bad", "sc2_full_ok"))
# identification half of H3: ids equal, one event per removed row, foreign present, missed absent; key set only
ident = len(valid) >= 3 and all(m["tables"][t]["event_ids_equal_removed"] and m["tables"][t]["event_foreign_present"] and m["tables"][t]["event_missed_absent"] for m in valid for t in ("sc2_pk_bad", "sc2_pk_ok"))
anyfail = any((not m["tables"][t]["event_ids_equal_removed"]) or (t.startswith("sc2_full") and m["tables"][t]["before_cells_mismatching"] > 0) for m in valid for t in T)
print("\nverdict rule applied:")
print(f"  FULL mode (H2 on corrupted + control, all valid runs, >=3 valid): {'PASS' if full_ok else ('FAIL' if anyfail else 'INCONCLUSIVE')}")
print(f"  default identity, identification half (event key set == removed set, corrupted + control): {'PASS' if ident else ('FAIL' if anyfail else 'INCONCLUSIVE')}")
print(f"  default identity, H3 as worded (non-key columns null or absent): {'held' if judged('H3') else 'FAILED'}")
for m in valid:
    x = m["tables"]["sc2_pk_bad"]
    print(f"  {m['run']} default-identity non-key values present={x.get('before_nonkey_values_present')} equal-to-type-default={x.get('before_nonkey_equal_type_default')} equal-to-true-value={x.get('before_nonkey_equal_true_value')} neither={x.get('before_nonkey_neither')}")
