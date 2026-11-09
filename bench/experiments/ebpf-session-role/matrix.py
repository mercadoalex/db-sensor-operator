#!/usr/bin/env python3
"""Test matrix and analysis for run.sh. Subcommands: `run` (execute the tests) and `analyze` (judge the hypotheses).

Every test runs psql in a client pod (client-b, and client-a for T1a) against a throwaway PostgreSQL over TCP.
Each test is preceded by a marker query (SELECT 'MARK:<test>:<rep>') sent over a separate connection from the same
client, so the events of a test are the ones between its marker and the next one, from the same cgroup.
Ground truth ("did the bypass actually happen?") is read in the same session with GT after the test statement.
"""
import json, os, re, subprocess, sys, time

NS = os.environ.get("NS", "bench")
CTX = "kind-" + os.environ.get("CLUSTER", "db-sensor-bench")
OUT = os.environ["OUT"]
IP = os.environ.get("SRV_IP", "")
REPS = int(os.environ.get("REPS", "3"))
NODE = os.environ.get("NODE", "db-sensor-bench-control-plane")
GT = "SELECT current_setting('session_replication_role')"
PLAIN, TLS = 5432, 5433
LONG = "/* " + "x" * 300 + " */ SET session_replication_role = replica"
BYPASS_SQL = "SET session_replication_role = replica"

# id, description, kind, psql args (the GT query is appended unless the test places it itself), stdin, port, client, tls
TESTS = [
    ("T1", "base case", "bypass", ["-c", BYPASS_SQL], None, PLAIN, "client-b", False),
    ("T1a", "base case from the other client", "bypass", ["-c", BYPASS_SQL], None, PLAIN, "client-a", False),
    ("T2", "lower case and TO", "bypass", ["-c", "set SESSION_REPLICATION_ROLE to replica"], None, PLAIN, "client-b", False),
    ("T3", "comment and no spaces", "bypass", ["-c", "/* x */ SET session_replication_role=replica"], None, PLAIN, "client-b", False),
    ("T4", "set_config function", "bypass", ["-c", "SELECT set_config('session_replication_role','replica',false)"], None, PLAIN, "client-b", False),
    ("T5", "SET LOCAL inside a transaction (separate messages)", "bypass",
     ["-c", "BEGIN", "-c", "SET LOCAL session_replication_role = replica", "-c", GT, "-c", "COMMIT"], None, PLAIN, "client-b", False),
    ("T6", "text built on the server (DO block)", "bypass",
     ["-c", "DO $$ BEGIN EXECUTE 'SET session_' || 'replication_role = replica'; END $$"], None, PLAIN, "client-b", False),
    ("T6b", "name split in the client query", "bypass",
     ["-c", "SELECT set_config('session_' || 'replication_role', 'replica', false)"], None, PLAIN, "client-b", False),
    ("T7", "control: SELECT 1", "control", ["-c", "SELECT 1"], None, PLAIN, "client-b", False),
    ("T8", "300-character comment before the SET", "bypass", ["-c", LONG], None, PLAIN, "client-b", False),
    ("T9", "extended protocol (Parse), value as a bind parameter", "bypass",
     ["-f", "-"], "SELECT set_config('session_replication_role', $1, false) \\bind replica \\g\n" + GT + ";\n", PLAIN, "client-b", False),
    ("T10", "two statements in one message", "bypass", ["-c", "SELECT 1; " + BYPASS_SQL], None, PLAIN, "client-b", False),
    ("T11", "same as T1 over TLS", "bypass", ["-c", BYPASS_SQL], None, TLS, "client-b", True),
    ("T12", "control: the name inside a string", "control", ["-c", "SELECT 'session_replication_role'"], None, PLAIN, "client-b", False),
    ("T13", "control: SHOW the parameter", "control", ["-c", "SHOW session_replication_role"], None, PLAIN, "client-b", False),
]

def kx(client, role, port, tls, args, stdin=None):
    cmd = ["kubectl", "--context", CTX, "-n", NS, "exec", "-i", client, "--", "env",
           "PGSSLMODE=" + ("require" if tls else "disable"), "psql", "-X", "-tA", "-h", IP, "-p", str(port),
           "-U", role, "-d", "postgres"] + args
    p = subprocess.run(cmd, input=stdin, capture_output=True, text=True, timeout=60)
    return p.stdout + p.stderr

def ground_truth(out):
    lines = [l.strip() for l in out.splitlines()]
    vals = [l for l in lines if l in ("replica", "origin", "local")]
    return "replica" if "replica" in vals else ("origin" if vals else "unknown")

def run():
    results = []
    plan = [(t, rep, "postgres") for rep in range(1, REPS + 1) for t in TESTS]
    plan += [(t, 0, "app") for t in TESTS if t[2] == "bypass" and t[0] != "T1a"]   # non-superuser pass, rep 0
    for (tid, desc, kind, args, stdin, port, client, tls), rep, role in plan:
        label = tid if role == "postgres" else tid + "@app"
        marker = "MARK:%s:%d" % (label, rep)
        kx(client, role if role == "app" else "postgres", PLAIN, False, ["-c", "SELECT '%s'" % marker])
        a = list(args)
        if not any(x == GT for x in a) and "-f" not in a:
            a += ["-c", GT]
        out = kx(client, role, port, tls, a, stdin)
        results.append({"test": tid, "label": label, "rep": rep, "role": role, "client": client, "kind": kind,
                        "port": port, "tls": tls, "marker": marker, "gt": ground_truth(out),
                        "denied": "permission denied" in out, "out": out.strip()[-300:]})
        time.sleep(0.15)
    kx("client-b", "postgres", PLAIN, False, ["-c", "SELECT 'MARK:END:0'"])
    json.dump(results, open(os.path.join(OUT, "tests.json"), "w"), indent=1)
    print("ran %d tests" % len(results))

# ---------------------------------------------------------------- detectors
def strip_comments(s): return re.sub(r"/\*.*?\*/", " ", s, flags=re.S)
RX = [re.compile(r"\bset\s+(session\s+|local\s+)?session_replication_role\s*(=|to\b)"),
      re.compile(r"set_config\s*\(\s*'session_replication_role'")]
def d_naive(s): return s.startswith("SET session_replication_role")
def d_sub(s): return "session_replication_role" in s.lower()
def d_rx(s):
    t = strip_comments(s).lower()
    return any(r.search(t) for r in RX)
DET = {"naive": d_naive, "substring": d_sub, "regex": d_rx}
def is_gt(s): return s.strip().rstrip(";").strip() == GT

def pod_of(cg, cache={}):
    if cg in cache: return cache[cg]
    r = subprocess.run(["docker", "exec", NODE, "find", "/sys/fs/cgroup", "-inum", str(cg)], capture_output=True, text=True)
    m = re.search(r"pod([0-9a-f_]+)\.slice", r.stdout)
    uid = m.group(1).replace("_", "-") if m else None
    pods = subprocess.run(["kubectl", "--context", CTX, "get", "pods", "-A", "-o",
                           "jsonpath={range .items[*]}{.metadata.uid} {.metadata.name}{'\\n'}{end}"],
                          capture_output=True, text=True).stdout.split("\n")
    name = next((l.split()[1] for l in pods if l.startswith(str(uid))), None) if uid else None
    cache[cg] = name
    return name

def parse_events(path):
    ev = []
    for line in open(path, errors="replace"):
        if not line.startswith("EV|"): continue
        p = line.rstrip("\n").split("|", 5)
        if len(p) == 6: ev.append({"t": int(p[1]), "cg": int(p[2]), "type": p[3], "len": int(p[4]), "sql": p[5]})
    return sorted(ev, key=lambda e: e["t"])

def parse_server_log(text):
    rows = []
    for line in text.splitlines():
        m = re.search(r"\s(\d+\.\d+\.\d+\.\d+)\s+LOG:\s+(?:statement|execute [^:]*): (.*)$", line)
        if m: rows.append({"ip": m.group(1), "sql": m.group(2)})
    return rows

def analyze():
    tests = json.load(open(os.path.join(OUT, "tests.json")))
    ev = parse_events(os.path.join(OUT, "events.txt"))
    plain = parse_server_log(open(os.path.join(OUT, "server-plain.log"), errors="replace").read())
    tlsl = parse_server_log(open(os.path.join(OUT, "server-tls.log"), errors="replace").read())
    ok_all = True
    def chk(name, cond, extra=""):
        nonlocal ok_all
        print(("PASS  " if cond else "FAIL  ") + name + (("   [" + extra + "]") if extra else ""))
        ok_all &= bool(cond)
    # marker positions
    marks = [e for e in ev if e["sql"].startswith("SELECT 'MARK:")]
    mark_t = {}
    for e in marks:
        m = re.match(r"SELECT 'MARK:([^']*)'", e["sql"])
        if m: mark_t["MARK:" + m.group(1)] = e
    sent = [t["marker"] for t in tests] + ["MARK:END:0"]
    missing = [m for m in sent if m not in mark_t]
    ordered = sorted(mark_t.values(), key=lambda e: e["t"])
    summary = {}
    for t in tests:
        m = mark_t.get(t["marker"])
        t["events"] = []
        t["obs"] = {}
        if not m: continue
        nxt = next((e for e in ordered if e["t"] > m["t"]), None)
        hi = nxt["t"] if nxt else m["t"] + 10**10
        t["events"] = [e for e in ev if e["cg"] == m["cg"] and m["t"] < e["t"] < hi and not is_gt(e["sql"])]
        t["pod"] = pod_of(m["cg"])
        for name, fn in DET.items():
            t["obs"][name] = any(fn(e["sql"]) for e in t["events"])
        t["flagged_pods"] = sorted({pod_of(e["cg"]) for e in t["events"] if d_rx(e["sql"])})
    # server side, segmented by the same markers
    srv_marks = [i for i, r in enumerate(plain) if r["sql"].startswith("SELECT 'MARK:")]
    def srv_window(marker):
        for k, i in enumerate(srv_marks):
            if plain[i]["sql"] == "SELECT '%s'" % marker:
                j = srv_marks[k + 1] if k + 1 < len(srv_marks) else len(plain)
                return [r for r in plain[i + 1:j] if not is_gt(r["sql"])]
        return []
    for t in tests:
        rows = tlsl if t["tls"] else srv_window(t["marker"])
        if t["tls"]:
            rows = [r for r in tlsl if not is_gt(r["sql"])]
        t["srv_regex"] = any(d_rx(r["sql"]) for r in rows) if not t["tls"] else None
    tls_hits = sum(1 for r in tlsl if d_rx(r["sql"]) and not is_gt(r["sql"]))
    # the TLS log has no markers and no user: it also holds the one attempt made as the role that is not a superuser
    tls_hits -= sum(1 for t in tests if t["tls"] and t["role"] == "app")
    # ---- aggregate per test over the superuser repetitions
    main = [t for t in tests if t["role"] == "postgres"]
    ids = [x[0] for x in TESTS]
    print("\nPer test, over %d repetitions as the superuser (bypass happened = ground truth in the same session)\n" % REPS)
    print("%-5s %-8s %-6s %-9s %-10s %-7s %-9s %s" % ("test", "kind", "bypass", "naive", "substring", "regex", "srv-log", "description"))
    desc = {x[0]: x[1] for x in TESTS}
    agg = {}
    for tid in ids:
        rs = [t for t in main if t["test"] == tid]
        n = len(rs)
        b = sum(1 for t in rs if t["gt"] == "replica")
        c = {k: sum(1 for t in rs if t["obs"].get(k)) for k in DET}
        s = tls_hits if rs[0]["tls"] else sum(1 for t in rs if t["srv_regex"])
        agg[tid] = {"n": n, "bypass": b, **c, "srv": s}
        print("%-5s %-8s %-6s %-9s %-10s %-7s %-9s %s" % (tid, rs[0]["kind"], "%d/%d" % (b, n), "%d/%d" % (c["naive"], n),
              "%d/%d" % (c["substring"], n), "%d/%d" % (c["regex"], n), "%d/%d" % (s, n), desc[tid]))
    byp = [i for i in ids if i != "T1a" and agg[i]["n"] and TESTS[ids.index(i)][2] == "bypass"]
    ctrl = [i for i in ids if TESTS[ids.index(i)][2] == "control"]
    full = lambda i, k: agg[i][k] == agg[i]["n"]
    # ---- hypotheses
    print("\nHypotheses (written in run.sh before the first run)\n")
    chk("H0 (instrument): every marker query reached the probe (no lost events)", not missing, "missing: %s" % missing[:5])
    chk("H0b (ground truth): every bypass test really set the role to replica; every control left it at origin",
        all(agg[i]["bypass"] == agg[i]["n"] for i in byp) and all(agg[i]["bypass"] == 0 for i in ctrl),
        "bypass: %s" % {i: agg[i]["bypass"] for i in byp if agg[i]["bypass"] != agg[i]["n"]})
    t1 = [t for t in main if t["test"] == "T1"]
    t1a = [t for t in main if t["test"] == "T1a"]
    chk("H1: T1 is detected and attributed to client-b, T1a to client-a, in every repetition, and T7 raises no alarm",
        all(t["obs"]["regex"] and t["flagged_pods"] == ["client-b"] for t in t1) and
        all(t["obs"]["regex"] and t["flagged_pods"] == ["client-a"] for t in t1a) and agg["T7"]["regex"] == 0,
        "T1 pods %s, T1a pods %s" % ([t["flagged_pods"] for t in t1][:1], [t["flagged_pods"] for t in t1a][:1]))
    chk("H2: the naive prefix detector catches at most 3 of the 11 bypass variants",
        sum(1 for i in byp if full(i, "naive")) <= 3, "catches %d of %d" % (sum(1 for i in byp if full(i, "naive")), len(byp)))
    expect = {"T1", "T2", "T3", "T4", "T5", "T9", "T10"}
    got = {i for i in byp if full(i, "regex")}
    chk("H3: the tolerant regex catches exactly T1 T2 T3 T4 T5 T9 T10 and misses T6 T6b (text built later), T8 (beyond the 199-character window) and T11 (TLS)",
        got == expect, "caught %s" % sorted(got))
    chk("H4: the substring detector raises false alarms on the controls T12 and T13; the regex does not",
        full("T12", "substring") and full("T13", "substring") and agg["T12"]["regex"] == 0 and agg["T13"]["regex"] == 0)
    mis = [(t["label"], t["flagged_pods"], t["client"]) for t in tests if t["obs"].get("regex") and t["flagged_pods"] != [t["client"]]]
    chk("H5: no detected event is attributed to the wrong pod", not mis, "misattributed: %s" % mis[:3])
    srv_got = {i for i in byp if (agg[i]["srv"] == agg[i]["n"])}
    chk("H6: log_statement=all with the same regex catches the 7 above plus T8 and T11 (9 of 11), and also misses T6 and T6b",
        srv_got == expect | {"T8", "T11"}, "caught %s" % sorted(srv_got))
    app = [t for t in tests if t["role"] == "app"]
    chk("H7: as a role that is not a superuser, no variant sets the role (permission denied), while the probe still sees the attempts",
        app and all(t["gt"] != "replica" for t in app) and sum(1 for t in app if t["obs"].get("regex")) >= 6,
        "bypassed %d of %d; attempts seen %d" % (sum(1 for t in app if t["gt"] == "replica"), len(app), sum(1 for t in app if t["obs"].get("regex"))))
    json.dump({"per_test": agg, "ok": bool(ok_all)}, open(os.path.join(OUT, "summary.json"), "w"), indent=1)
    print("\nevents captured: %d; raw data in %s" % (len(ev), OUT))
    sys.exit(0 if ok_all else 1)

if __name__ == "__main__":
    {"run": run, "analyze": analyze}[sys.argv[1]]()
