#!/usr/bin/env python3
"""Tables for findings2.md, computed from evidence/ (read-only). Writes evidence/findings2-tables.txt."""
import glob, json, math, os, re, subprocess, sys
HERE = os.path.dirname(os.path.abspath(__file__))
EV = os.path.join(HERE, "evidence")


def cp(k, n, a=0.05):
    """exact (Clopper-Pearson) 95% interval"""
    def cdf(x, p):
        return sum(math.comb(n, i) * p**i * (1 - p)**(n - i) for i in range(0, x + 1))
    def solve(f, lo=0.0, hi=1.0):
        for _ in range(60):
            m = (lo + hi) / 2
            if f(m):
                hi = m
            else:
                lo = m
        return (lo + hi) / 2
    lower = 0.0 if k == 0 else solve(lambda p: 1 - cdf(k - 1, p) >= a / 2)
    upper = 1.0 if k == n else solve(lambda p: cdf(k, p) <= a / 2, 0, 1) if False else None
    if k == n:
        upper = 1.0
    else:
        lo, hi = 0.0, 1.0
        for _ in range(60):
            m = (lo + hi) / 2
            if cdf(k, m) > a / 2:
                lo = m
            else:
                hi = m
        upper = (lo + hi) / 2
    return lower, upper


def fisher(a, n1, b, n2):
    """two-sided Fisher exact p for a/n1 vs b/n2"""
    K = a + b
    N = n1 + n2
    def pm(x):
        return math.comb(n1, x) * math.comb(n2, K - x) / math.comb(N, K)
    p0 = pm(a)
    return sum(pm(x) for x in range(max(0, K - n2), min(n1, K) + 1) if pm(x) <= p0 + 1e-12)


def load(prefix):
    out = []
    for d in sorted(glob.glob(os.path.join(EV, prefix))):
        sp = os.path.join(d, "summary.json")
        if os.path.exists(sp):
            out.append(json.load(open(sp)))
    return out


def an(prefix):
    r = subprocess.run([sys.executable, os.path.join(HERE, "analyze2.py"), prefix], capture_output=True, text=True).stdout
    return [json.loads(l) for l in r.splitlines()]


L = []
P = L.append
P("== ghost-rate trials (graceful termination, no events) ==")
rates = {}
for g in ("G0-default", "G0b-default", "G3-rollout", "G1-k8s-lease", "G1b-k8s-lease", "G2-prestop"):
    p = os.path.join(EV, g, "ghost-trials.json")
    if not os.path.exists(p):
        continue
    rows = json.load(open(p))
    n = len(rows)
    nolog = sum(1 for r in rows if not r["log_has_unsubscribe_line"])
    bus = sum(1 for r in rows if r["still_online_8s_after_gone"])
    rates[g] = (nolog, n)
    lo, hi = cp(nolog, n)
    P("%-16s n=%2d  no-unsubscribe-line=%2d (%.0f%%, 95%% CI %.0f-%.0f%%)  bus-sub-still-online-8s=%2d" % (g, n, nolog, 100.0 * nolog / n, 100 * lo, 100 * hi, bus))
d0 = (rates["G0-default"][0] + rates["G0b-default"][0], rates["G0-default"][1] + rates["G0b-default"][1])
d1 = (rates["G1-k8s-lease"][0] + rates["G1b-k8s-lease"][0], rates["G1-k8s-lease"][1] + rates["G1b-k8s-lease"][1])
P("pooled default delete: %d/%d ; pooled k8s-lease: %d/%d ; Fisher exact p=%.4g" % (d0[0], d0[1], d1[0], d1[1], fisher(d0[0], d0[1], d1[0], d1[1])))
dd = (d0[0] + rates["G3-rollout"][0], d0[1] + rates["G3-rollout"][1])
P("pooled default delete+rollout: %d/%d vs lease %d/%d Fisher p=%.4g" % (dd[0], dd[1], d1[0], d1[1], fisher(dd[0], dd[1], d1[0], d1[1])))
P("prestop vs default delete: %d/%d vs %d/%d Fisher p=%.3g" % (rates["G2-prestop"][0], rates["G2-prestop"][1], d0[0], d0[1], fisher(rates["G2-prestop"][0], rates["G2-prestop"][1], d0[0], d0[1])))

P("\n== SIGKILL protocols ==")
for title, pre in (("K1 default bus (Y*-K1)", "Y*-K1-*"), ("K1 with k8s-lease election (L-K1)", "L-K1-*"), ("K2 default bus (E at +60 s)", "Y*-K2-*"),
                   ("M5GC = K1 on short-heartbeat bus (F1)", "M-M5GC-*"), ("M5G (E2 before reap)", "M-M5G-?")):
    rows = an(pre.split("*")[0]) if False else None
    ds = sorted(glob.glob(os.path.join(EV, pre)))
    res = []
    for d in ds:
        sp = os.path.join(d, "summary.json")
        if not os.path.exists(sp):
            continue
        S = json.load(open(sp))
        res.append((S["name"], S.get("outcome"), S.get("suspend_latency_s")))
    P("%s: %s" % (title, res))

P("\n== hand-over analysis (analyze2) ==")
for pre in ("Y", "L-K1", "M-", "Q-"):
    for r in an(pre):
        P("%s %s lat=%s handed_to_killed_pod=%s hb_timeout_after_kill=%s" % (r["name"], r.get("outcome"), r.get("latency"), r.get("handed_to_killed_pod"), r.get("hb_timeout_after_kill")))
open(os.path.join(EV, "findings2-tables.txt"), "w").write("\n".join(L) + "\n")
print("\n".join(L))
