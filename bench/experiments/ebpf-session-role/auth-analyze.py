#!/usr/bin/env python3
"""Analysis for auth-handshake.sh. Judges the hypotheses written in that script's header, before the first run."""
import os, re, sys

OUT = os.environ["OUT"]
REPS = int(os.environ.get("REPS", "3"))
TESTPASS = os.environ.get("TESTPASS", "")


def parse(path):
    events = []
    for line in open(path, errors="replace"):
        if not line.startswith("EV|"):
            continue
        parts = line.rstrip("\n").split("|", 4)
        if len(parts) != 5:
            continue
        hex_part, _, txt_part = parts[4].partition("|TXT=")
        hex_part = hex_part[len("HEX="):] if hex_part.startswith("HEX=") else hex_part
        events.append({"t": int(parts[1]), "cg": int(parts[2]), "len": int(parts[3]),
                        "hex": hex_part, "raw": txt_part})
    return sorted(events, key=lambda e: e["t"])


def main():
    ev = parse(os.path.join(OUT, "raw-events.txt"))
    marks = [e for e in ev if e["raw"].startswith("SELECT 'MARK:AUTH:")]
    mark_t = {}
    for e in marks:
        m = re.match(r"SELECT 'MARK:AUTH:(\d+)'", e["raw"])
        if m:
            mark_t[int(m.group(1))] = e["t"]

    ok_all = True

    def chk(name, cond, extra=""):
        nonlocal ok_all
        print(("PASS  " if cond else "FAIL  ") + name + ((" [" + extra + "]") if extra else ""))
        ok_all &= bool(cond)

    if not ev:
        print("FAIL  instrument: zero events captured (the probe did not run; check docker logs ebpf-probe-raw)")
        sys.exit(1)
    if not mark_t:
        print("FAIL  instrument: zero markers found in the capture (connections ran but were not seen)")
        sys.exit(1)

    # Each marker is the FIRST statement sent on its own fresh connection, right after that connection's handshake.
    # So a connection's window is (previous marker's time, this marker's time] -- the handshake precedes its marker.
    windows = {}
    reps_sorted = sorted(mark_t)
    for i, rep in enumerate(reps_sorted):
        hi = mark_t[rep]
        lo = max((mark_t[r] for r in reps_sorted if mark_t[r] < hi), default=0)
        windows[rep] = [e for e in ev if lo < e["t"] <= hi]

    print("\nEvents per connection window (handshake that precedes each connection's own marker)\n")
    per_rep_findings = []
    for rep in reps_sorted:
        w = windows[rep]
        # The StartupMessage (hex 00 00 00 xx 00 03 00 00 ...) carries "user\0<name>\0database\0<name>\0..." as a
        # sequence of NUL-terminated strings; str() from the client reads only up to the first one ("user"). The
        # full parameter list is visible in the hex dump and is decoded here instead of relied on from TXT.
        startup = [e for e in w if e["hex"].startswith("000000") and "7573657200" in e["hex"]]  # "user\0" in hex
        # The gs2-header/nonce ("n,,n=...,r=...") sits right after the mechanism name inside the SAME
        # SASLInitialResponse message; TXT (str() from a fixed offset) stops at the NUL after "SCRAM-SHA-256", so
        # the nonce is read from the hex dump instead, which has no such cut.
        mechanism = [e for e in w if "SCRAM-SHA-256" in e["raw"]]
        client_first = [e for e in w if "6e2c2c6e3d" in e["hex"]]  # hex for "n,,n="
        client_final = [e for e in w if re.search(r"c=biws,r=.*,p=", e["raw"])]
        plaintext_pw = [e for e in w if TESTPASS and TESTPASS in e["raw"]]

        def decode_startup(e):
            raw = bytes.fromhex(e["hex"])
            fields = raw[8:].split(b"\x00")  # skip length(4) + protocol version(4)
            return dict(zip(fields[0::2], fields[1::2]))

        startup_fields = decode_startup(startup[0]) if startup else {}
        print("  rep %d: %d events; startup=%d scram-mech=%d client-first=%d client-final=%d plaintext-password=%d"
              % (rep, len(w), len(startup), len(mechanism), len(client_first), len(client_final), len(plaintext_pw)))
        per_rep_findings.append({"startup": bool(startup), "mechanism": bool(mechanism), "client_first": bool(client_first),
                                  "client_final": bool(client_final), "plaintext_pw": bool(plaintext_pw), "n_events": len(w),
                                  "startup_user": startup_fields.get(b"user"), "startup_db": startup_fields.get(b"database")})

    print("\n  StartupMessage fields decoded from the hex dump, per repetition:")
    for rep, f in zip(reps_sorted, per_rep_findings):
        print("    rep %d: user=%r database=%r" % (rep, f["startup_user"], f["startup_db"]))

    print()
    chk("H1: the probe captures sendto() calls before the first query message (handshake visible to the same hook)",
        all(f["n_events"] > 0 for f in per_rep_findings))
    chk("H2: the StartupMessage's 'user' parameter is readable in clear text",
        all(f["startup"] and f["startup_user"] == b"probeuser" for f in per_rep_findings),
        "users seen: %s" % [f["startup_user"] for f in per_rep_findings])
    chk("H3: the client's half of the SCRAM exchange is identifiable by structure (mechanism name, client-first "
        "with a nonce, client-final with a proof); the server's half (salt, iteration count) is not sent by the "
        "client process this probe watches, so it is not expected here",
        all(f["mechanism"] and f["client_first"] and f["client_final"] for f in per_rep_findings))
    chk("H4: no plaintext password (or trivial equivalent) appears in any captured message",
        all(not f["plaintext_pw"] for f in per_rep_findings))
    chk("H5: the result is consistent across repetitions (same booleans in every rep)",
        len({tuple(sorted((k, v) for k, v in f.items() if k not in ("startup_user", "startup_db", "n_events")))
             for f in per_rep_findings}) == 1
        if per_rep_findings else False)

    print("\nraw data: %s" % OUT)
    sys.exit(0 if ok_all else 1)


if __name__ == "__main__":
    main()
