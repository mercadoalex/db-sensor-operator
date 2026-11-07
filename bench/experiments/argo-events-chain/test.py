#!/usr/bin/env python3
"""Measurements for the Debezium -> Kafka -> Argo Events -> Job-suspend chain.

Usage: test.py <phase> ...   phases: shape, legal, forbidden, restart-sensor, restart-eventsource
Prerequisite: up.sh (all pods running). Each phase resets the Job to suspend=false first.
"""
import json
import subprocess
import sys
import time

CTX = "kind-db-sensor-bench"


def kc(*args, stdin=None, check=False):
    r = subprocess.run(["kubectl", "--context", CTX, *args], input=stdin, capture_output=True, text=True)
    if check and r.returncode:
        raise RuntimeError(r.stderr)
    return r.stdout.strip()


def sql(stmt):
    return kc("-n", "bench", "exec", "-i", "bench-postgres-0", "--", "psql", "-U", "bench", "-d", "bench", "-At", stdin=stmt)


def suspended():
    return kc("-n", "bench", "get", "job", "payments-writer", "-o", "jsonpath={.spec.suspend}") == "true"


def running_pods():
    out = kc("-n", "bench", "get", "pods", "-l", "job-name=payments-writer", "--no-headers")
    return [l for l in out.splitlines() if l]


def reset():
    kc("-n", "bench", "patch", "job", "payments-writer", "--type=merge", "-p", '{"spec":{"suspend":false}}')
    sql("update public.payments set status='settled' where id=1;")
    time.sleep(6)
    kc("-n", "bench", "patch", "job", "payments-writer", "--type=merge", "-p", '{"spec":{"suspend":false}}')


def last_message():
    out = kc("-n", "bench", "exec", "deploy/argo-kafka", "--", "/opt/kafka/bin/kafka-console-consumer.sh",
             "--bootstrap-server", "localhost:9092", "--topic", "argotest.public.payments",
             "--from-beginning", "--timeout-ms", "8000")
    lines = [l for l in out.splitlines() if l.startswith("{")]
    return json.loads(lines[-1]) if lines else None


def wait_suspended(t0, limit=60.0):
    while time.time() - t0 < limit:
        if suspended():
            return time.time() - t0
        time.sleep(0.25)
    return None


def phase_shape():
    sql("update public.payments set status='settled' where id=6;")
    time.sleep(3)
    m = last_message()
    print("keys:", list(m.keys()) if m else None)
    if m:
        print("op=%s before=%s after=%s txId=%s ts_ms=%s" % (
            m.get("op"), m.get("before"), m.get("after"), (m.get("source") or {}).get("txId"), m.get("ts_ms")))


def phase_legal():
    reset()
    print("start: suspend=%s" % suspended())
    # legal: pending -> settled, and settled -> settled (a touch), neither is forbidden
    sql("update public.payments set status='pending' where id=7;")  # settled-> no: id 7 default pending already
    sql("update public.payments set status='settled' where id=7;")
    sql("update public.payments set amount=amount where id=2;")
    time.sleep(15)
    print("after 3 legal updates (pending->settled, no-op touch): suspend=%s  (expected False)" % suspended())


def phase_forbidden():
    reset()
    print("start: suspend=%s pods=%d" % (suspended(), len(running_pods())))
    t0 = time.time()
    sql("update public.payments set status='pending' where id=1;")  # settled -> pending: forbidden
    t_commit = time.time() - t0
    dt = wait_suspended(t0)
    print("forbidden update returned after %.2fs; Job suspended after %s s from the start of the call" % (
        t_commit, ("%.2f" % dt) if dt else "NOT within 60 s"))
    time.sleep(5)
    print("pods now:", running_pods())


def phase_restart(component):
    reset()
    sel = {"sensor": ("sensor-name=settled-to-pending"), "eventsource": ("eventsource-name=payments-cdc")}[component]
    print("deleting the %s pod while the stream is flowing" % component)
    kc("-n", "argo-events", "delete", "pod", "-l", sel, "--wait=false")
    sql("update public.payments set status='pending' where id=1;")  # forbidden, produced while down
    t0 = time.time()
    dt = wait_suspended(t0, limit=120)
    print("forbidden update produced while the %s was down: Job suspended after %s s" % (
        component, ("%.1f" % dt) if dt else "NOT within 120 s"))
    # count triggers fired for that single event
    time.sleep(3)
    logs = kc("-n", "argo-events", "logs", "-l", "sensor-name=settled-to-pending", "--tail=400")
    print("trigger executions in the current sensor log:", logs.count("successfully processed") + logs.count("Successfully processed"))


if __name__ == "__main__":
    for ph in sys.argv[1:]:
        print("\n== %s" % ph)
        if ph == "shape":
            phase_shape()
        elif ph == "legal":
            phase_legal()
        elif ph == "forbidden":
            phase_forbidden()
        elif ph == "restart-sensor":
            phase_restart("sensor")
        elif ph == "restart-eventsource":
            phase_restart("eventsource")
        else:
            print("unknown phase")
