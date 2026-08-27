#!/usr/bin/env python3
"""E4 workload: one producer emitting three named objects, four
consumers on distinct branches, and a joining report.

Payloads are deterministic (derived from the object name), so every
consumer can verify both size and SHA-256 against a locally regenerated
reference. Emission order is fixed: alert, features, snapshot.
"""
import hashlib
import os
import sys
import time

from wl import WlTask

SIZES = {"alert": 1_000_000, "features": 200_000_000, "snapshot": 200_000_000}
PEER_OF = {"actuator": ("serve-alert", "alert"),
           "analyze-a": ("serve-features", "features"),
           "analyze-b": ("serve-features", "features"),
           "archive": ("serve-snapshot", "snapshot")}


def gen(name, size):
    """Deterministic payload: a 1 MiB block keyed by the object name,
    repeated. Fast enough to regenerate for verification at 200 MB."""
    block = hashlib.sha256(name.encode()).digest() * 32768
    return (block * (size // len(block) + 1))[:size]


task = WlTask()
name = task.name
runtime = float(os.environ.get("WL_RUNTIME", "0") or 0)

if name == "produce":
    if runtime:
        time.sleep(runtime)
    for out in ("alert", "features", "snapshot"):
        payload = gen(out, SIZES[out])
        d = hashlib.sha256(payload).hexdigest()
        print(f"[produce] emit {out}: {len(payload)} bytes sha={d[:16]}",
              flush=True)
        task.send_raw(out, payload)
        del payload
elif name in PEER_OF:
    peer, obj = PEER_OF[name]
    data = task.recv_raw(peer=peer)
    got = hashlib.sha256(data).hexdigest()
    want = hashlib.sha256(gen(obj, SIZES[obj])).hexdigest()
    ok = len(data) == SIZES[obj] and got == want
    print(f"[{name}] from {peer}: {len(data)} bytes sha={got[:16]} "
          f"verify={'OK' if ok else 'MISMATCH'}", flush=True)
    if not ok:
        sys.exit(1)
    if runtime:
        time.sleep(runtime)
    task.send_raw(b"done")
elif name == "report":
    for p in ("actuator", "analyze-a", "analyze-b", "archive"):
        task.recv_raw(peer=p)
    if runtime:
        time.sleep(runtime)
    print("[report] all four branches joined", flush=True)
    task.send_raw(b"report")
print(f"[{name}] done", flush=True)
task.close()
