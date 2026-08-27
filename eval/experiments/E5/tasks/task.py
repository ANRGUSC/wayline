#!/usr/bin/env python3
"""E5 workload: a seven-task DAG whose every edge is a separately
declared named object, with node-dependent execution time.

runtime(t, n) = work[t] / speed[n], implemented in the container so the
real execution matches the separable cost model SAGA is given (the
bridge should therefore fit it with ~zero RMSE).
"""
import hashlib
import os
import sys
import time

from wl import WlTask

SPEED = {"anrg-1": 1.0, "anrg-3": 1.0, "anrg-4": 1.0, "anrg-5": 1.0,
         "anrg-6": 2.0, "anrg-7": 2.0, "anrg-8": 2.0, "anrg-9": 0.25}
WORK = {"source": 4, "a": 8, "b": 16, "c": 12, "j1": 14, "j2": 14, "sink": 4}
MB = 1_000_000
# task -> [(output name, bytes)]
OUTPUTS = {
    "source": [("to-a", 1 * MB), ("to-b", 20 * MB), ("to-c", 60 * MB)],
    "a": [("to-j1", 5 * MB)],
    "b": [("to-j1", 20 * MB), ("to-j2", 2 * MB)],
    "c": [("to-j2", 40 * MB)],
    "j1": [("to-sink", 10 * MB)],
    "j2": [("to-sink", 10 * MB)],
    "sink": [],
}
# task -> [object key it consumes]
INPUTS = {
    "a": ["source.to-a"], "b": ["source.to-b"], "c": ["source.to-c"],
    "j1": ["a.to-j1", "b.to-j1"], "j2": ["b.to-j2", "c.to-j2"],
    "sink": ["j1.to-sink", "j2.to-sink"],
}


def gen(key, size):
    block = hashlib.sha256(key.encode()).digest() * 32768  # 1 MiB
    return (block * (size // len(block) + 1))[:size]


def sizeof(key):
    prod, obj = key.split(".", 1)
    for name, n in OUTPUTS[prod]:
        if name == obj:
            return n
    raise KeyError(key)


task = WlTask()
name = task.name
node = os.environ.get("NODE_NAME", "")
# A store-mediated lowering may route an input through a data vertex;
# WL_INPUT_PEERS then names the vertices to read instead of the objects.
peers = [p for p in os.environ.get("WL_INPUT_PEERS", "").split(",") if p]

for i, key in enumerate(INPUTS.get(name, [])):
    peer = peers[i] if i < len(peers) else key
    data = task.recv_raw(peer=peer)
    want_size = sizeof(key)
    got = hashlib.sha256(data).hexdigest()
    want = hashlib.sha256(gen(key, want_size)).hexdigest()
    ok = len(data) == want_size and got == want
    print(f"[{name}] in {key} via {peer}: {len(data)}B sha={got[:16]} "
          f"verify={'OK' if ok else 'MISMATCH'}", flush=True)
    if not ok:
        sys.exit(1)
    del data

work = WORK[name]
speed = SPEED.get(node, 1.0)
delay = work / speed
print(f"[{name}] node={node} work={work} speed={speed} runtime={delay:.2f}s",
      flush=True)
time.sleep(delay)

for obj, size in OUTPUTS[name]:
    key = f"{name}.{obj}"
    payload = gen(key, size)
    print(f"[{name}] emit {obj}: {size}B sha={hashlib.sha256(payload).hexdigest()[:16]}",
          flush=True)
    task.send_raw(obj, payload)
    del payload
if not OUTPUTS[name]:
    task.send_raw(b"done")
print(f"[{name}] done", flush=True)
task.close()
