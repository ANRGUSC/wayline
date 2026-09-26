#!/usr/bin/env python3
"""E11 task: read named inputs, run a fixed number of SHA-256 iterations
(so runtime follows the node's clock), emit named outputs.

Env: E11_ITERS, E11_INPUTS ("prod.obj,..."), E11_OUTPUTS ("obj:bytes,...").
`python task.py --bench [seconds]` prints the node's rate in Mhash/s.
"""
import hashlib
import os
import sys
import time


def spin(n):
    d = b"wl-e11"
    for _ in range(n):
        d = hashlib.sha256(d).digest()
    return d


if len(sys.argv) > 1 and sys.argv[1] == "--bench":
    dur = float(sys.argv[2]) if len(sys.argv) > 2 else 3.0
    spin(200_000)                                   # let the clock ramp up
    n, t0 = 0, time.time()
    while time.time() - t0 < dur:
        spin(20_000)
        n += 20_000
    print(f"RATE {os.environ.get('NODE_NAME', '')} {n / (time.time() - t0) / 1e6:.4f}", flush=True)
    sys.exit(0)

from wl import WlTask

task = WlTask()
name = task.name
for key in [k for k in os.environ.get("E11_INPUTS", "").split(",") if k]:
    data = task.recv_raw(peer=key)
    print(f"[{name}] in {key}: {len(data)}B", flush=True)
    del data
iters = int(os.environ["E11_ITERS"])
t0 = time.time()
spin(iters)
print(f"[{name}] node={os.environ.get('NODE_NAME', '')} iters={iters} compute={time.time() - t0:.2f}s", flush=True)
outs = [o.split(":") for o in os.environ.get("E11_OUTPUTS", "").split(",") if o]
for obj, size in outs:
    task.send_raw(obj, b"\0" * int(size))
if not outs:
    task.send_raw(b"done")
task.close()
