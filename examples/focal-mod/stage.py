#!/usr/bin/env python3
"""One FOCAL pipeline stage, chosen by FOCAL_ROLE (source | fft | encode |
fuse_head) and FOCAL_MOD (seismic | audio). The same script runs as a pod
(container stage) or as a call on a warm runner (function stage).

Inputs and outputs are numpy arrays (np.save bytes) sent as Wayline named
objects. FOCAL_BATCH windows per run; FOCAL_SEED makes sources reproducible,
so the sink's logits can be checked against the unsplit model.
"""
import io
import os
import time

import numpy as np

import focal_stages as F
from wl import WlTask


def pack(a):
    b = io.BytesIO(); np.save(b, a, allow_pickle=False); return b.getvalue()


def unpack(b):
    return np.load(io.BytesIO(b), allow_pickle=False)


task = WlTask()
role, mod = os.environ["FOCAL_ROLE"], os.environ.get("FOCAL_MOD", "")
batch, seed = int(os.environ.get("FOCAL_BATCH", "64")), int(os.environ.get("FOCAL_SEED", "0"))
t0 = time.time()
if role == "source":
    task.send_raw("samples", pack(F.source(mod, batch, seed + F.MODS.index(mod))))
elif role == "fft":
    task.send_raw("spectrum", pack(F.fft(unpack(task.recv_raw(peer=f"source-{mod}.samples")))))
elif role == "encode":
    task.send_raw("features", pack(F.encode(mod, unpack(task.recv_raw(peer=f"fft-{mod}.spectrum")))))
elif role == "fuse_head":
    feats = {m: unpack(task.recv_raw(peer=f"encoder-{m}.features")) for m in F.MODS}
    logits = F.fuse_head(feats)
    task.send_raw("logits", pack(logits))
    print(f"[fuse-head] logits {logits.shape} argmax {np.bincount(logits.argmax(1), minlength=logits.shape[1]).tolist()}", flush=True)
else:
    raise SystemExit(f"unknown FOCAL_ROLE {role!r}")
print(f"[{task.name}] {role} {mod} batch={batch} {time.time() - t0:.3f}s", flush=True)
task.close()
