#!/usr/bin/env python3
"""
IoBT Mission Snapshot — infer task (infer-1 .. infer-4).

Receives preprocessed features from preprocess-i, simulates heavy
inference via a CPU busy loop, and outputs a small JSON dict of
fake object detections.
"""

import hashlib
import os
import random
import time

from wl_sdk import WlTask

task = WlTask()

RUNTIME = task.expected_runtime or 10.0

print(f"[{task.name}] node={task.node}  runtime={RUNTIME}s", flush=True)

# --- receive features from preprocess-i ---
t0 = time.perf_counter()
features = task.recv_raw()
elapsed_recv = time.perf_counter() - t0

print(f"[{task.name}] recv_raw() -> {len(features)} bytes in {elapsed_recv:.3f}s", flush=True)

# --- simulate inference (CPU busy loop) ---
t1 = time.perf_counter()
# Seed RNG from task name for deterministic detections
rng = random.Random(task.name)
classes = ["vehicle", "person", "uav", "building", "antenna"]
# Busy-spin computation with periodic hashing
iters = 0
while time.perf_counter() - t1 < RUNTIME:
    hashlib.sha256(features[iters % len(features):iters % len(features) + 4096]).digest()
    iters += 1

elapsed_infer = time.perf_counter() - t1
print(f"[{task.name}] inference loop: {iters} iterations in {elapsed_infer:.3f}s", flush=True)

# --- generate fake detections ---
n_detections = rng.randint(5, 15)
detections = []
for i in range(n_detections):
    det = {
        "id": f"{task.name}-det-{i}",
        "class": rng.choice(classes),
        "confidence": round(rng.uniform(0.4, 0.99), 3),
        "bbox": [
            rng.randint(0, 800),
            rng.randint(0, 600),
            rng.randint(20, 200),
            rng.randint(20, 200),
        ],
        "sensor": task.name.replace("infer-", "capture-"),
    }
    detections.append(det)

result = {
    "source": task.name,
    "node": task.node,
    "num_detections": len(detections),
    "detections": detections,
    "inference_time_s": round(elapsed_infer, 3),
    "input_size_bytes": len(features),
}

print(f"[{task.name}] produced {len(detections)} detections", flush=True)

# --- send JSON to fuse-tracks ---
t2 = time.perf_counter()
task.send(result)
elapsed_send = time.perf_counter() - t2

print(f"[{task.name}] send() completed in {elapsed_send:.3f}s", flush=True)
print(f"[{task.name}] done", flush=True)
task.close()
