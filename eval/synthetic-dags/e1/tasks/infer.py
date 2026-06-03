#!/usr/bin/env python3
"""
IoBT infer — Argo port. Same busy-loop inference and detection
generation as the Wayline version. Reads features from /in/<dep>/output,
writes a JSON detection list to /out/output.
"""
import glob
import hashlib
import json
import os
import random
import time

TASK_NAME = os.environ["E1_TASK_NAME"]
RUNTIME   = float(os.environ.get("E1_RUNTIME", "10.0"))
NODE_NAME = os.environ.get("NODE_NAME", "?")
print(f"[{TASK_NAME}] node={NODE_NAME}  runtime={RUNTIME}s", flush=True)

input_files = sorted(glob.glob("/in/*/output"))
assert len(input_files) == 1, f"expected 1 input, got {input_files}"
t0 = time.perf_counter()
with open(input_files[0], "rb") as f:
    features = f.read()
print(f"[{TASK_NAME}] read {len(features)} bytes in {time.perf_counter()-t0:.3f}s", flush=True)

# Busy-spin inference for the runtime budget.
t1 = time.perf_counter()
rng = random.Random(TASK_NAME)
classes = ["vehicle", "person", "uav", "building", "antenna"]
iters = 0
while time.perf_counter() - t1 < RUNTIME:
    off = (iters * 4096) % max(1, len(features) - 4096)
    hashlib.sha256(features[off:off + 4096]).digest()
    iters += 1
elapsed_infer = time.perf_counter() - t1
print(f"[{TASK_NAME}] inference loop: {iters} iterations in {elapsed_infer:.3f}s", flush=True)

n = rng.randint(5, 15)
detections = [{
    "id": f"{TASK_NAME}-det-{i}",
    "class": rng.choice(classes),
    "confidence": round(rng.uniform(0.4, 0.99), 3),
    "bbox": [rng.randint(0,800), rng.randint(0,600), rng.randint(20,200), rng.randint(20,200)],
    "sensor": TASK_NAME.replace("infer-", "capture-"),
} for i in range(n)]

result = {
    "source": TASK_NAME,
    "node": NODE_NAME,
    "num_detections": n,
    "detections": detections,
    "inference_time_s": round(elapsed_infer, 3),
    "input_size_bytes": len(features),
}

os.makedirs("/out", exist_ok=True)
with open("/out/output", "w") as f:
    json.dump(result, f)
print(f"[{TASK_NAME}] produced {n} detections; wrote /out/output", flush=True)
print(f"[{TASK_NAME}] done", flush=True)
