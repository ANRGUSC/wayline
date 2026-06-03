#!/usr/bin/env python3
"""
IoBT preprocess — Argo port. Computes SHA-256 over 64 KB chunks of the
input bytes, pads the digest to E1_DATA_SIZE bytes (no compression),
writes /out/output. Reads input from /in/<dep>/output where <dep> is the
single capture-i upstream.
"""
import glob
import hashlib
import os
import time

TASK_NAME = os.environ["E1_TASK_NAME"]
DATA_SIZE = int(os.environ.get("E1_DATA_SIZE", "100000000"))
RUNTIME   = float(os.environ.get("E1_RUNTIME", "5.0"))
NODE_NAME = os.environ.get("NODE_NAME", "?")
print(f"[{TASK_NAME}] node={NODE_NAME}  data_size={DATA_SIZE}  runtime={RUNTIME}s", flush=True)

# Find the single input file under /in/<dep>/output
input_files = sorted(glob.glob("/in/*/output"))
assert len(input_files) == 1, f"expected 1 input, got {input_files}"
t0 = time.perf_counter()
with open(input_files[0], "rb") as f:
    raw = f.read()
elapsed_recv = time.perf_counter() - t0
print(f"[{TASK_NAME}] read {len(raw)} bytes from {input_files[0]} in {elapsed_recv:.3f}s", flush=True)

t1 = time.perf_counter()
CHUNK = 65536
hashes = []
for off in range(0, len(raw), CHUNK):
    hashes.append(hashlib.sha256(raw[off:off + CHUNK]).digest())
digest_blob = b"".join(hashes)
print(f"[{TASK_NAME}] computed {len(hashes)} chunk hashes ({len(digest_blob)} bytes)", flush=True)

elapsed_so_far = time.perf_counter() - t1
remaining = max(0, RUNTIME - elapsed_so_far)
if remaining > 0:
    time.sleep(remaining)

# Pad the digest up to DATA_SIZE; matches the "no compression" assumption.
if len(digest_blob) >= DATA_SIZE:
    features = digest_blob[:DATA_SIZE]
else:
    pad = DATA_SIZE - len(digest_blob)
    if len(raw) >= pad:
        features = digest_blob + raw[:pad]
    else:
        reps = pad // len(raw) + 1
        features = digest_blob + (raw * reps)[:pad]
print(f"[{TASK_NAME}] feature extraction done; output={len(features)} bytes", flush=True)

os.makedirs("/out", exist_ok=True)
t2 = time.perf_counter()
with open("/out/output", "wb") as f:
    f.write(features)
print(f"[{TASK_NAME}] wrote /out/output in {time.perf_counter()-t2:.3f}s", flush=True)
print(f"[{TASK_NAME}] done", flush=True)
