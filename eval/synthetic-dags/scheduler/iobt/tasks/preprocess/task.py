#!/usr/bin/env python3
"""
IoBT Mission Snapshot — preprocess task (preprocess-1 .. preprocess-4).

Receives raw image bytes from capture-i, computes a sha256 digest over
64 KB chunks (simulating feature extraction), and produces a feature blob
of WL_DATA_SIZE bytes. No compression is applied — output size is driven
entirely by the template's dataSize field.
"""

import hashlib
import os
import time

from wl_sdk import WlTask

task = WlTask()

DATA_SIZE = task.expected_data_size or 100_000_000  # default 100 MB (no compression)
RUNTIME = task.expected_runtime or 5.0

print(f"[{task.name}] node={task.node}  data_size={DATA_SIZE}  runtime={RUNTIME}s", flush=True)

# --- receive raw from capture-i ---
t0 = time.perf_counter()
raw = task.recv_raw()
elapsed_recv = time.perf_counter() - t0

print(f"[{task.name}] recv_raw() -> {len(raw)} bytes in {elapsed_recv:.3f}s", flush=True)

# --- feature extraction: hash every 64 KB chunk ---
t1 = time.perf_counter()
CHUNK = 65536
hashes = []
for off in range(0, len(raw), CHUNK):
    h = hashlib.sha256(raw[off:off + CHUNK]).digest()  # 32 bytes each
    hashes.append(h)

digest_blob = b"".join(hashes)
print(f"[{task.name}] computed {len(hashes)} chunk hashes ({len(digest_blob)} bytes digest)", flush=True)

# --- sleep remaining runtime budget ---
elapsed_so_far = time.perf_counter() - t1
remaining = max(0, RUNTIME - elapsed_so_far)
if remaining > 0:
    time.sleep(remaining)

# --- produce feature blob ---
# No compression: output matches DATA_SIZE. Build from digest bytes repeated
# to fill DATA_SIZE (or append raw bytes if digest alone is smaller than DATA_SIZE).
if len(digest_blob) >= DATA_SIZE:
    features = digest_blob[:DATA_SIZE]
else:
    # Prefix with the digest, then fill the rest by reusing the raw bytes we
    # received — this avoids synthesising entirely new data and keeps the
    # output size faithful to the "no compression" assumption.
    remaining = DATA_SIZE - len(digest_blob)
    if len(raw) >= remaining:
        features = digest_blob + raw[:remaining]
    else:
        reps = remaining // len(raw) + 1
        features = digest_blob + (raw * reps)[:remaining]

elapsed_proc = time.perf_counter() - t1
print(f"[{task.name}] feature extraction done in {elapsed_proc:.3f}s  output={len(features)} bytes", flush=True)

# --- send to infer-i ---
t2 = time.perf_counter()
task.send_raw(features)
elapsed_send = time.perf_counter() - t2

print(f"[{task.name}] send_raw() completed in {elapsed_send:.3f}s", flush=True)
print(f"[{task.name}] done", flush=True)
task.close()
