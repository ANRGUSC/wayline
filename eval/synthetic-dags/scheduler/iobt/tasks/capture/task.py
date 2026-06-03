#!/usr/bin/env python3
"""
IoBT Mission Snapshot — capture task (capture-1 .. capture-4).

Simulates a sensor node capturing a raw ISR image burst.
Produces a large raw blob of WL_DATA_SIZE bytes with a deterministic
pattern seeded by the task name (so each sensor's output is distinct).
"""

import hashlib
import os
import time

from wl_sdk import WlTask

task = WlTask()

DATA_SIZE = task.expected_data_size or 100_000_000  # default 100 MB
RUNTIME = task.expected_runtime or 3.0

print(f"[{task.name}] node={task.node}  data_size={DATA_SIZE}  runtime={RUNTIME}s", flush=True)

# --- simulate capture delay ---
if RUNTIME > 0:
    time.sleep(min(RUNTIME, 5.0))

# --- generate deterministic raw burst ---
# Use a repeating block seeded by task name so each sensor's data differs.
t0 = time.perf_counter()
seed = hashlib.md5(task.name.encode()).digest()  # 16-byte seed
block = seed * 4096  # 64 KB block
reps = DATA_SIZE // len(block) + 1
raw = (block * reps)[:DATA_SIZE]
elapsed_gen = time.perf_counter() - t0

print(f"[{task.name}] generated {len(raw)} bytes in {elapsed_gen:.3f}s", flush=True)

# --- send to preprocess-i ---
t1 = time.perf_counter()
task.send_raw(raw)
elapsed_send = time.perf_counter() - t1

print(f"[{task.name}] send_raw() completed in {elapsed_send:.3f}s", flush=True)
print(f"[{task.name}] done", flush=True)
task.close()
