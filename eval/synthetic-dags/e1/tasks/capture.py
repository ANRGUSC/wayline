#!/usr/bin/env python3
"""
IoBT capture — Argo-side port of
eval/synthetic-dags/scheduler/iobt/tasks/capture/task.py.

The compute logic is byte-for-byte identical to the Wayline version; only
the I/O is different:
  - Wayline version reads task config from env via WlTask and writes
    output via task.send_raw.
  - This version reads E1_TASK_NAME, E1_DATA_SIZE, E1_RUNTIME directly
    and writes /out/output. Argo handles cross-task transfer via S3.
"""
import hashlib
import os
import time

TASK_NAME = os.environ["E1_TASK_NAME"]
DATA_SIZE = int(os.environ.get("E1_DATA_SIZE", "100000000"))
RUNTIME   = float(os.environ.get("E1_RUNTIME", "3.0"))

NODE_NAME = os.environ.get("NODE_NAME", "?")
print(f"[{TASK_NAME}] node={NODE_NAME}  data_size={DATA_SIZE}  runtime={RUNTIME}s", flush=True)

if RUNTIME > 0:
    time.sleep(min(RUNTIME, 5.0))

t0 = time.perf_counter()
seed = hashlib.md5(TASK_NAME.encode()).digest()
block = seed * 4096
reps = DATA_SIZE // len(block) + 1
raw = (block * reps)[:DATA_SIZE]
elapsed_gen = time.perf_counter() - t0
print(f"[{TASK_NAME}] generated {len(raw)} bytes in {elapsed_gen:.3f}s", flush=True)

os.makedirs("/out", exist_ok=True)
t1 = time.perf_counter()
with open("/out/output", "wb") as f:
    f.write(raw)
elapsed_write = time.perf_counter() - t1
print(f"[{TASK_NAME}] wrote /out/output in {elapsed_write:.3f}s", flush=True)
print(f"[{TASK_NAME}] done", flush=True)
