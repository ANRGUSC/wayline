#!/usr/bin/env python3
"""
Generic heterogeneous-compute task — Argo port of
eval/synthetic-dags/scheduler/hetero-compute/tasks/task.py. Used by both the
hetero-compute and wide-pipeline-flex benchmarks.

CPU-bound SHA-256 iterations scale with E1_RUNTIME so faster nodes
genuinely finish sooner. Reads inputs from every /in/<dep>/output;
writes a payload of E1_DATA_SIZE bytes to /out/output.
"""
import glob
import hashlib
import os
import time

TASK_NAME = os.environ["E1_TASK_NAME"]
RUNTIME   = float(os.environ.get("E1_RUNTIME", "5.0"))
DATA_SIZE = int(os.environ.get("E1_DATA_SIZE", "0"))
NODE_NAME = os.environ.get("NODE_NAME", "?")
print(f"[{TASK_NAME}] node={NODE_NAME}  runtime={RUNTIME}s  data_size={DATA_SIZE}", flush=True)


def cpu_work(iterations: int) -> bytes:
    data = b"wl-benchmark-payload"
    for _ in range(iterations):
        data = hashlib.sha256(data).digest()
    return data


# Read every dependency input (multi-input fan-in supported).
deps = sorted(glob.glob("/in/*/output"))
for d in deps:
    sz = os.path.getsize(d)
    print(f"[{TASK_NAME}] read {sz} bytes from {d}", flush=True)

iterations = int(RUNTIME * 1_000_000)
print(f"[{TASK_NAME}] running {iterations:,} hash iterations", flush=True)
t0 = time.time()
_ = cpu_work(iterations)
elapsed = time.time() - t0
print(f"[{TASK_NAME}] compute done in {elapsed:.1f}s (target {RUNTIME}s)", flush=True)

os.makedirs("/out", exist_ok=True)
if DATA_SIZE > 0:
    payload = b"\x00" * DATA_SIZE
    with open("/out/output", "wb") as f:
        f.write(payload)
    print(f"[{TASK_NAME}] wrote {DATA_SIZE} bytes", flush=True)
else:
    with open("/out/output", "wb") as f:
        f.write(b"done")
    print(f"[{TASK_NAME}] sentinel output", flush=True)
print(f"[{TASK_NAME}] done", flush=True)
