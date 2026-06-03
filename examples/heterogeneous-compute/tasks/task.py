"""
Heterogeneous compute task: performs real CPU work instead of sleeping.

The workload scales with WL_RUNTIME — higher runtime hint means more
iterations. Actual wall-clock time depends on the node's CPU speed,
so faster nodes genuinely finish sooner. This creates real per-node
runtime differences that the profiler can measure.
"""

import os
import time
import hashlib
from wl.api import WlTask


def cpu_work(iterations: int) -> bytes:
    """CPU-bound: repeated SHA-256 hashing."""
    data = b"wl-benchmark-payload"
    for _ in range(iterations):
        data = hashlib.sha256(data).digest()
    return data


def main():
    task = WlTask()
    name = task.name

    print(f"[{name}] started on node {task.node}")
    print(f"[{name}] deps={task.dependencies}  succs={task.successors}")

    # Receive from dependencies.
    if task.dependencies:
        print(f"[{name}] waiting for {len(task.dependencies)} dep(s)...")
        inputs = task.recv_all_raw()
        for dep, data in inputs.items():
            print(f"[{name}] received from {dep}: {len(data)} bytes")

    # Do real CPU work. Scale iterations so ~1M iterations ≈ 1s on a fast node.
    # Slower nodes will take longer — that's the point.
    target_seconds = task.expected_runtime or 5
    iterations = int(target_seconds * 1_000_000)

    print(f"[{name}] running {iterations:,} hash iterations (target ~{target_seconds}s on fast node)...")
    t0 = time.time()
    result = cpu_work(iterations)
    elapsed = time.time() - t0
    print(f"[{name}] compute done in {elapsed:.1f}s (target was {target_seconds}s)")

    # Generate output payload.
    out_bytes = task.expected_data_size or 0
    if task.successors:
        if out_bytes > 0:
            payload = b"\x00" * out_bytes
            print(f"[{name}] sending {out_bytes} bytes to {task.successors}")
            task.send_raw(payload)
            del payload
        else:
            task.send_raw(b"done")
    else:
        print(f"[{name}] leaf task — nothing to send")

    task.close()
    print(f"[{name}] done (total {time.time() - t0:.1f}s)")


if __name__ == "__main__":
    main()
