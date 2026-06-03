"""
Generic task script for multi-ODAG HEFT scheduling test.

Behaviour is controlled entirely by the WlTask metadata injected by the
controller (dependencies, successors, dataSize, runtime).  The script:

  1. Receives data from ALL dependencies (if any).
  2. Simulates work by sleeping for the expected runtime.
  3. Generates a raw binary payload matching the declared dataSize.
  4. Sends the payload to successors (if any), then closes.

Uses send_raw() to transmit full-size data without JSON double-copy OOM.
"""

import os
import time
from wl_sdk.api import WlTask


def main():
    task = WlTask()
    name = task.name

    print(f"[{name}] started on node {task.node}")
    print(f"[{name}] deps={task.dependencies}  succs={task.successors}")
    print(f"[{name}] expected runtime={task.expected_runtime}s  "
          f"dataSize={task.expected_data_size} bytes")

    # ── Step 1: receive from dependencies (raw bytes, no JSON) ─
    inputs = {}
    if task.dependencies:
        print(f"[{name}] waiting for {len(task.dependencies)} dependency(ies)...")
        inputs = task.recv_all_raw()
        for dep, data in inputs.items():
            print(f"[{name}] received from {dep}: {len(data)} bytes")
            del data  # free memory immediately
        inputs = {k: None for k in inputs}  # keep keys, free payload

    # ── Step 2: simulate work ───────────────────────────────────
    work_time = task.expected_runtime or 5
    print(f"[{name}] working for {work_time}s ...")
    time.sleep(work_time)

    # ── Step 3: build full-size binary payload ──────────────────
    out_bytes = task.expected_data_size or 0

    # ── Step 4: send (if not a leaf) and close ──────────────────
    if task.successors:
        if out_bytes > 0:
            # Generate raw binary payload — no JSON, no double-copy.
            print(f"[{name}] generating {out_bytes} bytes payload...")
            payload = b"\x00" * out_bytes
            print(f"[{name}] sending {out_bytes} bytes to {task.successors}")
            task.send_raw(payload)
            del payload  # free memory immediately
        else:
            print(f"[{name}] sending empty marker to {task.successors}")
            task.send_raw(b"done")
    else:
        print(f"[{name}] leaf task — nothing to send")

    task.close()
    print(f"[{name}] done")


if __name__ == "__main__":
    main()
