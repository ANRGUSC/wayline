"""Warm-runner task for the E17 schedule-repair study (the MILCOM 2026 case
study on the testbed): one task of a simulated DAG, executed on this node.

Compute, one of:
  WL_WORK     real work (Tier 2): that many iterations of the task kernel,
              zlib level 6 over a 256 KB block, so the runtime is whatever
              this node's hardware takes;
  WL_RUNTIME  emulated work (Tier 1): sleep that long, the task's runtime on
              its assigned node (its runtimeProfile entry: simulated cost /
              node speed, scaled to seconds), injected by the controller.

Then one named output per out-edge with that edge's size in bytes
(WL_EDGE_SIZES="out:bytes,..."), so the data plane moves the simulated bytes
over the emulated links. Payloads are incompressible. A zero-size edge in the
simulation (precedence only) sends 1 byte.
"""
import os
import random
import time
import zlib

from wl.api import WlTask

_BLOCK = os.urandom(1 << 20)
# The kernel's input: 256 KB over a 16-letter alphabet (compresses about
# 2:1), built at C speed because the runner re-runs this module per call.
_KERNEL = random.Random(1).randbytes(1 << 16).translate(bytes(b"abcdefghijklmnop"[i % 16] for i in range(256))) * 4


def payload(n: int) -> bytes:
    reps, rem = divmod(n, len(_BLOCK))
    return _BLOCK * reps + _BLOCK[:rem]


def main() -> None:
    task = WlTask()
    work = int(os.environ.get("WL_WORK", "0") or 0)
    if work > 0:
        for _ in range(work):
            zlib.compress(_KERNEL, 6)
    else:
        time.sleep(max(0.0, float(os.environ.get("WL_RUNTIME", "0") or 0)))
    sizes = {}
    for item in os.environ.get("WL_EDGE_SIZES", "").split(","):
        if item:
            name, n = item.rsplit(":", 1)
            sizes[name] = int(n)
    for out in [o for o in os.environ.get("WL_OUTPUTS", "").split(",") if o]:
        task.send_raw(out, payload(max(1, sizes.get(out, 1))))
    task.close()


main()
