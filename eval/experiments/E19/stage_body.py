"""One task of an E17 Tier 2 DAG, for the baseline systems (Ray, Dask, Argo).

The same work as Wayline's E17 runner (E17/runner/stage.py): WORK iterations
of the task kernel (zlib level 6 over a 256 KB block, the kernel the Tier 2
rates were measured with), then one incompressible payload per out-edge of
that edge's size. Returns (start, end, {child: bytes}) with the node's wall
clock, so every system's makespan is measured the same way: first task start
to last task end.
"""
import os
import random
import time
import zlib

_BLOCK = os.urandom(1 << 20)
_KERNEL = random.Random(1).randbytes(1 << 16).translate(bytes(b"abcdefghijklmnop"[i % 16] for i in range(256))) * 4


def payload(n):
    reps, rem = divmod(n, len(_BLOCK))
    return _BLOCK * reps + _BLOCK[:rem]


def stage(work, outs, *inputs):
    t0 = time.time()
    _ = sum(len(x) for x in inputs)   # touch every input
    for _ in range(int(work)):
        zlib.compress(_KERNEL, 6)
    res = {c: payload(max(1, int(n))) for c, n in outs.items()}
    return t0, time.time(), res


def bench(seconds=5.0):
    """Kernel iterations per second on this node, in this interpreter."""
    t0, n = time.time(), 0
    while time.time() - t0 < seconds:
        zlib.compress(_KERNEL, 6)
        n += 1
    return n / (time.time() - t0)
