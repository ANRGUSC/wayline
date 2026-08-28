#!/usr/bin/env python3
"""E6 Part A generic synthetic task.

Executes one task of a WfChef-derived workflow instance: reads its
declared named inputs, verifies each against the deterministic content
function, emulates the scaled compute time, and emits its named outputs.

Everything is env-driven so ONE image serves all seven workflows:

  WL_WORK_MS       emulated compute time in milliseconds
  WL_OUT_SPECS     outputs to emit: "name:bytes[,name:bytes...]"
  WL_INPUT_KEYS    canonical inputs: "producer.obj[,producer.obj...]"
  WL_INPUT_SIZES   expected bytes per input: "producer.obj:bytes,..."
  WL_INPUT_PEERS   store-lowering override: read these peers instead of
                   the producers, positionally matched to WL_INPUT_KEYS

Content is a pure function of (canonical key, size), so any consumer can
verify any object without out-of-band state; the harness greps the
verify= lines. Compute is emulated with sleep: these are synthetic
executors of real recipe structures, not the scientific codes -- stated
in the paper, per the honesty condition.
"""
import hashlib
import os
import sys
import time

from wl import WlTask


def gen(key: str, size: int) -> bytes:
    block = hashlib.sha256(key.encode()).digest() * 32768  # 1 MiB
    return (block * (size // len(block) + 1))[:size]


def main() -> None:
    task = WlTask()
    name = task.name

    keys = [k for k in os.environ.get("WL_INPUT_KEYS", "").split(",") if k]
    peers = [p for p in os.environ.get("WL_INPUT_PEERS", "").split(",") if p]
    sizes = {}
    for ent in os.environ.get("WL_INPUT_SIZES", "").split(","):
        if ":" in ent:
            k, v = ent.rsplit(":", 1)
            sizes[k] = int(v)

    ok = True
    for i, key in enumerate(keys):
        peer = peers[i] if i < len(peers) else key
        data = task.recv_raw(peer=peer)
        want = sizes.get(key)
        good = (want is None or len(data) == want) and \
            hashlib.sha256(data).hexdigest() == \
            hashlib.sha256(gen(key, len(data))).hexdigest()
        print(f"[{name}] in {key} via {peer}: {len(data)}B "
              f"verify={'OK' if good else 'MISMATCH'}", flush=True)
        ok = ok and good
        del data
    if not ok:
        sys.exit(1)

    work_ms = int(os.environ.get("WL_WORK_MS", "0"))
    time.sleep(work_ms / 1000.0)

    outs = [o for o in os.environ.get("WL_OUT_SPECS", "").split(",") if o]
    for ent in outs:
        oname, nbytes = ent.rsplit(":", 1)
        payload = gen(f"{name}.{oname}", int(nbytes))
        print(f"[{name}] out {oname}: {len(payload)}B", flush=True)
        task.send_raw(oname, payload)
        del payload
    if not outs:
        task.send_raw(b"done")
    task.close()


if __name__ == "__main__":
    main()
