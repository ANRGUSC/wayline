#!/usr/bin/env python3
"""E8 workload task. Role by WL_ROLE.

  produce : emit WL_NOBJ named objects obj0..obj{N-1}, each WL_BYTES bytes,
            digest-distinct (content keyed by "<task>.<name>"). One object
            (default) for Part D; up to 128 for Part B.
  gate    : Part B only -- depend on the producers and block until the run
            is deleted, keeping the objects live for realization patching.
            Emits nothing; a long sleep with a hard ceiling.
  consume : Part D -- receive the single named object from the producer and
            verify length + SHA-256 locally, then finish.

Content is a pure function of (key, size) so any node verifies without
out-of-band state.
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
    role = os.environ["WL_ROLE"]
    nbytes = int(os.environ.get("WL_BYTES", "1048576"))

    if role == "produce":
        nobj = int(os.environ.get("WL_NOBJ", "1"))
        for i in range(nobj):
            name = f"obj{i}"
            payload = gen(f"{task.name}.{name}", nbytes)
            task.send_raw(name, payload)
        print(f"[{task.name}] emitted {nobj} object(s) x {nbytes}B", flush=True)
        task.close()
        return

    if role == "gate":
        # Keep the run live so its objects stay available for realization.
        # Bounded so a lost delete cannot wedge the node forever.
        print(f"[{task.name}] gate open", flush=True)
        time.sleep(int(os.environ.get("WL_GATE_S", "280")))
        task.send_raw(b"gate")
        task.close()
        return

    if role == "consume":
        # A named object is read by its "producer.object" key (E5/E6
        # pattern), not the producer's default output.
        producer = os.environ.get("WL_DEPS", "").split(",")[0]
        key = f"{producer}.obj0"
        data = task.recv_raw(peer=key)
        got = hashlib.sha256(data).hexdigest()
        want = hashlib.sha256(gen(key, nbytes)).hexdigest()
        ok = (len(data) == nbytes and got == want)
        print(f"[{task.name}] recv {len(data)}B sha={got[:16]} "
              f"verify={'OK' if ok else 'MISMATCH'}", flush=True)
        if not ok:
            sys.exit(1)
        task.send_raw(b"ok")
        task.close()
        return

    sys.exit(f"unknown WL_ROLE {role}")


if __name__ == "__main__":
    main()
