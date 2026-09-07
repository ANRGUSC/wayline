#!/usr/bin/env python3
"""E7 workload task. One image, role by WL_ROLE.

  produce : compute WL_WORK_S, emit named object 'payload' of exactly
            WL_BYTES bytes (deterministic content keyed by 'produce.payload').
  consume : read the payload via the serve vertex, verify length and
            SHA-256, then emit a tiny result object carrying the digest.
  report  : read the three consumer results, verify all match the
            expected digest, emit 'ok'.

Content is a pure function of (key, size) so any node can verify without
out-of-band state; the harness greps the verify= lines. A consumer must
never see a short or mismatched payload -- that is the safety property
E7 exists to check, so verification is strict and exits nonzero on any
discrepancy.
"""
import hashlib
import os
import sys
import time

from wl import WlTask

ROLE = os.environ["WL_ROLE"]
PAYLOAD_KEY = "produce.payload"


def gen(key: str, size: int) -> bytes:
    block = hashlib.sha256(key.encode()).digest() * 32768  # 1 MiB
    return (block * (size // len(block) + 1))[:size]


def expected_digest(size: int) -> str:
    return hashlib.sha256(gen(PAYLOAD_KEY, size)).hexdigest()


def main() -> None:
    task = WlTask()
    name = task.name
    time.sleep(float(os.environ.get("WL_WORK_S", "0")))

    if ROLE == "produce":
        n = int(os.environ["WL_BYTES"])
        payload = gen(PAYLOAD_KEY, n)
        print(f"[{name}] emit payload {len(payload)}B "
              f"sha={hashlib.sha256(payload).hexdigest()[:16]}", flush=True)
        task.send_raw("payload", payload)
        task.close()
        return

    if ROLE == "consume":
        # Single dependency: the serve vertex, which aliases the payload
        # under its own name. Read the payload as the producer's local
        # output would be read.
        peer = os.environ.get("WL_DEPS", "").split(",")[0]
        data = task.recv_raw(peer=peer)
        n = int(os.environ["WL_BYTES"])
        got = hashlib.sha256(data).hexdigest()
        want = expected_digest(n)
        ok = (len(data) == n and got == want)
        print(f"[{name}] recv {len(data)}B via {peer} sha={got[:16]} "
              f"verify={'OK' if ok else 'MISMATCH'}", flush=True)
        if not ok:
            sys.exit(1)
        del data
        # Emit the digest as the default output for the report to read.
        task.send_raw(got.encode())
        task.close()
        return

    if ROLE == "report":
        peers = [p for p in os.environ.get("WL_DEPS", "").split(",") if p]
        n = int(os.environ["WL_BYTES"])
        want = expected_digest(n)
        allok = True
        for p in peers:
            d = task.recv_raw(peer=p).decode(errors="replace").strip()
            good = (d == want)
            print(f"[{name}] consumer {p} digest={d[:16]} "
                  f"verify={'OK' if good else 'MISMATCH'}", flush=True)
            allok = allok and good
        if not allok:
            sys.exit(1)
        print(f"[{name}] report verify=OK ({len(peers)} consumers)", flush=True)
        task.send_raw(b"ok")
        task.close()
        return

    sys.exit(f"unknown WL_ROLE {ROLE}")


if __name__ == "__main__":
    main()
