"""Task bodies shared by the E16 Ray driver and Argo task (generated from
../ray/e16_ray.py by argo/build: do not edit)."""
import hashlib
import json
import random
import time

import numpy as np

RATE = 2.568e6  # hashes/s on a full-clock UP board (as ft_recovery.py)


# ─── task bodies (as the Wayline tasks') ─────────────────────────────────────

def spin(n):
    d = b"wl-e11"
    for _ in range(n):
        d = hashlib.sha256(d).digest()


def body(name, role, runtime, size, inputs):
    t0 = time.perf_counter()
    if role == "capture":
        time.sleep(min(runtime, 5.0))
        block = hashlib.md5(name.encode()).digest() * 4096
        return np.frombuffer((block * (size // len(block) + 1))[:size], dtype=np.uint8)
    if role == "preprocess":
        raw = inputs[0].tobytes()
        digest = b"".join(hashlib.sha256(raw[o:o + 65536]).digest() for o in range(0, len(raw), 65536))
        time.sleep(max(0.0, runtime - (time.perf_counter() - t0)))
        rest = size - len(digest)
        feats = digest + (raw * (rest // max(len(raw), 1) + 1))[:rest] if rest > 0 else digest[:size]
        return np.frombuffer(feats, dtype=np.uint8)
    if role == "infer":
        feats = inputs[0].tobytes()
        i = 0
        while time.perf_counter() - t0 < runtime:
            hashlib.sha256(feats[i % len(feats):i % len(feats) + 4096]).digest()
            i += 1
        rng = random.Random(name)
        dets = [{"id": f"{name}-{k}", "confidence": round(rng.uniform(0.4, 0.99), 3)}
                for k in range(rng.randint(5, 15))]
        return np.frombuffer(json.dumps({"source": name, "detections": dets}).encode(), dtype=np.uint8)
    if role in ("fuse", "report"):
        merged = [json.loads(x.tobytes()) for x in inputs]
        time.sleep(max(0.0, runtime - (time.perf_counter() - t0)))
        return np.frombuffer(json.dumps({"source": name, "inputs": len(merged)}).encode(), dtype=np.uint8)
    if role == "hash":
        _ = [x.nbytes for x in inputs]
        spin(int(runtime * RATE))
        return np.zeros(size, dtype=np.uint8)
    raise ValueError(role)


