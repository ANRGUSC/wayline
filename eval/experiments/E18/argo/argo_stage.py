#!/usr/bin/env python3
"""E16 Argo stage for GDTM and FOCAL: the same stage code as the Wayline
stages (examples/gdtm/stage.py, examples/focal-mod/stage.py), reading its
inputs from the files Argo downloaded into /in (in dependency order) and
writing each named output to /out/<name> for Argo to upload.

  python argo_stage.py gdtm <role> <mod> <node> <threads> <start> <frames>
  python argo_stage.py focal <role> <mod> <batch> <seed> <threads>

Runs in the workload's own images (wl-gdtm, wl-gdtm-model, wl-focal), each
of which has the modules its roles import.
"""
import glob
import io
import os
import sys
import time

import numpy as np


def pack(a):
    b = io.BytesIO(); np.save(b, a, allow_pickle=False); return b.getvalue()


def unpack(b):
    return np.load(io.BytesIO(b), allow_pickle=False)


def pack_many(**arrays):
    b = io.BytesIO(); np.savez(b, **arrays); return b.getvalue()


def unpack_many(b):
    return dict(np.load(io.BytesIO(b), allow_pickle=False))


def pack_jpegs(codes):
    return pack_many(data=np.concatenate(codes), offsets=np.cumsum([0] + [len(c) for c in codes]))


def unpack_jpegs(b):
    d = unpack_many(b)
    o = d["offsets"]
    return [d["data"][o[i]:o[i + 1]] for i in range(len(o) - 1)]


def threads(n):
    try:
        import torch
        torch.set_num_threads(n)
    except ImportError:
        pass
    try:
        import cv2
        cv2.setNumThreads(n)
    except ImportError:
        pass


GDTM_MODS = {"zed": "zed_camera_left", "depth": "realsense_camera_depth",
             "radar": "range_doppler", "audio": "mic_waveform"}
GDTM_NODES = {"n1": "node_1", "n2": "node_2", "n3": "node_3"}
GDTM_CALIB = (5.0, 0.0)  # examples/gdtm/gen_gdtm.py CALIB


def gdtm(role, mod, node, start, frames, inputs):
    """Outputs as {name: bytes}, named as in gen_gdtm.stages."""
    if role == "source":
        import gdtm_glue as G
        out = {}
        for short, full in GDTM_MODS.items():
            vals = G.read(GDTM_NODES[node], full, start, frames)
            out[short] = pack_jpegs(vals) if full in G.JPEG else pack(vals)
        return out
    if role == "prep":
        import gdtm_glue as G
        raw = inputs[0]
        full = GDTM_MODS[mod]
        return {"x": pack(G.PREP[full](unpack_jpegs(raw) if full in G.JPEG else unpack(raw)))}
    if role == "embed":
        import gdtm_model as M
        return {"emb": pack(M.embed(GDTM_MODS[mod], GDTM_NODES[node], unpack(inputs[0])))}
    if role == "fuse":
        import gdtm_model as M
        keys = [(GDTM_MODS[m], GDTM_NODES[n]) for n in GDTM_NODES for m in GDTM_MODS]
        means, covs = M.fuse({k: unpack(b) for k, b in zip(keys, inputs)})
        return {"det": pack_many(means=means, covs=covs)}
    if role == "track":
        import gdtm_glue as G
        det = unpack_many(inputs[0])
        pos, cov = G.track(det["means"], det["covs"], *GDTM_CALIB)
        return {"track": pack_many(pos=pos, cov=cov, det_means=det["means"])}
    raise SystemExit(f"unknown GDTM role {role!r}")


def focal(role, mod, batch, seed, inputs):
    import focal_stages as F
    if role == "source":
        return {"samples": pack(F.source(mod, batch, seed + F.MODS.index(mod)))}
    if role == "fft":
        return {"spectrum": pack(F.fft(unpack(inputs[0])))}
    if role == "encode":
        return {"features": pack(F.encode(mod, unpack(inputs[0])))}
    if role == "fuse_head":
        return {"logits": pack(F.fuse_head({m: unpack(b) for m, b in zip(F.MODS, inputs)}))}
    raise SystemExit(f"unknown FOCAL role {role!r}")


def main():
    workload, role = sys.argv[1], sys.argv[2]
    inputs = [open(p, "rb").read() for p in sorted(glob.glob("/in/*"))]
    t0 = time.time()
    if workload == "gdtm":
        mod, node, n, start, frames = sys.argv[3], sys.argv[4], int(sys.argv[5]), int(sys.argv[6]), int(sys.argv[7])
        threads(n)
        out = gdtm(role, mod, node, start, frames, inputs)
    else:
        mod, batch, seed, n = sys.argv[3], int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6])
        threads(n)
        out = focal(role, mod, batch, seed, inputs)
    os.makedirs("/out", exist_ok=True)
    for name, data in out.items():
        with open(f"/out/{name}", "wb") as f:
            f.write(data)
    print(f"[{workload}] {role} {mod} {time.time() - t0:.3f}s", flush=True)


if __name__ == "__main__":
    main()
