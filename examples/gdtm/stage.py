#!/usr/bin/env python3
"""One stage of the GDTM tracker, chosen by GDTM_ROLE, on GDTM_NODE's
GDTM_MOD. The same script runs as a pod (container stage) or as a call on a
warm runner (function stage); each role imports only what its image has.

  source  replay one node's four sensors for the window        functions image
  prep    the dataset's preprocessing for one (modality, node)  functions image
  embed   backbone + that node's adapter                        model image
  fuse    cross-attention fusion and output head                model image
  track   covariance calibration and the Kalman filter          functions image

Arrays travel as np.save bytes (JPEG streams as np.savez of bytes + offsets),
never pickles, so the two images' numpy versions need not match.
"""
import io
import os
import time

import numpy as np

from wl import WlTask

MODS = {"zed": "zed_camera_left", "depth": "realsense_camera_depth",
        "radar": "range_doppler", "audio": "mic_waveform"}
NODES = {"n1": "node_1", "n2": "node_2", "n3": "node_3"}


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
    import torch
    torch.set_num_threads(n)
    try:
        import cv2
        cv2.setNumThreads(n)
    except ImportError:
        pass


task = WlTask()
env = os.environ
role, mod, node = env["GDTM_ROLE"], env.get("GDTM_MOD", ""), env.get("GDTM_NODE", "")
start, frames = int(env.get("GDTM_START", "0")), int(env.get("GDTM_FRAMES", "30"))
threads(int(env.get("GDTM_THREADS", "1")))
t0 = time.time()
if role == "source":
    import gdtm_glue as G
    for short, full in MODS.items():
        vals = G.read(NODES[node], full, start, frames)
        task.send_raw(short, pack_jpegs(vals) if full in G.JPEG else pack(vals))
elif role == "prep":
    import gdtm_glue as G
    raw = task.recv_raw(peer=f"source-{node}.{mod}")
    x = G.PREP[MODS[mod]](unpack_jpegs(raw) if MODS[mod] in G.JPEG else unpack(raw))
    task.send_raw("x", pack(x))
elif role == "embed":
    import gdtm_model as M
    x = unpack(task.recv_raw(peer=f"prep-{mod}-{node}.x"))
    task.send_raw("emb", pack(M.embed(MODS[mod], NODES[node], x)))
elif role == "fuse":
    import gdtm_model as M
    embeds = {(MODS[m], NODES[n]): unpack(task.recv_raw(peer=f"embed-{m}-{n}.emb")) for n in NODES for m in MODS}
    means, covs = M.fuse(embeds)
    task.send_raw("det", pack_many(means=means, covs=covs))
elif role == "track":
    import gdtm_glue as G
    det = unpack_many(task.recv_raw(peer="fuse.det"))
    pos, cov = G.track(det["means"], det["covs"], float(env["GDTM_CALIB_A"]), float(env["GDTM_CALIB_B"]))
    task.send_raw("track", pack_many(pos=pos, cov=cov, det_means=det["means"]))
    print(f"[track] {len(pos)} frames, last position {pos[-1].round(1).tolist()} cm", flush=True)
else:
    raise SystemExit(f"unknown GDTM_ROLE {role!r}")
print(f"[{task.name}] {role} {mod} {node} frames={frames} {time.time() - t0:.3f}s", flush=True)
task.close()
