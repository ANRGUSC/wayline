#!/usr/bin/env python3
"""Check the function stages (current stack, no mmcv) against the repo's own
data path: replay + preprocessing against reference.py's pipeline outputs,
and the tracker against its calibrated Kalman track. Runs in the functions
image.

Usage: check_glue.py <reference.npz> [--start 0] [--frames 30]
"""
import argparse
import json

import numpy as np

import gdtm_glue as G

NODES = ["node_1", "node_2", "node_3"]


def maxdiff(a, b):
    a, b = np.asarray(a, np.float64), np.asarray(b, np.float64)
    if a.shape != b.shape:
        return {"shape": [list(a.shape), list(b.shape)]}
    return {"max_abs": float(np.max(np.abs(a - b))), "max_ref": float(np.max(np.abs(b))),
            "exact": bool(np.array_equal(a, b))}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("reference")
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--frames", type=int, default=30)
    a = ap.parse_args()
    ref = np.load(a.reference)
    out = {}
    for node in NODES:
        for mod in G.FILES:
            x = G.PREP[mod](G.read(node, mod, a.start, a.frames))
            out[f"prep/{mod}/{node}"] = maxdiff(x, ref[f"x/{mod}/{node}"])
    ab = ref["calib_ab"]
    pos, cov = G.track(ref["det_means"], ref["det_covs"], float(ab[0]), float(ab[1]))
    out["track/pos"] = maxdiff(pos, ref["track_means"])
    out["track/cov"] = maxdiff(cov, ref["track_covs"])
    print(json.dumps(out, indent=1))
