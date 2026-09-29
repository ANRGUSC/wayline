#!/usr/bin/env python3
"""GDTM's unsplit early-fusion model on one window, through the repo's own
data path (HDF5Dataset, its mmdet pipelines, collate), its test loop
(model(data, return_loss=False), as mmtrack.apis.multi_gpu_test runs it) and
its evaluation (calibrate_outputs, track_eval), on CPU. Then the split model
stages (gdtm_model.embed, gdtm_model.fuse) on the same inputs, compared with
the unsplit ones. Runs in the model image.

Writes an .npz with every stage boundary: pipeline outputs per (modality,
node), adapter embeddings, detections, calibrated tracked positions and the
OptiTrack ground truth, for check_glue.py and the Wayline runs to compare to.

Usage: reference.py <segment-dir> <out.npz> [--start 0] [--frames 30] [--threads 4]
"""
import argparse
import json
import tempfile
import time
from collections import defaultdict

import numpy as np
import torch


class _NoEvent:
    """forward_track creates CUDA timing events it never reads; CPU has none."""
    def __init__(self, *a, **k):
        pass

    def record(self, *a, **k):
        pass


torch.cuda.Event = _NoEvent

import gdtm_model as G  # noqa: E402
from make_segment import cut  # noqa: E402
from mmcv.parallel import MMDataParallel  # noqa: E402
from mmtrack.datasets import build_dataloader, build_dataset  # noqa: E402
from mmtrack.models import build_model  # noqa: E402


def maxdiff(a, b):
    a, b = np.asarray(a, np.float64), np.asarray(b, np.float64)
    return float(np.max(np.abs(a - b))), float(np.max(np.abs(b)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("segment")
    ap.add_argument("out")
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--frames", type=int, default=30)
    ap.add_argument("--threads", type=int, default=4)
    a = ap.parse_args()
    torch.set_num_threads(a.threads)

    win = tempfile.mkdtemp()
    stamps = cut(a.segment, f"{win}/test", a.start, a.frames)
    cfg = G.config()
    t = cfg.data.test
    t.cacher_cfg.hdf5_fnames = [f.replace("~/Desktop/mcp-sample-dataset/test", f"{win}/test")
                                for f in t.cacher_cfg.hdf5_fnames]
    t.cacher_cfg.cache_dir = f"{win}/cache/"
    ds = build_dataset(t)
    keys = [k for k in ds[0][-1] if k[0] != "mocap"]
    assert keys == G.ORDER, f"dataset key order {keys} != gdtm_model.ORDER"
    assert len(ds) == a.frames, f"dataset has {len(ds)} frames, expected {a.frames}"

    # Unsplit: the released model, the repo's loader and test loop.
    model = build_model(cfg.model)
    model.load_state_dict(G.full_state_dict(), strict=True)
    model.eval()
    hooked = defaultdict(list)
    for mod, node in keys:
        model.models[f"{mod}_{node}"].register_forward_hook(
            lambda m, i, o, k=(mod, node): hooked[k].append(o.detach().numpy().copy()))
    runner = MMDataParallel(model)  # no GPU: scatters to CPU
    loader = build_dataloader(ds, samples_per_gpu=1, workers_per_gpu=0, dist=False, shuffle=False)
    results = defaultdict(list)
    t0 = time.time()
    for data in loader:
        with torch.no_grad():
            result = runner(data, return_loss=False)
        for k, v in result.items():
            results[k].append(v)
    t_unsplit = time.time() - t0
    det_means = torch.stack([m.reshape(-1) for m in results["det_means"]]).numpy()
    det_covs = torch.stack([c.reshape(2, 2) for c in results["det_covs"]]).numpy()

    res_json = f"{G.WEIGHTS}/val_res.json"
    calib = ds.calibrate_outputs(results, res_json, "nll")
    gt = ds.collect_gt()
    _, vid = ds.track_eval(calib, gt)
    track_means = torch.cat(vid["track_means"]).numpy()
    track_covs = torch.cat(vid["track_covs"]).numpy()
    gt_pos = gt["all_gt_pos"][:, 0, :].numpy()
    err = np.linalg.norm(gt_pos - track_means, axis=1)

    # Split: the stages on the repo's own pipeline outputs.
    X = {k: np.stack([ds[i][-1][k]["img"].data.numpy() for i in range(len(ds))]) for k in keys}
    stage_s = {}
    emb = {}
    for k in keys:
        t0 = time.time()
        emb[k] = G.embed(k[0], k[1], X[k])
        stage_s["embed/%s/%s" % k] = time.time() - t0
    t0 = time.time()
    s_means, s_covs = G.fuse(emb)
    stage_s["fuse"] = time.time() - t0
    ab = G.calibration(res_json)[0]
    calib_mine = np.stack([ab[0] * c + ab[1] * np.eye(2, dtype=np.float32) for c in det_covs])
    calib_repo = torch.stack([c[0] for c in calib["det_covs"]]).numpy()

    report = {
        "frames": a.frames, "start": a.start, "stamps": [stamps[0], stamps[-1]], "threads": a.threads,
        "order": ["%s/%s" % k for k in keys],
        "unsplit_s": t_unsplit, "split_stage_s": stage_s,
        "calibration_ab": ab,
        "diff_embed": {"%s/%s" % k: maxdiff(emb[k], np.concatenate(hooked[k])) for k in keys},
        "diff_det_means": maxdiff(s_means, det_means), "diff_det_covs": maxdiff(s_covs, det_covs),
        "diff_calibration": maxdiff(calib_mine, calib_repo),
        "track_error_cm": {"mean": float(err.mean()), "median": float(np.median(err)),
                           "last_half_mean": float(err[len(err) // 2:].mean())},
        "shapes": {"%s/%s" % k: list(X[k].shape) for k in keys},
    }
    arrays = {f"x/{m}/{n}": X[(m, n)] for m, n in keys}
    arrays.update({f"emb/{m}/{n}": np.concatenate(hooked[(m, n)]) for m, n in keys})
    arrays.update(det_means=det_means, det_covs=det_covs, track_means=track_means, track_covs=track_covs,
                  gt_pos=gt_pos, calib_ab=np.array(ab))
    np.savez(a.out, **arrays)
    print(json.dumps(report, indent=1))
    with open(a.out.rsplit(".", 1)[0] + ".json", "w") as f:
        json.dump(report, f, indent=1)


if __name__ == "__main__":
    main()
