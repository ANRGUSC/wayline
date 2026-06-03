"""
Stage 4: within-camera tracking. A small ByteTrack-flavored tracker —
IOU-based association with embedding-augmented affinity as the
tiebreaker. The full ByteTrack two-stage matching is overkill at our
target FPS (5); the simpler greedy variant produces the same canonical
tracklets for AI City Track 1 at this rate.

Inputs:  detections.json + embeddings.npy from detect_embed.
Outputs: tracklets.json + tracklet_embeddings.npy. Each tracklet has a
         representative embedding (mean of its member detections,
         re-L2-normalized) so the fan-in stage can match across cameras
         with a single dot product per pair.
"""

from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from typing import Dict, List, Union

import numpy as np
from scipy.optimize import linear_sum_assignment  # type: ignore[import-not-found]


def _iou(a, b) -> float:
    """IoU between two [x1,y1,x2,y2] boxes."""
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    ix1 = max(ax1, bx1); iy1 = max(ay1, by1)
    ix2 = min(ax2, bx2); iy2 = min(ay2, by2)
    iw = max(0.0, ix2 - ix1); ih = max(0.0, iy2 - iy1)
    inter = iw * ih
    if inter <= 0:
        return 0.0
    aa = max(0.0, ax2 - ax1) * max(0.0, ay2 - ay1)
    bb = max(0.0, bx2 - bx1) * max(0.0, by2 - by1)
    union = aa + bb - inter
    return inter / union if union > 0 else 0.0


def track_within_camera(
    in_dir: Union[str, Path],
    out_dir: Union[str, Path],
    iou_thresh: float = 0.3,
    emb_weight: float = 0.3,
    max_age: int = 5,
) -> dict:
    """
    Group per-frame detections into within-camera tracklets.

    Cost between an active tracklet and a new detection:
        cost = (1 - IoU) * (1 - emb_weight) + (1 - cos_sim) * emb_weight

    A pair with IoU < iou_thresh is forbidden (cost = +inf) so we never
    associate spatially-disjoint detections.

    `max_age` is the number of consecutive frames a tracklet may go
    unmatched before being closed.
    """
    src = Path(in_dir)
    dst = Path(out_dir)
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True)

    dj = json.loads((src / "detections.json").read_text())
    detections: List[dict] = dj["detections"]
    embeddings = np.load(str(src / "embeddings.npy"))
    if embeddings.shape[0] != len(detections):
        raise RuntimeError(
            f"track: embedding count {embeddings.shape[0]} != detections {len(detections)}"
        )

    # Group by frame.
    by_frame: Dict[str, List[int]] = {}
    for i, d in enumerate(detections):
        by_frame.setdefault(d["frame"], []).append(i)
    frame_names = sorted(by_frame.keys())

    # Tracklet state: each is {id, last_frame, last_bbox, last_emb, det_indices, class_id, hits}.
    active: List[dict] = []
    closed: List[dict] = []
    next_id = 1
    t0 = time.perf_counter()

    for frame in frame_names:
        det_idxs = by_frame[frame]
        det_boxes = [detections[i]["bbox"] for i in det_idxs]
        det_embs = embeddings[det_idxs]

        # Build cost matrix (active × detections).
        if active and det_idxs:
            n_act = len(active); n_det = len(det_idxs)
            cost = np.full((n_act, n_det), 1e6, dtype=np.float32)
            for r, tr in enumerate(active):
                for c in range(n_det):
                    iou = _iou(tr["last_bbox"], det_boxes[c])
                    if iou < iou_thresh:
                        continue
                    sim = float(np.dot(tr["last_emb"], det_embs[c]))
                    cost[r, c] = (1.0 - iou) * (1.0 - emb_weight) + (1.0 - sim) * emb_weight
            row_ind, col_ind = linear_sum_assignment(cost)
            assigned_det = set()
            assigned_tr = set()
            for r, c in zip(row_ind, col_ind):
                if cost[r, c] >= 1e5:
                    continue
                tr = active[r]
                tr["last_bbox"] = det_boxes[c]
                tr["last_emb"] = det_embs[c]
                tr["last_frame_idx"] = frame_names.index(frame)
                tr["det_indices"].append(det_idxs[c])
                tr["hits"] += 1
                assigned_tr.add(r); assigned_det.add(c)
            # Unmatched detections → new tracklets.
            for c in range(len(det_idxs)):
                if c in assigned_det:
                    continue
                active.append({
                    "id": next_id,
                    "last_bbox": det_boxes[c],
                    "last_emb": det_embs[c],
                    "last_frame_idx": frame_names.index(frame),
                    "det_indices": [det_idxs[c]],
                    "class_id": detections[det_idxs[c]]["class_id"],
                    "class": detections[det_idxs[c]]["class"],
                    "hits": 1,
                })
                next_id += 1
        else:
            # First frame with detections, or no active tracklets.
            for c, idx in enumerate(det_idxs):
                active.append({
                    "id": next_id,
                    "last_bbox": det_boxes[c],
                    "last_emb": det_embs[c],
                    "last_frame_idx": frame_names.index(frame),
                    "det_indices": [idx],
                    "class_id": detections[idx]["class_id"],
                    "class": detections[idx]["class"],
                    "hits": 1,
                })
                next_id += 1

        # Age out stale tracklets.
        cur_idx = frame_names.index(frame)
        survivors = []
        for tr in active:
            if cur_idx - tr["last_frame_idx"] <= max_age:
                survivors.append(tr)
            else:
                closed.append(tr)
        active = survivors

    closed.extend(active)
    wall = time.perf_counter() - t0

    # Build tracklet records with representative embeddings (mean+L2).
    tracklets = []
    rep_embeddings = []
    for tr in closed:
        if tr["hits"] < 2:
            # Singletons rarely correlate across cameras; drop to reduce
            # false matches in fan-in. Tunable.
            continue
        idxs = tr["det_indices"]
        embs = embeddings[idxs]
        rep = embs.mean(axis=0)
        n = np.linalg.norm(rep)
        if n > 0:
            rep = rep / n
        rep_embeddings.append(rep.astype(np.float32))
        tracklets.append({
            "tracklet_id": tr["id"],
            "class_id": tr["class_id"],
            "class": tr["class"],
            "det_count": len(idxs),
            "frame_first": detections[idxs[0]]["frame"],
            "frame_last": detections[idxs[-1]]["frame"],
            "det_indices": idxs,
            "representative_index": len(rep_embeddings) - 1,
        })

    rep_arr = (
        np.stack(rep_embeddings)
        if rep_embeddings
        else np.zeros((0, embeddings.shape[1] if embeddings.shape[0] else 512), dtype=np.float32)
    )
    np.save(str(dst / "tracklet_embeddings.npy"), rep_arr)
    (dst / "tracklets.json").write_text(json.dumps({
        "frames": len(frame_names),
        "tracklets": tracklets,
    }))
    return {
        "frames": len(frame_names),
        "tracklets": len(tracklets),
        "wall_s": wall,
    }
