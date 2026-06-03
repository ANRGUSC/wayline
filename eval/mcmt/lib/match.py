"""
Stage 5 (fan-in): match per-camera tracklets across cameras to assign
global vehicle IDs. Pairwise cosine similarity on representative
embeddings + Hungarian assignment, with a similarity threshold below
which tracklets are kept as singleton globals (a vehicle seen in only
one camera).

Input directory layout (one subdir per camera):
    in_dir/
      cam-1/tracklets.json
      cam-1/tracklet_embeddings.npy
      cam-2/...
      cam-3/...
      cam-4/...

Output:
    out_dir/global_tracks.json
"""

from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from typing import List, Union

import numpy as np
from scipy.optimize import linear_sum_assignment  # type: ignore[import-not-found]


def cross_camera_match(
    in_dir: Union[str, Path],
    out_dir: Union[str, Path],
    sim_thresh: float = 0.55,
) -> dict:
    """
    Read per-camera tracklets + embeddings from in_dir/<camera>/..., assign
    global vehicle IDs.

    sim_thresh: cosine-similarity floor for considering two cross-camera
    tracklets the same global vehicle. Pairs below this stay separate
    globals. Tuned on AI City Track 1 dev; survives a wide band so this
    knob isn't on the critical path.
    """
    src = Path(in_dir)
    dst = Path(out_dir)
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True)

    # Discover per-camera subdirs.
    cam_dirs = sorted(
        p for p in src.iterdir()
        if p.is_dir() and (p / "tracklets.json").is_file()
    )
    if not cam_dirs:
        raise RuntimeError(f"cross_camera_match: no camera subdirs under {src}")

    # Load tracklets and reps per camera.
    cam_tracklets: List[dict] = []   # one entry per camera: {camera, tracklets, embs}
    for cd in cam_dirs:
        tj = json.loads((cd / "tracklets.json").read_text())
        embs = np.load(str(cd / "tracklet_embeddings.npy"))
        cam_tracklets.append({
            "camera": cd.name,
            "tracklets": tj["tracklets"],
            "embs": embs,
        })

    t0 = time.perf_counter()

    # Union-Find over (cam_idx, tracklet_local_idx) tuples.
    # Build a flat list of nodes first.
    flat: List[tuple] = []  # (cam_idx, local_idx, embedding, tracklet)
    for ci, cam in enumerate(cam_tracklets):
        for li, tr in enumerate(cam["tracklets"]):
            ri = tr["representative_index"]
            flat.append((ci, li, cam["embs"][ri], tr))

    parent = list(range(len(flat)))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    # For each pair of cameras (i < j), Hungarian-match their tracklets,
    # and union pairs whose cosine similarity >= sim_thresh.
    for i in range(len(cam_tracklets)):
        for j in range(i + 1, len(cam_tracklets)):
            tr_i = cam_tracklets[i]["tracklets"]
            tr_j = cam_tracklets[j]["tracklets"]
            if not tr_i or not tr_j:
                continue
            emb_i = cam_tracklets[i]["embs"]
            emb_j = cam_tracklets[j]["embs"]
            # rep indices line up with tracklets list order.
            sim = emb_i @ emb_j.T  # (Ti, Tj) cosine since rows are L2-normalized
            cost = 1.0 - sim
            ri, rj = linear_sum_assignment(cost)
            for a, b in zip(ri, rj):
                if sim[a, b] >= sim_thresh:
                    # Map (cam_local) back to flat index.
                    flat_a = sum(len(c["tracklets"]) for c in cam_tracklets[:i]) + a
                    flat_b = sum(len(c["tracklets"]) for c in cam_tracklets[:j]) + b
                    union(flat_a, flat_b)

    # Collect components.
    groups: dict = {}
    for x in range(len(flat)):
        root = find(x)
        groups.setdefault(root, []).append(x)

    global_tracks = []
    for gi, (root, members) in enumerate(sorted(groups.items()), start=1):
        camera_path = []
        classes: dict = {}
        for m in members:
            ci, li, _emb, tr = flat[m]
            camera_path.append({
                "camera": cam_tracklets[ci]["camera"],
                "tracklet_id": tr["tracklet_id"],
                "class": tr["class"],
                "frame_first": tr["frame_first"],
                "frame_last": tr["frame_last"],
                "det_count": tr["det_count"],
            })
            classes[tr["class"]] = classes.get(tr["class"], 0) + tr["det_count"]
        # Vehicle class by majority det count.
        rep_class = max(classes, key=classes.get)  # type: ignore[arg-type]
        global_tracks.append({
            "global_id": gi,
            "class": rep_class,
            "camera_path": camera_path,
            "cameras": sorted({hop["camera"] for hop in camera_path}),
        })

    wall = time.perf_counter() - t0
    (dst / "global_tracks.json").write_text(json.dumps({
        "sim_thresh": sim_thresh,
        "n_cameras": len(cam_tracklets),
        "n_input_tracklets": len(flat),
        "n_global_tracks": len(global_tracks),
        "global_tracks": global_tracks,
        "wall_s": wall,
    }, indent=2))
    return {
        "cameras": len(cam_tracklets),
        "input_tracklets": len(flat),
        "global_tracks": len(global_tracks),
        "wall_s": wall,
    }
