"""
Stage 6: terminal aggregation. Reads global_tracks.json and produces
report.json — the canonical answer to "how many unique vehicles
traversed the monitored area, and what was each one's camera path."

This is the file scripts/verify_reports.py diffs between Wayline and Argo
runs to confirm both systems computed the same answer.
"""

from __future__ import annotations

import json
import shutil
import time
from collections import Counter
from pathlib import Path
from typing import Union


def generate_report(
    in_dir: Union[str, Path],
    out_dir: Union[str, Path],
) -> dict:
    """
    Reduce global_tracks.json to a small, diffable summary.

    The output has two sections:
      counts   — unique vehicle count per class (the headline metric)
      tracks   — per-global-vehicle camera path (sorted, deterministic)

    Determinism matters for the cross-system correctness diff. Both Wayline
    and Argo runs share the same input clips and seeded models, so this
    report is bit-equivalent across systems when the data plane is honest.
    """
    src = Path(in_dir)
    dst = Path(out_dir)
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True)

    gtj = json.loads((src / "global_tracks.json").read_text())
    tracks = gtj["global_tracks"]

    t0 = time.perf_counter()
    counts: Counter = Counter()
    summarized = []
    for tr in tracks:
        counts[tr["class"]] += 1
        # Hop sequence ordered by first-frame across each tracklet for
        # deterministic comparison.
        hops_sorted = sorted(tr["camera_path"], key=lambda h: (h["frame_first"], h["camera"]))
        summarized.append({
            "global_id": tr["global_id"],
            "class": tr["class"],
            "cameras": [h["camera"] for h in hops_sorted],
            "hop_count": len(hops_sorted),
            "det_count_total": sum(h["det_count"] for h in hops_sorted),
        })
    summarized.sort(key=lambda t: (t["class"], -t["det_count_total"], t["global_id"]))
    wall = time.perf_counter() - t0

    report = {
        "schema": "mcmt/v1",
        "n_global_tracks": len(tracks),
        "counts_by_class": dict(sorted(counts.items())),
        "tracks": summarized,
        "wall_s": wall,
    }
    (dst / "report.json").write_text(json.dumps(report, indent=2))
    return {
        "tracks": len(tracks),
        "unique_classes": len(counts),
        "wall_s": wall,
    }
