#!/usr/bin/env python3
"""
Cross-system correctness diff for mcmt.

Compares the Wayline run's report.json against the Argo run's report.json.
Both systems run the same task containers (lib functions are pure +
inference is seeded), so the canonical answer must be identical.

  scripts/verify_reports.py <wl_report> <argo_report>

Exits 0 if reports are equivalent, 1 if they diverge.

Equivalence rules:
  - n_global_tracks: exact match.
  - counts_by_class: exact match (dict equality).
  - tracks: same length AND each pair (sorted by (class, det_count_total))
    has identical `class`, `cameras` list, and hop_count.

Floating-point fields (wall_s timing) are intentionally NOT compared —
they differ by design across systems.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def _load(path: Path) -> dict:
    if not path.is_file():
        raise FileNotFoundError(f"report not found: {path}")
    return json.loads(path.read_text())


def _diff_reports(a: dict, b: dict) -> list[str]:
    errs: list[str] = []
    if a.get("n_global_tracks") != b.get("n_global_tracks"):
        errs.append(
            f"n_global_tracks: wayline={a.get('n_global_tracks')} argo={b.get('n_global_tracks')}"
        )
    if a.get("counts_by_class") != b.get("counts_by_class"):
        errs.append(
            f"counts_by_class differ:\n  wayline  = {a.get('counts_by_class')}\n  argo = {b.get('counts_by_class')}"
        )

    ta, tb = a.get("tracks", []), b.get("tracks", [])
    if len(ta) != len(tb):
        errs.append(f"tracks length: wayline={len(ta)} argo={len(tb)}")
        return errs
    for i, (ra, rb) in enumerate(zip(ta, tb)):
        if ra["class"] != rb["class"]:
            errs.append(f"tracks[{i}].class: wayline={ra['class']} argo={rb['class']}")
        if ra["cameras"] != rb["cameras"]:
            errs.append(f"tracks[{i}].cameras: wayline={ra['cameras']} argo={rb['cameras']}")
        if ra["hop_count"] != rb["hop_count"]:
            errs.append(f"tracks[{i}].hop_count: wayline={ra['hop_count']} argo={rb['hop_count']}")
    return errs


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    wl_path = Path(sys.argv[1])
    argo_path = Path(sys.argv[2])

    wl_report = _load(wl_path)
    argo_report = _load(argo_path)

    errs = _diff_reports(wl_report, argo_report)
    if errs:
        print(f"FAIL: reports diverge\n  wayline : {wl_path}\n  argo: {argo_path}")
        for e in errs:
            print(f"  - {e}")
        return 1

    print(
        f"OK: reports equivalent — n_global_tracks={wl_report['n_global_tracks']} "
        f"classes={list(wl_report['counts_by_class'].keys())}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
