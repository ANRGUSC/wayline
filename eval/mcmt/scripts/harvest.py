#!/usr/bin/env python3
"""
Consolidate per-cell summary.csv files into a single all.csv ready for
plotting.

  scripts/harvest.py [results_root] [out_csv]

Defaults: results/  →  results/all.csv
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else "results")
    out_path = Path(sys.argv[2] if len(sys.argv) > 2 else root / "all.csv")

    cells = sorted(p for p in root.iterdir() if p.is_dir() and (p / "summary.csv").is_file())
    if not cells:
        print(f"no cells under {root}", file=sys.stderr)
        return 1

    header = ["cell", "n_cameras", "duration_s", "rep", "system",
              "run_name", "phase", "makespan_s", "wall_s", "report_ok"]
    with out_path.open("w", newline="") as out:
        w = csv.writer(out)
        w.writerow(header)
        for cd in cells:
            cell = cd.name
            # Expect names like "n4-d60".
            try:
                n_part, d_part = cell.split("-")
                n = int(n_part[1:]); d = int(d_part[1:])
            except (ValueError, IndexError):
                n = ""; d = ""
            with (cd / "summary.csv").open() as f:
                r = csv.DictReader(f)
                for row in r:
                    w.writerow([
                        cell, n, d,
                        row.get("rep", ""), row.get("system", ""),
                        row.get("run_name", ""), row.get("phase", ""),
                        row.get("makespan_s", ""), row.get("wall_s", ""),
                        row.get("report_ok", ""),
                    ])
    print(f"wrote {out_path}  ({sum(1 for _ in out_path.open()) - 1} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
