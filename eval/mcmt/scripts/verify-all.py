#!/usr/bin/env python3
"""
Block 2 — real-workload correctness validation.

Walks every cell × rep under results/, pairs the Wayline report against the
Argo report from the same rep, and emits a table summarising:

  - cell, rep
  - Wayline n_global_tracks, counts_by_class, hop_counts
  - Argo n_global_tracks, counts_by_class, hop_counts
  - report.md5 from summary.csv (the byte-for-byte hash)
  - verify status: OK if equivalent under verify_reports rules, FAIL otherwise

Writes two artifacts:
  - results/correctness.csv    : per-rep rows
  - results/correctness.md     : summary table for the paper

A cell is reported correct iff every rep's Wayline and Argo reports pair
equivalently (n_global_tracks match, counts_by_class match, per-track
class/cameras/hop_count match).
"""
from __future__ import annotations

import csv
import json
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent

sys.path.insert(0, str(HERE))
from verify_reports import _diff_reports  # type: ignore  # noqa: E402


def load_report(path: Path) -> dict | None:
    if not path.is_file() or path.stat().st_size == 0:
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return None


def main() -> int:
    results_dir = ROOT / "results"
    rows: list[dict] = []

    for cell_dir in sorted(results_dir.glob("n4-*-pilot")):
        if cell_dir.is_symlink():
            continue
        summary = cell_dir / "summary.csv"
        if not summary.is_file():
            continue
        cell = cell_dir.name.replace("-pilot", "")
        # Map rep -> {wayline, argo} hashes for cross-ref. Skip non-integer
        # rep values (e.g. "retry-1" appended by argo-retry.sh — those
        # have no paired Wayline rep, only fill in the cell statistics).
        hash_map: dict[int, dict[str, str]] = {}
        for r in csv.DictReader(summary.open()):
            try:
                rep = int(r.get("rep") or 0)
            except ValueError:
                continue
            hash_map.setdefault(rep, {})[r["system"]] = r.get("report_md5", "")

        for rep_dir in sorted(cell_dir.glob("rep*-wayline")):
            rep_num = int(rep_dir.name.replace("rep", "").replace("-wayline", ""))
            argo_dir = cell_dir / f"rep{rep_num}-argo"
            wl_rep  = load_report(rep_dir / "report.json")
            argo_rep = load_report(argo_dir / "report.json")

            row = {
                "cell": cell, "rep": rep_num,
                "wl_md5":  hash_map.get(rep_num, {}).get("wayline", ""),
                "argo_md5": hash_map.get(rep_num, {}).get("argo", ""),
            }
            if wl_rep is None or argo_rep is None:
                row["status"] = "NO_REPORT"
                row["wl_tracks"] = ""; row["argo_tracks"] = ""
                row["wl_classes"] = ""; row["argo_classes"] = ""
                row["errors"] = ""
                rows.append(row)
                continue

            errs = _diff_reports(wl_rep, argo_rep)
            row["status"] = "OK" if not errs else "FAIL"
            row["wl_tracks"]  = wl_rep.get("n_global_tracks", "")
            row["argo_tracks"] = argo_rep.get("n_global_tracks", "")
            row["wl_classes"]  = json.dumps(wl_rep.get("counts_by_class", {}), sort_keys=True)
            row["argo_classes"] = json.dumps(argo_rep.get("counts_by_class", {}), sort_keys=True)
            row["errors"] = "; ".join(errs)
            rows.append(row)

    out_csv = results_dir / "correctness.csv"
    cols = ["cell","rep","status","wl_tracks","argo_tracks","wl_classes",
            "argo_classes","wl_md5","argo_md5","errors"]
    with out_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {out_csv}  ({len(rows)} rows)")

    # Per-cell summary
    by_cell: dict[str, list[dict]] = {}
    for r in rows:
        by_cell.setdefault(r["cell"], []).append(r)

    md = ["# Real-workload correctness validation", "",
          "Per-cell Wayline↔Argo report equivalence across all reps.",
          "Equivalence rule: identical `n_global_tracks`, `counts_by_class`, "
          "and per-track `class`/`cameras`/`hop_count`.",
          "",
          "| cell | reps | OK | FAIL | NO_REPORT | n_tracks (mean) | classes |",
          "|------|------|----|------|-----------|-----------------|---------|"]
    for cell, rs in sorted(by_cell.items()):
        n = len(rs)
        ok   = sum(1 for r in rs if r["status"]=="OK")
        fail = sum(1 for r in rs if r["status"]=="FAIL")
        noo  = sum(1 for r in rs if r["status"]=="NO_REPORT")
        tracks = [int(r["wl_tracks"]) for r in rs if r["wl_tracks"] not in ("","NO_REPORT")]
        tm = f"{statistics.mean(tracks):.1f}" if tracks else "—"
        classes = sorted(set().union(*(
            tuple(json.loads(r["wl_classes"]).keys()) for r in rs
            if r["wl_classes"] not in ("", "NO_REPORT")
        ))) if any(r["wl_classes"] for r in rs) else []
        md.append(f"| {cell} | {n} | {ok} | {fail} | {noo} | {tm} | {','.join(classes)} |")

    md.append("")
    # If any FAIL, list them
    fails = [r for r in rows if r["status"] == "FAIL"]
    if fails:
        md.append("## Failed reps")
        md.append("")
        for r in fails:
            md.append(f"- **{r['cell']} rep {r['rep']}** — {r['errors']}")
        md.append("")
    no_rep = [r for r in rows if r["status"] == "NO_REPORT"]
    if no_rep:
        md.append("## Missing reports")
        md.append("")
        for r in no_rep:
            md.append(f"- {r['cell']} rep {r['rep']}")
        md.append("")

    out_md = results_dir / "correctness.md"
    out_md.write_text("\n".join(md) + "\n")
    print(f"wrote {out_md}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
