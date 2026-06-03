#!/usr/bin/env python3
"""
Generate the mcmt figures from results/all.csv (produced by
scripts/harvest.py).

Produces, under figures/:
  e1v-makespan-box.{png,pdf}      — per-cell makespan boxplot Wayline vs Argo
  e1v-makespan-vs-duration.{png,pdf} — sensitivity to clip duration at N=4
  e1v-makespan-vs-cameras.{png,pdf}  — sensitivity to camera count at D=60
  e1v-speedup.{png,pdf}           — Wayline/Argo makespan ratio per cell
  e1v-summary.md                  — text summary table

Usage:
  scripts/plot.py [results_root]
"""

from __future__ import annotations

import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib  # type: ignore[import-not-found]
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # type: ignore[import-not-found]


def _read_all(path: Path) -> list[dict]:
    if not path.is_file():
        sys.exit(f"missing {path} — run scripts/harvest.py first")
    rows: list[dict] = []
    with path.open() as f:
        for row in csv.DictReader(f):
            # Filter to terminal-Succeeded rows; we only plot good runs.
            if row.get("phase") != "Succeeded":
                continue
            try:
                row["n_cameras"] = int(row["n_cameras"])
                row["duration_s"] = int(row["duration_s"])
                # Argo doesn't have a separate makespan field; fall back to wall.
                ms = row["makespan_s"]
                row["_metric"] = float(ms) if ms not in ("", "?") else float(row["wall_s"])
            except (KeyError, ValueError):
                continue
            rows.append(row)
    return rows


def _by_cell_system(rows: list[dict]) -> dict[tuple[str, str], list[float]]:
    """Group makespan values by (cell, system)."""
    out: dict[tuple[str, str], list[float]] = defaultdict(list)
    for r in rows:
        out[(r["cell"], r["system"])].append(r["_metric"])
    return out


def _plot_box(grp: dict[tuple[str, str], list[float]], out_dir: Path) -> None:
    cells = sorted({c for c, _ in grp.keys()})
    if not cells:
        print("no rows to plot")
        return
    wl_data = [grp.get((c, "wayline"), []) for c in cells]
    argo_data = [grp.get((c, "argo"), []) for c in cells]

    fig, ax = plt.subplots(figsize=(max(6, 1.0 + 0.8 * len(cells)), 4.2))
    width = 0.35
    pos = list(range(len(cells)))
    bp1 = ax.boxplot(wl_data, positions=[p - width / 2 for p in pos], widths=width,
                     patch_artist=True, boxprops=dict(facecolor="#4c78a8"))
    bp2 = ax.boxplot(argo_data, positions=[p + width / 2 for p in pos], widths=width,
                     patch_artist=True, boxprops=dict(facecolor="#e45756"))
    ax.set_xticks(pos)
    ax.set_xticklabels(cells, rotation=15)
    ax.set_ylabel("Makespan (s)")
    ax.set_title("mcmt: Wayline vs Argo makespan, per cell")
    ax.legend([bp1["boxes"][0], bp2["boxes"][0]], ["Wayline (HEFT)", "Argo + MinIO"])
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    for ext in ("png", "pdf"):
        fig.savefig(out_dir / f"e1v-makespan-box.{ext}", dpi=140)
    plt.close(fig)


def _plot_sensitivity(rows: list[dict], axis: str, fixed_key: str, fixed_val: int, out_dir: Path) -> None:
    """One series per system, x = axis value, y = mean makespan ± stdev."""
    subset = [r for r in rows if r[fixed_key] == fixed_val]
    by_sys: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    for r in subset:
        by_sys[r["system"]][r[axis]].append(r["_metric"])
    if not by_sys:
        return

    fig, ax = plt.subplots(figsize=(6, 4))
    for sys_label, by_x in sorted(by_sys.items()):
        xs = sorted(by_x.keys())
        means = [statistics.mean(by_x[x]) for x in xs]
        stds = [statistics.pstdev(by_x[x]) if len(by_x[x]) > 1 else 0 for x in xs]
        ax.errorbar(xs, means, yerr=stds, marker="o", capsize=3, label=sys_label.upper())
    label_axis = "Clip duration (s)" if axis == "duration_s" else "Cameras (N)"
    ax.set_xlabel(label_axis)
    ax.set_ylabel("Makespan (s)")
    fixed_label = "N=" + str(fixed_val) if fixed_key == "n_cameras" else f"D={fixed_val}s"
    ax.set_title(f"mcmt sensitivity ({fixed_label})")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    stem = "e1v-makespan-vs-duration" if axis == "duration_s" else "e1v-makespan-vs-cameras"
    for ext in ("png", "pdf"):
        fig.savefig(out_dir / f"{stem}.{ext}", dpi=140)
    plt.close(fig)


def _plot_speedup(grp: dict[tuple[str, str], list[float]], out_dir: Path) -> None:
    cells = sorted({c for c, _ in grp.keys()})
    speedups = []
    labels = []
    for c in cells:
        wayline = grp.get((c, "wayline"), [])
        argo = grp.get((c, "argo"), [])
        if not wayline or not argo:
            continue
        speedups.append(statistics.mean(argo) / statistics.mean(wayline))
        labels.append(c)
    if not speedups:
        return
    fig, ax = plt.subplots(figsize=(max(6, 1.0 + 0.8 * len(labels)), 3.8))
    bars = ax.bar(range(len(labels)), speedups, color="#4c78a8")
    ax.axhline(1.0, color="grey", linestyle="--", linewidth=1)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=15)
    ax.set_ylabel("Argo / Wayline makespan")
    ax.set_title("mcmt: Wayline speedup over Argo+MinIO")
    for b, s in zip(bars, speedups):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.02,
                f"{s:.2f}×", ha="center", va="bottom", fontsize=9)
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    for ext in ("png", "pdf"):
        fig.savefig(out_dir / f"e1v-speedup.{ext}", dpi=140)
    plt.close(fig)


def _summary_md(rows: list[dict], grp: dict[tuple[str, str], list[float]], out_dir: Path) -> None:
    cells = sorted({c for c, _ in grp.keys()})
    lines = [
        "# mcmt summary",
        "",
        "| cell | system | n | mean (s) | std | p95 | correctness OK |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    ok_by_cell: dict[str, list[bool]] = defaultdict(list)
    for r in rows:
        if r.get("report_ok") in ("true", "false"):
            ok_by_cell[r["cell"]].append(r["report_ok"] == "true")
    for c in cells:
        for sysname in ("wayline", "argo"):
            data = grp.get((c, sysname), [])
            if not data:
                continue
            mean = statistics.mean(data)
            std = statistics.pstdev(data) if len(data) > 1 else 0
            p95 = sorted(data)[max(0, int(round(0.95 * len(data))) - 1)]
            ok_list = ok_by_cell.get(c, [])
            ok_frac = (sum(ok_list) / len(ok_list)) if ok_list else float("nan")
            lines.append(
                f"| {c} | {sysname.upper()} | {len(data)} | "
                f"{mean:.2f} | {std:.2f} | {p95:.2f} | "
                f"{'%.0f%%' % (100 * ok_frac) if ok_list else 'n/a'} |"
            )
    (out_dir / "e1v-summary.md").write_text("\n".join(lines) + "\n")


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else "results")
    out_dir = Path("figures")
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = _read_all(root / "all.csv")
    grp = _by_cell_system(rows)
    _plot_box(grp, out_dir)
    _plot_sensitivity(rows, axis="duration_s", fixed_key="n_cameras", fixed_val=4, out_dir=out_dir)
    _plot_sensitivity(rows, axis="n_cameras", fixed_key="duration_s", fixed_val=60, out_dir=out_dir)
    _plot_speedup(grp, out_dir)
    _summary_md(rows, grp, out_dir)
    print(f"figures written to {out_dir}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
