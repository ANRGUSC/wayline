#!/usr/bin/env python3
"""
E0 figures.

Reads results/all.csv (produced by harvest.py) and emits:

  figures/e0-e2e.png            and .pdf
  figures/e0-decomposition.png  and .pdf
  figures/e0-summary.md         (text summary + PASS/FAIL verdict)

The headline figure (e0-e2e) is a 2-panel grouped bar chart:
  panel 1: same-node      | x-axis = payload (1/10/100/500 MB),
  panel 2: cross-node     | bars = system (Wayline, MinIO), y = mean E2E (s)

The decomposition figure stacks compute / producer-hold / consumer-wait
/ transfer-visible per system per cell.

Stats use runs 5..N (warm window) by run-index order. With 20 reps per
cell this is N=16; with the smoke set (2 reps) it is N=2 unfiltered.

Usage:
  python plot.py                              # reads results/all.csv
  python plot.py --csv path/to/all.csv
"""

import argparse
import csv
import pathlib
import statistics
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


PAYLOADS = ["1MB", "10MB", "100MB", "500MB"]
COLOCS   = ["same", "cross"]
SYSTEMS  = ["wayline", "minio"]

SYSTEM_LABEL = {"wayline": "Wayline", "minio": "MinIO (baseline)"}
SYSTEM_COLOR = {"wayline": "#2563eb", "minio": "#dc2626"}


def load(csv_path: pathlib.Path):
    rows = []
    with csv_path.open() as f:
        for r in csv.DictReader(f):
            try:
                r["bytes"] = int(r["bytes"]) if r["bytes"] else 0
                for k in ("t0", "t1", "t1p", "t2", "t3", "t_found", "t4",
                          "e2e", "compute", "send_or_upload",
                          "producer_hold", "consumer_wait",
                          "poll_wait", "download_time", "transfer_visible"):
                    if k in r:
                        r[k] = float(r[k]) if r[k] else None
                rows.append(r)
            except Exception as e:
                print(f"[load] skipping row: {e}", file=sys.stderr)
    return rows


def warm_window(values, warm_from=5):
    """Drop the first warm_from-1 indices (1-indexed runs <5)."""
    if len(values) >= warm_from:
        return values[warm_from - 1:]
    return values


def cell_stats(rows, system, coloc, payload, metric):
    cell = [r for r in rows
            if r["system"] == system
            and r["colocation"] == coloc
            and r["payload_label"] == payload
            and r[metric] is not None]
    cell.sort(key=lambda r: r["run_name"])
    vals = [r[metric] for r in cell]
    warm = warm_window(vals)
    if not warm:
        return None
    return {
        "mean": statistics.mean(warm),
        "std": statistics.pstdev(warm) if len(warm) > 1 else 0.0,
        "p95": sorted(warm)[int(0.95 * (len(warm) - 1))],
        "n": len(warm),
    }


def plot_e2e(rows, out_dir: pathlib.Path):
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5), sharey=True)
    width = 0.35

    for ax, coloc in zip(axes, COLOCS):
        xs = list(range(len(PAYLOADS)))
        for i, system in enumerate(SYSTEMS):
            means = []
            stds = []
            for payload in PAYLOADS:
                s = cell_stats(rows, system, coloc, payload, "e2e")
                means.append(s["mean"] if s else 0)
                stds.append(s["std"]  if s else 0)
            offset = (i - 0.5) * width
            ax.bar([x + offset for x in xs], means, width,
                   yerr=stds, capsize=3,
                   label=SYSTEM_LABEL[system],
                   color=SYSTEM_COLOR[system],
                   edgecolor="black", linewidth=0.5)

        ax.set_xticks(xs)
        ax.set_xticklabels(PAYLOADS)
        ax.set_xlabel("Payload size")
        ax.set_title(f"{coloc}-node")
        ax.grid(True, axis="y", alpha=0.3)
        ax.legend(loc="upper left", fontsize=9)

    axes[0].set_ylabel("End-to-end time (s)")
    fig.suptitle("E0 — Producer-compute-start → consumer-data-ready", fontsize=12)
    fig.tight_layout()
    for ext in ("png", "pdf"):
        fig.savefig(out_dir / f"e0-e2e.{ext}", dpi=150)
    plt.close(fig)


def plot_decomposition(rows, out_dir: pathlib.Path):
    """One panel per colocation; x = (payload x system) groups; stacked bars."""
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharey=True)

    components = [
        ("compute",          "#9ca3af", "Compute (5 s)"),
        ("producer_hold",    "#f59e0b", "Producer hold-time"),
        ("consumer_wait",    "#10b981", "Consumer wait-time"),
        ("transfer_visible", "#3b82f6", "Transfer-visible at consumer"),
    ]

    for ax, coloc in zip(axes, COLOCS):
        labels = []
        bottoms = []
        for payload in PAYLOADS:
            for system in SYSTEMS:
                labels.append(f"{payload}\n{SYSTEM_LABEL[system].split(' ')[0]}")

        for ci, (metric, color, lbl) in enumerate(components):
            heights = []
            for payload in PAYLOADS:
                for system in SYSTEMS:
                    s = cell_stats(rows, system, coloc, payload, metric)
                    heights.append(max(0.0, s["mean"]) if s else 0.0)
            xs = list(range(len(heights)))
            if ci == 0:
                ax.bar(xs, heights, color=color, edgecolor="black",
                       linewidth=0.5, label=lbl)
                bottoms = list(heights)
            else:
                ax.bar(xs, heights, bottom=bottoms, color=color,
                       edgecolor="black", linewidth=0.5, label=lbl)
                bottoms = [b + h for b, h in zip(bottoms, heights)]

        ax.set_xticks(list(range(len(labels))))
        ax.set_xticklabels(labels, fontsize=8)
        ax.set_title(f"{coloc}-node")
        ax.grid(True, axis="y", alpha=0.3)
        if ax is axes[0]:
            ax.set_ylabel("Time (s)")
            ax.legend(loc="upper left", fontsize=8)

    fig.suptitle("E0 — Time decomposition (mean, warm window)", fontsize=12)
    fig.tight_layout()
    for ext in ("png", "pdf"):
        fig.savefig(out_dir / f"e0-decomposition.{ext}", dpi=150)
    plt.close(fig)


def write_summary(rows, out_dir: pathlib.Path):
    lines = ["# E0 Results Summary", ""]
    lines.append("Stats are taken from the warm window (runs 5+, when ≥5 reps exist).")
    lines.append("")
    lines.append("## E2E mean / std / p95 by cell")
    lines.append("")
    lines.append("| Coloc | Payload | System | n | mean (s) | std (s) | p95 (s) |")
    lines.append("|---|---|---|---:|---:|---:|---:|")
    for coloc in COLOCS:
        for payload in PAYLOADS:
            for system in SYSTEMS:
                s = cell_stats(rows, system, coloc, payload, "e2e")
                if s is None:
                    lines.append(f"| {coloc} | {payload} | {SYSTEM_LABEL[system]} | 0 | – | – | – |")
                    continue
                lines.append(
                    f"| {coloc} | {payload} | {SYSTEM_LABEL[system]} | "
                    f"{s['n']} | {s['mean']:.3f} | {s['std']:.3f} | {s['p95']:.3f} |"
                )

    lines.append("")
    lines.append("## Wayline vs MinIO ratios (warm mean)")
    lines.append("")
    lines.append("| Coloc | Payload | MinIO (s) | Wayline (s) | Ratio (MinIO/Wayline) |")
    lines.append("|---|---|---:|---:|---:|")
    pass_targets = []
    for coloc in COLOCS:
        for payload in PAYLOADS:
            d = cell_stats(rows, "wayline",   coloc, payload, "e2e")
            m = cell_stats(rows, "minio", coloc, payload, "e2e")
            if d is None or m is None or d["mean"] <= 0:
                lines.append(f"| {coloc} | {payload} | – | – | – |")
                continue
            ratio = m["mean"] / d["mean"]
            lines.append(f"| {coloc} | {payload} | {m['mean']:.3f} | {d['mean']:.3f} | {ratio:.2f}× |")
            if coloc == "same" and payload in ("100MB", "500MB"):
                pass_targets.append((payload, ratio))

    lines.append("")
    lines.append("## Pass/fail")
    lines.append("")
    if not pass_targets:
        lines.append("⚠️ No same-node ≥100MB results yet — verdict deferred.")
    else:
        min_ratio = min(r for _, r in pass_targets)
        if min_ratio >= 2.0:
            lines.append(f"✅ **PASS** — minimum same-node ≥100MB ratio = **{min_ratio:.2f}×** (≥ 2× target).")
        elif min_ratio >= 1.5:
            lines.append(f"🟡 **CONDITIONAL** — minimum same-node ≥100MB ratio = **{min_ratio:.2f}×** (between 1.5× and 2×).")
        else:
            lines.append(f"❌ **FAIL** — minimum same-node ≥100MB ratio = **{min_ratio:.2f}×** (< 1.5× — STOP and reconsider C1).")

    (out_dir / "e0-summary.md").write_text("\n".join(lines) + "\n")
    print(f"[plot] wrote {out_dir/'e0-summary.md'}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=pathlib.Path,
                    default=pathlib.Path(__file__).parent / "results" / "all.csv")
    ap.add_argument("--out", type=pathlib.Path,
                    default=pathlib.Path(__file__).parent / "figures")
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    rows = load(args.csv)
    if not rows:
        print(f"[plot] no rows in {args.csv}", file=sys.stderr)
        sys.exit(1)

    plot_e2e(rows, args.out)
    plot_decomposition(rows, args.out)
    write_summary(rows, args.out)
    print(f"[plot] figures in {args.out}")


if __name__ == "__main__":
    main()
