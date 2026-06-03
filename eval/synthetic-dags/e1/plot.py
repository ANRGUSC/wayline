#!/usr/bin/env python3
"""E1 plotter + table generator.

Reads the six summary.csv files under results/{wayline,argo}/{iobt,hetero,wpf}/,
computes warm-window stats (runs 5-20, N=16), and emits:

  figures/e1-makespan-bars.{png,pdf}   — mean makespan by benchmark x system
  figures/e1-makespan-box.{png,pdf}    — distribution per cell as box plots
  figures/e1-summary.md                — markdown table for the paper

Usage:  python plot.py
"""
from __future__ import annotations

import csv
import pathlib
import statistics
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RESULTS = pathlib.Path(__file__).parent / "results"
FIGS = pathlib.Path(__file__).parent / "figures"
FIGS.mkdir(parents=True, exist_ok=True)

BENCHMARKS = ["iobt", "hetero", "wpf"]
SYSTEMS = ["wayline", "argo"]
SYSTEM_LABEL = {"wayline": "Wayline", "argo": "Argo Workflows"}
SYSTEM_COLOR = {"wayline": "#2563eb", "argo": "#dc2626"}
BENCH_LABEL = {"iobt": "iobt", "hetero": "hetero-compute", "wpf": "wide-pipeline-flex"}

WARM_FROM = 5


def load(sys_: str, bm: str) -> list[float]:
    path = RESULTS / sys_ / bm / "summary.csv"
    out: list[float] = []
    with path.open() as f:
        for r in csv.DictReader(f):
            if r["phase"] == "Succeeded" and r["makespan"] not in ("", "?"):
                try:
                    out.append(float(r["makespan"]))
                except ValueError:
                    pass
    return out


def warm(vs: list[float]) -> list[float]:
    return vs[WARM_FROM - 1 :] if len(vs) >= WARM_FROM else vs


def stats(vs: list[float]) -> dict:
    if not vs:
        return {"n": 0, "mean": 0.0, "std": 0.0, "p95": 0.0}
    mean = statistics.mean(vs)
    std = statistics.pstdev(vs) if len(vs) > 1 else 0.0
    p95 = sorted(vs)[int(0.95 * (len(vs) - 1))]
    return {"n": len(vs), "mean": mean, "std": std, "p95": p95}


def plot_bars(cells: dict):
    fig, ax = plt.subplots(figsize=(7.5, 3.8))
    width = 0.35
    xs = list(range(len(BENCHMARKS)))
    for i, sys_ in enumerate(SYSTEMS):
        means = [cells[(sys_, bm)]["mean"] for bm in BENCHMARKS]
        stds = [cells[(sys_, bm)]["std"] for bm in BENCHMARKS]
        ax.bar(
            [x + (i - 0.5) * width for x in xs],
            means,
            width,
            yerr=stds,
            capsize=3.5,
            label=SYSTEM_LABEL[sys_],
            color=SYSTEM_COLOR[sys_],
            edgecolor="black",
            linewidth=0.5,
        )
        # Numeric labels above each bar
        for x, m in zip(xs, means):
            ax.text(
                x + (i - 0.5) * width,
                m + 5,
                f"{m:.0f}",
                ha="center",
                fontsize=9,
                color=SYSTEM_COLOR[sys_],
            )
    ax.set_xticks(xs)
    ax.set_xticklabels([BENCH_LABEL[bm] for bm in BENCHMARKS])
    ax.set_ylabel("Makespan (s)")
    ax.set_title(
        "E1 — End-to-end makespan, Wayline vs Argo Workflows (warm window, N=16)"
    )
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend(loc="upper left", fontsize=10)
    fig.tight_layout()
    for ext in ("png", "pdf"):
        fig.savefig(FIGS / f"e1-makespan-bars.{ext}", dpi=150)
    plt.close(fig)


def plot_box(samples: dict):
    fig, ax = plt.subplots(figsize=(7.5, 3.8))
    positions = []
    data = []
    colors = []
    labels = []
    width = 0.7
    for bi, bm in enumerate(BENCHMARKS):
        for si, sys_ in enumerate(SYSTEMS):
            positions.append(bi * 3 + si)
            data.append(samples[(sys_, bm)])
            colors.append(SYSTEM_COLOR[sys_])
            labels.append(f"{BENCH_LABEL[bm]}\n{SYSTEM_LABEL[sys_]}")
    bp = ax.boxplot(
        data,
        positions=positions,
        widths=width,
        patch_artist=True,
        showmeans=False,
        medianprops={"color": "black"},
    )
    for patch, c in zip(bp["boxes"], colors):
        patch.set_facecolor(c)
        patch.set_alpha(0.7)
        patch.set_edgecolor("black")
    ax.set_xticks(positions)
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylabel("Makespan (s)")
    ax.set_title(
        "E1 — Makespan distributions per benchmark per system (warm window, N=16)"
    )
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    for ext in ("png", "pdf"):
        fig.savefig(FIGS / f"e1-makespan-box.{ext}", dpi=150)
    plt.close(fig)


def main():
    cells = {}
    samples = {}
    for sys_ in SYSTEMS:
        for bm in BENCHMARKS:
            warm_vals = warm(load(sys_, bm))
            cells[(sys_, bm)] = stats(warm_vals)
            samples[(sys_, bm)] = warm_vals

    plot_bars(cells)
    plot_box(samples)

    # Markdown table
    lines = ["# E1 Results Summary", ""]
    lines.append(f"Warm window: runs {WARM_FROM}..20, N=16 per cell.")
    lines.append("")
    lines.append("## Per-cell stats")
    lines.append("")
    lines.append("| Benchmark | System | n | mean (s) | std (s) | p95 (s) |")
    lines.append("|---|---|---:|---:|---:|---:|")
    for bm in BENCHMARKS:
        for sys_ in SYSTEMS:
            c = cells[(sys_, bm)]
            lines.append(
                f"| {BENCH_LABEL[bm]} | {SYSTEM_LABEL[sys_]} | "
                f"{c['n']} | {c['mean']:.2f} | {c['std']:.2f} | {c['p95']:.2f} |"
            )

    lines.append("")
    lines.append("## Wayline vs Argo ratio (Argo/Wayline, warm mean)")
    lines.append("")
    lines.append("| Benchmark | Wayline (s) | Argo (s) | Ratio | Std ratio |")
    lines.append("|---|---:|---:|---:|---:|")
    for bm in BENCHMARKS:
        d = cells[("wayline", bm)]
        a = cells[("argo", bm)]
        ratio = a["mean"] / d["mean"] if d["mean"] else float("nan")
        std_ratio = a["std"] / d["std"] if d["std"] else float("nan")
        lines.append(
            f"| {BENCH_LABEL[bm]} | {d['mean']:.2f} | "
            f"{a['mean']:.2f} | **{ratio:.2f}×** | {std_ratio:.2f}× |"
        )

    (FIGS / "e1-summary.md").write_text("\n".join(lines) + "\n")
    print("[plot] wrote figures + summary to", FIGS)
    # Print summary to stdout for the operator
    print("\n".join(lines[-15:]))


if __name__ == "__main__":
    main()
