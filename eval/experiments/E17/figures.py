#!/usr/bin/env python3
"""E17 report figures from analyze.py's runs.csv files and the paper re-run summaries.

  figures.py --runs T1.csv [T1b.csv ...] --runs2 T2.csv --paper-orig S.json --paper-aligned S.json --out DIR
"""
import argparse
import csv
import math
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

POL = ["wait", "reroute", "local@0", "global@0"]
NAME = {"wait": "wait", "reroute": "reroute", "local@0": "local", "global@0": "global", "oracle": "oracle"}
COL = {"wait": "#6b6b6b", "reroute": "#1f77b4", "local": "#e08a00", "global": "#b2182b", "oracle": "#2a9d55"}
GENS = ["single", "flapping", "mobility", "jamming"]
INK = "#222222"
plt.rcParams.update({"font.size": 9, "axes.edgecolor": "#888888", "axes.labelcolor": INK, "xtick.color": INK,
                     "ytick.color": INK, "axes.spines.top": False, "axes.spines.right": False})


def load(paths):
    rows = []
    for p in paths:
        for r in csv.DictReader(open(p)):
            for k in ("sim_norm", "tb_norm", "sim_ratio", "tb_ratio", "sim_aligned_norm", "sim_aligned_ratio",
                      "sim_fixed_ratio", "inflation"):
                r[k] = float(r[k]) if r.get(k) not in (None, "", "None") else None
            rows.append(r)
    return [r for r in rows if r["sim_norm"] and r["tb_norm"]]


def gmean(xs):
    xs = [x for x in xs if x]
    return math.exp(sum(math.log(x) for x in xs) / len(xs)) if xs else float("nan")


def scatter(ax, rows, key, title):
    for p, c in COL.items():
        rs = [r for r in rows if r["policy"] == p and r.get(key)]
        ax.scatter([r[key] for r in rs], [r["tb_norm"] for r in rs], s=11, c=c, alpha=.75, lw=0, label=p)
    lo, hi = 0.5, 8
    ax.plot([lo, hi], [lo, hi], color=INK, lw=.8)
    ax.fill_between([lo, hi], [lo * .9, hi * .9], [lo * 1.1, hi * 1.1], color="#999999", alpha=.15, lw=0)
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_xlim(lo, hi); ax.set_ylim(lo, hi)
    ticks = [0.5, 0.75, 1, 1.5, 2, 3, 5, 8]
    for a in (ax.xaxis, ax.yaxis):
        a.set_major_locator(matplotlib.ticker.FixedLocator(ticks))
        a.set_major_formatter(matplotlib.ticker.FixedFormatter([f"{t:g}" for t in ticks]))
        a.set_minor_locator(matplotlib.ticker.NullLocator())
    ax.set_xlabel("simulated makespan / M0")
    ax.set_title(title, fontsize=9, loc="left")


def fig_fidelity(t1, t2, out):
    fig, axs = plt.subplots(1, 3, figsize=(11, 3.8), sharey=True)
    scatter(axs[0], t1, "sim_norm", "Tier 1 vs the paper's simulator")
    scatter(axs[1], t1, "sim_aligned_norm", "Tier 1 vs the corrected, aligned simulator")
    scatter(axs[2], t2, "sim_norm", "Tier 2 (real hardware) vs its simulation")
    axs[0].set_ylabel("testbed makespan / M0")
    axs[0].legend(fontsize=7, frameon=False, loc="upper left")
    for ax in axs:
        ax.text(0.97, 0.03, "band: ±10%", transform=ax.transAxes, ha="right", fontsize=7, color="#666666")
    fig.tight_layout()
    fig.savefig(os.path.join(out, "fidelity.png"), dpi=170)
    plt.close(fig)


def fig_ladder(t1, out):
    fig, axs = plt.subplots(1, 4, figsize=(11, 3.2))
    variants = [("sim_ratio", "paper's simulator", "#c7d4dd"), ("sim_aligned_ratio", "aligned simulator", "#7d9db3"),
                ("tb_ratio", "testbed", "#22313a")]
    for ax, g in zip(axs, GENS):
        rs = [r for r in t1 if r["generator"] == g]
        for i, (k, lab, c) in enumerate(variants):
            vals = [gmean([r[k] for r in rs if r["label"] == p]) for p in POL]
            ax.bar([x + (i - 1) * 0.27 for x in range(len(POL))], vals, 0.25, color=c, label=lab)
        ax.axhline(1, color=INK, lw=.6)
        ax.set_xticks(range(len(POL))); ax.set_xticklabels([NAME[p] for p in POL])
        ax.set_title(g, fontsize=9, loc="left")
        top = max(gmean([r[k] for r in rs if r["label"] == p]) for p in POL for k, _, _ in variants)
        ax.set_ylim(0.9, max(1.15, top * 1.06))
    axs[0].set_ylabel("makespan / oracle (geo. mean)")
    h, l = axs[0].get_legend_handles_labels()
    fig.legend(h, l, loc="upper center", ncol=3, fontsize=8, frameon=False)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    fig.savefig(os.path.join(out, "ladder.png"), dpi=170)
    plt.close(fig)


def fig_paper(orig, aligned, out):
    import json
    so, sa = json.load(open(orig)), json.load(open(aligned))
    fig, axs = plt.subplots(2, 4, figsize=(11, 5), sharex=True)
    pols = ["wait", "reroute", "local", "global"]
    for j, g in enumerate(GENS):
        for i, (s, lab) in enumerate(((so, "as published"), (sa, "corrected + aligned"))):
            ax = axs[i][j]
            ccrs = sorted(s["ccr_trend"][g]["wait"], key=float)
            for p in pols:
                ax.plot([float(c) for c in ccrs], [s["ccr_trend"][g][p][c] for c in ccrs], marker="o", ms=3,
                        lw=1.6, color=COL[p], label=p, ls="--" if p == "wait" else "-", zorder=3 if p == "wait" else 2)
            ax.set_xscale("log")
            ax.xaxis.set_major_locator(matplotlib.ticker.FixedLocator([0.2, 0.5, 1, 2, 5]))
            ax.xaxis.set_major_formatter(matplotlib.ticker.FixedFormatter(["0.2", "0.5", "1", "2", "5"]))
            ax.xaxis.set_minor_locator(matplotlib.ticker.NullLocator())
            ax.axhline(1, color=INK, lw=.5)
            ax.set_title(f"{g} · {lab}", fontsize=8.5, loc="left")
            ax.set_ylim(0.95, 1.5 if g != "flapping" else 2.8)
            if j == 0:
                ax.set_ylabel("makespan / oracle")
            if i == 1:
                ax.set_xlabel("CCR")
    h, l = axs[0][0].get_legend_handles_labels()
    fig.legend(h, l, loc="upper center", ncol=4, fontsize=8, frameon=False)
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(os.path.join(out, "paper_ccr.png"), dpi=170)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", nargs="+", required=True)
    ap.add_argument("--runs2", nargs="+", required=True)
    ap.add_argument("--paper-orig", required=True)
    ap.add_argument("--paper-aligned", required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    t1, t2 = load(a.runs), load(a.runs2)
    fig_fidelity(t1, t2, a.out)
    fig_ladder(t1, a.out)
    fig_paper(a.paper_orig, a.paper_aligned, a.out)
    print("figures ->", a.out)


if __name__ == "__main__":
    main()
