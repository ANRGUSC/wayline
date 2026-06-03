#!/usr/bin/env python3
"""
Plot the scheduler sweep:
 1. Makespan distribution (box+swarm) per config, per ODAG.
 2. Makespan vs. run index (convergence).
 3. Predicted vs. actual makespan scatter (per HEFT config).
 4. Per-task node placement heatmap (iobt only).
Writes PNGs to figures/.
"""
import json
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

EVAL = Path(__file__).resolve().parent
# Override with env vars for plotting an archived sweep:
#   RESULTS_DIR=results/archive-matrix-v1 FIGS_DIR=results/archive-matrix-v1/figures python3 plot-results.py
import os
RESULTS = Path(os.environ.get("RESULTS_DIR", EVAL / "results")).resolve()
FIGS = Path(os.environ.get("FIGS_DIR", EVAL / "figures")).resolve()
FIGS.mkdir(parents=True, exist_ok=True)

ODAG_ORDER = ["iobt", "hetero-compute", "wide-pipeline-flex"]
CONFIG_COLORS = {
    "random": "#888888",
    "heft": "#1f77b4",
    "heft-eps": "#2ca02c",
    "heft-eps05": "#17becf",
    "heft-eps20": "#9467bd",
}


def load_odag(odag: str):
    """Return {config: [ {iter, run, phase, makespan, wall, predicted, placement}, ... ]}."""
    out = {}
    od_dir = RESULTS / odag
    if not od_dir.is_dir():
        return out
    for cfg_dir in sorted(od_dir.iterdir()):
        if not cfg_dir.is_dir():
            continue
        summary = cfg_dir / "summary.csv"
        if not summary.exists():
            continue
        runs = []
        with summary.open() as f:
            next(f)  # header
            for line in f:
                it, run, phase, ms, wall = line.strip().split(",")
                record = {
                    "iter": int(it),
                    "run": run,
                    "phase": phase,
                    "makespan": float(ms) if ms not in ("", "?") else None,
                    "wall": float(wall) if wall not in ("", "?") else None,
                }
                # Pull predicted makespan + placement from the per-run JSON.
                rj = cfg_dir / f"{run}.json"
                if rj.exists():
                    try:
                        obj = json.loads(rj.read_text())
                        st = obj.get("status", {})
                        # The ODAG CRD exposes the predicted schedule via
                        # `predictedTasks[]` with per-entry estEnd; the scalar
                        # "predicted makespan" is the maximum estEnd across
                        # tasks. There's no pre-computed scalar field.
                        predicted_tasks = st.get("predictedTasks") or []
                        efts = [t.get("estEnd") for t in predicted_tasks if t.get("estEnd") is not None]
                        record["predicted"] = max(efts) if efts else None
                        record["placement"] = {
                            t.get("name"): t.get("node")
                            for t in st.get("tasks", []) if t.get("name")
                        }
                    except json.JSONDecodeError:
                        pass
                runs.append(record)
        out[cfg_dir.name] = runs
    return out


def plot_makespan_distribution(all_data):
    fig, axes = plt.subplots(1, len(ODAG_ORDER), figsize=(14, 4.5), sharey=False)
    if len(ODAG_ORDER) == 1:
        axes = [axes]
    for ax, odag in zip(axes, ODAG_ORDER):
        data = all_data.get(odag, {})
        distribution_skip = {"heft-eps05", "heft-eps20"}
        labels, values, colors = [], [], []
        for cfg, runs in sorted(data.items()):
            if cfg in distribution_skip:
                continue
            vals = [r["makespan"] for r in runs if r["makespan"] is not None and r["iter"] > 4]
            if not vals:
                continue
            labels.append(cfg)
            values.append(vals)
            colors.append(CONFIG_COLORS.get(cfg, "#cccccc"))
        if not values:
            ax.set_title(f"{odag} (no data)")
            continue
        bp = ax.boxplot(values, labels=labels, patch_artist=True, showmeans=True)
        for patch, c in zip(bp["boxes"], colors):
            patch.set_facecolor(c)
            patch.set_alpha(0.6)
        ax.set_title(odag)
        ax.set_ylabel("makespan (s)")
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis="x", rotation=20)
    fig.suptitle("Makespan distribution by config (runs 5–N, warm)")
    fig.tight_layout()
    fig.savefig(FIGS / "makespan-distribution.png", dpi=140)
    plt.close(fig)
    print(f"wrote {FIGS / 'makespan-distribution.png'}")


def plot_convergence(all_data):
    fig, axes = plt.subplots(1, len(ODAG_ORDER), figsize=(14, 4.5))
    if len(ODAG_ORDER) == 1:
        axes = [axes]
    convergence_skip = {"heft-eps05", "heft-eps20"}
    for ax, odag in zip(axes, ODAG_ORDER):
        data = all_data.get(odag, {})
        for cfg, runs in sorted(data.items()):
            if cfg in convergence_skip:
                continue
            xs = [r["iter"] for r in runs if r["makespan"] is not None]
            ys = [r["makespan"] for r in runs if r["makespan"] is not None]
            if not xs:
                continue
            ax.plot(xs, ys, marker="o", linewidth=1.5, markersize=4,
                    color=CONFIG_COLORS.get(cfg, "#cccccc"), label=cfg, alpha=0.85)
        ax.set_title(odag)
        ax.set_xlabel("run #")
        ax.set_ylabel("makespan (s)")
        ymax = 75 if "archive-matrix-v2" in str(FIGS) else 56
        ax.set_ylim(34, ymax)
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(FIGS / "makespan-convergence.png", dpi=140)
    plt.close(fig)
    print(f"wrote {FIGS / 'makespan-convergence.png'}")


def plot_prediction_pairs(all_data):
    """One figure per (benchmark, HEFT config) pair, showing actual and
    HEFT-predicted makespan over run index. Skips the random config (its
    predictions are computed against random placements and don't tell a
    convergence story)."""
    pred_dir = FIGS / "prediction-pairs"
    pred_dir.mkdir(exist_ok=True)

    for odag in ODAG_ORDER:
        data = all_data.get(odag, {})
        for cfg, runs in sorted(data.items()):
            if cfg == "random":
                continue
            xs_a = [r["iter"] for r in runs if r["makespan"] is not None]
            ys_a = [r["makespan"] for r in runs if r["makespan"] is not None]
            xs_p = [r["iter"] for r in runs if r.get("predicted") is not None]
            ys_p = [r["predicted"] for r in runs if r.get("predicted") is not None]
            if not (xs_a or xs_p):
                continue

            fig, ax = plt.subplots(figsize=(7, 4.2))
            color = CONFIG_COLORS.get(cfg, "#1f77b4")
            ax.plot(xs_a, ys_a, marker="o", linewidth=1.8, markersize=5,
                    color=color, label="actual", alpha=0.9)
            ax.plot(xs_p, ys_p, marker="o", linewidth=1.4, markersize=5,
                    color=color, linestyle="--", markerfacecolor="none",
                    label="HEFT predicted", alpha=0.75)
            ax.set_title(f"{odag} — {cfg}")
            ax.set_xlabel("run #")
            ax.set_ylabel("makespan (s)")
            ax.grid(True, alpha=0.3)
            ax.legend(fontsize=10, loc="upper right")

            # Annotate warm mean prediction error in the top-left corner so
            # it doesn't collide with the legend.
            if xs_a and xs_p:
                act_map = dict(zip(xs_a, ys_a))
                pred_map = dict(zip(xs_p, ys_p))
                common = sorted(set(xs_a) & set(xs_p))
                warm_errs = [abs(act_map[i] - pred_map[i]) / act_map[i] * 100
                             for i in common if i > 4]
                if warm_errs:
                    ax.text(0.02, 0.98,
                            f"warm mean |error|: {np.mean(warm_errs):.1f}%",
                            transform=ax.transAxes, ha="left", va="top", fontsize=9,
                            bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="#888", alpha=0.85))

            fig.tight_layout()
            out = pred_dir / f"{odag}-{cfg}.png"
            fig.savefig(out, dpi=140)
            plt.close(fig)
            print(f"wrote {out}")


def plot_prediction_scatter(all_data):
    fig, axes = plt.subplots(1, len(ODAG_ORDER), figsize=(14, 4.5))
    if len(ODAG_ORDER) == 1:
        axes = [axes]
    for ax, odag in zip(axes, ODAG_ORDER):
        data = all_data.get(odag, {})
        for cfg, runs in sorted(data.items()):
            if cfg == "random":
                continue
            xs, ys = [], []
            for r in runs:
                if r.get("predicted") is None or r.get("makespan") is None:
                    continue
                xs.append(r["predicted"])
                ys.append(r["makespan"])
            if not xs:
                continue
            ax.scatter(xs, ys, s=18, alpha=0.7, label=cfg,
                       color=CONFIG_COLORS.get(cfg, "#cccccc"))
        # y = x reference
        if ax.has_data():
            lim = max(ax.get_xlim()[1], ax.get_ylim()[1])
            ax.plot([0, lim], [0, lim], "k--", linewidth=0.8, alpha=0.5)
        ax.set_title(odag)
        ax.set_xlabel("predicted makespan (s)")
        ax.set_ylabel("actual makespan (s)")
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=8)
    fig.suptitle("Prediction accuracy — actual vs HEFT-predicted makespan")
    fig.tight_layout()
    fig.savefig(FIGS / "prediction-scatter.png", dpi=140)
    plt.close(fig)
    print(f"wrote {FIGS / 'prediction-scatter.png'}")


def plot_iobt_placement(all_data):
    """Fraction of infer-i assignments to each compute node, per config."""
    odag = "iobt"
    data = all_data.get(odag, {})
    if not data:
        return
    infer_names = ["infer-1", "infer-2", "infer-3", "infer-4"]
    compute_nodes = ["anrg-6", "anrg-7", "anrg-8"]
    configs = sorted(data.keys())
    if not configs:
        return

    counts = {cfg: {n: 0 for n in compute_nodes} for cfg in configs}
    totals = {cfg: 0 for cfg in configs}
    for cfg, runs in data.items():
        for r in runs:
            placement = r.get("placement", {})
            for t in infer_names:
                node = placement.get(t)
                if node in counts[cfg]:
                    counts[cfg][node] += 1
                    totals[cfg] += 1

    fig, ax = plt.subplots(figsize=(7.5, 4.5))
    width = 0.25
    x = np.arange(len(configs))
    for i, node in enumerate(compute_nodes):
        frac = [counts[c][node] / totals[c] if totals[c] else 0 for c in configs]
        ax.bar(x + (i - 1) * width, frac, width=width,
               label=node.replace("anrg-", "node-"))
    ax.set_xticks(x)
    ax.set_xticklabels(configs, rotation=20)
    ax.set_ylabel("share of infer-i placements")
    ax.set_ylim(0, 1)
    ax.set_title("iobt — infer-i placement share across compute nodes")
    ax.legend()
    ax.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(FIGS / "iobt-infer-placement.png", dpi=140)
    plt.close(fig)
    print(f"wrote {FIGS / 'iobt-infer-placement.png'}")


def main():
    all_data = {odag: load_odag(odag) for odag in ODAG_ORDER}
    plot_makespan_distribution(all_data)
    plot_convergence(all_data)
    plot_prediction_scatter(all_data)
    plot_prediction_pairs(all_data)
    plot_iobt_placement(all_data)

    # Print a concise summary table.
    print("\n=== Summary (mean makespan of runs 5+) ===")
    print(f"{'odag':<22} {'config':<14} {'N':>3} {'mean':>7} {'std':>7} {'p95':>7}")
    for odag in ODAG_ORDER:
        for cfg, runs in sorted(all_data.get(odag, {}).items()):
            vals = [r["makespan"] for r in runs if r["makespan"] is not None and r["iter"] > 4]
            if not vals:
                continue
            arr = np.array(vals)
            print(f"{odag:<22} {cfg:<14} {len(arr):>3} {arr.mean():>7.2f} {arr.std():>7.2f} {np.percentile(arr,95):>7.2f}")


if __name__ == "__main__":
    main()
