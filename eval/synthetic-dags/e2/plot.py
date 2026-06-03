#!/usr/bin/env python3
"""
E2 figure: K8s scheduler-plugins NetworkOverhead head-to-head.

Reads results/{iobt,hetero,wpf}/summary.csv for Argo+NetworkOverhead;
reuses /home/anrg/wayline/eval/synthetic-dags/e1/results/argo/<bm>/summary.csv
for Argo+default; reuses .../results/wayline/<bm>/summary.csv for Wayline.
Emits figures/e2-comparison.{pdf,png}.

Warm window is runs 5..N (with 20 reps per cell, N=16).
"""
import csv
import pathlib
import statistics

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


HERE = pathlib.Path(__file__).parent
RESULTS_NO = HERE / "results"                # Argo+NetworkOverhead
RESULTS_E1 = HERE.parent / "e1" / "results"   # wayline + argo
FIGS = HERE / "figures"
FIGS.mkdir(parents=True, exist_ok=True)

BENCHMARKS = ["iobt", "hetero", "wpf"]
BENCH_LABEL = {"iobt": "iobt", "hetero": "hetero-compute", "wpf": "wide-pipeline-flex"}

WARM_FROM = 5


def load_csv(path: pathlib.Path) -> list[float]:
    out: list[float] = []
    with path.open() as f:
        for r in csv.DictReader(f):
            if r.get("phase") == "Succeeded" and r.get("makespan") not in ("", None, "?"):
                try:
                    out.append(float(r["makespan"]))
                except ValueError:
                    pass
    return out


def warm(vs: list[float]) -> list[float]:
    return vs[WARM_FROM - 1 :] if len(vs) >= WARM_FROM else vs


def mean(vs: list[float]) -> float:
    return statistics.mean(vs) if vs else 0.0


def main() -> None:
    series: dict[str, list[float]] = {"wayline": [], "argo": [], "argo_no": []}
    for bm in BENCHMARKS:
        wayline = warm(load_csv(RESULTS_E1 / "wayline" / bm / "summary.csv"))
        argo = warm(load_csv(RESULTS_E1 / "argo" / bm / "summary.csv"))
        argo_no = warm(load_csv(RESULTS_NO / bm / "summary.csv"))
        series["wayline"].append(mean(wayline))
        series["argo"].append(mean(argo))
        series["argo_no"].append(mean(argo_no))

    labels = ["Wayline", "Argo (default)", "Argo + NetworkOverhead"]
    colors = ["#2563eb", "#dc2626", "#7c2d12"]
    keys = ["wayline", "argo", "argo_no"]

    fig, ax = plt.subplots(figsize=(7.5, 3.8))
    xs = list(range(len(BENCHMARKS)))
    width = 0.25
    for i, key in enumerate(keys):
        means = series[key]
        ax.bar(
            [x + (i - 1) * width for x in xs],
            means,
            width,
            label=labels[i],
            color=colors[i],
            edgecolor="black",
            linewidth=0.5,
        )
        for x, m in zip(xs, means):
            ax.text(
                x + (i - 1) * width,
                m + 3,
                f"{m:.1f}",
                ha="center",
                fontsize=8,
                color=colors[i],
            )
    ax.set_xticks(xs)
    ax.set_xticklabels([BENCH_LABEL[bm] for bm in BENCHMARKS])
    ax.set_ylabel("Makespan (s)")
    ax.set_title(
        "E2 — Wayline vs Argo (default scheduler) vs Argo+NetworkOverhead"
    )
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend(loc="upper left", fontsize=9)
    fig.tight_layout()
    for ext in ("png", "pdf"):
        fig.savefig(FIGS / f"e2-comparison.{ext}", dpi=150)
    plt.close(fig)
    print(f"[plot] wrote figures to {FIGS}")
    for i, bm in enumerate(BENCHMARKS):
        print(
            f"{bm}: Wayline={series['wayline'][i]:.2f} "
            f"Argo={series['argo'][i]:.2f} "
            f"Argo+NO={series['argo_no'][i]:.2f}"
        )


if __name__ == "__main__":
    main()
