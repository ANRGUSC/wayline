#!/usr/bin/env python3
"""
Paired bootstrap CIs for the AI City results.

For each cell, computes paired deltas delta_i = Argo_i - Wayline_i,
their mean, and a 95% bootstrap percentile CI over 10k resamples.
Also reports the per-cell win count (number of paired reps where
delta_i > 0).

  scripts/paired-ci.py  [results_dir]

Writes results/paired-ci.csv and results/paired-ci.md.
"""
from __future__ import annotations

import csv
import random
import statistics
import sys
from pathlib import Path

RESAMPLES = 10_000
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent


def paired_deltas(rows: list[dict]) -> list[int]:
    """Pair Wayline rep N with Argo rep N (the same rep label), keeping
    only pairs where both sides are Succeeded with a numeric makespan."""
    by_rep: dict[str, dict[str, int]] = {}
    for r in rows:
        if r.get("phase") != "Succeeded":
            continue
        m = r.get("makespan_s", "")
        if m in ("", "?"):
            continue
        try:
            ms = int(m)
        except ValueError:
            continue
        rep = r.get("rep", "")
        sys_ = r.get("system", "")
        by_rep.setdefault(rep, {})[sys_] = ms

    deltas = []
    for rep in sorted(by_rep, key=lambda x: (0, int(x)) if x.isdigit() else (1, x)):
        d = by_rep[rep]
        if "wayline" in d and "argo" in d:
            deltas.append(d["argo"] - d["wayline"])
    return deltas


def bootstrap_ci(samples: list[int], n_resamples: int, alpha: float, rng: random.Random) -> tuple[float, float]:
    if not samples:
        return (float("nan"), float("nan"))
    means = []
    for _ in range(n_resamples):
        resampled = [samples[rng.randrange(len(samples))] for _ in samples]
        means.append(statistics.mean(resampled))
    means.sort()
    lo = means[int(n_resamples * alpha / 2)]
    hi = means[int(n_resamples * (1 - alpha / 2))]
    return lo, hi


def main() -> int:
    results = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "results"
    rng = random.Random(42)
    rows_out: list[dict] = []

    for cell_dir in sorted(results.glob("n4-*-pilot")):
        if cell_dir.is_symlink(): continue
        sum_csv = cell_dir / "summary.csv"
        if not sum_csv.is_file(): continue
        cell = cell_dir.name.replace("-pilot", "")
        rows = list(csv.DictReader(sum_csv.open()))
        deltas = paired_deltas(rows)
        if not deltas:
            continue
        mean = statistics.mean(deltas)
        wins = sum(1 for d in deltas if d > 0)
        lo, hi = bootstrap_ci(deltas, RESAMPLES, 0.05, rng)
        argo_succ = [int(r["makespan_s"]) for r in rows
                     if r["system"]=="argo" and r["phase"]=="Succeeded"
                     and r["makespan_s"] not in ("", "?")]
        argo_mean = statistics.mean(argo_succ) if argo_succ else 0
        pct = mean / argo_mean * 100 if argo_mean else 0
        pct_lo = lo / argo_mean * 100 if argo_mean else 0
        pct_hi = hi / argo_mean * 100 if argo_mean else 0
        rows_out.append({
            "cell": cell,
            "n_pairs": len(deltas),
            "mean_delta_s": round(mean, 1),
            "ci_lo_s": round(lo, 1),
            "ci_hi_s": round(hi, 1),
            "speedup_pct": round(pct, 1),
            "ci_lo_pct": round(pct_lo, 1),
            "ci_hi_pct": round(pct_hi, 1),
            "wins": wins,
            "deltas": deltas,
        })

    # CSV
    out_csv = results / "paired-ci.csv"
    cols = ["cell","n_pairs","mean_delta_s","ci_lo_s","ci_hi_s",
            "speedup_pct","ci_lo_pct","ci_hi_pct","wins"]
    with out_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader(); w.writerows(rows_out)
    print(f"wrote {out_csv}\n")

    # Markdown report
    md = ["# Paired bootstrap CIs (AI City MCMT, 20 reps/cell)",
          "",
          f"95% percentile CIs from {RESAMPLES:,} bootstrap resamples of "
          f"`Argo_i - Wayline_i`. Wins is the count of paired reps where "
          f"Wayline strictly beat Argo.",
          "",
          "| cell | n pairs | mean Δ (s) | 95% CI (s) | speedup | 95% CI (%) | wins |",
          "|------|---------|------------|------------|---------|-----------|------|"]
    for r in rows_out:
        md.append(f"| {r['cell']} | {r['n_pairs']} | "
                  f"{r['mean_delta_s']:+.1f} | "
                  f"[{r['ci_lo_s']:+.1f}, {r['ci_hi_s']:+.1f}] | "
                  f"{r['speedup_pct']:+.1f}% | "
                  f"[{r['ci_lo_pct']:+.1f}%, {r['ci_hi_pct']:+.1f}%] | "
                  f"{r['wins']}/{r['n_pairs']} |")
    md.append("")
    out_md = results / "paired-ci.md"
    out_md.write_text("\n".join(md) + "\n")
    print(f"wrote {out_md}\n")

    # Print to stdout
    print(f"{'cell':<15} {'n':<4} {'mean Δ':<8} {'95% CI':<18} {'speedup':<10} {'wins':<6}")
    print("-" * 75)
    for r in rows_out:
        print(f"{r['cell']:<15} {r['n_pairs']:<4} "
              f"{r['mean_delta_s']:>+6.1f}s  "
              f"[{r['ci_lo_s']:+.1f}, {r['ci_hi_s']:+.1f}]s   "
              f"{r['speedup_pct']:>+5.1f}%   "
              f"{r['wins']}/{r['n_pairs']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
