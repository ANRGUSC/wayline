#!/usr/bin/env python3
"""
Analyze the heterogeneity campaign results.

Produces, per cell (mode x net):
  - makespan vs rep (does HEFT improve as profiles converge?)
  - profile convergence: mean |runtime EMA change| and coverage (#(task,node)
    pairs profiled, total samples) vs rep
  - predicted-vs-actual makespan error vs rep
And a cross-cell summary comparing classical vs resource-aware HEFT in the warm
regime (last WARM reps), with/without network shaping.

Usage: analyze.py [results_dir]   (default: ./results)
"""
import sys
import os
import csv
import statistics as st

RESULTS = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
CELLS = ["ra-net", "classical-net", "ra-nonet", "classical-nonet"]
WARM = 5  # last N reps define the converged/warm regime


def load_csv(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def num(x):
    try:
        return float(x)
    except (ValueError, TypeError):
        return None


def cell_makespan(cell):
    rows = load_csv(os.path.join(RESULTS, cell, "makespan.csv"))
    return [(int(r["rep"]), num(r["actual_ms"]), num(r["predicted_ms"]), r["phase"]) for r in rows]


def profile_convergence(cell):
    """Per rep: #pairs, total samples, and mean abs change in learned runtime
    vs the previous rep (the EMA settling signal)."""
    rows = load_csv(os.path.join(RESULTS, cell, "profiles.csv"))
    by_rep = {}
    for r in rows:
        by_rep.setdefault(int(r["rep"]), {})[(r["task"], r["node"])] = num(r["runtime_learned"])
    out = []
    prev = None
    for rep in sorted(by_rep):
        cur = by_rep[rep]
        pairs = len(cur)
        samples = sum(1 for _ in cur)  # placeholder; sample counts in tasks.csv-free path
        if prev is not None:
            common = set(cur) & set(prev)
            deltas = [abs(cur[k] - prev[k]) for k in common if cur[k] is not None and prev[k] is not None]
            mchange = round(st.mean(deltas), 3) if deltas else 0.0
        else:
            mchange = None
        out.append((rep, pairs, mchange))
        prev = cur
    return out


def main():
    print("=" * 74)
    print("HETEROGENEITY CAMPAIGN ANALYSIS")
    print("=" * 74)

    warm_ms = {}
    for cell in CELLS:
        ms = cell_makespan(cell)
        if not ms:
            print(f"\n## {cell}: (no data)")
            continue
        print(f"\n## {cell}   ({len(ms)} reps)")
        print("  rep  actual_ms  predicted_ms  pred_err%   phase")
        for rep, a, p, ph in ms:
            err = f"{round(100*(p-a)/a,1):>6}" if (a and p) else "     ?"
            print(f"  {rep:>3}  {str(a):>9}  {str(p):>12}  {err:>8}   {ph}")
        warm = [a for rep, a, p, ph in ms if a and rep > max(1, len(ms) - WARM)]
        if warm:
            warm_ms[cell] = warm
            print(f"  warm(last {len(warm)}) makespan: mean={round(st.mean(warm),1)}s "
                  f"median={round(st.median(warm),1)}s min={min(warm)}s max={max(warm)}s")

        conv = profile_convergence(cell)
        if conv:
            print("  profile convergence (rep: #pairs, mean|Δruntime| vs prev):")
            print("   " + "  ".join(f"r{rep}:{pairs}p/{mc}" for rep, pairs, mc in conv))

    # cross-cell warm comparison
    print("\n" + "=" * 74)
    print("WARM-REGIME COMPARISON (classical vs resource-aware HEFT)")
    print("=" * 74)
    def m(c): return round(st.mean(warm_ms[c]), 1) if c in warm_ms else None
    for net in ["net", "nonet"]:
        ra, cl = m(f"ra-{net}"), m(f"classical-{net}")
        tag = "shaped network" if net == "net" else "unshaped 1GbE"
        if ra and cl:
            print(f"  [{tag}]  resource-aware={ra}s  classical={cl}s  "
                  f"classical/ra={round(cl/ra,2)}x")
        else:
            print(f"  [{tag}]  ra={ra}  classical={cl}")
    rn, ro = m("ra-net"), m("ra-nonet")
    if rn and ro:
        print(f"  [net effect, ra]  shaped={rn}s vs unshaped={ro}s  ratio={round(rn/ro,2)}x")


if __name__ == "__main__":
    main()
