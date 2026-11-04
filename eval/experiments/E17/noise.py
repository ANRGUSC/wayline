#!/usr/bin/env python3
"""E17 noise floor: the testbed against itself, beside the testbed against the simulator.

  noise.py RUNS_A.csv RUNS_C.csv

Both files are analyze.py runs.csv for the same scenarios run twice on the
testbed (campaign A, repeat 0, and campaign C, repeat 1, normally on other
lanes). For every (scenario, label) present in both: the relative difference
between the two testbed runs (makespan / M0), and each one's difference from
the simulation. If the simulation is as close to the testbed as the testbed is
to itself, the remaining gap is noise, not model error.
"""
import csv
import statistics
import sys


def load(path):
    out = {}
    for r in csv.DictReader(open(path)):
        if r["tb_norm"] in ("", "None") or r["sim_norm"] in ("", "None"):
            continue
        out[(r["scenario"], r["label"])] = (float(r["tb_norm"]), float(r["sim_norm"]), r["lane"], r["generator"])
    return out


def pct(xs, q):
    xs = sorted(xs)
    return xs[int(q * (len(xs) - 1))] if xs else float("nan")


def main():
    a, c = load(sys.argv[1]), load(sys.argv[2])
    keys = sorted(set(a) & set(c))
    rows = []
    for k in keys:
        ta, s, la, g = a[k]
        tc, _, lc, _ = c[k]
        rows.append((k, abs(ta / tc - 1), abs(ta / s - 1), abs(tc / s - 1), la != lc, g))
    print(f"{len(rows)} (scenario, label) pairs run twice; {sum(r[4] for r in rows)} on different lanes")
    for name, sel in (("all", lambda r: True), ("flapping", lambda r: r[5] == "flapping"),
                      ("jamming", lambda r: r[5] == "jamming"), ("mobility", lambda r: r[5] == "mobility"),
                      ("single", lambda r: r[5] in ("single", "episode"))):
        rs = [r for r in rows if sel(r)]
        if not rs:
            continue
        tt = [r[1] for r in rs]
        ts = [r[2] for r in rs] + [r[3] for r in rs]
        print(f"  {name:9s} testbed vs testbed: median {statistics.median(tt)*100:.1f}% p90 {pct(tt, .9)*100:.1f}% "
              f"| testbed vs sim: median {statistics.median(ts)*100:.1f}% p90 {pct(ts, .9)*100:.1f}%  (n={len(rs)})")
    worst = sorted(rows, key=lambda r: -r[1])[:8]
    print("largest testbed-vs-testbed differences:")
    for k, d, *_ in worst:
        print(f"  {k[0]:40s} {k[1]:10s} {d*100:.0f}%  (A {a[k][0]:.3f}, C {c[k][0]:.3f}, sim {a[k][1]:.3f})")


if __name__ == "__main__":
    main()
