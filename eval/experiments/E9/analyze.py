#!/usr/bin/env python3
"""Summarise an E9 results directory: median phase times per (layout, d), and
how much of a chain's makespan is per-task overhead.

Usage: python3 analyze.py results-pilot
Accepts both the pilot's column set (start, sdk_init) and the current one.
"""
import csv
import statistics as st
import sys

d = sys.argv[1] if len(sys.argv) > 1 else "results-pilot"
rows = list(csv.DictReader(open(f"{d}/tasks.csv")))
runs = list(csv.DictReader(open(f"{d}/runs.csv")))
if "sdk_init" in rows[0]:                       # pilot schema: fold sdk_init into start
    for r in rows:
        r["start"] = str(float(r["start"]) + float(r["sdk_init"]))
        r["critical_overhead"] = str(sum(float(r[k]) for k in ("dispatch", "schedule", "start", "input", "handoff")))

phases = ["dispatch", "schedule", "start", "input", "compute", "handoff", "exit", "critical_overhead"]
med = lambda g, p: st.median(float(r[p]) for r in g if r[p] != "")
print(f"{'layout':6} {'d':>5} " + " ".join(f"{p:>9}" for p in phases[:-1]) + "  crit/task")
for lay in sorted({r["layout"] for r in rows}):
    for dd in sorted({r["d"] for r in rows}, key=float):
        g = [r for r in rows if r["layout"] == lay and r["d"] == dd]
        if g:
            print(f"{lay:6} {float(dd):5.1f} " + " ".join(f"{med(g, p):9.3f}" for p in phases))
print()
for run in runs:
    wall, comp = float(run["wall"]), float(run["sum_compute"])
    print(f"{run['run']:26} wall {wall:6.2f}s  compute {comp:5.1f}s  "
          f"overhead {wall - comp:5.2f}s ({100 * (wall - comp) / wall:4.1f}% of wall)")
