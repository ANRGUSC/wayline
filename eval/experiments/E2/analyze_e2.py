#!/usr/bin/env python3
"""E2 analysis: arm table + per-run adaptation timelines.

Usage: python3 analyze_e2.py <results-dir>
"""
import csv
import glob
import json
import os
import statistics as st
import sys
from collections import defaultdict

RES = sys.argv[1] if len(sys.argv) > 1 else "results"
ARMS = ["clean-direct", "fixed-direct", "static-relay", "adaptive-relay"]


def mb(v):
    try:
        return f"{int(v)/1e6:.0f}"
    except (TypeError, ValueError):
        return ""


rows = list(csv.DictReader(open(os.path.join(RES, "runs.csv"))))
by = defaultdict(list)
for r in rows:
    by[r["arm"]].append(r)

print("=" * 92)
print(f"{'arm':<16}{'n':>3}{'completed':>11}{'makespan':>10}"
      f"{'del 3>7':>9}{'del 3>8':>9}{'del 7>8':>9}{'relay pods':>12}{'digest':>8}")
print("-" * 92)
for arm in ARMS:
    rs = by.get(arm, [])
    if not rs:
        continue
    done = [r for r in rs if r["phase"] == "Succeeded"]
    mks = [int(r["makespan_s"]) for r in done if r["makespan_s"]]
    med = f"{st.median(mks):.0f}s" if mks else "censored"
    d37 = st.median([int(r["delivered_3_7"] or 0) for r in rs])
    d38 = st.median([int(r["delivered_3_8"] or 0) for r in rs])
    d78 = st.median([int(r["delivered_7_8"] or 0) for r in rs])
    pods = max(int(r["relay_pods"] or 0) for r in rs)
    dg = "ok" if all(r["digest"] == "ok" for r in rs) else "MISMATCH"
    print(f"{arm:<16}{len(rs):>3}{f'{len(done)}/{len(rs)}':>11}{med:>10}"
          f"{mb(d37):>9}{mb(d38):>9}{mb(d78):>9}{pods:>12}{dg:>8}")
print("=" * 92)
print("delivered = successful flows only (attempted-but-failed excluded)")

for r in rows:
    run = r["run"]
    path = os.path.join(RES, f"events-{run}.json")
    if not run or not os.path.exists(path):
        continue
    d = json.load(open(path))
    ev = d["events"]
    t0 = next((t for t, n in ev if n.startswith("t0")), ev[0][0] if ev else 0)
    print(f"\n--- {r['arm']} rep {r['rep']} ({run}) "
          f"phase={r['phase']} makespan={r['makespan_s'] or 'censored'}")
    for t, n in ev:
        print(f"   t={t - t0:+7.1f}s  {n}")
    for t, snap in d.get("object_history", []):
        try:
            objs = json.loads(snap)
        except (json.JSONDecodeError, TypeError):
            continue
        for o in objs:
            cps = " ".join(f"{c['node']}:{c['state']}" for c in o["copies"])
            print(f"   t={t - t0:+7.1f}s  copies[{o['object']}] {cps} "
                  f"serving={o.get('servingCopy', '')}")
