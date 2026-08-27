#!/usr/bin/env python3
"""E5 analysis: policy fidelity and realization effects.

Reports, per the E5 spec: completion and makespan, observed per-node
order against the order SAGA returned, schedule hashes against the
frozen references, cost-model RMSE, constraint-override and fallback
counts, bytes per directed pair, gateway in/out bytes, digests and
restarts, and the batch throughput / latency / busy-time CoV.

It closes with the decision the pilot exists to answer: do at least two
of the three policies produce DIFFERENT schedules, and do their
outcomes differ by more than the run-to-run spread?

NOTE ON "FIDELITY".  What this script measures is ENACTMENT fidelity:
did Wayline place and order tasks exactly as the policy decided (hash
against the frozen reference, observed per-node order against the order
SAGA returned).  It deliberately does NOT compare SAGA's estimated
finish time against the measured makespan.  Those are not comparable
without correction: the estimate is built from declared runtimes, while
the measured makespan also spans pod admission, container start, input
read, and handoff.  That gap runs tens of percent (36.9% on iot, 36.0%
on wide-pipeline-flex), so reading it as scheduler error is wrong.  The
predictive-accuracy claim belongs to the calibrated simulator, which
adds per-task overhead and lands within 0.3-5.3%.

Usage: analyze_e5.py <results-dir>
"""
import csv
import json
import os
import statistics as st
import sys
from collections import defaultdict

RES = sys.argv[1] if len(sys.argv) > 1 else "results-pilot"
ISO = ["iso-heft-direct", "iso-maxtp-direct", "iso-olb-direct",
       "iso-heft-store", "iso-maxtp-store"]
BATCH = ["batch-heft-direct", "batch-maxtp-direct", "batch-olb-direct"]
ARMS = ISO + BATCH


def num(v, d=None):
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


def r_sched(arm):
    """arm name -> scheduler, e.g. 'batch-heft-direct' -> 'heft'."""
    return arm.split("-")[1]


def med(rs, k):
    v = [x for x in (num(r[k]) for r in rs) if x is not None]
    return st.median(v) if v else None


def fmt(v, s="{:.1f}"):
    return s.format(v) if v is not None else "-"


rows = list(csv.DictReader(open(os.path.join(RES, "runs.csv"))))
by = defaultdict(list)
for r in rows:
    by[r["arm"]].append(r)

prov = os.path.join(RES, "PROVENANCE.txt")
if os.path.exists(prov):
    for line in open(prov):
        if line.startswith(("repo_commit", "controller_image",
                            "data_agent_image", "seed", "frozen_")):
            print("  " + line.strip())

blocks = sorted({r["block"] for r in rows})
print(f"\nruns={len(rows)}  blocks={len(blocks)}  "
      f"valid={sum(r['valid'] == 'True' for r in rows)}/{len(rows)}")

bad = [r for r in rows if r["valid"] != "True"]
if bad:
    print("\n== INVALID RUNS (excluded from every comparison below) ==")
    for r in bad:
        print(f"  block {r['block']:>2} {r['arm']:<20} {r['invalid_reasons']}")
ok = [r for r in rows if r["valid"] == "True"]
okby = defaultdict(list)
for r in ok:
    okby[r["arm"]].append(r)

# ---- fidelity: did Wayline enact what the scheduler decided? ---------
print("\n== policy fidelity (valid runs only) ==")
print(f"{'arm':<20}{'hash=frozen':>12}{'order=sched':>12}{'RMSE':>10}"
      f"{'overrides':>11}{'fallbacks':>11}{'enact':>8}")
for a in ARMS:
    rs = okby[a]
    if not rs:
        continue
    # Only HEFT and MaxTP have frozen references; OLB is run live, so a
    # blank here means "not applicable", not "mismatched".
    if any(r["hash_matches_frozen"] in ("", "n/a") for r in rs):
        hcol = "n/a"
    else:
        hcol = f"{sum(r['hash_matches_frozen'] == 'True' for r in rs)}/{len(rs)}"
    od = sum(r["order_matches_schedule"] == "True" for r in rs)
    en = sum(r["enact_order_confirmed"] == "True" for r in rs)
    rm = med(rs, "rmse")
    print(f"{a:<20}{hcol:>12}{f'{od}/{len(rs)}':>12}"
          f"{(f'{rm:.1e}' if rm is not None else '-'):>10}"
          f"{st.median([num(r['constraint_overrides'], 0) for r in rs]):>11.0f}"
          f"{st.median([num(r['fallbacks'], 0) for r in rs]):>11.0f}"
          f"{f'{en}/{len(rs)}':>8}")

# ---- isolated regime: makespan --------------------------------------
print("\n== isolated regime (one DAG in flight) ==")
print(f"{'arm':<20}{'completed':>11}{'makespan med':>14}{'p95':>9}"
      f"{'gw bytes in/out':>18}{'digest':>9}{'restarts':>10}")
for a in ISO:
    rs = okby[a]
    if not rs:
        continue
    done = sum(int(num(r["completed"], 0)) for r in rs)
    runs = sum(int(num(r["runs"], 0)) for r in rs)
    dg = all(r["digests_ok"] == "True" for r in rs)
    gwio = f"{med(rs, 'gw_bytes_in') or 0:.0f}/{med(rs, 'gw_bytes_out') or 0:.0f}"
    print(f"{a:<20}{f'{done}/{runs}':>11}{fmt(med(rs,'makespan_med')):>14}"
          f"{fmt(med(rs,'makespan_p95')):>9}{gwio:>18}"
          f"{('ok' if dg else 'CHECK'):>9}"
          f"{max(num(r['restarts'], 0) for r in rs):>10.0f}")

# ---- batch regime: throughput, latency, balance ----------------------
print("\n== batch regime (8 in flight, 24 DAGs) ==")
print(f"{'arm':<20}{'DAGs/min':>10}{'batch s':>10}{'lat med':>10}"
      f"{'lat p95':>10}{'busy CoV':>10}")
for a in BATCH:
    rs = okby[a]
    if not rs:
        continue
    print(f"{a:<20}{fmt(med(rs,'dags_per_min'),'{:.2f}'):>10}"
          f"{fmt(med(rs,'batch_seconds'),'{:.0f}'):>10}"
          f"{fmt(med(rs,'latency_med')):>10}{fmt(med(rs,'latency_p95')):>10}"
          f"{fmt(med(rs,'busy_cov'),'{:.3f}'):>10}")

# ---- realization effect at identical placement ----------------------
print("\n== realization effect (same schedule, direct vs store) ==")
for algo in ("heft", "maxtp"):
    d, s = okby[f"iso-{algo}-direct"], okby[f"iso-{algo}-store"]
    if not (d and s):
        continue
    md, ms = med(d, "makespan_med"), med(s, "makespan_med")
    if md and ms:
        print(f"  {algo:<6} direct {md:6.1f}s   store {ms:6.1f}s   "
              f"ratio {ms/md:.2f}x")
    hd = {r["placement"] for r in d}
    hs = {r["placement"] for r in s}
    print(f"         placement identical across realizations: {hd == hs}")

# ---- the decision this pilot exists to make -------------------------
print("\n== DISCRIMINATION CHECK ==")
sched = {}
for a in ISO + BATCH:
    for r in okby[a]:
        sched.setdefault(r["scheduler"], set()).add(r["schedule_hash"])
distinct = {k: v for k, v in sched.items() if v}
print("  schedule hash by policy:")
seen = {}
for k, v in sorted(distinct.items()):
    for h in v:
        seen.setdefault(h, []).append(k)
    print(f"    {k:<8} {sorted(x[:12] for x in v)}")
npol = len(distinct)
nhash = len({h for v in distinct.values() for h in v})
print(f"  {npol} policies produced {nhash} distinct schedule(s): "
      f"{'DISCRIMINATING' if nhash >= 2 else 'NOT DISCRIMINATING'}")

for label, arms, key, unit in (
        ("isolated makespan", ISO[:3], "makespan_med", "s"),
        ("batch throughput", BATCH, "dags_per_min", "DAGs/min"),
        ("batch balance (CoV)", BATCH, "busy_cov", "")):
    vals = {}
    for a in arms:
        rs = okby[a]
        if not rs:
            continue
        v = [x for x in (num(x2[key]) for x2 in rs) if x is not None]
        if v:
            vals[r_sched(a)] = v
    if len(vals) < 2:
        continue
    print(f"  {label}:")
    lo = hi = None
    for k, v in sorted(vals.items()):
        spread = (max(v) - min(v)) if len(v) > 1 else 0.0
        print(f"    {k:<8} median {st.median(v):7.2f}{unit}  "
              f"within-policy spread {spread:.2f}")
        m = st.median(v)
        lo = m if lo is None else min(lo, m)
        hi = m if hi is None else max(hi, m)
    worst = max(((max(v) - min(v)) if len(v) > 1 else 0.0)
                for v in vals.values())
    gap = hi - lo
    verdict = ("separated" if gap > worst else
               "NOT separated (gap within run-to-run spread)")
    print(f"    across-policy gap {gap:.2f} vs worst within-policy "
          f"spread {worst:.2f}: {verdict}")
