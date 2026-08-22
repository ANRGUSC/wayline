#!/usr/bin/env python3
"""Reproduce the E1 numbers from the archived campaign results.

Reads results-e1/ (shaped matrix) and results-e1-flat/ (unshaped 1 GbE),
plus the e2 Argo+MinIO runs of the same DAGs as the referent. Prints arm
statistics, realization taxes with bootstrap CIs, the pod-realized vertex
overhead, byte concentration at the store node, and the Argo referent
decomposition. Pure JSON analysis; no simulator required.

Fidelity replays (simulator-based) live with the SAGA checkout; see
saga feature/resource-aware and the plan's E1/E1b entries.
"""
import csv
import glob
import json
import os
import random
import statistics as st

HERE = os.path.dirname(os.path.abspath(__file__))
SHAPED = os.path.join(HERE, "results-e1")
FLAT = os.path.join(HERE, "results-e1-flat")
E2 = os.path.join(HERE, "..", "synthetic-dags", "e2", "results")
STORE_NODE = "anrg-9"
DAGS = [("iot", "iobt"), ("hetero", "hetero"), ("wpf", "wpf")]


def runs(root, arm):
    d = json.load(open(os.path.join(root, f"{arm}.json")))
    return [i for i in d["items"] if i["status"].get("phase") == "Succeeded"]


def mks(root, arm):
    return [r["status"]["makespan"] for r in runs(root, arm)]


def boot_ratio(a, b, n=4000, seed=7):
    rng = random.Random(seed)
    v = sorted(st.median(rng.choices(a, k=len(a)))
               / st.median(rng.choices(b, k=len(b))) for _ in range(n))
    return v[int(0.025 * n)], v[int(0.975 * n)]


def store_bytes(root, arm):
    inn, out = [], []
    for r in runs(root, arm):
        fl = [f for f in r["status"].get("actualNetworkFlows", [])
              if f["dataSize"] > 1e6]
        inn.append(sum(f["dataSize"] for f in fl if f["dstNode"] == STORE_NODE))
        out.append(sum(f["dataSize"] for f in fl if f["srcNode"] == STORE_NODE))
    return st.median(inn) / 1e6, st.median(out) / 1e6


def argo(dag_key):
    out = []
    for row in csv.DictReader(open(os.path.join(E2, dag_key, "summary.csv"))):
        if row["phase"] == "Succeeded":
            out.append(float(row["makespan"]))
    return out


def line(root, arm):
    m = mks(root, arm)
    return m, f"median {st.median(m):6.1f}s  [{min(m):.0f}, {max(m):.0f}]  n={len(m)}"


print("=" * 74)
print("E1: realization tax at two network operating points")
print(f"{'dag':<8}{'condition':<9}{'direct':>20}{'store':>22}{'tax (95% CI)':>22}")
for dag, key in DAGS:
    for cond, root in (("flat", FLAT), ("shaped", SHAPED)):
        dm, _ = line(root, f"{dag}-direct")
        sm, _ = line(root, f"{dag}-store")
        lo, hi = boot_ratio(sm, dm)
        print(f"{dag:<8}{cond:<9}{st.median(dm):>18.1f}s{st.median(sm):>20.1f}s"
              f"{st.median(sm)/st.median(dm):>12.2f}x [{lo:.2f},{hi:.2f}]")

print()
print("E6: pod-realized vs agent-native data vertices (hetero, shaped)")
pod = mks(SHAPED, "hetero-store-pod")
nat = mks(SHAPED, "hetero-store")
lo, hi = boot_ratio(pod, nat)
print(f"  agent-native {st.median(nat):.1f}s  pod-realized {st.median(pod):.1f}s"
      f"  -> +{st.median(pod)-st.median(nat):.1f}s, {st.median(pod)/st.median(nat):.2f}x"
      f" [{lo:.2f},{hi:.2f}]  (~{(st.median(pod)-st.median(nat))/3:.1f}s per"
      f" critical-path vertex)")

print()
print(f"Byte concentration at {STORE_NODE} (median MB in/out per run, bulk flows)")
for dag, _ in DAGS:
    for cond, root in (("flat", FLAT), ("shaped", SHAPED)):
        si, so = store_bytes(root, f"{dag}-store")
        di, do = store_bytes(root, f"{dag}-direct")
        print(f"  {dag:<8}{cond:<8} direct {di:5.0f}/{do:<5.0f}  store {si:5.0f}/{so:<5.0f}")

print()
print("Argo+MinIO referent (e2 runs, same DAGs, shaped matrix, store on anrg-9)")
for dag, key in DAGS:
    am = argo(key)
    dm = st.median(mks(SHAPED, f"{dag}-direct"))
    em = st.median(mks(SHAPED, f"{dag}-store"))
    frac = (em - dm) / (st.median(am) - dm) * 100
    print(f"  {dag:<8} argo median {st.median(am):6.1f}s (n={len(am)})  "
          f"emulation {em:.1f}s  direct {dm:.1f}s  "
          f"-> topology explains {frac:.0f}% of the gap")
