#!/usr/bin/env python3
"""E3 analysis: completion, paired comparisons, latencies, byte paths,
and the operational counters. Usage: analyze_e3.py <results-dir>
"""
import csv
import json
import os
import statistics as st
import sys
from collections import defaultdict

RES = sys.argv[1] if len(sys.argv) > 1 else "results-paper"
ARMS = ["fixed-clean", "always-clean", "fixed-loss",
        "always-loss", "adaptive-loss", "adaptive-clear"]
OBJ_MB = 300.0


def num(v, d=None):
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


rows = list(csv.DictReader(open(os.path.join(RES, "runs.csv"))))
by = defaultdict(list)
for r in rows:
    by[r["arm"]].append(r)
blocks = defaultdict(dict)
for r in rows:
    blocks[r["block"]][r["arm"]] = r

prov = os.path.join(RES, "PROVENANCE.txt")
if os.path.exists(prov):
    for line in open(prov):
        if line.startswith(("repo_commit", "controller_image",
                            "data_agent_image", "seed")):
            print("  " + line.strip())

print(f"\nruns={len(rows)}  blocks={len(blocks)}")

# ---- completion fractions -------------------------------------------
print("\n== completion (fixed-loss censored by design) ==")
print(f"{'arm':<16}{'completed':>11}{'makespan med':>14}{'IQR':>12}")
for a in ARMS:
    rs = by[a]
    done = [r for r in rs if r["phase"] == "Succeeded"]
    mk = sorted(int(r["makespan_s"]) for r in done if r["makespan_s"])
    med = f"{st.median(mk):.0f}s" if mk else "censored"
    iqr = f"[{mk[len(mk)//4]},{mk[(3*len(mk))//4]}]" if mk else "-"
    print(f"{a:<16}{f'{len(done)}/{len(rs)}':>11}{med:>14}{iqr:>12}")

# ---- paired comparisons ---------------------------------------------
def paired(a, b, field, conv=float):
    out = []
    for blk in blocks.values():
        if a in blk and b in blk:
            va, vb = conv_or_none(blk[a][field], conv), conv_or_none(blk[b][field], conv)
            if va is not None and vb is not None:
                out.append(va - vb)
    return out


def conv_or_none(v, conv):
    try:
        return conv(v)
    except (TypeError, ValueError):
        return None


print("\n== paired within matching arms (per block) ==")
for a, b, field, unit in (
        ("adaptive-loss", "always-loss", "makespan_s", "s"),
        ("always-clean", "fixed-clean", "makespan_s", "s"),
        ("adaptive-clear", "fixed-clean", "makespan_s", "s"),
        ("adaptive-loss", "always-loss", "replica_residence_s", "s"),
        ("adaptive-clear", "always-clean", "replica_residence_s", "s"),
        ("adaptive-loss", "always-loss", "replica_storage_MB_s", "MB*s"),
        ("adaptive-clear", "always-clean", "replica_storage_MB_s", "MB*s")):
    d = paired(a, b, field)
    if not d:
        continue
    base = [conv_or_none(blk[b][field], float) for blk in blocks.values()
            if b in blk and conv_or_none(blk[b][field], float) is not None]
    pct = (st.median(d) / st.median(base) * 100) if base and st.median(base) else 0
    print(f"  {a} - {b}  [{field}]: n={len(d)} median={st.median(d):+.1f}{unit}"
          f" ({pct:+.0f}%)  range=[{min(d):+.1f},{max(d):+.1f}]")

# ---- latencies -------------------------------------------------------
# Recomputed from the saved traces with arm-correct semantics: the
# replica-request instant is the policy's risk patch for adaptive arms
# and the submission patch for always-on arms. (The CSV field measures
# from the policy's FIRST patch, which for always-loss is the later
# rebind, yielding a meaningless negative.)
def policy_patches(run):
    path = os.path.join(RES, f"policy-{run}.log")
    out = {}
    if not os.path.exists(path):
        return out
    for line in open(path):
        parts = line.split()
        if "patched=" not in line or len(parts) < 5:
            continue
        label = parts[2]
        obs = pat = None
        for tok in parts:
            if tok.startswith("observed="):
                obs = float(tok.split("=", 1)[1])
            if tok.startswith("patched="):
                pat = float(tok.split("=", 1)[1])
        if obs and pat:
            out[label] = (obs, pat)
    return out


def events_of(run):
    path = os.path.join(RES, f"events-{run}.json")
    if not os.path.exists(path):
        return {}, {}
    d = json.load(open(path))
    ev = {}
    for t, n in d.get("events", []):
        ev.setdefault(n, t)
    return ev, d


risk_to_patch, patch_to_install_adaptive, patch_to_install_always = [], [], []
for r in rows:
    run = r["run"]
    if not run:
        continue
    pp = policy_patches(run)
    ev, _ = events_of(run)
    replica_patch = None
    if r["arm"] in ("adaptive-loss", "adaptive-clear") and "risk->replicate" in pp:
        obs, pat = pp["risk->replicate"]
        risk_to_patch.append(pat - obs)
        replica_patch = pat
    elif r["arm"] in ("always-clean", "always-loss"):
        replica_patch = ev.get("submit:replica-requested")
    inst = ev.get("backup:installed")
    if replica_patch and inst:
        if r["arm"] in ("adaptive-loss", "adaptive-clear"):
            patch_to_install_adaptive.append(inst - replica_patch)
        else:
            patch_to_install_always.append(inst - replica_patch)

print("\n== adaptation latencies (median [min,max], n) ==")
for v, label in ((sorted(risk_to_patch), "risk -> patch"),
                 (sorted(patch_to_install_adaptive),
                  "risk patch -> installed (adaptive)"),
                 (sorted(patch_to_install_always),
                  "submit patch -> installed (always)"),
                 (sorted(x for x in (num(r["loss_to_rebind_s"]) for r in rows)
                         if x is not None), "loss -> serving rebind"),
                 (sorted(x for x in (num(r["rebind_to_consumer_install_s"])
                                     for r in rows) if x is not None),
                  "rebind -> consumer input")):
    if v:
        print(f"  {label:<36} {st.median(v):>7.2f}s  "
              f"[{v[0]:.2f},{v[-1]:.2f}]  n={len(v)}")
print("  (always-on patches at submission, so its install waits for the "
      "object to exist; controller logs are 1s-resolution, so sub-second "
      "rebind values may read slightly negative)")

# ---- byte paths + digests -------------------------------------------
print("\n== path bytes (median MB delivered) and digest ==")
print(f"{'arm':<16}{'3->7':>8}{'3->8':>8}{'7->8':>8}{'digest':>10}"
      f"{'attempted 3->8':>16}")
for a in ARMS:
    rs = by[a]
    def med(k):
        v = [num(r[k], 0) for r in rs]
        return st.median(v) / 1e6 if v else 0
    ok = all(r["digest"] == "ok" for r in rs if r["phase"] == "Succeeded")
    print(f"{a:<16}{med('delivered_3_7'):>8.0f}{med('delivered_3_8'):>8.0f}"
          f"{med('delivered_7_8'):>8.0f}{('ok' if ok else 'CHECK'):>10}"
          f"{med('attempted_3_8'):>16.0f}")

# ---- counters --------------------------------------------------------
print("\n== counters ==")
print(f"{'arm':<16}{'patches':>9}{'evictions':>11}{'cancels':>9}"
      f"{'restarts':>10}{'backup pods':>13}{'failed flows':>14}")
for a in ARMS:
    rs = by[a]
    ev = sum(1 for r in rs if r["backup_evict_rel_event_s"] not in ("", None))
    print(f"{a:<16}{st.median([num(r['patches'],0) for r in rs]):>9.0f}"
          f"{ev:>11}{st.median([num(r['cancels'],0) for r in rs]):>9.0f}"
          f"{max(num(r['restarts'],0) for r in rs):>10.0f}"
          f"{max(num(r['relay_pods'],0) for r in rs):>13.0f}"
          f"{st.median([num(r['failed_flows'],0) for r in rs]):>14.0f}")

# ---- hygiene ---------------------------------------------------------
print("\n== hygiene ==")
print(f"  caps verified live:  {sum(r['cap_verified']=='True' for r in rows)}/{len(rows)}")
print(f"  qdiscs clean after:  {sum(r['qdisc_clean_after']=='True' for r in rows)}/{len(rows)}")
print(f"  infra failures:      {sum('InfraFail' in r['phase'] or 'SubmitFail' in r['phase'] for r in rows)}")
print(f"  digest correct:      "
      f"{sum(r['digest']=='ok' for r in rows if r['phase']=='Succeeded')}"
      f"/{sum(r['phase']=='Succeeded' for r in rows)} of successes")
