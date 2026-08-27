#!/usr/bin/env python3
"""E4 analysis: per-arm payloads/makespans, paired block comparisons,
and the pilot acceptance criteria. Usage: analyze_e4.py <results-dir>
"""
import csv
import json
import os
import statistics as st
import sys
from collections import defaultdict

RES = sys.argv[1] if len(sys.argv) > 1 else "results-pilot"
ARMS = ["fixed", "alert-only", "features-only", "snapshot-only", "all-outputs"]
EXPECTED_MB = {"fixed": 400, "alert-only": 401, "features-only": 200,
               "snapshot-only": 600, "all-outputs": 401}
EXPECTED_OBJS = {"fixed": set(), "alert-only": {"alert"},
                 "features-only": {"features"},
                 "snapshot-only": {"snapshot"},
                 "all-outputs": {"alert", "features", "snapshot"}}

rows = list(csv.DictReader(open(os.path.join(RES, "runs.csv"))))
by = defaultdict(list)
for r in rows:
    by[r["arm"]].append(r)
blocks = defaultdict(dict)
for r in rows:
    blocks[r["block"]][r["arm"]] = r


def f(v, d=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


print(f"runs={len(rows)}  blocks={len(blocks)}  seed={rows[0]['seed']}")
print("\n== per arm ==")
print(f"{'arm':<15}{'n':>3}{'done':>6}{'makespan':>10}{'IQR':>11}"
      f"{'capMB':>8}{'expect':>8}{'wire%':>7}{'objects on anrg-7':>22}")
for a in ARMS:
    rs = by[a]
    done = [r for r in rs if r["phase"] == "Succeeded"]
    mk = sorted(int(r["makespan_s"]) for r in done if r["makespan_s"])
    cap = [f(r["cap_MB"]) for r in rs if r["cap_MB"]]
    objs = set()
    for r in rs:
        objs |= {o for o in r["copies_on_target"].split(",") if o}
    med_cap = st.median(cap) if cap else 0
    exp = EXPECTED_MB[a]
    print(f"{a:<15}{len(rs):>3}{len(done):>6}"
          f"{(f'{st.median(mk):.0f}s' if mk else 'censored'):>10}"
          f"{(f'[{mk[0]},{mk[-1]}]' if mk else '-'):>11}"
          f"{med_cap:>8.0f}{exp:>8}{(med_cap/exp-1)*100:>6.1f}%"
          f"{(','.join(sorted(objs)) or 'none'):>22}")

print("\n== paired within blocks (makespan) ==")
def paired(a, b):
    out = []
    for blk in blocks.values():
        if a in blk and b in blk and blk[a]["makespan_s"] and blk[b]["makespan_s"]:
            out.append(int(blk[a]["makespan_s"]) - int(blk[b]["makespan_s"]))
    return out
for a, b in (("features-only", "fixed"), ("snapshot-only", "fixed"),
             ("alert-only", "fixed"), ("all-outputs", "features-only"),
             ("all-outputs", "fixed")):
    d = paired(a, b)
    if d:
        print(f"  {a} - {b}: n={len(d)} median={st.median(d):+.0f}s "
              f"range=[{min(d):+d},{max(d):+d}]  "
              f"{'faster in ALL blocks' if all(x < 0 for x in d) else ('slower in ALL blocks' if all(x > 0 for x in d) else 'mixed')}")

print("\n== acceptance criteria ==")
def chk(label, cond):
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")

done_all = all(r["phase"] == "Succeeded" for r in rows)
chk(f"{len(rows)}/{len(rows)} complete with correct digests",
    done_all and all(r["digests_ok"] == "True" for r in rows))
chk("no restarts, no pods on anrg-7",
    all(int(f(r["restarts"])) == 0 for r in rows) and
    all(int(f(r["target_pods"])) == 0 for r in rows))
vex_ok = all(int(f(r["vertex_exec_total"])) == 3 for r in rows)
chk("each data vertex executes exactly once (3 per run)", vex_ok)
if not vex_ok:
    bad = [(r["order"], r["arm"], r["vertex_exec_total"]) for r in rows
           if int(f(r["vertex_exec_total"])) != 3]
    print(f"        counts != 3: {bad}  (log-window artifact if the run "
          f"Succeeded with verified digests)")
chk("selective arms install ONLY the requested object(s)",
    all({o for o in r["copies_on_target"].split(",") if o} == EXPECTED_OBJS[r["arm"]]
        for r in rows))
chk("delivered payloads match expectation (within 8% wire overhead)",
    all(abs(f(r["cap_MB"]) / EXPECTED_MB[r["arm"]] - 1) < 0.08
        for r in rows if r["cap_MB"]))
fo = [f(r["cap_MB"]) for r in by["features-only"]]
fx = [f(r["cap_MB"]) for r in by["fixed"]]
chk("features-only carries ~half the capped payload of fixed",
    fo and fx and abs(st.median(fo) / st.median(fx) - 0.5) < 0.06)
chk("features-only faster than fixed in EVERY block",
    all(x < 0 for x in paired("features-only", "fixed")))
sn = [f(r["cap_MB"]) for r in by["snapshot-only"]]
chk("snapshot-only ~600MB and slower than fixed",
    sn and abs(st.median(sn) / 600 - 1) < 0.08 and
    all(x > 0 for x in paired("snapshot-only", "fixed")))
ao = [f(r["cap_MB"]) for r in by["all-outputs"]]
chk("all-outputs ~= fixed payload and materially slower than features-only",
    ao and fx and abs(st.median(ao) / st.median(fx) - 1) < 0.06 and
    all(x > 0 for x in paired("all-outputs", "features-only")))
al = [f(r["cap_MB"]) for r in by["alert-only"]]
chk("alert-only stays close to fixed (revising the wrong object doesn't help)",
    al and fx and abs(st.median(al) / st.median(fx) - 1) < 0.06 and
    abs(st.median(paired("alert-only", "fixed"))) <= 3)
chk("caps verified live and qdiscs clean after every run",
    all(r["cap_verified"] == "True" for r in rows) and
    all(r["qdisc_clean_after"] == "True" for r in rows))
