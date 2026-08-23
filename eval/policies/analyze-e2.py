"""E2: does the calibrated simulator reproduce the deployed scheduler
ordering on the FREED workloads (where the idealized pre-check sim
predicted a DataGravity win on wpf that hardware inverted)?

Profiles: per-(task,node) median occupancy pooled from the E2 campaign
itself (all arms of a workload), falling back to the task median for
(task,node) pairs unseen in other arms. Replay: same calibrated
network + contention model as the E8 ranking study.
"""
import json
import os
import statistics as st
import sys

sys.path.insert(0, "/Users/malikh/Desktop/wayline-nsdi/saga/src")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from saga import TaskGraph                                   # noqa: E402
from saga.contention import simulate_placement               # noqa: E402
from testbed import (EVAL, NODES, NIC, CORES, network,       # noqa: E402
                     bytes_of, occupancy, cpu_of)
E2 = "/Users/malikh/Desktop/wayline-nsdi/wayline/eval/policies/results-e2"


def spearman(a, b):
    def rank(x):
        order = sorted(range(len(x)), key=lambda i: x[i])
        r = [0.0] * len(x)
        i = 0
        while i < len(order):
            j = i
            while j + 1 < len(order) and x[order[j + 1]] == x[order[i]]:
                j += 1
            for k in range(i, j + 1):
                r[order[k]] = (i + j) / 2 + 1
            i = j + 1
        return r
    ra, rb = rank(a), rank(b)
    return pearson(ra, rb)


def pearson(a, b):
    ma, mb = st.mean(a), st.mean(b)
    num = sum((x - ma) * (y - mb) for x, y in zip(a, b))
    da = sum((x - ma) ** 2 for x in a) ** 0.5
    db = sum((y - mb) ** 2 for y in b) ** 0.5
    return num / (da * db) if da and db else float("nan")
ARMS = ["cpop", "gravity", "heft", "minmin", "olb", "random"]
WLS = ["het", "iot", "wpf"]
NET = network()
CAPS = {n: CORES for n in NODES}
NICS = {n: NIC for n in NODES}


def runs(wl, arm):
    d = json.load(open(f"{E2}/e2-{wl}-{arm}.json"))
    return [i for i in d["items"] if i["status"].get("phase") == "Succeeded"]


def profile(wl):
    per_node, per_task = {}, {}
    for arm in ARMS:
        for r in runs(wl, arm):
            for t in r["status"]["tasks"]:
                o = occupancy(t)
                if o is None:
                    continue
                per_node.setdefault((t["name"], t["node"]), []).append(o)
                per_task.setdefault(t["name"], []).append(o)
    return ({k: st.median(v) for k, v in per_node.items()},
            {k: st.median(v) for k, v in per_task.items()})


def replay(run, dur_node, dur_task):
    spec = {t["name"]: t for t in run["spec"]["tasks"]}
    place = {t["name"]: t["node"] for t in run["status"]["tasks"]
             if t.get("node")}
    if set(place) != set(spec):
        return None
    tg = TaskGraph.create(
        tasks=[(n, 1.0) for n in spec],
        dependencies=[(d, n, bytes_of(spec[d].get("dataSize", 0)))
                      for n, t in spec.items()
                      for d in t.get("dependencies", [])])
    sim = simulate_placement(
        NET, tg, place,
        task_cpu={n: cpu_of(t) for n, t in spec.items()},
        node_cpu=CAPS, nic_speed=NICS,
        task_duration=lambda t, n: dur_node.get((t, n), dur_task.get(t, 0.0)))
    return sim.makespan


for wl in WLS:
    dur_node, dur_task = profile(wl)
    meas_med, pred_med, all_m, all_p, errs = {}, {}, [], [], []
    for arm in ARMS:
        m, p = [], []
        for r in runs(wl, arm):
            pr = replay(r, dur_node, dur_task)
            if pr is None:
                continue
            m.append(r["status"]["makespan"])
            p.append(pr)
            errs.append(abs(pr - m[-1]) / m[-1] * 100)
        meas_med[arm], pred_med[arm] = st.median(m), st.median(p)
        all_m += m
        all_p += p
    mv = [meas_med[a] for a in ARMS]
    pv = [pred_med[a] for a in ARMS]
    rank_m = [a for _, a in sorted(zip(mv, ARMS))]
    rank_p = [a for _, a in sorted(zip(pv, ARMS))]
    print(f"===== {wl}  ({len(all_m)} runs replayed)")
    for arm in ARMS:
        print(f"  {arm:<9}{meas_med[arm]:>8.1f}s meas {pred_med[arm]:>8.1f}s pred")
    print(f"  measured rank : {' < '.join(rank_m)}")
    print(f"  predicted rank: {' < '.join(rank_p)}")
    print(f"  arm-level Spearman rho = {spearman(mv, pv):.2f}   "
          f"winner agree: {rank_m[0] == rank_p[0]}")
    print(f"  run-level Pearson r = {pearson(all_m, all_p):.2f}   "
          f"median |err| = {st.median(errs):.1f}%")
