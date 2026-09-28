#!/usr/bin/env python3
"""E11 Ray driver: the dag.json workload on Ray, same task body as Wayline
(fixed SHA-256 iterations, zero-filled outputs of each edge's size).

Modes
  default  Ray's own placement, restricted to each task's allowed nodes
           (NodeLabelSchedulingStrategy, label wl-node In allowed)
  plan     each task pinned (hard NodeAffinity) to the node in --plan,
           submitted in the plan's order when that order is topological

Runs inside the head pod:
  python e11_ray.py --dag dag.json --rates rates.json --mode plan --plan plan.json --cpus 2
Prints one JSON line: makespan, placement, per-task [start, end].
"""
import argparse
import hashlib
import json
import socket
import time

import numpy as np
import ray
from ray.util.scheduling_strategies import (In, NodeAffinitySchedulingStrategy,
                                            NodeLabelSchedulingStrategy)


@ray.remote
def run_task(name, iters, sizes, *inputs):
    t0 = time.time()
    _ = sum(int(x.nbytes) for x in inputs)
    d = b"wl-e11"
    for _ in range(iters):
        d = hashlib.sha256(d).digest()
    outs = [np.zeros(s, dtype=np.uint8) for s in sizes]
    meta = {"task": name, "node": socket.gethostname(), "start": t0, "end": time.time()}
    return (meta, *outs) if outs else meta


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dag", required=True)
    ap.add_argument("--rates", required=True)
    ap.add_argument("--mode", choices=["default", "plan"], required=True)
    ap.add_argument("--plan")
    ap.add_argument("--cpus", type=float, default=None, help="override every task's own cpu")
    args = ap.parse_args()

    d = json.load(open(args.dag))
    rates = json.load(open(args.rates))
    ref = max(rates.values())
    T = d["tasks"]
    ray.init(address="auto", log_to_driver=False)
    node_id = {n["NodeManagerHostname"]: n["NodeID"] for n in ray.nodes() if n["Alive"]}
    plan = json.load(open(args.plan)) if args.plan else {}

    order = plan.get("order") or d["order"]
    seen, topo = set(), True
    for t in order:
        if any(p not in seen for p, _ in T[t]["inputs"]):
            topo = False
        seen.add(t)
    if not topo or set(order) != set(T):
        order = d["order"]

    refs, metas = {}, {}
    t0 = time.time()
    for t in order:
        s = T[t]
        opts = {"num_returns": 1 + len(s["outputs"]), "num_cpus": args.cpus or s.get("cpu", 2)}
        if args.mode == "plan":
            opts["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                node_id=node_id[plan["placement"][t]], soft=False)
        else:
            opts["scheduling_strategy"] = NodeLabelSchedulingStrategy(
                hard={"wl-node": In(*s["allowed"])})
        ins = [refs[f"{p}.{o}"] for p, o in s["inputs"]]
        out = run_task.options(**opts).remote(
            t, int(s["work"] * ref * 1e6), [n for _, n in s["outputs"]], *ins)
        out = out if isinstance(out, list) else [out]
        metas[t] = out[0]
        for (o, _), r in zip(s["outputs"], out[1:]):
            refs[f"{t}.{o}"] = r
    m = ray.get([metas[t] for t in d["order"]])
    makespan = time.time() - t0
    print(json.dumps({"mode": args.mode, "makespan": round(makespan, 3), "submitted": t0,
                      "placement": {x["task"]: x["node"] for x in m},
                      "tasks": {x["task"]: [x["start"], x["end"]] for x in m}}))


if __name__ == "__main__":
    main()
