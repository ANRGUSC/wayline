#!/usr/bin/env python3
"""E10 Ray driver: E5's seven-task DAG on Ray.

Same workload as the Wayline arm: every edge a named object of E5's size,
runtime(t, n) = WORK[t] / SPEED[n] (sleep, as in the E5 task image), 5 CPUs
per task (E5's pod request) on 8-CPU workers.

Modes
  default  Ray's own scheduling (locality-aware hybrid policy)
  plan     each task pinned to the node Wayline's HEFT placed it on
           (NodeAffinitySchedulingStrategy, hard), submitted in plan order

Runs inside the head pod:  python e10_ray.py --mode plan --plan plan.json
Prints one JSON line: makespan, placement, per-task [start, end] (worker clock).
"""
import argparse
import json
import socket
import time

import numpy as np
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

SPEED = {"anrg-1": 1.0, "anrg-3": 1.0, "anrg-4": 1.0, "anrg-5": 1.0,
         "anrg-6": 2.0, "anrg-7": 2.0, "anrg-8": 2.0, "anrg-9": 0.25}
WORK = {"source": 4, "a": 8, "b": 16, "c": 12, "j1": 14, "j2": 14, "sink": 4}
MB = 1_000_000
OUTPUTS = {
    "source": [("to-a", 1 * MB), ("to-b", 20 * MB), ("to-c", 60 * MB)],
    "a": [("to-j1", 5 * MB)], "b": [("to-j1", 20 * MB), ("to-j2", 2 * MB)],
    "c": [("to-j2", 40 * MB)], "j1": [("to-sink", 10 * MB)],
    "j2": [("to-sink", 10 * MB)], "sink": [],
}
INPUTS = {
    "a": ["source.to-a"], "b": ["source.to-b"], "c": ["source.to-c"],
    "j1": ["a.to-j1", "b.to-j1"], "j2": ["b.to-j2", "c.to-j2"],
    "sink": ["j1.to-sink", "j2.to-sink"],
}
ORDER = ["source", "a", "b", "c", "j1", "j2", "sink"]


@ray.remote(num_cpus=5)
def run_task(name, *inputs):
    t0 = time.time()
    host = socket.gethostname()
    _ = sum(int(x.nbytes) for x in inputs)                  # inputs are materialized locally
    time.sleep(WORK[name] / SPEED[host])
    outs = [np.zeros(size, dtype=np.uint8) for _, size in OUTPUTS[name]]
    meta = {"task": name, "node": host, "start": t0, "end": time.time()}
    return (meta, *outs) if outs else meta


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["default", "plan"], required=True)
    ap.add_argument("--plan", help="JSON {placement: {task: node}, order: [tasks]}")
    args = ap.parse_args()

    ray.init(address="auto", log_to_driver=False)
    node_id = {n["NodeManagerHostname"]: n["NodeID"] for n in ray.nodes() if n["Alive"]}
    plan = json.load(open(args.plan)) if args.plan else {}
    order = plan.get("order", ORDER)

    refs, metas = {}, {}
    t0 = time.time()
    for t in order:
        opts = {"num_returns": 1 + len(OUTPUTS[t])}
        if args.mode == "plan":
            opts["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
                node_id=node_id[plan["placement"][t]], soft=False)
        ins = [refs[k] for k in INPUTS.get(t, [])]
        out = run_task.options(**opts).remote(t, *ins)
        out = out if isinstance(out, list) else [out]
        metas[t] = out[0]
        for (obj, _), ref in zip(OUTPUTS[t], out[1:]):
            refs[f"{t}.{obj}"] = ref
    m = ray.get([metas[t] for t in ORDER])
    makespan = time.time() - t0
    print(json.dumps({"mode": args.mode, "makespan": round(makespan, 3),
                      "placement": {x["task"]: x["node"] for x in m},
                      "tasks": {x["task"]: [x["start"], x["end"]] for x in m}}))


if __name__ == "__main__":
    main()
