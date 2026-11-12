#!/usr/bin/env python3
"""Run one E19 DAG on Ray, every task pinned to its planned node.

Runs inside the Ray head pod (same Python and Ray as the workers):
  python drive_ray.py --dag /tmp/dag.json --body /tmp/stage_body.py
dag.json: {"tasks": [{"name", "work", "node_ip", "deps": [parent...], "outs": {child: bytes}}],
           "cpus": {node_ip: ray CPUs}}
Each task requests all of its node's Ray CPUs, so a node runs one task at a
time (the simulator's node queue), and is pinned with a hard node affinity.
A consumer receives exactly the bytes of its in-edges: each producer returns
one object per out-edge (num_returns), and a consumer takes its parents'
objects for it. Prints RESULT <json> with per-task start/end and the makespan.
"""
import argparse
import importlib.util
import json
import time

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

ap = argparse.ArgumentParser()
ap.add_argument("--dag", required=True)
ap.add_argument("--body", required=True)
a = ap.parse_args()
dag = json.load(open(a.dag))
spec = importlib.util.spec_from_file_location("stage_body", a.body)
body = importlib.util.module_from_spec(spec)
spec.loader.exec_module(body)

ray.init(address="auto", log_to_driver=False)
node_id = {n["NodeManagerAddress"]: n["NodeID"] for n in ray.nodes() if n["Alive"]}
BODY_SRC = open(a.body).read()


@ray.remote
def run_task(work, children, src, *inputs):
    import types
    mod = types.ModuleType("stage_body")
    exec(compile(src, "stage_body", "exec"), mod.__dict__)
    t0, t1, res = mod.stage(work, {c: n for c, n in children}, *inputs)
    if not children:
        return (t0, t1)
    return [(t0, t1)] + [res[c] for c, _ in children]


tasks = {t["name"]: t for t in dag["tasks"]}
order = []
seen = set()
while len(order) < len(tasks):
    for t in dag["tasks"]:
        if t["name"] not in seen and all(p in seen for p in t["deps"]):
            order.append(t["name"])
            seen.add(t["name"])
refs = {}       # task -> [timing ref, edge refs...]
edge = {}       # (parent, child) -> object ref
src_ref = ray.put(BODY_SRC)
start = time.time()
for name in order:
    t = tasks[name]
    children = sorted(t["outs"].items())
    inputs = [edge[(p, name)] for p in t["deps"]]
    out = run_task.options(
        num_returns=1 + len(children), num_cpus=dag["cpus"][t["node_ip"]],
        scheduling_strategy=NodeAffinitySchedulingStrategy(node_id[t["node_ip"]], soft=False),
        name=name, max_retries=0).remote(t["work"], children, src_ref, *inputs)
    out = out if isinstance(out, list) else [out]
    refs[name] = out
    for (c, _), r in zip(children, out[1:]):
        edge[(name, c)] = r
times = {n: ray.get(r[0]) for n, r in refs.items()}
first = min(s for s, _ in times.values())
last = max(e for _, e in times.values())
print("RESULT " + json.dumps({"system": "ray", "makespan": last - first, "submitted": start,
                              "tasks": {n: {"start": s, "end": e} for n, (s, e) in times.items()}}), flush=True)
