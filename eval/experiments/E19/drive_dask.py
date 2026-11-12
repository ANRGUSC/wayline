#!/usr/bin/env python3
"""Run one E19 DAG on Dask, every task pinned to its planned node's worker.

Runs inside the Dask scheduler pod:
  python drive_dask.py --dag /tmp/dag.json --body /tmp/stage_body.py --scheduler tcp://10.20.0.1:8786
dag.json: as drive_ray.py, with "cpus": {node_ip: the worker's CPU resource}.
Each task requests its worker's whole CPU resource (one task at a time per
node) and may run only there. A producer's result holds every out-edge; a
pinned getitem on the producer's worker extracts each edge, so a consumer
moves exactly the bytes of its in-edges. Prints RESULT <json>.
"""
import argparse
import json
import operator
import time

from dask.distributed import Client

ap = argparse.ArgumentParser()
ap.add_argument("--dag", required=True)
ap.add_argument("--body", required=True)
ap.add_argument("--scheduler", required=True)
a = ap.parse_args()
dag = json.load(open(a.dag))
SRC = open(a.body).read()

client = Client(a.scheduler)
workers = {}
for addr, w in client.scheduler_info()["workers"].items():
    workers.setdefault(w["host"], []).append(addr)


def run_task(work, children, src, *inputs):
    import types
    mod = types.ModuleType("stage_body")
    exec(compile(src, "stage_body", "exec"), mod.__dict__)
    t0, t1, res = mod.stage(work, dict(children), *inputs)
    return (t0, t1, res)


def edge_of(result, child):
    return result[2][child]


def timing_of(result):
    return result[0], result[1]


tasks = {t["name"]: t for t in dag["tasks"]}
order, seen = [], set()
while len(order) < len(tasks):
    for t in dag["tasks"]:
        if t["name"] not in seen and all(p in seen for p in t["deps"]):
            order.append(t["name"])
            seen.add(t["name"])
src = client.scatter(SRC, broadcast=True)
futs, edges, timings = {}, {}, {}
start = time.time()
for name in order:
    t = tasks[name]
    w = workers[t["node_ip"]]
    inputs = [edges[(p, name)] for p in t["deps"]]
    f = client.submit(run_task, t["work"], sorted(t["outs"].items()), src, *inputs, key=f"{name}-run",
                      workers=w, allow_other_workers=False, pure=False,
                      resources={"CPU": dag["cpus"][t["node_ip"]]})
    futs[name] = f
    for c in t["outs"]:
        edges[(name, c)] = client.submit(edge_of, f, c, key=f"{name}-to-{c}", workers=w,
                                         allow_other_workers=False, pure=False)
    timings[name] = client.submit(timing_of, f, key=f"{name}-t", workers=w, allow_other_workers=False, pure=False)
times = {n: v for n, v in zip(timings, client.gather(list(timings.values())))}
first = min(s for s, _ in times.values())
last = max(e for _, e in times.values())
print("RESULT " + json.dumps({"system": "dask", "makespan": last - first, "submitted": start,
                              "tasks": {n: {"start": s, "end": e} for n, (s, e) in times.items()}}), flush=True)
