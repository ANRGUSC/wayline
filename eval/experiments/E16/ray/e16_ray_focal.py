#!/usr/bin/env python3
"""E16 Ray driver for FOCAL: the same stages as examples/focal-mod/stage.py
(focal_stages), as Ray tasks pinned to the plan Wayline used. Objects are
named as on Wayline ("source-audio.samples", ...). Modes as in e16_ray.py:
plan (lineage recovery) or copies (application copies of the objects
Wayline's policy copied; lines marked APP-COPIES).

Runs inside the head pod of a cluster built from examples/focal-mod/Dockerfile.ray:
  python e16_ray_focal.py --plan plan.json [--copies copies.json]
Prints "DONE <task> <unix>" as tasks finish, then "RESULT <json>".
"""
import argparse
import io
import json
import sys
import time

import numpy as np
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

sys.path.insert(0, "/app")
import gen_focal  # noqa: E402  (stage list and pins)

BATCH, SEED = 64, 0


def pack(a):
    b = io.BytesIO(); np.save(b, a, allow_pickle=False); return b.getvalue()


def unpack(b):
    return np.load(io.BytesIO(b), allow_pickle=False)


def execute(name, role, mod, cpu, inputs):
    import torch
    torch.set_num_threads(cpu)
    import focal_stages as F
    t0 = time.time()
    if role == "source":
        outs = [pack(F.source(mod, BATCH, SEED + F.MODS.index(mod)))]
    elif role == "fft":
        outs = [pack(F.fft(unpack(inputs[0])))]
    elif role == "encode":
        outs = [pack(F.encode(mod, unpack(inputs[0])))]
    elif role == "fuse_head":
        outs = [pack(F.fuse_head({m: unpack(b) for m, b in zip(F.MODS, inputs)}))]
    else:
        raise ValueError(role)
    meta = {"task": name, "node": ray.util.get_node_ip_address(), "start": t0, "end": time.time()}
    return (meta, *outs)


def resolve(alternatives):
    """An input: the primary object, then its copies (APP-COPIES)."""
    for i, ref in enumerate(alternatives):
        try:
            return ray.get(ref)
        except ray.exceptions.RayError:
            if i == len(alternatives) - 1:
                raise


@ray.remote
def run(name, role, mod, cpu, *inputs):
    return execute(name, role, mod, cpu, list(inputs))


@ray.remote
def run_alts(name, role, mod, cpu, inputs):  # APP-COPIES
    return execute(name, role, mod, cpu, [resolve(a) for a in inputs])


@ray.remote(num_cpus=0)
def hold(x):  # APP-COPIES
    return x


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True)
    ap.add_argument("--copies")
    a = ap.parse_args()
    plan = json.load(open(a.plan))
    copies = json.load(open(a.copies)) if a.copies else {}
    ray.init(address="auto", log_to_driver=False)
    node_id = {n["NodeManagerHostname"]: n["NodeID"] for n in ray.nodes() if n["Alive"]}

    objs, alts, metas, order = {}, {}, {}, []
    t0 = time.time()
    for task, role, mod, inputs, output, rt, cpu in gen_focal.stages():
        pinned = task in gen_focal.PINS
        target = gen_focal.PINS.get(task) or plan[task]
        name_out = f"{task}.{output}"
        opts = {"num_returns": 2, "num_cpus": float(cpu), "name": task,
                "scheduling_strategy": NodeAffinitySchedulingStrategy(node_id[target], soft=not pinned)}
        if name_out in copies:
            opts["max_retries"] = 0  # APP-COPIES
        keys = [f"{p}.{o}" for p, o in inputs]
        if any(k in alts for k in keys):  # APP-COPIES
            refs = run_alts.options(**opts).remote(task, role, mod, int(cpu), [[objs[k]] + alts.get(k, []) for k in keys])
        else:
            refs = run.options(**opts).remote(task, role, mod, int(cpu), *[objs[k] for k in keys])
        metas[task], objs[name_out] = refs[0], refs[1]
        order.append(task)
        for dst in copies.get(name_out, []):  # APP-COPIES
            alts.setdefault(name_out, []).append(
                hold.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node_id[dst], soft=False)).remote(refs[1]))

    pending = {metas[t]: t for t in order}
    done, errors = {}, {}
    while pending:
        ready, _ = ray.wait(list(pending), num_returns=1)
        name = pending.pop(ready[0])
        try:
            done[name] = ray.get(ready[0])
            print(f"DONE {name} {done[name]['end']:.3f}", flush=True)
        except ray.exceptions.RayError as e:
            errors[name] = f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"
            print(f"FAILED {name} {errors[name]}", flush=True)
    ok = "fuse-head" in done and not errors
    print("RESULT " + json.dumps({"phase": "Succeeded" if ok else "Failed", "completion": time.time() - t0,
                                  "tasks": done, "errors": errors}), flush=True)


if __name__ == "__main__":
    sys.exit(main())
