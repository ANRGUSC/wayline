#!/usr/bin/env python3
"""E16 Ray driver for GDTM: the same stages as examples/gdtm/stage.py
(gdtm_glue for source, prep and track; gdtm_model for embed and fuse), as
Ray tasks pinned to the plan Wayline used. Objects are named as on Wayline
("source-n2.zed", "prep-zed-n2.x", ...). Models are loaded once per Ray
worker process and cached, as a Ray user would.

Modes as in e16_ray.py: plan (lineage recovery) or copies (application
copies of the objects Wayline's policy copied; lines marked APP-COPIES).

Runs inside the head pod of a cluster built from examples/gdtm/Dockerfile.ray:
  python e16_ray_gdtm.py --plan plan.json [--copies copies.json]
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
import gen_gdtm  # noqa: E402  (stage list, sizes, pins, calibration)

MODS = {"zed": "zed_camera_left", "depth": "realsense_camera_depth",
        "radar": "range_doppler", "audio": "mic_waveform"}
NODES = {"n1": "node_1", "n2": "node_2", "n3": "node_3"}
SENSORS = {"n1": "anrg-4", "n2": "anrg-5", "n3": "anrg-9"}  # gen_gdtm.template's default pins


def pack(a):
    b = io.BytesIO(); np.save(b, a, allow_pickle=False); return b.getvalue()


def unpack(b):
    return np.load(io.BytesIO(b), allow_pickle=False)


def pack_many(**arrays):
    b = io.BytesIO(); np.savez(b, **arrays); return b.getvalue()


def unpack_many(b):
    return dict(np.load(io.BytesIO(b), allow_pickle=False))


def pack_jpegs(codes):
    return pack_many(data=np.concatenate(codes), offsets=np.cumsum([0] + [len(c) for c in codes]))


def unpack_jpegs(b):
    d = unpack_many(b)
    o = d["offsets"]
    return [d["data"][o[i]:o[i + 1]] for i in range(len(o) - 1)]


_CACHE = {}


def cached(key, build):
    if key not in _CACHE:
        _CACHE[key] = build()
    return _CACHE[key]


def threads(n):
    import torch
    torch.set_num_threads(n)
    try:
        import cv2
        cv2.setNumThreads(n)
    except ImportError:
        pass


def execute(name, role, mod, node, cpu, start, frames, inputs):
    threads(cpu)
    t0 = time.time()
    if role == "source":
        import gdtm_glue as G
        outs = []
        for short, full in MODS.items():
            vals = G.read(NODES[node], full, start, frames)
            outs.append(pack_jpegs(vals) if full in G.JPEG else pack(vals))
    elif role == "prep":
        import gdtm_glue as G
        raw = inputs[0]
        outs = [pack(G.PREP[MODS[mod]](unpack_jpegs(raw) if MODS[mod] in G.JPEG else unpack(raw)))]
    elif role == "embed":
        import gdtm_model as M
        modules = cached(("embed", mod, node), lambda: M.load_embedder(MODS[mod], NODES[node]))
        outs = [pack(M.embed(MODS[mod], NODES[node], unpack(inputs[0]), modules=modules))]
    elif role == "fuse":
        import gdtm_model as M
        model = cached(("fuse",), M.load_fusion)
        keys = [(MODS[m], NODES[n]) for n in NODES for m in MODS]
        means, covs = M.fuse({k: unpack(b) for k, b in zip(keys, inputs)}, model=model)
        outs = [pack_many(means=means, covs=covs)]
    elif role == "track":
        import gdtm_glue as G
        det = unpack_many(inputs[0])
        pos, cov = G.track(det["means"], det["covs"], *gen_gdtm.CALIB)
        outs = [pack_many(pos=pos, cov=cov, det_means=det["means"])]
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
def run(name, role, mod, node, cpu, start, frames, *inputs):
    return execute(name, role, mod, node, cpu, start, frames, list(inputs))


@ray.remote
def run_alts(name, role, mod, node, cpu, start, frames, inputs):  # APP-COPIES
    return execute(name, role, mod, node, cpu, start, frames, [resolve(a) for a in inputs])


@ray.remote(num_cpus=0)
def hold(x):  # APP-COPIES
    return x


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True)
    ap.add_argument("--copies")
    ap.add_argument("--frames", type=int, default=30)
    ap.add_argument("--start", type=int, default=0)
    a = ap.parse_args()
    plan = json.load(open(a.plan))
    copies = json.load(open(a.copies)) if a.copies else {}
    ray.init(address="auto", log_to_driver=False)
    node_id = {n["NodeManagerHostname"]: n["NodeID"] for n in ray.nodes() if n["Alive"]}

    objs, alts, metas, order = {}, {}, {}, []
    t0 = time.time()
    for task, role, mod, node, kind, inputs, outputs, rt, cpu in gen_gdtm.stages(a.frames):
        pinned = role == "source"
        target = SENSORS[node] if pinned else plan[task]
        names = [f"{task}.{o}" for o in outputs]
        opts = {"num_returns": 1 + len(names), "num_cpus": float(cpu),
                "scheduling_strategy": NodeAffinitySchedulingStrategy(node_id[target], soft=not pinned)}
        if any(n in copies for n in names):
            opts["max_retries"] = 0  # APP-COPIES: a lost copied object must raise, not be reconstructed
        keys = [f"{p}.{o}" for p, o in inputs]
        args = (task, role, mod, node, int(cpu), a.start, a.frames)
        if any(k in alts for k in keys):  # APP-COPIES
            refs = run_alts.options(**opts).remote(*args, [[objs[k]] + alts.get(k, []) for k in keys])
        else:
            refs = run.options(**opts).remote(*args, *[objs[k] for k in keys])
        metas[task] = refs[0]
        order.append(task)
        for n, r in zip(names, refs[1:]):
            objs[n] = r
            for dst in copies.get(n, []):  # APP-COPIES
                alts.setdefault(n, []).append(
                    hold.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node_id[dst], soft=False)).remote(r))

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
    ok = "track" in done and not errors
    print("RESULT " + json.dumps({"phase": "Succeeded" if ok else "Failed", "completion": time.time() - t0,
                                  "tasks": done, "errors": errors}), flush=True)


if __name__ == "__main__":
    sys.exit(main())
