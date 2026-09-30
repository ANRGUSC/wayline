#!/usr/bin/env python3
"""E16 Ray driver: the iot and chain workloads on Ray, pinned to the plan
Wayline used, with the same task bodies.

Modes
  plan    each task pinned to its planned node (soft affinity, so Ray may
          re-run it elsewhere; pinned sources hard). Lost objects are
          recovered by Ray's lineage reconstruction, as Ray does by default.
  copies  as plan, plus application-level copies: every object Wayline's
          policy copied is copied by a pinned `hold` task to the same node,
          its producer is made non-retryable (so a lost object raises instead
          of being reconstructed), and every consumer falls back to the copy
          when the primary is lost. This is what a Ray user must write to
          emulate plan-driven copies; the lines marked APP-COPIES are that
          code.

Runs inside the head pod:
  python e16_ray.py --workload iot --plan plan.json [--copies copies.json]
Prints "DONE <task> <unix>" as tasks finish, then "RESULT <json>".
"""
import argparse
import hashlib
import json
import random
import sys
import time

import numpy as np
import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

RATE = 2.568e6  # hashes/s on a full-clock UP board (as ft_recovery.py)


# ─── task bodies (as the Wayline tasks') ─────────────────────────────────────

def spin(n):
    d = b"wl-e11"
    for _ in range(n):
        d = hashlib.sha256(d).digest()


def body(name, role, runtime, size, inputs):
    t0 = time.perf_counter()
    if role == "capture":
        time.sleep(min(runtime, 5.0))
        block = hashlib.md5(name.encode()).digest() * 4096
        return np.frombuffer((block * (size // len(block) + 1))[:size], dtype=np.uint8)
    if role == "preprocess":
        raw = inputs[0].tobytes()
        digest = b"".join(hashlib.sha256(raw[o:o + 65536]).digest() for o in range(0, len(raw), 65536))
        time.sleep(max(0.0, runtime - (time.perf_counter() - t0)))
        rest = size - len(digest)
        feats = digest + (raw * (rest // max(len(raw), 1) + 1))[:rest] if rest > 0 else digest[:size]
        return np.frombuffer(feats, dtype=np.uint8)
    if role == "infer":
        feats = inputs[0].tobytes()
        i = 0
        while time.perf_counter() - t0 < runtime:
            hashlib.sha256(feats[i % len(feats):i % len(feats) + 4096]).digest()
            i += 1
        rng = random.Random(name)
        dets = [{"id": f"{name}-{k}", "confidence": round(rng.uniform(0.4, 0.99), 3)}
                for k in range(rng.randint(5, 15))]
        return np.frombuffer(json.dumps({"source": name, "detections": dets}).encode(), dtype=np.uint8)
    if role in ("fuse", "report"):
        merged = [json.loads(x.tobytes()) for x in inputs]
        time.sleep(max(0.0, runtime - (time.perf_counter() - t0)))
        return np.frombuffer(json.dumps({"source": name, "inputs": len(merged)}).encode(), dtype=np.uint8)
    if role == "hash":
        _ = [x.nbytes for x in inputs]
        spin(int(runtime * RATE))
        return np.zeros(size, dtype=np.uint8)
    raise ValueError(role)


def resolve(alternatives):
    """An input: the primary object, then its copies (APP-COPIES: the
    fallback loop; without copies this is a plain ray.get, and a lost
    primary is reconstructed by Ray's lineage)."""
    for i, ref in enumerate(alternatives):
        try:
            return ray.get(ref)
        except ray.exceptions.RayError:
            if i == len(alternatives) - 1:
                raise


def execute(name, role, runtime, size, inputs):
    start = time.time()
    data = body(name, role, runtime, size, inputs)
    return {"task": name, "node": ray.util.get_node_ip_address(), "start": start, "end": time.time()}, data


@ray.remote
def run(name, role, runtime, size, *inputs):
    """Plain Ray: inputs are arguments, resolved (and reconstructed if lost)
    by Ray before the task starts."""
    return execute(name, role, runtime, size, list(inputs))


@ray.remote
def run_alts(name, role, runtime, size, inputs):
    """APP-COPIES: inputs passed as [primary, copy...] lists and resolved
    by the task itself, so it can fall back to a copy."""
    return execute(name, role, runtime, size, [resolve(alts) for alts in inputs])


@ray.remote(num_cpus=0)
def hold(x):
    """APP-COPIES: a copy of an object whose primary lives on this node."""
    return x


# ─── workloads ───────────────────────────────────────────────────────────────

def iot_tasks(base):
    role = lambda n: n.split("-")[0].replace("generate", "report")
    out = []
    for t in base["tasks"]:
        size = parse_size(t.get("dataSize", "1MB"))
        cpu = parse_cpu(t.get("resources", {}).get("cpu", "1"))
        pins = (t.get("constraints") or {}).get("nodeNames") or []
        out.append({"name": t["name"], "role": role(t["name"]), "runtime": float(t.get("runtime", 1)),
                    "size": size, "cpu": cpu, "deps": t.get("dependencies", []), "pinned": bool(pins)})
    return out


def chain_tasks():
    return [{"name": "p", "role": "hash", "runtime": 12.0, "size": 8_000_000, "cpu": 2, "deps": [], "pinned": False},
            {"name": "c", "role": "hash", "runtime": 20.0, "size": 0, "cpu": 2, "deps": ["p"], "pinned": False}]


def parse_size(s):
    s = str(s).upper()
    for suf, mul in (("GB", 10**9), ("MB", 10**6), ("KB", 10**3)):
        if s.endswith(suf):
            return int(float(s[:-len(suf)]) * mul)
    return int(float(s))


def parse_cpu(s):
    s = str(s)
    return float(s[:-1]) / 1000 if s.endswith("m") else float(s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workload", choices=["iot", "chain"], required=True)
    ap.add_argument("--plan", required=True, help="{task: node}")
    ap.add_argument("--copies", help="{object: [node]}: application-level copies")
    ap.add_argument("--iot-base", default="/tmp/iot-base.json")
    a = ap.parse_args()

    tasks = iot_tasks(json.load(open(a.iot_base))) if a.workload == "iot" else chain_tasks()
    plan = json.load(open(a.plan))
    copies = json.load(open(a.copies)) if a.copies else {}
    ray.init(address="auto", log_to_driver=False)
    node_id = {n["NodeManagerHostname"]: n["NodeID"] for n in ray.nodes() if n["Alive"]}

    data, alts, metas = {}, {}, {}
    t0 = time.time()
    for t in tasks:
        name = t["name"]
        opts = {"num_returns": 2, "num_cpus": t["cpu"],
                "scheduling_strategy": NodeAffinitySchedulingStrategy(node_id[plan[name]], soft=not t["pinned"])}
        if name in copies:
            opts["max_retries"] = 0  # APP-COPIES: a lost object must raise, not be reconstructed
        if any(d in alts for d in t["deps"]):  # APP-COPIES: an input has a copy to fall back to
            meta, out = run_alts.options(**opts).remote(
                name, t["role"], t["runtime"], t["size"], [[data[d]] + alts.get(d, []) for d in t["deps"]])
        else:
            meta, out = run.options(**opts).remote(name, t["role"], t["runtime"], t["size"],
                                                   *[data[d] for d in t["deps"]])
        data[name], metas[name] = out, meta
        for node in copies.get(name, []):  # APP-COPIES: pin a copy on each chosen node
            alts.setdefault(name, []).append(
                hold.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node_id[node], soft=False))
                .remote(out))

    pending = {metas[t["name"]]: t["name"] for t in tasks}
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
    sink = tasks[-1]["name"]
    ok = sink in done and not errors.get(sink)
    print("RESULT " + json.dumps({"phase": "Succeeded" if ok else "Failed", "completion": time.time() - t0,
                                  "tasks": done, "errors": errors}), flush=True)


if __name__ == "__main__":
    sys.exit(main())
