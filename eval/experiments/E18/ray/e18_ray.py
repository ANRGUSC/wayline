#!/usr/bin/env python3
"""E18 Ray driver (E16's e16_ray.py on lane addressing): the iot and chain
workloads on Ray, pinned to the plan Wayline made, with the same task bodies.

Modes
  plan    each task pinned to its planned node (soft affinity, so Ray may
          re-run it elsewhere; pinned sources hard). Lost objects are
          recovered by Ray's lineage reconstruction, as Ray does by default.
  policy  as copies, plus Wayline's repair decisions (--repairs): when the
          consumer's node is lost, the application re-runs it on the node
          Wayline's repair policy chose (next to the copy), then re-protects its
          inputs on the node Wayline's copy policy chose; a second loss moves it
          there. The lines marked APP-POLICY are that code.
  copies  as plan, plus application-level copies: every object Wayline's
          policy copied is copied by a pinned `hold` task to the same node,
          its producer is made non-retryable (so a lost object raises instead
          of being reconstructed), and every consumer falls back to the copy
          when the primary is lost. This is what a Ray user must write to
          emulate plan-driven copies; the lines marked APP-COPIES are that
          code.

Runs inside the head pod:
  python e18_ray.py --workload iot --plan plan.json --nodes nodes.json [--pins pins.json]
                    [--copies copies.json]
(nodes.json: {lane node: data IP}; pins.json: {task: node} for pinned tasks,
which iot also marks in iot-base.json.)
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


def chain_tasks(workload="chain"):
    """The chain (8 MB), chain-link (30 MB), and fan (chain-link plus a short
    second consumer d), as in e16.py's templates."""
    mb = 8 if workload == "chain" else 30
    out = [{"name": "p", "role": "hash", "runtime": 12.0, "size": mb * 1_000_000, "cpu": 2, "deps": [], "pinned": False},
           {"name": "c", "role": "hash", "runtime": 20.0, "size": 0, "cpu": 2, "deps": ["p"], "pinned": False}]
    if workload == "fan":
        out.append({"name": "d", "role": "hash", "runtime": 1.0, "size": 0, "cpu": 1, "deps": ["p"], "pinned": False})
    return out


def lane_node_ids(nodes_file):
    """{lane node name: Ray NodeID}: Ray nodes are found by their data IP
    (NodeManagerAddress, the --node-ip-address each was started with)."""
    ips = json.load(open(nodes_file))
    by_ip = {n["NodeManagerAddress"]: n["NodeID"] for n in ray.nodes() if n["Alive"]}
    return {name: by_ip[ip] for name, ip in ips.items() if ip in by_ip}


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
    ap.add_argument("--workload", choices=["iot", "chain", "chain-link", "fan"], required=True)
    ap.add_argument("--plan", required=True, help="{task: node}")
    ap.add_argument("--copies", help="{object: [node]}: application-level copies")
    ap.add_argument("--nodes", required=True, help="{lane node: data IP}")
    ap.add_argument("--pins", help="{task: node}")
    ap.add_argument("--iot-base", default="/tmp/iot-base.json")
    ap.add_argument("--repairs", help='{"task": t, "nodes": [repair node, ...], "reprotect": [node, ...]}')
    a = ap.parse_args()

    tasks = iot_tasks(json.load(open(a.iot_base))) if a.workload == "iot" else chain_tasks(a.workload)
    plan = json.load(open(a.plan))
    copies = json.load(open(a.copies)) if a.copies else {}
    ray.init(address="auto", log_to_driver=False)
    node_id = lane_node_ids(a.nodes)
    pins = json.load(open(a.pins)) if a.pins else {}
    repairs = json.load(open(a.repairs)) if a.repairs else None   # APP-POLICY
    spec = {t["name"]: t for t in tasks}

    data, alts, metas = {}, {}, {}
    t0 = time.time()
    for t in tasks:
        name = t["name"]
        pinned = t["pinned"] or name in pins
        target = pins.get(name) or plan[name]
        opts = {"num_returns": 2, "num_cpus": t["cpu"], "name": name,
                "scheduling_strategy": NodeAffinitySchedulingStrategy(node_id[target], soft=not pinned)}
        if name in copies:
            opts["max_retries"] = 0  # APP-COPIES: a lost object must raise, not be reconstructed
        if repairs and name == repairs["task"]:
            opts["max_retries"] = 0  # APP-POLICY: a lost run raises, and the application re-places it
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
    done, errors, repaired = {}, {}, []
    while pending:
        ready, _ = ray.wait(list(pending), num_returns=1)
        name = pending.pop(ready[0])
        try:
            done[name] = ray.get(ready[0])
            print(f"DONE {name} {done[name]['end']:.3f}", flush=True)
        except ray.exceptions.RayError as e:
            if repairs and name == repairs["task"] and len(repaired) < len(repairs["nodes"]):
                # APP-POLICY: re-run the task where Wayline's repair policy put it, reading the surviving copies
                k = len(repaired)
                node = repairs["nodes"][k]
                t = spec[name]
                meta, out = run_alts.options(num_returns=2, num_cpus=t["cpu"], name=name, max_retries=0,
                                             scheduling_strategy=NodeAffinitySchedulingStrategy(node_id[node], soft=False)
                                             ).remote(name, t["role"], t["runtime"], t["size"],
                                                      [[data[d]] + alts.get(d, []) for d in t["deps"]])
                data[name], metas[name] = out, meta
                pending[meta] = name
                repaired.append({"at": time.time(), "to": node, "error": f"{type(e).__name__}"})
                print(f"REPAIRED {name} -> {node} {time.time():.3f}", flush=True)
                if k < len(repairs.get("reprotect", [])):
                    # APP-POLICY: re-protect the inputs on the node Wayline's copy policy chose
                    to = repairs["reprotect"][k]
                    for d in t["deps"]:
                        src = alts.get(d, [data[d]])[-1]
                        alts.setdefault(d, []).append(
                            hold.options(scheduling_strategy=NodeAffinitySchedulingStrategy(node_id[to], soft=False))
                            .remote(src))
                    print(f"REPROTECTED {name} inputs -> {to} {time.time():.3f}", flush=True)
                continue
            errors[name] = f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"
            print(f"FAILED {name} {errors[name]}", flush=True)
    ok = len(done) == len(tasks) and not errors
    print("RESULT " + json.dumps({"phase": "Succeeded" if ok else "Failed", "completion": time.time() - t0,
                                  "tasks": done, "errors": errors, "repaired": repaired}), flush=True)


if __name__ == "__main__":
    sys.exit(main())
