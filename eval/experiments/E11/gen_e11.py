#!/usr/bin/env python3
"""E11 workload: a seeded random layered DAG of CPU-bound tasks.

Every edge is a named object. Each task may run on a random subset of
round(frac * |nodes|) workers. Work is expressed in seconds at full clock;
the container turns it into a fixed number of SHA-256 iterations, so the
real runtime on a node follows that node's clock.

  dag(seed, nodes, ...)             -> dict (written to dag.json)
  template(name, sched, dag, rates) -> ODAGTemplate YAML
  bwconfig(nodes, mbit)             -> uniform wl-network-profile ConfigMap
"""
import json
import random

REG = "192.168.1.163:5000/wl-e11:latest"
MB = 1_000_000


def dag(seed=11, nodes=(), layers=(1, 5, 5, 5, 3, 1), frac=0.8,
        work=(2.0, 10.0), size_mb=(1, 30), max_parents=3, cpus=(2,)):
    rng = random.Random(seed)
    crng = random.Random(seed * 7919 + 1)   # own stream: CPU draws leave the DAG unchanged
    k = max(1, round(frac * len(nodes)))
    names, by_layer = [], []
    for li, width in enumerate(layers):
        by_layer.append([f"t{li}{chr(97 + i)}" for i in range(width)])
    tasks = {}
    for li, layer in enumerate(by_layer):
        for t in layer:
            parents = []
            if li > 0:
                prev = by_layer[li - 1]
                parents = rng.sample(prev, rng.randint(1, min(max_parents, len(prev))))
            tasks[t] = {"work": round(rng.uniform(*work), 2),
                        "cpu": crng.choice(cpus),
                        "allowed": sorted(rng.sample(list(nodes), k)),
                        "inputs": [[p, f"to-{t}"] for p in sorted(parents)],
                        "outputs": []}
            names.append(t)
    # every non-sink task must feed something; orphan outputs go to a random next-layer task
    for li, layer in enumerate(by_layer[:-1]):
        for t in layer:
            if not any(p == t for c in by_layer[li + 1] for p, _ in tasks[c]["inputs"]):
                c = rng.choice(by_layer[li + 1])
                tasks[c]["inputs"].append([t, f"to-{c}"])
    for c, spec in tasks.items():
        for p, obj in spec["inputs"]:
            tasks[p]["outputs"].append([obj, rng.randint(*size_mb) * MB])
    return {"seed": seed, "nodes": list(nodes), "frac": frac, "order": names, "tasks": tasks}


def template(name, scheduler, d, rates, cpu=None, enact="order", runner=None, image=REG,
             warm_tasks=None):
    """rates: {node: Mhash/s}; the full-clock reference rate is the max.
    cpu: request for every task, or None for each task's own "cpu".
    runner: run tasks warm on this runner; warm_tasks limits that to a subset
    (the rest run as pods)."""
    ref = max(rates.values())
    out = [f"""apiVersion: wl.io/v1
kind: ODAGTemplate
metadata:
  name: {name}
  namespace: wl-system
spec:
  description: 'E11: CPU-bound random DAG on frequency-capped nodes, 80% constraints.'
  scheduler: {scheduler}
  schedulerConfig:
    enactOrder: {enact}
  profiling:
    enabled: false
    runtimeSource: manual
    bandwidthSource: external
  retention:
    maxRuns: 40
    data:
      policy: keepLatest
      keepRuns: 1
  defaults:
    runtime: 4
    dataSize: 1MB
  tasks:"""]
    for t in d["order"]:
        s = d["tasks"][t]
        iters = int(s["work"] * ref * 1e6)
        deps = sorted({p for p, _ in s["inputs"]})
        L = [f"  - name: {t}", f"    image: {image}", "    command: [python, task.py]",
             f"    dependencies: [{', '.join(deps)}]"]
        if runner and (warm_tasks is None or t in warm_tasks):
            L.append(f"    runner: {runner}")          # warm: a call on the node's runner, no pod
        if s["inputs"]:
            L.append("    inputs:")
            for p, o in s["inputs"]:
                L += [f"    - producer: {p}", f"      object: {o}"]
        if s["outputs"]:
            L.append("    outputs:")
            for o, size in s["outputs"]:
                L += [f"    - name: {o}", f"      dataSize: \"{size}\""]
        L.append(f"    runtime: {max(1, round(s['work']))}")  # CRD: integer; runtimeProfile carries the real costs
        L.append("    runtimeProfile:")
        L += [f"      {n}: {round(iters / (rates[n] * 1e6), 3)}" for n in s["allowed"]]
        L += ["    resources:", f"      cpu: \"{cpu if cpu is not None else s.get('cpu', 2)}\"", "      memory: 512Mi",
              "    constraints:", f"      nodeNames: [{', '.join(s['allowed'])}]",
              "    env:",
              "    - name: E11_ITERS", f"      value: \"{iters}\"",
              "    - name: E11_INPUTS",
              f"      value: \"{','.join(f'{p}.{o}' for p, o in s['inputs'])}\"",
              "    - name: E11_OUTPUTS",
              f"      value: \"{','.join(f'{o}:{n}' for o, n in s['outputs'])}\""]
        out.append("\n".join(L))
    return "\n".join(out) + "\n"


def scale_sizes(d, k):
    """Multiply every edge's size by k; the DAG is otherwise unchanged."""
    if k != 1:
        for t in d["tasks"].values():
            t["outputs"] = [[o, int(n * k)] for o, n in t["outputs"]]
    return d


def bwmatrix(matrix, default):
    """wl-network-profile ConfigMap from {(u, v): bytes/s}."""
    body = "\n".join(f"  {u}_to_{v}: \"{int(b)}\"" for (u, v), b in sorted(matrix.items()))
    return f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: wl-network-profile
  namespace: wl-system
data:
  defaultBandwidth: "{int(default)}"
{body}
"""


def bwconfig(nodes, mbit=942.0):
    bps = int(mbit * 1e6 / 8)
    body = "\n".join(f"  {u}_to_{v}: \"{bps}\"" for u in nodes for v in nodes if u != v)
    return f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: wl-network-profile
  namespace: wl-system
data:
  defaultBandwidth: "{bps}"
{body}
"""


if __name__ == "__main__":
    import sys
    d = dag(nodes=["anrg-1", "anrg-3", "anrg-4", "anrg-5", "anrg-6", "anrg-7", "anrg-8", "anrg-9"])
    json.dump(d, sys.stdout, indent=1)
