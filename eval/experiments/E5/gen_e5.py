#!/usr/bin/env python3
"""Generate E5 artifacts: the direct template, the symmetric bandwidth
ConfigMap, and (given a frozen schedule) the store-mediated lowering.

Usage:
  gen_e5.py direct   <scheduler> [enactOrder]   > e5-<...>.yml
  gen_e5.py bwconfig                            > e5-bandwidth.yml
  gen_e5.py store    <frozen-schedule.json>     > e5-store-<algo>.yml
"""
import json
import sys

REG = "192.168.1.163:5000/wl-e5:latest"
MB = 1_000_000
EDGE = ["anrg-1", "anrg-3", "anrg-4", "anrg-5"]
COMPUTE = ["anrg-6", "anrg-7", "anrg-8"]
GATEWAY = "anrg-9"
NODES = EDGE + COMPUTE + [GATEWAY]
SPEED = {**{n: 1.0 for n in EDGE}, **{n: 2.0 for n in COMPUTE}, GATEWAY: 0.25}
WORK = {"source": 4, "a": 8, "b": 16, "c": 12, "j1": 14, "j2": 14, "sink": 4}
OUTPUTS = {
    "source": [("to-a", 1 * MB), ("to-b", 20 * MB), ("to-c", 60 * MB)],
    "a": [("to-j1", 5 * MB)], "b": [("to-j1", 20 * MB), ("to-j2", 2 * MB)],
    "c": [("to-j2", 40 * MB)], "j1": [("to-sink", 10 * MB)],
    "j2": [("to-sink", 10 * MB)], "sink": [],
}
# consumer -> [(producer, object)]
INPUTS = {
    "a": [("source", "to-a")], "b": [("source", "to-b")],
    "c": [("source", "to-c")],
    "j1": [("a", "to-j1"), ("b", "to-j1")],
    "j2": [("b", "to-j2"), ("c", "to-j2")],
    "sink": [("j1", "to-sink"), ("j2", "to-sink")],
}
ORDER = ["source", "a", "b", "c", "j1", "j2", "sink"]

# Symmetric matrix, in bytes/sec. B = 942 Mbit/s from E0.
B_MBIT = 942.0


def rate(u, v):
    if u == GATEWAY or v == GATEWAY:
        return B_MBIT / 16
    same = (u in EDGE and v in EDGE) or (u in COMPUTE and v in COMPUTE)
    return B_MBIT if same else B_MBIT / 8


def bps(mbit):
    return int(mbit * 1e6 / 8)


def runtime_profile(task):
    return {n: round(WORK[task] / SPEED[n], 4) for n in NODES}


def emit_task(t, pin=None, input_peers=None, deps=None, inputs=None):
    """Emit one task. `deps`/`inputs` override the logical wiring, which
    the store lowering uses to route each consumer through the data
    vertex holding its object instead of straight to the producer."""
    lines = [f"  - name: {t}",
             f"    image: {REG}",
             "    command: [python, task.py]"]
    d = deps if deps is not None else sorted({p for p, _ in INPUTS.get(t, [])})
    lines.append(f"    dependencies: [{', '.join(d)}]" if d
                 else "    dependencies: []")
    ins = inputs if inputs is not None else [
        {"producer": p, "object": o} for p, o in INPUTS.get(t, [])]
    if ins:
        lines.append("    inputs:")
        for spec in ins:
            lines.append(f"    - producer: {spec['producer']}")
            if spec.get("object"):
                lines.append(f"      object: {spec['object']}")
    if OUTPUTS[t]:
        lines.append("    outputs:")
        for obj, size in OUTPUTS[t]:
            lines.append(f"    - name: {obj}")
            lines.append(f"      dataSize: \"{size}\"")
    lines.append(f"    runtime: {WORK[t]}")
    lines.append("    runtimeProfile:")
    for n, v in runtime_profile(t).items():
        lines.append(f"      {n}: {v}")
    lines.append("    resources:")
    lines.append("      cpu: \"5\"")
    lines.append("      memory: 1Gi")
    if pin:
        lines.append("    constraints:")
        lines.append(f"      nodeNames: [{pin}]")
    if input_peers:
        lines.append("    env:")
        lines.append("    - name: WL_INPUT_PEERS")
        lines.append(f"      value: \"{','.join(input_peers)}\"")
    return "\n".join(lines)


def header(name, scheduler, extra_cfg=""):
    return f"""apiVersion: wl.io/v1
kind: ODAGTemplate
metadata:
  name: {name}
  namespace: wl-system
spec:
  description: 'E5 policy fidelity: named-object DAG, node-dependent runtimes.'
  scheduler: {scheduler}
  schedulerConfig:
    enactOrder: serial{extra_cfg}
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
  tasks:"""


def direct(name, scheduler):
    return header(name, scheduler) + "\n" + "\n".join(
        emit_task(t) for t in ORDER) + "\n"


def bwconfig():
    data = {}
    for u in NODES:
        for v in NODES:
            if u != v:
                data[f"{u}_to_{v}"] = str(bps(rate(u, v)))
    body = "\n".join(f"  {k}: \"{v}\"" for k, v in sorted(data.items()))
    return f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: wl-network-profile
  namespace: wl-system
data:
  defaultBandwidth: "{bps(B_MBIT)}"
{body}
"""


def store(frozen_path):
    """Store-mediated lowering: one pod-less data vertex per named object,
    pinned to the gateway, with every consumer rewired to read from the
    vertex. Application placement and per-node order are replayed from
    the frozen schedule; the scheduler is not re-run."""
    fr = json.load(open(frozen_path))
    place = fr["placement"]
    order = fr["order"]
    algo = fr["algorithm_short"]
    out = [header(f"e5-store-{algo}", "random",
                  extra_cfg="\n    nodeOrder:" + "".join(
                      f"\n      {n}: [{', '.join(ts)}]"
                      for n, ts in order.items() if ts))]
    for t in ORDER:
        vs = [f"v-{p}-{o}" for p, o in INPUTS.get(t, [])]
        out.append(emit_task(
            t, pin=place[t],
            deps=vs,
            inputs=[{"producer": v} for v in vs],
            input_peers=vs or None))
    for prod, objs in OUTPUTS.items():
        for obj, size in objs:
            v = f"v-{prod}-{obj}"
            out.append("\n".join([
                f"  - name: {v}",
                f"    image: {REG}",
                "    command: [python, task.py]",
                f"    dependencies: [{prod}]",
                "    inputs:",
                f"    - producer: {prod}",
                f"      object: {obj}",
                f"    dataSize: \"{size}\"",
                "    runtime: 0",
                "    type: data",
                "    constraints:",
                f"      nodeNames: [{GATEWAY}]"]))
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "direct":
        sched = sys.argv[2]
        short = sys.argv[3] if len(sys.argv) > 3 else sched.split("/")[-1][:8]
        print(direct(f"e5-{short}", sched))
    elif cmd == "bwconfig":
        print(bwconfig())
    elif cmd == "store":
        print(store(sys.argv[2]))
    else:
        sys.exit(f"unknown command {cmd}")
