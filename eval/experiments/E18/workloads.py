"""E18 workloads: E16's templates, generated for a lane.

  iot          14 tasks, 4 pinned cameras (camera#1..4), 80-150 MB captures
  gdtm         GDTM early-fusion tracker, 29 tasks, sources pinned to sensor#1..3
  focal        FOCAL on MOD, 7 tasks, sources pinned to source#1 (seismic), source#2 (audio)
  chain        p (12 s, 8 MB) -> c (20 s); profiles co-locate both on `victim`
  chain-link   p (12 s, 30 MB) on `victim` -> c (20 s) on `dst`
  fan          chain-link plus d (1 s) on `holder`, a second consumer of p's output
  chain-rR-sM  warm chain, p runs R s and emits M MB (E16's sweep and window)
  *-warm       every task a call on a warm runner (the dispatch Ray uses)

GDTM and FOCAL come from examples/ (gen_gdtm.py, gen_focal.py) with the
registry and pins replaced; iot from iot-base.json (E16's, with role
placeholders); the chains are E16's ft_recovery / ft_link templates with
runtime profiles over the lane's nodes.

Chain tasks hash a fixed number of SHA-256 rounds (E11_ITERS = runtime x
RATE, an UP board at full clock), so their runtime on a node of another
class is runtime / speed(class): the profiles carry that, from the lane's
speed factors (lanes.SPEED, calibration). iot's bodies run for their runtime
(sleep and time-bounded loops); its compute stages get the same per-class
profiles (runtime / speed), its pinned captures keep their runtime. GDTM and
FOCAL keep the scalar runtimes of E16.

Image tags come from images.py (E18_IMAGES: mx1, the multi-arch builds, by
default; e16 for E16's amd64-only tags).
"""
import copy
import json
import os
import sys

import arms
import images
import kube

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
REG = kube.REG
NS = kube.NS
E11_IMAGE = images.ref("wl-e11")
RATE = 2.568e6            # hashes/s on a full-clock UP board (E11 calibration)
SWEEP = [(r, mb) for r in (2, 12, 30) for mb in (8, 100)]
RUNNER_IMAGES = {"iot": images.ref("wl-runner-iot"), "e11": images.ref("wl-runner-e11"),
                 "gdtm": images.ref("wl-gdtm"), "gdtm-model": images.ref("wl-gdtm-model"),
                 "focal": images.ref("wl-focal")}


class WorkloadError(ValueError):
    pass


def _examples():
    for sub in ("gdtm", "focal-mod"):
        p = os.path.join(REPO, "examples", sub)
        if p not in sys.path:
            sys.path.insert(0, p)


def gen_gdtm():
    _examples()
    import gen_gdtm as g
    g.IMAGES = {"glue": images.ref("wl-gdtm"), "model": images.ref("wl-gdtm-model")}
    return g


def gen_focal():
    _examples()
    import gen_focal as g
    g.IMAGE = images.ref("wl-focal")
    return g


def _yaml(text):
    import yaml  # PyYAML: in ~/testbed/venv
    return yaml.safe_load(text)


# ------------------------------------------------------------------ chains
def _speed(lane, node):
    f = lane.speed_of(node)
    if f is None:
        raise WorkloadError(f"no speed factor for class {lane.by_name[node]['class']} "
                            f"(run `e18.py calibrate` or pass --speed)")
    return f


def prof(lane, values, default):
    """runtimeProfile over the lane: values {node: s on an UP board}, else default."""
    return {n: round(values.get(n, default) / _speed(lane, n), 3) for n in lane.names}


def e11_task(name, runtime, profile, inputs=(), outputs=(), cpu="2", runner=None):
    """A task of E11's image: E11_ITERS SHA-256 rounds, named inputs and
    outputs (as eval/functional/ft_recovery.py's)."""
    t = {"name": name, "image": E11_IMAGE, "command": ["python", "task.py"],
         "dependencies": sorted({p for p, _ in inputs}), "runtime": runtime, "runtimeProfile": profile,
         "resources": {"cpu": cpu, "memory": "512Mi"},
         "env": [{"name": "E11_ITERS", "value": str(int(runtime * RATE))},
                 {"name": "E11_INPUTS", "value": ",".join(f"{p}.{o}" for p, o in inputs)},
                 {"name": "E11_OUTPUTS", "value": ",".join(f"{o}:{mb * 1000000}" for o, mb in outputs)}]}
    if inputs:
        t["inputs"] = [{"producer": p, "object": o} for p, o in inputs]
    if outputs:
        t["outputs"] = [{"name": o, "dataSize": f"{mb}MB"} for o, mb in outputs]
    if runner:
        t["runner"] = runner
    return t


def chain(lane, warm=False, runtime=12, mb=8):
    """ft_recovery's p -> c: profiles make HEFT co-locate both on victim."""
    v = lane.resolve("victim")
    r = "e11" if warm else None
    slow = round(runtime * 4 / 3, 1)     # 16 for the 12 s producer, as E16
    return [e11_task("p", runtime, prof(lane, {v: runtime}, slow), outputs=[("emb", mb)], runner=r),
            e11_task("c", 20, prof(lane, {v: 20}, 24), inputs=[("p", "emb")], runner=r)]


def chain_link(lane):
    """ft_link's p on victim -> c on dst, a planned transfer of 30 MB."""
    v, d = lane.resolve("victim"), lane.resolve("dst")
    if v == d:
        raise WorkloadError("chain-link needs distinct victim and dst nodes")
    return [e11_task("p", 12, prof(lane, {v: 12}, 16), outputs=[("emb", 30)]),
            e11_task("c", 20, prof(lane, {d: 20, v: 24}, 30), inputs=[("p", "emb")])]


def fan(lane):
    """chain-link plus a short consumer d of p's output on holder: after p
    finishes, holder has a copy of p.emb."""
    h = lane.resolve("holder")
    return chain_link(lane) + [e11_task("d", 1, prof(lane, {h: 1}, 5), inputs=[("p", "emb")], cpu="1")]


def _spec(tasks, description, extra=None):
    spec = {"description": description, "scheduler": "heft",
            "profiling": {"enabled": False, "runtimeSource": "manual", "bandwidthSource": "external"},
            "retention": {"maxRuns": 10, "data": {"policy": "keepLatest", "keepRuns": 1}},
            "defaults": {"runtime": 4, "dataSize": "1MB"}, "tasks": tasks}
    spec.update(extra or {})
    return spec


# --------------------------------------------------------------------- iot
# iot-pi: the same DAG with captures and features a tenth the size (8-15 MB),
# for lanes whose cameras are Pi 3s: a Pi 3's 100 Mbit/s NIC moves E16's
# 150 MB capture in ~13 s at best, longer under its own preprocessing load.
PI_SCALE = 0.1


def _scaled(size, k):
    s = str(size).upper()
    for unit, mult in (("GB", 10**9), ("MB", 10**6), ("KB", 10**3)):
        if s.endswith(unit):
            return str(max(1, int(float(s[:-2]) * mult * k)))
    return size


def iot_base(lane, scale=1.0):
    """iot-base.json with the registry, image tags (images.py) and camera roles resolved;
    scale shrinks the captures and features (iot-pi)."""
    text = open(os.path.join(HERE, "iot-base.json")).read().replace("${REG}", REG)
    base = json.loads(text)
    if scale != 1.0:
        for t in base["tasks"]:
            if t["name"].startswith(("capture", "preprocess")):
                t["dataSize"] = _scaled(t.get("dataSize", "0"), scale)
    for t in base["tasks"]:
        t["image"] = images.retag(t["image"])
        nn = (t.get("constraints") or {}).get("nodeNames")
        if nn:
            t["constraints"] = {"nodeNames": [lane.resolve(x[1:]) if x.startswith("@") else x for x in nn]}
    return base


def _iot_profiles(lane, tasks):
    """Compute stages (everything but the pinned captures) take runtime / speed(class) on each node, the same factor
    as the chain's kernel, so the planner sees a Pi as slower and the bodies (which wait out their runtime) take that
    long. Captures are sensor-bound and keep their runtime."""
    if any(lane.speed_of(n) is None for n in lane.names):
        return tasks
    for t in tasks:
        if not t["name"].startswith("capture"):
            t["runtimeProfile"] = prof(lane, {}, float(t.get("runtime", 1)))
    return tasks


def iot(lane, warm=False, scale=1.0):
    base = iot_base(lane, scale)
    tasks = _iot_profiles(lane, [dict(t, runner="iot") if warm else dict(t) for t in base["tasks"]])
    spec = _spec(tasks, "E16 iot: 4 pinned cameras, 80-150 MB captures" if scale == 1.0
                 else f"iot-pi: E16 iot with captures x{scale} (Pi-class cameras)")
    spec["defaults"] = base["defaults"]
    return spec


# ------------------------------------------------------------- GDTM, FOCAL
def gdtm_pins(lane):
    g = gen_gdtm()
    return {f"source-{n}": lane.resolve(f"sensor#{k + 1}") for k, n in enumerate(g.NODES)}


def gdtm(lane, mode="mixed"):
    g = gen_gdtm()
    sensors = tuple(lane.resolve(f"sensor#{k + 1}") for k in range(len(g.NODES)))
    return _yaml(g.template("x", mode, 30, 0, "heft", sensors))["spec"]


def focal_pins(lane):
    return {"source-seismic": lane.resolve("source#1"), "source-audio": lane.resolve("source#2")}


def focal(lane, mode="mixed"):
    g = gen_focal()
    pins = focal_pins(lane)
    return _yaml(g.template("x", mode, 64, 0, "heft", pins["source-seismic"], pins["source-audio"]))["spec"]


# --------------------------------------------------------------- registry
GEN = {
    "iot": lambda lane: iot(lane),
    "iot-warm": lambda lane: iot(lane, warm=True),
    "iot-pi": lambda lane: iot(lane, scale=PI_SCALE),
    "iot-pi-warm": lambda lane: iot(lane, warm=True, scale=PI_SCALE),
    "gdtm": lambda lane: gdtm(lane, "mixed"),
    "gdtm-warm": lambda lane: gdtm(lane, "warm"),
    "focal": lambda lane: focal(lane, "mixed"),
    "focal-warm": lambda lane: focal(lane, "warm"),
    "chain": lambda lane: _spec(chain(lane), "E16 chain: co-located expensive producer"),
    "chain-warm": lambda lane: _spec(chain(lane, warm=True), "E16 chain on warm runners"),
    "chain-link": lambda lane: _spec(chain_link(lane), "E16 chain-link: re-planning around a degraded link"),
    "fan": lambda lane: _spec(fan(lane), "E16 fan: a second copy on another consumer's node"),
}
for _r, _mb in SWEEP:
    GEN[f"chain-r{_r}-s{_mb}"] = (lambda r, mb: lambda lane: _spec(
        chain(lane, warm=True, runtime=r, mb=mb), f"E16 sweep: p {r} s, {mb} MB, warm"))(_r, _mb)


def template_name(workload, arm):
    return f"e18-{workload}-{arm.lower()}"


def task_archs(t):
    """Architectures a task can run on: its image's, and its runner's for a
    warm task (a warm task runs as a pod of its image when no runner answers)."""
    archs = kube.image_archs(t["image"])
    if t.get("runner"):
        archs = archs & kube.image_archs(RUNNER_IMAGES.get(t["runner"], ""))
    return archs


def build(workload, arm, lane, arch_mode="strict", fence=True, name=None):
    """The ODAGTemplate (a dict) of a workload for a Wayline arm on a lane.

    fence: give every unpinned task constraints.nodeNames = the lane's nodes
    when the lane is a subset of the schedulable workers (the controller
    plans over every schedulable worker otherwise). arch_mode: "strict"
    refuses a task whose image lacks a lane node's architecture; "constrain"
    narrows its nodeNames to the nodes it can run on. Returns (template,
    notes) where notes lists every constraint added."""
    if workload not in GEN:
        raise WorkloadError(f"unknown workload {workload!r}")
    spec = kube.rewrite_registry(copy.deepcopy(GEN[workload](lane)))
    spec["realizationPolicy"] = arms.policy(arm)
    notes = []
    lane_archs = set(lane.archs())
    for t in spec["tasks"]:
        archs = task_archs(t)
        if not archs:
            raise WorkloadError(f"{workload}/{t['name']}: image {t['image']} (runner {t.get('runner')}) "
                                f"not in the registry")
        pins = (t.get("constraints") or {}).get("nodeNames")
        if pins:
            for p in pins:
                if p not in lane.by_name:
                    raise WorkloadError(f"{workload}/{t['name']}: pinned to {p}, outside the lane")
                if lane.by_name[p]["arch"] not in archs:
                    raise WorkloadError(f"{workload}/{t['name']}: pinned to {p} ({lane.by_name[p]['arch']}), "
                                        f"but {t['image']} is {sorted(archs)} only")
            continue
        missing = lane_archs - archs
        if missing and arch_mode == "strict":
            raise WorkloadError(f"{workload}/{t['name']}: {t['image']}"
                                f"{' (runner ' + t['runner'] + ')' if t.get('runner') else ''} has no "
                                f"{sorted(missing)} build for this lane (build it, or --arch constrain)")
        allowed = lane.allowed(archs)
        if not allowed:
            raise WorkloadError(f"{workload}/{t['name']}: no lane node can run {t['image']}")
        if missing or (fence and lane.is_subset()):
            t["constraints"] = {"nodeNames": allowed}
            notes.append({"task": t["name"], "nodeNames": len(allowed),
                          "why": "arch" if missing else "lane fence"})
    tmpl = {"apiVersion": "wl.io/v1", "kind": "ODAGTemplate",
            "metadata": {"name": name or template_name(workload, arm), "namespace": NS,
                         "labels": {kube.EXPERIMENT_LABEL: kube.EXPERIMENT}},
            "spec": spec}
    return tmpl, notes


def runners_of(workload, lane):
    """Runner names a workload's warm tasks call."""
    return sorted({t["runner"] for t in GEN[workload](lane)["tasks"] if t.get("runner")})


def images_of(workload, lane):
    spec = kube.rewrite_registry(GEN[workload](lane))
    out = {t["image"] for t in spec["tasks"]}
    out |= {RUNNER_IMAGES[r] for r in runners_of(workload, lane)}
    return sorted(out)


def baseline_workload(workload, lane, plan=None):
    """(workload name the baseline drivers know, its iot base) for a cell's workload. With the plan, each iot
    task's runtime is its planned node's (the same per-node runtimes Wayline's tasks get)."""
    name, base = ("iot", iot_base(lane, PI_SCALE)) if workload.startswith("iot-pi") else (workload, iot_base(lane))
    if plan and name == "iot":
        tasks = _iot_profiles(lane, [dict(t) for t in base["tasks"]])
        for t in tasks:
            node = plan.get(t["name"])
            if node and t.get("runtimeProfile", {}).get(node) is not None:
                t["runtime"] = t["runtimeProfile"][node]
        base = dict(base, tasks=tasks)
    return name, base
