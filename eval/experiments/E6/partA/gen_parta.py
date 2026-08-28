#!/usr/bin/env python3
"""E6 Part A: WfChef instance JSON -> Wayline ODAG templates.

Reads the committed instance files (determinism by artifact: the
instances were generated once and frozen; this script never regenerates
them) and emits, per workflow:

  sched   scheduler saga/heft, all tasks constrained off the gateway.
          Used once to freeze a placement; not an experimental arm.
  frozen  placement + per-node order replayed from the freeze; direct.
  store   same frozen schedule, one pod-less data vertex per named
          object pinned to the gateway; consumers rewired.

Scaling: ONE factor s per workflow, applied to BOTH runtimes and file
sizes: s = TARGET_SUM_RT / sum(runtime). Uniform joint scaling preserves
topology, intra-workflow ratios, and the recipe's bytes-per-compute-
second character exactly; scaling data to a common volume target instead
would manufacture communication weight for compute-dominated recipes
(blast's inter-task data would need x1470). Factors are recorded in
manifest.json.

Objects: one named object per producer, containing the producer's
consumed output files. This is byte-exact for these instances: grouping
each producer's consumed outputs by reader-set yields at most one group
per producer in all seven workflows (asserted below, so drift in a
regenerated instance fails loudly). Unconsumed (leaf) outputs stay
local to their producer and are excluded from objects; their scaled
bytes are recorded in the manifest.

External inputs (files no task produces -- reference databases etc.,
up to 246 GB in soykb) are NOT contract objects: they represent staged
node-local data, and the recipes' runtimes already include reading
them. Recorded per workflow in the manifest.

Usage:
  gen_parta.py <instances-dir> <out-dir> [--frozen <dir-with-frozen-json>]
"""
import argparse
import json
import pathlib
import re
import sys

TARGET_SUM_RT = 480.0
MIN_TASK_MS = 100
REGISTRY = "192.168.1.163:5000"
IMAGE = f"{REGISTRY}/wl-e6a:latest"
GATEWAY = "anrg-9"
WORKERS = ["anrg-1", "anrg-3", "anrg-4", "anrg-5", "anrg-6", "anrg-7", "anrg-8"]
WORKFLOWS = ["blast", "bwa", "cycles", "1000genome", "montage",
             "seismology", "soykb"]
OBJ = "o"  # single named object per producer (asserted exact)


def k8s_name(raw: str) -> str:
    n = re.sub(r"[^a-z0-9-]", "-", raw.lower().replace("_", "-"))
    return n.strip("-")


def load(instances: pathlib.Path, wf: str):
    d = json.loads((instances / f"{wf}.json").read_text())
    tasks = d["workflow"]["tasks"]
    by = {}
    for t in tasks:
        by[t["name"]] = t
    # producer map over output files
    prod, fsize = {}, {}
    for t in tasks:
        for f in t["files"]:
            if f["link"] == "output":
                prod[f["name"]] = t["name"]
                fsize[f["name"]] = f["sizeInBytes"]
    readers = {}
    external_in = 0
    for t in tasks:
        for f in t["files"]:
            if f["link"] == "input":
                if f["name"] in prod:
                    readers.setdefault(f["name"], set()).add(t["name"])
                else:
                    external_in += f["sizeInBytes"]
    # group consumed outputs by (producer, reader-set); assert <=1/producer
    groups = {}
    for fn, rs in readers.items():
        key = (prod[fn], frozenset(rs))
        groups[key] = groups.get(key, 0) + fsize[fn]
    per_prod = {}
    for (p, rs), size in groups.items():
        assert p not in per_prod, \
            f"{wf}: producer {p} has >1 reader-set group; " \
            f"one-object-per-producer no longer exact"
        per_prod[p] = (rs, size)
    unconsumed = sum(sz for fn, sz in fsize.items() if fn not in readers)
    return by, per_prod, external_in, unconsumed


def build(wf, instances, frozen_dir=None):
    by, objects, external_in, unconsumed = load(instances, wf)
    sum_rt = sum(t["runtimeInSeconds"] for t in by.values())
    s = TARGET_SUM_RT / sum_rt

    names = {t: k8s_name(t) for t in by}
    assert len(set(names.values())) == len(names), f"{wf}: name collision"

    frozen = None
    if frozen_dir:
        fp = pathlib.Path(frozen_dir) / f"frozen-{wf}.json"
        if fp.exists():
            frozen = json.loads(fp.read_text())

    def task_rows(store: bool):
        rows = []
        for raw, t in by.items():
            n = names[raw]
            work_ms = max(MIN_TASK_MS, round(t["runtimeInSeconds"] * s * 1000))
            deps, inkeys, insizes, peers = [], [], [], []
            for p_raw in t.get("parents", []):
                pn = names[p_raw]
                has_obj = (p_raw in objects
                           and raw in objects[p_raw][0])
                if has_obj:
                    key = f"{pn}.{OBJ}"
                    sz = max(1, round(objects[p_raw][1] * s))
                    inkeys.append(key)
                    insizes.append(f"{key}:{sz}")
                    if store:
                        v = f"v-{pn}"
                        deps.append(v)
                        peers.append(v)
                    else:
                        deps.append(pn)
                else:
                    # control-only edge: no data flows on it
                    deps.append(pn)
            outs = []
            if raw in objects:
                outs.append((OBJ, max(1, round(objects[raw][1] * s))))
            rows.append(dict(name=n, work_ms=work_ms, deps=deps,
                             inkeys=inkeys, insizes=insizes, peers=peers,
                             outs=outs, produces=raw in objects))
        return rows

    def emit(mode):
        store = (mode == "store")
        sched = "saga/heft" if mode == "sched" else "random"
        place = frozen["placement"] if (frozen and mode != "sched") else {}
        order = frozen["order"] if (frozen and mode != "sched") else {}
        head = (
            f"apiVersion: wl.io/v1\n"
            f"kind: ODAGTemplate\n"
            f"metadata:\n"
            f"  name: e6a-{k8s_name(wf)}-{mode}\n"
            f"  namespace: wl-system\n"
            f"spec:\n"
            f"  description: >\n"
            f"    E6 Part A {wf} ({mode}): WfChef-derived instance, scale\n"
            f"    factor {s:.5f} on runtimes and sizes; synthetic executors.\n"
            f"  scheduler: {sched}\n"
            f"  profiling:\n"
            f"    enabled: false\n"
            f"    runtimeSource: manual\n"
            f"    bandwidthSource: external\n"
            f"  defaults:\n"
            f"    runtime: 5\n"
            f"    dataSize: 1MB\n"
            f"  retention:\n"
            f"    maxRuns: 10\n"
            f"    data:\n"
            f"      policy: keepLatest\n"
            f"      keepRuns: 1\n"
        )
        if mode != "sched" and order:
            head += "  schedulerConfig:\n    enactOrder: serial\n    nodeOrder:\n"
            for node, seq in sorted(order.items()):
                if seq:
                    head += f"      {node}: [{', '.join(seq)}]\n"
        head += "  tasks:\n"

        body = []
        for r in task_rows(store):
            pin = ([place[r["name"]]] if place.get(r["name"])
                   else WORKERS)
            env = [("WL_WORK_MS", str(r["work_ms"]))]
            if r["outs"]:
                env.append(("WL_OUT_SPECS",
                            ",".join(f"{o}:{b}" for o, b in r["outs"])))
            if r["inkeys"]:
                env.append(("WL_INPUT_KEYS", ",".join(r["inkeys"])))
                env.append(("WL_INPUT_SIZES", ",".join(r["insizes"])))
            if r["peers"]:
                env.append(("WL_INPUT_PEERS", ",".join(r["peers"])))
            deps = ", ".join(f'"{d}"' for d in r["deps"])
            out_total = sum(b for _, b in r["outs"]) or 1
            blk = (
                f"    - name: {r['name']}\n"
                f"      image: {IMAGE}\n"
                f"      command: [\"python\", \"task.py\"]\n"
                f"      dependencies: [{deps}]\n"
                f"      dataSize: \"{out_total}\"\n"
                f"      runtime: {max(1, round(r['work_ms'] / 1000))}\n"
                f"      resources:\n"
                f"        cpu: \"1\"\n"
                f"        memory: \"512Mi\"\n"
                f"      constraints:\n"
                f"        nodeNames: [{', '.join(chr(34) + n + chr(34) for n in pin)}]\n"
            )
            if env:
                blk += "      env:\n"
                for k, v in env:
                    blk += f"        - {{ name: {k}, value: \"{v}\" }}\n"
            if r["outs"]:
                blk += "      outputs:\n"
                for o, b in r["outs"]:
                    blk += f"        - {{ name: {o}, dataSize: \"{b}\" }}\n"
            if r["inkeys"]:
                blk += "      inputs:\n"
                for key in r["inkeys"]:
                    pn, o = key.rsplit(".", 1)
                    if store:
                        blk += f"        - {{ producer: v-{pn} }}\n"
                    else:
                        blk += f"        - {{ producer: {pn}, object: {o} }}\n"
            body.append(blk)

        if store:
            for raw, (rs, size) in sorted(objects.items()):
                pn = names[raw]
                sz = max(1, round(size * s))
                body.append(
                    f"    - name: v-{pn}\n"
                    f"      type: data\n"
                    f"      image: {IMAGE}\n"
                    f"      command: [\"true\"]\n"
                    f"      dependencies: [\"{pn}\"]\n"
                    f"      dataSize: \"{sz}\"\n"
                    f"      runtime: 0\n"
                    f"      constraints:\n"
                    f"        nodeNames: [\"{GATEWAY}\"]\n"
                    f"      inputs:\n"
                    f"        - {{ producer: {pn}, object: {OBJ} }}\n")
        return head + "".join(body)

    manifest = dict(
        workflow=wf, tasks=len(by), objects=len(objects),
        scale_factor=round(s, 6),
        original_sum_runtime_s=round(sum_rt, 1),
        scaled_sum_runtime_s=round(sum_rt * s, 1),
        original_object_bytes=sum(v for _, v in objects.values()),
        scaled_object_bytes=sum(max(1, round(v * s))
                                for _, v in objects.values()),
        external_input_bytes_not_transferred=external_in,
        unconsumed_output_bytes_local=unconsumed,
        min_task_ms_floor=MIN_TASK_MS,
        note=("scale factor applied jointly to runtimes and sizes to "
              "preserve the recipe's bytes-per-compute-second; external "
              "inputs are staged data, not contract objects; executors "
              "are synthetic"),
    )
    return emit, manifest


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("instances")
    ap.add_argument("out")
    ap.add_argument("--frozen", default=None)
    a = ap.parse_args()
    instances = pathlib.Path(a.instances)
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    manifests = {}
    for wf in WORKFLOWS:
        emit, man = build(wf, instances, a.frozen)
        modes = ["sched"] + (["frozen", "store"] if a.frozen else [])
        for mode in modes:
            (out / f"e6a-{k8s_name(wf)}-{mode}.yml").write_text(emit(mode))
        manifests[wf] = man
        print(f"{wf:12} tasks={man['tasks']:3} objects={man['objects']:3} "
              f"s={man['scale_factor']:.5f} "
              f"scaled_obj={man['scaled_object_bytes']/1e6:8.2f}MB "
              f"modes={modes}")
    (out / "manifest.json").write_text(json.dumps(manifests, indent=2))


if __name__ == "__main__":
    main()
