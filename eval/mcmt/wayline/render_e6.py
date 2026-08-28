#!/usr/bin/env python3
"""Render E6 Part B templates: MCMT with named output objects.

Three variants from one definition, so the arms cannot drift apart:

  sched   scheduler chooses compute-tier placement. Used once to produce
          the schedule we freeze; not an experimental arm.
  frozen  application placement AND per-node order replayed from the
          frozen schedule; direct realization.
  store   the same frozen placement and order, with one pod-less data
          vertex per named object pinned to the gateway, so every
          intermediate crosses anrg-9. Consumers are rewired to read the
          vertex.

`frozen` and `store` differ only in the data path, which is what makes
the realization comparison controlled.

Named objects, one per producer:
  decode-i -> frames    preprocess-i -> prepped   detect-embed-i -> dets
  track-i  -> tracks    cross-camera-match -> matches
`report` is terminal and writes to a hostPath.

Usage:
  render_e6.py sched  <n_cameras> <clip_s> <fmt> [-o out.yml]
  render_e6.py frozen <n_cameras> <clip_s> <fmt> <frozen.json> [-o out.yml]
  render_e6.py store  <n_cameras> <clip_s> <fmt> <frozen.json> [-o out.yml]
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location("mcmt_render", _HERE / "render.py")
R = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(R)

GATEWAY = R.AGGREGATION_NODE

# One named object per producing stage.
OBJ = {
    "decode": "frames",
    "preprocess": "prepped",
    "detect_embed": "dets",
    "track": "tracks",
    "cross_camera_match": "matches",
}


def _obj_key(producer: str, stage: str) -> str:
    return f"{producer}.{OBJ[stage]}"


def _vertex_name(producer: str, stage: str) -> str:
    return f"v-{producer}-{OBJ[stage]}"


def _emit(name, image_tag, command, deps, stage, constraints_nodes, env,
          camera=None, outputs=None, inputs=None):
    """R._emit_task plus the outputs/inputs stanzas it predates."""
    block = R._emit_task(
        name=name, image_tag=image_tag, command=command, deps=deps,
        stage=stage, constraints_nodes=constraints_nodes, env=env,
        camera=camera,
    )
    extra = ""
    if outputs:
        extra += "      outputs:\n"
        for oname, osize in outputs:
            extra += f"        - {{ name: {oname}, dataSize: \"{osize}\" }}\n"
    if inputs:
        extra += "      inputs:\n"
        for producer, obj in inputs:
            if obj:
                extra += (f"        - {{ producer: {producer}, "
                          f"object: {obj} }}\n")
            else:
                extra += f"        - {{ producer: {producer} }}\n"
    return block + extra


def _emit_vertex(vname, producer, stage, size):
    """Pod-less data vertex pinned to the gateway."""
    return (
        f"    - name: {vname}\n"
        f"      type: data\n"
        f"      image: {R.REGISTRY}/wl-vemcmt-report:latest\n"
        f"      command: [\"true\"]\n"
        f"      dependencies: [\"{producer}\"]\n"
        f"      dataSize: \"{size}\"\n"
        f"      runtime: 0\n"
        f"      constraints:\n"
        f"        nodeNames: [\"{GATEWAY}\"]\n"
        f"      inputs:\n"
        f"        - {{ producer: {producer}, object: {OBJ[stage]} }}\n"
    )


def render(mode, n_cameras, clip_duration, fmt, frozen=None,
           template_name=None):
    if mode != "sched" and not frozen:
        raise ValueError(f"mode {mode} requires a frozen schedule")
    place = frozen["placement"] if frozen else {}
    order = frozen["order"] if frozen else {}

    scheduler = "saga/heft" if mode == "sched" else "random"
    name = template_name or f"e6-mcmt-{mode}"

    head = (
        f"apiVersion: wl.io/v1\n"
        f"kind: ODAGTemplate\n"
        f"metadata:\n"
        f"  name: {name}\n"
        f"  namespace: wl-system\n"
        f"spec:\n"
        f"  description: >\n"
        f"    E6 MCMT ({mode}): {n_cameras} cameras x {clip_duration}s,\n"
        f"    named output objects; "
        f"{'store-lowered through ' + GATEWAY if mode == 'store' else 'direct realization'}.\n"
        f"  scheduler: {scheduler}\n"
        f"  profiling:\n"
        f"    enabled: false\n"
        f"    runtimeSource: manual\n"
        f"    bandwidthSource: external\n"
        f"  defaults:\n"
        f"    runtime: 5\n"
        f"    dataSize: \"10MB\"\n"
        f"  retention:\n"
        f"    maxRuns: 25\n"
        f"    data:\n"
        f"      policy: keepLatest\n"
        f"      keepRuns: 2\n"
        f"      maxSizePerNode: \"10Gi\"\n"
    )
    if mode in ("frozen", "store") and order:
        head += "  schedulerConfig:\n    enactOrder: serial\n    nodeOrder:\n"
        for node, seq in sorted(order.items()):
            if seq:
                head += f"      {node}: [{', '.join(seq)}]\n"
    head += "  tasks:\n"

    tasks, vertices = [], []
    store = (mode == "store")

    def pin(task, default):
        return [place[task]] if place.get(task) else default

    def wire(consumer, producer, stage):
        """Return (deps, inputs, env_keys, env_peers) for one edge."""
        key = _obj_key(producer, stage)
        if store:
            v = _vertex_name(producer, stage)
            return [v], [(v, None)], key, v
        return [producer], [(producer, OBJ[stage])], key, None

    for i in range(1, n_cameras + 1):
        sensor = R._sensor_node_for(i)
        clip = f"/dataset/cam-{i}/clip_{clip_duration}s.mp4"
        dec, pre = f"decode-{i}", f"preprocess-{i}"
        det, trk = f"detect-embed-{i}", f"track-{i}"

        tasks.append(_emit(
            name=dec, image_tag="vemcmt-decode",
            command=["python3", "wl_decode_task.py"], deps=[], stage="decode",
            constraints_nodes=[sensor], camera=i,
            env={"VEMCMT_CAMERA": f"cam-{i}", "VEMCMT_CLIP_PATH": clip,
                 "VEMCMT_FPS": "5", "WL_OUT_NAME": OBJ["decode"]},
            outputs=[(OBJ["decode"], R.DATASIZE_HINT["decode"])]))

        d, ins, k, pr = wire(pre, dec, "decode")
        env = {"VEMCMT_TARGET_SIZE": "640", "VEMCMT_FMT": fmt,
               "WL_OUT_NAME": OBJ["preprocess"], "WL_INPUT_KEYS": k}
        if pr:
            env["WL_INPUT_PEERS"] = pr
        tasks.append(_emit(
            name=pre, image_tag="vemcmt-preprocess",
            command=["python", "wl_preprocess_task.py"], deps=d,
            stage="preprocess", constraints_nodes=[sensor], env=env,
            outputs=[(OBJ["preprocess"], R.DATASIZE_HINT["preprocess"])],
            inputs=ins))
        if store:
            vertices.append(_emit_vertex(_vertex_name(dec, "decode"), dec,
                                         "decode", R.DATASIZE_HINT["decode"]))

        d, ins, k, pr = wire(det, pre, "preprocess")
        env = {"VEMCMT_DEVICE": "GPU",
               "VEMCMT_DET_MODEL": "/models/yolov8n.xml",
               "VEMCMT_REID_MODEL": "/models/osnet_x0_25.xml",
               "WL_OUT_NAME": OBJ["detect_embed"], "WL_INPUT_KEYS": k}
        if pr:
            env["WL_INPUT_PEERS"] = pr
        tasks.append(_emit(
            name=det, image_tag="vemcmt-detect-embed",
            command=["python3", "wl_detect_embed_task.py"], deps=d,
            stage="detect_embed",
            constraints_nodes=pin(det, R.COMPUTE_NODES), env=env,
            outputs=[(OBJ["detect_embed"], R.DATASIZE_HINT["detect_embed"])],
            inputs=ins))
        if store:
            vertices.append(_emit_vertex(_vertex_name(pre, "preprocess"), pre,
                                         "preprocess",
                                         R.DATASIZE_HINT["preprocess"]))

        d, ins, k, pr = wire(trk, det, "detect_embed")
        env = {"WL_OUT_NAME": OBJ["track"], "WL_INPUT_KEYS": k}
        if pr:
            env["WL_INPUT_PEERS"] = pr
        tasks.append(_emit(
            name=trk, image_tag="vemcmt-track",
            command=["python", "wl_track_task.py"], deps=d, stage="track",
            constraints_nodes=pin(trk, R.COMPUTE_NODES), env=env,
            outputs=[(OBJ["track"], R.DATASIZE_HINT["track"])], inputs=ins))
        if store:
            vertices.append(_emit_vertex(_vertex_name(det, "detect_embed"),
                                         det, "detect_embed",
                                         R.DATASIZE_HINT["detect_embed"]))

    # Fan-in.
    deps, ins, keys, peers = [], [], [], []
    for i in range(1, n_cameras + 1):
        trk = f"track-{i}"
        d, ii, k, pr = wire("cross-camera-match", trk, "track")
        deps += d
        ins += ii
        keys.append(k)
        if pr:
            peers.append(pr)
        if store:
            vertices.append(_emit_vertex(_vertex_name(trk, "track"), trk,
                                         "track", R.DATASIZE_HINT["track"]))
    env = {"VEMCMT_SIM_THRESH": "0.55", "WL_OUT_NAME": OBJ["cross_camera_match"],
           "WL_INPUT_KEYS": ",".join(keys)}
    if peers:
        env["WL_INPUT_PEERS"] = ",".join(peers)
    tasks.append(_emit(
        name="cross-camera-match", image_tag="vemcmt-cross-camera-match",
        command=["python", "wl_cross_camera_match_task.py"], deps=deps,
        stage="cross_camera_match", constraints_nodes=[GATEWAY], env=env,
        outputs=[(OBJ["cross_camera_match"],
                  R.DATASIZE_HINT["cross_camera_match"])], inputs=ins))

    d, ins, k, pr = wire("report", "cross-camera-match", "cross_camera_match")
    env = {"VEMCMT_REPORT_ROOT": "/reports", "WL_INPUT_KEYS": k}
    if pr:
        env["WL_INPUT_PEERS"] = pr
    tasks.append(_emit(
        name="report", image_tag="vemcmt-report",
        command=["python", "wl_report_task.py"], deps=d, stage="report",
        constraints_nodes=[GATEWAY], env=env, inputs=ins))
    if store:
        vertices.append(_emit_vertex(
            _vertex_name("cross-camera-match", "cross_camera_match"),
            "cross-camera-match", "cross_camera_match",
            R.DATASIZE_HINT["cross_camera_match"]))

    return head + "\n".join(tasks) + ("\n" + "".join(vertices) if vertices else "")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["sched", "frozen", "store"])
    ap.add_argument("n_cameras", type=int)
    ap.add_argument("clip_duration", type=int)
    ap.add_argument("fmt", choices=["png", "jpg"])
    ap.add_argument("frozen", nargs="?")
    ap.add_argument("-o", "--out")
    ap.add_argument("--name")
    a = ap.parse_args()
    fr = json.load(open(a.frozen)) if a.frozen else None
    y = render(a.mode, a.n_cameras, a.clip_duration, a.fmt, fr, a.name)
    if a.out:
        Path(a.out).write_text(y)
    else:
        sys.stdout.write(y)


if __name__ == "__main__":
    main()
