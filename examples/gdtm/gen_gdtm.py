#!/usr/bin/env python3
"""ODAGTemplate for GDTM's early-fusion tracker over one window of frames.

  source-nK --> prep-<mod>-nK --> embed-<mod>-nK --\\
  (K = 1..3 sensor nodes; mod = zed, depth, radar, audio)  fuse --> track

mode mixed (the pipeline's real shape):
  source  container, image wl-gdtm: on a sensor node this is the capture
          driver (ZED SDK, librealsense, TI mmWave, ReSpeaker); here it
          replays the node's recording
  prep    function on the "gdtm" runner (numpy/OpenCV/torch glue)
  embed   container, image wl-gdtm-model (the repo's code and checkpoint on
  fuse    its pinned mmcv-full 1.x / torch 1.12 stack)
  track   function on the "gdtm" runner (the repo's Kalman filter)
mode pods: every stage a pod.  mode warm: every stage a function; embed and
fuse on the "gdtm-model" runner (the model image, weights preloaded).

Usage: gen_gdtm.py <name> [--mode mixed] [--frames 30] [--start 0]
                         [--scheduler heft] [--sensors anrg-4,anrg-5,anrg-9]
"""
import argparse

REGISTRY = "192.168.1.163:5000"
IMAGES = {"glue": f"{REGISTRY}/wl-gdtm:latest", "model": f"{REGISTRY}/wl-gdtm-model:latest"}
RUNNERS = {"glue": "gdtm", "model": "gdtm-model"}
# Covariance calibration (a, b) for the single detector, chosen by the
# repo's calibrate_outputs from the checkpoint's validation grid search
# (val/res.json); reference.py reports the same pair.
CALIB = (5.0, 0.0)
MODS = ["zed", "depth", "radar", "audio"]
NODES = ["n1", "n2", "n3"]
NPY = 128
# Per-frame bytes: source (recorded), prep (float32 CHW), embed (L x 512 float32).
SRC = {"zed": 54300, "depth": 14800, "radar": 256 * 16 * 4, "audio": 1056 * 6 * 4}
PREP = {"zed": 3 * 270 * 480 * 4, "depth": 3 * 270 * 360 * 4, "radar": 16 * 256 * 4, "audio": 4 * 32 * 32 * 4}
EMB = {"zed": 135 * 512 * 4, "depth": 108 * 512 * 4, "radar": 16 * 512 * 4, "audio": 16 * 512 * 4}
# Seconds per frame on an i3-N305 at default clocks with 2 threads
# (reference.py split_stage_s, 30 frames), and CPU requests.
EMBED_S = {"zed": 0.32, "depth": 0.25, "radar": 0.003, "audio": 0.018}
PREP_S = {"zed": 0.006, "depth": 0.004, "radar": 0.0002, "audio": 0.001}
EMBED_CPU = {"zed": "2", "depth": "2", "radar": "1", "audio": "1"}


def stages(frames):
    """(task, role, mod, node, image kind, inputs [(producer, object)],
    outputs {name: bytes}, runtime s, cpu)"""
    out = []
    for n in NODES:
        out.append((f"source-{n}", "source", "", n, "glue", [],
                    {m: SRC[m] * frames + NPY + (8 * frames + 256 if m in ("zed", "depth") else 0) for m in MODS},
                    0.3, "1"))
    for n in NODES:
        for m in MODS:
            out.append((f"prep-{m}-{n}", "prep", m, n, "glue", [(f"source-{n}", m)],
                        {"x": PREP[m] * frames + NPY}, PREP_S[m] * frames + 0.2, "1"))
            out.append((f"embed-{m}-{n}", "embed", m, n, "model", [(f"prep-{m}-{n}", "x")],
                        {"emb": EMB[m] * frames + NPY}, EMBED_S[m] * frames + 0.2, EMBED_CPU[m]))
    out.append(("fuse", "fuse", "", "", "model", [(f"embed-{m}-{n}", "emb") for n in NODES for m in MODS],
                {"det": 24 * frames + 600}, 0.07 * frames + 0.2, "1"))
    out.append(("track", "track", "", "", "glue", [("fuse", "det")], {"track": 32 * frames + 900}, 0.3, "1"))
    return out


def function(mode, role):
    return {"mixed": role in ("prep", "track"), "pods": False, "warm": True}[mode]


def template(name, mode="mixed", frames=30, start=0, scheduler="heft", sensors=("anrg-4", "anrg-5", "anrg-9")):
    pins = {f"source-{n}": s for n, s in zip(NODES, sensors)}
    out = [f"""apiVersion: wl.io/v1
kind: ODAGTemplate
metadata:
  name: {name}
  namespace: wl-system
spec:
  description: 'GDTM early-fusion tracker (3 sensor nodes x 4 modalities), {frames} frames, {mode}.'
  scheduler: {scheduler}
  schedulerConfig:
    enactOrder: none
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
    runtime: 1
    dataSize: 1MB
  tasks:"""]
    for task, role, mod, node, kind, inputs, outputs, rt, cpu in stages(frames):
        deps = sorted({p for p, _ in inputs})
        L = [f"  - name: {task}", f"    image: {IMAGES[kind]}", "    command: [python, stage.py]",
             f"    dependencies: [{', '.join(deps)}]"]
        if function(mode, role):
            L.append(f"    runner: {RUNNERS[kind]}")
        if inputs:
            L.append("    inputs:")
            for p, o in inputs:
                L += [f"    - producer: {p}", f"      object: {o}"]
        L.append("    outputs:")
        for o, size in outputs.items():
            L += [f"    - name: {o}", f"      dataSize: \"{size}\""]
        mem = "2Gi" if kind == "model" else "1Gi"
        L += [f"    runtime: {max(1, round(rt))}", "    resources:", f"      cpu: \"{cpu}\"", f"      memory: {mem}"]
        if task in pins:
            L += ["    constraints:", f"      nodeNames: [{pins[task]}]"]
        L += ["    env:", f"    - {{name: GDTM_ROLE, value: {role}}}", f"    - {{name: GDTM_MOD, value: \"{mod}\"}}",
              f"    - {{name: GDTM_NODE, value: \"{node}\"}}", f"    - {{name: GDTM_START, value: \"{start}\"}}",
              f"    - {{name: GDTM_FRAMES, value: \"{frames}\"}}", f"    - {{name: GDTM_THREADS, value: \"{cpu}\"}}"]
        if role == "track":
            L += [f"    - {{name: GDTM_CALIB_A, value: \"{CALIB[0]!r}\"}}", f"    - {{name: GDTM_CALIB_B, value: \"{CALIB[1]!r}\"}}"]
        out.append("\n".join(L))
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("name")
    ap.add_argument("--mode", choices=["mixed", "pods", "warm"], default="mixed")
    ap.add_argument("--frames", type=int, default=30)
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--scheduler", default="heft")
    ap.add_argument("--sensors", default="anrg-4,anrg-5,anrg-9")
    a = ap.parse_args()
    print(template(a.name, a.mode, a.frames, a.start, a.scheduler, tuple(a.sensors.split(","))), end="")
