#!/usr/bin/env python3
"""ODAGTemplate for the FOCAL (MOD) pipeline.

  source-seismic -> fft-seismic -> encoder-seismic --\
                                                       fuse-head
  source-audio   -> fft-audio   -> encoder-audio   --/

mode mixed: sources, FFTs and fuse-head are functions on the "focal" warm
runner; the encoders (the heavy PyTorch stages) are containers (pods).
mode pods: every stage a pod.  mode warm: every stage a function.

Usage: gen_focal.py <name> [--mode mixed] [--batch 64] [--seed 0]
                          [--scheduler heft] [--seismic-node anrg-4] [--audio-node anrg-9]
"""
import argparse

IMAGE = "192.168.1.163:5000/wl-focal:latest"
SEISMIC_S, AUDIO_S, FEAT = 20, 1600, 256            # MOD samples per segment, feature dim
NPY = 128                                             # np.save header


def sizes(batch):
    return {"source-seismic": batch * 10 * SEISMIC_S * 4 + NPY, "source-audio": batch * 10 * AUDIO_S * 4 + NPY,
            "fft-seismic": batch * 20 * SEISMIC_S * 4 + NPY, "fft-audio": batch * 20 * AUDIO_S * 4 + NPY,
            "encoder-seismic": batch * FEAT * 4 + NPY, "encoder-audio": batch * FEAT * 4 + NPY,
            "fuse-head": batch * 7 * 4 + NPY}


def template(name, mode="mixed", batch=64, seed=0, scheduler="heft", seismic_node="anrg-4", audio_node="anrg-9"):
    size = sizes(batch)
    # (task, role, mod, inputs [(producer, object)], output, runtime s, cpu)
    stages = [
        ("source-seismic", "source", "seismic", [], "samples", 0.3, "1"),
        ("source-audio", "source", "audio", [], "samples", 0.5, "1"),
        ("fft-seismic", "fft", "seismic", [("source-seismic", "samples")], "spectrum", 0.3, "1"),
        ("fft-audio", "fft", "audio", [("source-audio", "samples")], "spectrum", 0.5, "1"),
        ("encoder-seismic", "encode", "seismic", [("fft-seismic", "spectrum")], "features", 1.5, "2"),
        ("encoder-audio", "encode", "audio", [("fft-audio", "spectrum")], "features", 2.5, "2"),
        ("fuse-head", "fuse_head", "", [("encoder-seismic", "features"), ("encoder-audio", "features")], "logits", 0.3, "1"),
    ]
    warm = {"mixed": lambda r: r != "encode", "pods": lambda r: False, "warm": lambda r: True}[mode]
    pins = {"source-seismic": seismic_node, "source-audio": audio_node}
    out = [f"""apiVersion: wl.io/v1
kind: ODAGTemplate
metadata:
  name: {name}
  namespace: wl-system
spec:
  description: 'FOCAL (SW_Transformer) on MOD: seismic + audio vehicle classification, {mode}.'
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
    for task, role, mod, inputs, output, rt, cpu in stages:
        deps = sorted({p for p, _ in inputs})
        L = [f"  - name: {task}", f"    image: {IMAGE}", "    command: [python, stage.py]",
             f"    dependencies: [{', '.join(deps)}]"]
        if warm(role):
            L.append("    runner: focal")
        if inputs:
            L.append("    inputs:")
            for p, o in inputs:
                L += [f"    - producer: {p}", f"      object: {o}"]
        L += ["    outputs:", f"    - name: {output}", f"      dataSize: \"{size[task]}\"",
              f"    runtime: {max(1, round(rt))}", "    resources:", f"      cpu: \"{cpu}\"", "      memory: 1Gi"]
        if task in pins:
            L += ["    constraints:", f"      nodeNames: [{pins[task]}]"]
        L += ["    env:", f"    - {{name: FOCAL_ROLE, value: {role}}}", f"    - {{name: FOCAL_MOD, value: \"{mod}\"}}",
              f"    - {{name: FOCAL_BATCH, value: \"{batch}\"}}", f"    - {{name: FOCAL_SEED, value: \"{seed}\"}}",
              f"    - {{name: FOCAL_THREADS, value: \"{cpu}\"}}"]
        out.append("\n".join(L))
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("name")
    ap.add_argument("--mode", choices=["mixed", "pods", "warm"], default="mixed")
    ap.add_argument("--batch", type=int, default=64)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--scheduler", default="heft")
    ap.add_argument("--seismic-node", default="anrg-4")
    ap.add_argument("--audio-node", default="anrg-9")
    a = ap.parse_args()
    print(template(a.name, a.mode, a.batch, a.seed, a.scheduler, a.seismic_node, a.audio_node), end="")
