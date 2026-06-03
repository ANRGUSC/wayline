#!/usr/bin/env python3
"""
Render a concrete ODAGTemplate YAML for the mcmt workload.

The DAG is N copies of (decode → preprocess → detect_embed → track),
fanning into one cross_camera_match → report. The template is
parameterized by N_CAMERAS, CLIP_DURATION, and SCHEDULER.

Tier mapping (mirrors VideoEdge's edge / cluster / aggregation):

  sensor tier  (one camera per node):
      cam-1 → anrg-1     cam-3 → anrg-4
      cam-2 → anrg-3     cam-4 → anrg-5
      cam-5 → anrg-1     cam-7 → anrg-4     (wrap-around for N>4)
      cam-6 → anrg-3     cam-8 → anrg-5

  compute tier (detect_embed, track):
      anrg-6, 7, 8 — scheduler picks per task

  aggregation tier:
      anrg-9 — cross_camera_match, report

Output is written to stdout (or -o <path>).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REGISTRY = "192.168.1.163:5000"

SENSOR_NODES = ["anrg-1", "anrg-3", "anrg-4", "anrg-5"]
COMPUTE_NODES = ["anrg-6", "anrg-7", "anrg-8"]
AGGREGATION_NODE = "anrg-9"

# Where the dataset prep step stages clips on each sensor node.
DATASET_ROOT = "/var/lib/wl-workloads/aicity"
# Where report-tier tasks write report.json (hostPath on anrg-9).
REPORT_ROOT = "/var/lib/wl-workloads/reports"

# Render-group GID on the i3-N305 nodes' /dev/dri/renderD128 (probed at
# fix-#1 time). Membership lets the container open the iGPU device.
RENDER_GID = 992

# Per-stage runtime hints (seconds) for HEFT cold start. The profiler will
# learn the real numbers after 2-3 runs; these are just to avoid pathological
# placements on run 1.
RUNTIME_HINT = {
    "decode": 5,
    "preprocess": 4,
    "detect_embed": 12,
    "track": 2,
    "cross_camera_match": 2,
    "report": 1,
}

# Per-stage dataSize hints (uncompressed). Drives HEFT communication cost.
DATASIZE_HINT = {
    "decode": "200MB",
    "preprocess": "100MB",
    "detect_embed": "10MB",
    "track": "2MB",
    "cross_camera_match": "1MB",
    "report": "100KB",
}

# Resource requests by stage.
RESOURCES = {
    "decode":            {"cpu": "1",    "memory": "1Gi"},
    "preprocess":        {"cpu": "1",    "memory": "1Gi"},
    "detect_embed":      {"cpu": "2",    "memory": "2Gi"},
    "track":             {"cpu": "500m", "memory": "512Mi"},
    "cross_camera_match":{"cpu": "500m", "memory": "512Mi"},
    "report":            {"cpu": "100m", "memory": "128Mi"},
}


def _sensor_node_for(cam_idx_1based: int) -> str:
    """Wrap cameras onto sensor nodes round-robin. Tied to SENSOR_NODES."""
    return SENSOR_NODES[(cam_idx_1based - 1) % len(SENSOR_NODES)]


# --------------------------------------------------------------------------
# YAML emission — plain f-strings, no Jinja dependency.
# --------------------------------------------------------------------------

def _yaml_list(items):
    return "[" + ", ".join(items) + "]"


def _common_volumes_mounts(stage: str, camera: int | None = None) -> tuple[str, str]:
    """
    Build the per-task `volumes:` and `volumeMounts:` YAML fragments.

    All stages get /dev/dri (iGPU access) since detect_embed needs it and
    decode benefits from VAAPI. decode stages additionally mount the
    dataset hostPath read-only. The report stage mounts /reports
    read-write to persist report.json.
    """
    vols = [
        "        - name: dev-dri\n"
        "          hostPath: { path: /dev/dri, type: Directory }",
    ]
    mounts = [
        "        - { name: dev-dri, mountPath: /dev/dri }",
    ]
    if stage == "decode" and camera is not None:
        vols.append(
            "        - name: aicity-dataset\n"
            f"          hostPath: {{ path: {DATASET_ROOT}, type: Directory }}"
        )
        mounts.append(
            "        - { name: aicity-dataset, mountPath: /dataset, readOnly: true }"
        )
    if stage == "report":
        vols.append(
            "        - name: reports\n"
            f"          hostPath: {{ path: {REPORT_ROOT}, type: DirectoryOrCreate }}"
        )
        mounts.append(
            "        - { name: reports, mountPath: /reports }"
        )
    return "\n".join(vols), "\n".join(mounts)


def _emit_task(
    name: str, image_tag: str, command: list[str],
    deps: list[str], stage: str,
    constraints_nodes: list[str],
    env: dict[str, str],
    camera: int | None = None,
) -> str:
    res = RESOURCES[stage]
    rt = RUNTIME_HINT[stage]
    ds = DATASIZE_HINT[stage]
    vols, mounts = _common_volumes_mounts(stage, camera=camera)
    env_lines = "\n".join(
        f"        - {{ name: {k}, value: \"{v}\" }}" for k, v in env.items()
    )
    deps_str = _yaml_list([f'"{d}"' for d in deps]) if deps else "[]"
    constraints_str = _yaml_list([f'"{n}"' for n in constraints_nodes])
    command_str = _yaml_list([f'"{c}"' for c in command])
    return (
        f"    - name: {name}\n"
        f"      image: {REGISTRY}/wl-{image_tag}:latest\n"
        f"      command: {command_str}\n"
        f"      dependencies: {deps_str}\n"
        f"      dataSize: \"{ds}\"\n"
        f"      runtime: {rt}\n"
        f"      resources:\n"
        f"        cpu: \"{res['cpu']}\"\n"
        f"        memory: \"{res['memory']}\"\n"
        f"      constraints:\n"
        f"        nodeNames: {constraints_str}\n"
        f"      env:\n"
        f"{env_lines}\n"
        f"      volumes:\n"
        f"{vols}\n"
        f"      volumeMounts:\n"
        f"{mounts}\n"
        f"      securityContext:\n"
        f"        supplementalGroups: [{RENDER_GID}]\n"
    )


def render(n_cameras: int, clip_duration: int, scheduler: str, template_name: str,
           preprocess_fmt: str = "png") -> str:
    if n_cameras < 1 or n_cameras > 16:
        raise ValueError(f"n_cameras out of supported range: {n_cameras}")
    if clip_duration not in (30, 60, 120):
        raise ValueError(f"clip_duration must be 30/60/120, got {clip_duration}")

    head = (
        f"apiVersion: wl.io/v1\n"
        f"kind: ODAGTemplate\n"
        f"metadata:\n"
        f"  name: {template_name}\n"
        f"  namespace: wl-system\n"
        f"spec:\n"
        f"  description: >\n"
        f"    VideoEdge MCMT — {n_cameras} cameras × {clip_duration}s clips,\n"
        f"    scheduler={scheduler}. Per-camera decode → preprocess →\n"
        f"    detect_embed → track, fan-in to cross_camera_match → report.\n"
        f"  scheduler: {scheduler}\n"
        f"  profiling:\n"
        f"    enabled: true\n"
        f"    warmupRuns: 0\n"
        f"    minSamples: 2\n"
        f"    emaAlpha: 0.7\n"
        f"  defaults:\n"
        f"    runtime: 5\n"
        f"    dataSize: \"10MB\"\n"
        f"  retention:\n"
        f"    maxRuns: 25\n"
        f"    data:\n"
        f"      policy: keepLatest\n"
        f"      keepRuns: 2\n"
        f"      maxSizePerNode: \"10Gi\"\n"
        f"  tasks:\n"
    )

    tasks: list[str] = []

    # Per-camera fan-out.
    for i in range(1, n_cameras + 1):
        sensor = _sensor_node_for(i)
        clip = f"/dataset/cam-{i}/clip_{clip_duration}s.mp4"

        tasks.append(_emit_task(
            name=f"decode-{i}",
            image_tag="vemcmt-decode",
            command=["python3", "wl_decode_task.py"],
            deps=[],
            stage="decode",
            constraints_nodes=[sensor],
            env={
                "VEMCMT_CAMERA": f"cam-{i}",
                "VEMCMT_CLIP_PATH": clip,
                "VEMCMT_FPS": "5",
            },
            camera=i,
        ))
        tasks.append(_emit_task(
            name=f"preprocess-{i}",
            image_tag="vemcmt-preprocess",
            command=["python", "wl_preprocess_task.py"],
            deps=[f"decode-{i}"],
            stage="preprocess",
            constraints_nodes=[sensor],  # co-located with decode
            env={
                "VEMCMT_TARGET_SIZE": "640",
                "VEMCMT_FMT": preprocess_fmt,
            },
        ))
        tasks.append(_emit_task(
            name=f"detect-embed-{i}",
            image_tag="vemcmt-detect-embed",
            command=["python3", "wl_detect_embed_task.py"],
            deps=[f"preprocess-{i}"],
            stage="detect_embed",
            constraints_nodes=COMPUTE_NODES,
            env={
                "VEMCMT_DEVICE": "GPU",
                "VEMCMT_DET_MODEL": "/models/yolov8n.xml",
                "VEMCMT_REID_MODEL": "/models/osnet_x0_25.xml",
            },
        ))
        tasks.append(_emit_task(
            name=f"track-{i}",
            image_tag="vemcmt-track",
            command=["python", "wl_track_task.py"],
            deps=[f"detect-embed-{i}"],
            stage="track",
            constraints_nodes=COMPUTE_NODES,
            env={},
        ))

    # Fan-in.
    track_deps = [f"track-{i}" for i in range(1, n_cameras + 1)]
    tasks.append(_emit_task(
        name="cross-camera-match",
        image_tag="vemcmt-cross-camera-match",
        command=["python", "wl_cross_camera_match_task.py"],
        deps=track_deps,
        stage="cross_camera_match",
        constraints_nodes=[AGGREGATION_NODE],
        env={"VEMCMT_SIM_THRESH": "0.55"},
    ))
    tasks.append(_emit_task(
        name="report",
        image_tag="vemcmt-report",
        command=["python", "wl_report_task.py"],
        deps=["cross-camera-match"],
        stage="report",
        constraints_nodes=[AGGREGATION_NODE],
        env={"VEMCMT_REPORT_ROOT": "/reports"},
    ))

    return head + "\n".join(tasks)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cameras", type=int, default=4)
    ap.add_argument("--duration", type=int, default=60, choices=[30, 60, 120])
    ap.add_argument("--scheduler", default="heft", choices=["heft", "random"])
    ap.add_argument("--preprocess-fmt", default="png", choices=["png", "jpg"],
                    help="intermediate-frame format emitted by preprocess "
                         "(png = lossless, larger payloads; jpg = lossy q=88)")
    ap.add_argument("--name", default=None,
                    help="ODAGTemplate name; default vemcmt-N<n>-D<d>-<fmt>-<scheduler>")
    ap.add_argument("-o", "--output", default="-",
                    help="output file (default stdout)")
    args = ap.parse_args()

    name = args.name or f"vemcmt-n{args.cameras}-d{args.duration}-{args.preprocess_fmt}-{args.scheduler}"
    yaml = render(args.cameras, args.duration, args.scheduler, name,
                  preprocess_fmt=args.preprocess_fmt)
    if args.output == "-":
        sys.stdout.write(yaml)
    else:
        Path(args.output).write_text(yaml)
        print(f"wrote {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
