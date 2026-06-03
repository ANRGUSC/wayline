#!/usr/bin/env python3
"""
Render a Wayline ODAGTemplate with the *static* (deterministic) placement
that HEFT picked most often during the 20-rep matrix run.

This is the block-4 ablation template: removes scheduling variance so a
direct comparison against Wayline-HEFT and against Argo isolates the
contribution of the data plane from the contribution of the scheduler.

Hardcoded placement comes from analysis of the 20-rep D=120 PNG matrix
results (modal node per task). Each task is pinned via constraints to a
single node, so the scheduler has no choice. Same `scheduler: heft` keyword
is used because that's the only path that emits predicted schedules — but
with single-element constraint lists the choice is forced.

  scripts/render-static.py [--cameras N] [--duration D] [--preprocess-fmt png|jpg]
                           [--name <template-name>] [-o <output-path>]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Local copy of wayline.render's emit helpers so this script stands alone.
# We just override the constraint list per task.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from render import (  # noqa: E402
    REGISTRY, SENSOR_NODES, AGGREGATION_NODE, RENDER_GID,
    RUNTIME_HINT, DATASIZE_HINT, RESOURCES,
    DATASET_ROOT, REPORT_ROOT, _yaml_list, _emit_task, _sensor_node_for,
)

# Modal HEFT placement at N=4, D=120, PNG, observed across 20 reps.
# Sensor-tier tasks (decode, preprocess) are forced by sensor constraints
# in the base render — only compute-tier and aggregation-tier choices vary.
STATIC_COMPUTE = {
    # task_name -> single node (RR-matched to Argo render.py _compute_node_for)
    "detect-embed-1": "anrg-6",
    "detect-embed-2": "anrg-7",
    "detect-embed-3": "anrg-8",
    "detect-embed-4": "anrg-6",
    "track-1": "anrg-6",
    "track-2": "anrg-7",
    "track-3": "anrg-8",
    "track-4": "anrg-6",
}


def render(n_cameras: int, clip_duration: int, template_name: str,
           preprocess_fmt: str = "png") -> str:
    if n_cameras != 4:
        raise ValueError("static placement only defined for N=4 (run the matrix at other N first)")

    head = (
        f"apiVersion: wl.io/v1\n"
        f"kind: ODAGTemplate\n"
        f"metadata:\n"
        f"  name: {template_name}\n"
        f"  namespace: wl-system\n"
        f"spec:\n"
        f"  description: >\n"
        f"    VideoEdge MCMT static-placement ablation — {n_cameras} cameras x\n"
        f"    {clip_duration}s clips, {preprocess_fmt}. Every task pinned to a\n"
        f"    single node via constraints; scheduler choice is removed so the\n"
        f"    delta from Argo isolates the data plane.\n"
        f"  scheduler: heft\n"
        f"  profiling:\n"
        f"    enabled: false\n"
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
            constraints_nodes=[sensor],
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
            constraints_nodes=[STATIC_COMPUTE[f"detect-embed-{i}"]],
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
            constraints_nodes=[STATIC_COMPUTE[f"track-{i}"]],
            env={},
        ))

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
    ap.add_argument("--duration", type=int, default=120, choices=[30, 60, 120])
    ap.add_argument("--preprocess-fmt", default="png", choices=["png", "jpg"])
    ap.add_argument("--name", default=None)
    ap.add_argument("-o", "--output", default="-")
    args = ap.parse_args()

    name = args.name or f"vemcmt-n{args.cameras}-d{args.duration}-{args.preprocess_fmt}-static"
    yaml = render(args.cameras, args.duration, name,
                  preprocess_fmt=args.preprocess_fmt)
    if args.output == "-":
        sys.stdout.write(yaml)
    else:
        Path(args.output).write_text(yaml)
        print(f"wrote {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
