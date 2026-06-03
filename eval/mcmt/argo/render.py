#!/usr/bin/env python3
"""
Render a concrete Argo WorkflowTemplate YAML for mcmt.

Mirrors wayline/render.py: same tier pinning, same per-camera fan-out, same
fan-in. Artifact passing goes through the bound artifact repository
(the e0-bench MinIO on anrg-9 by default).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REGISTRY = "192.168.1.163:5000"

SENSOR_NODES = ["anrg-1", "anrg-3", "anrg-4", "anrg-5"]
COMPUTE_NODES = ["anrg-6", "anrg-7", "anrg-8"]
AGGREGATION_NODE = "anrg-9"

DATASET_ROOT = "/var/lib/wl-workloads/aicity"
REPORT_ROOT = "/var/lib/wl-workloads/reports"
RENDER_GID = 992


def _sensor_node_for(cam_idx_1based: int) -> str:
    return SENSOR_NODES[(cam_idx_1based - 1) % len(SENSOR_NODES)]


def _compute_node_for(cam_idx_1based: int) -> str:
    """Argo doesn't support multi-host nodeSelectors; pick deterministically.
    For N>3 cameras some compute nodes host more than one task — same as
    the synthetic IoBT port."""
    return COMPUTE_NODES[(cam_idx_1based - 1) % len(COMPUTE_NODES)]


def render(n_cameras: int, clip_duration: int, name: str, artifact_repo: str,
           preprocess_fmt: str = "png") -> str:
    head = (
        f"apiVersion: argoproj.io/v1alpha1\n"
        f"kind: WorkflowTemplate\n"
        f"metadata:\n"
        f"  name: {name}\n"
        f"  namespace: argo\n"
        f"spec:\n"
        f"  entrypoint: dag\n"
        f"  serviceAccountName: argo\n"
        f"  artifactRepositoryRef:\n"
        f"    configMap: artifact-repositories\n"
        f"    key: {artifact_repo}\n"
        f"  templates:\n"
        f"\n"
        f"    - name: dag\n"
        f"      dag:\n"
        f"        tasks:\n"
    )

    dag_lines = []

    # Per-camera fan-out.
    for i in range(1, n_cameras + 1):
        clip = f"/dataset/cam-{i}/clip_{clip_duration}s.mp4"
        # decode (no inputs)
        dag_lines.append(
            f"          - name: decode-{i}\n"
            f"            template: decode\n"
            f"            arguments:\n"
            f"              parameters:\n"
            f"                - {{ name: camera, value: cam-{i} }}\n"
            f"                - {{ name: clip,   value: \"{clip}\" }}\n"
            f"                - {{ name: node,   value: {_sensor_node_for(i)} }}\n"
        )
        # preprocess
        dag_lines.append(
            f"          - name: preprocess-{i}\n"
            f"            template: preprocess\n"
            f"            dependencies: [decode-{i}]\n"
            f"            arguments:\n"
            f"              parameters:\n"
            f"                - {{ name: node, value: {_sensor_node_for(i)} }}\n"
            f"              artifacts:\n"
            f"                - {{ name: input, from: \"{{{{tasks.decode-{i}.outputs.artifacts.output}}}}\" }}\n"
        )
        # detect_embed
        dag_lines.append(
            f"          - name: detect-embed-{i}\n"
            f"            template: detect-embed\n"
            f"            dependencies: [preprocess-{i}]\n"
            f"            arguments:\n"
            f"              parameters:\n"
            f"                - {{ name: node, value: {_compute_node_for(i)} }}\n"
            f"              artifacts:\n"
            f"                - {{ name: input, from: \"{{{{tasks.preprocess-{i}.outputs.artifacts.output}}}}\" }}\n"
        )
        # track
        dag_lines.append(
            f"          - name: track-{i}\n"
            f"            template: track\n"
            f"            dependencies: [detect-embed-{i}]\n"
            f"            arguments:\n"
            f"              parameters:\n"
            f"                - {{ name: node, value: {_compute_node_for(i)} }}\n"
            f"              artifacts:\n"
            f"                - {{ name: input, from: \"{{{{tasks.detect-embed-{i}.outputs.artifacts.output}}}}\" }}\n"
        )

    # Fan-in: cross_camera_match. Argo declares one artifact per upstream;
    # the wrapper looks for /in/track-N/output for each.
    track_deps = "[" + ",".join(f"track-{i}" for i in range(1, n_cameras + 1)) + "]"
    artifact_lines = "\n".join(
        f"                - {{ name: track-{i}, from: \"{{{{tasks.track-{i}.outputs.artifacts.output}}}}\", path: /in/track-{i}/output }}"
        for i in range(1, n_cameras + 1)
    )
    dag_lines.append(
        f"          - name: cross-camera-match\n"
        f"            template: cross-camera-match\n"
        f"            dependencies: {track_deps}\n"
        f"            arguments:\n"
        f"              parameters:\n"
        f"                - {{ name: node, value: {AGGREGATION_NODE} }}\n"
        f"              artifacts:\n"
        f"{artifact_lines}\n"
    )

    # Report
    dag_lines.append(
        f"          - name: report\n"
        f"            template: report\n"
        f"            dependencies: [cross-camera-match]\n"
        f"            arguments:\n"
        f"              parameters:\n"
        f"                - {{ name: node, value: {AGGREGATION_NODE} }}\n"
        f"              artifacts:\n"
        f"                - {{ name: input, from: \"{{{{tasks.cross-camera-match.outputs.artifacts.output}}}}\" }}\n"
    )

    # Per-stage container templates.
    # All stages mount /dev/dri via hostPath. decode also mounts the
    # aicity dataset; report mounts the report hostPath.
    container_templates = f"""
    - name: decode
      inputs:
        parameters:
          - {{ name: camera }}
          - {{ name: clip }}
          - {{ name: node }}
      nodeSelector:
        kubernetes.io/hostname: "{{{{inputs.parameters.node}}}}"
      securityContext:
        supplementalGroups: [{RENDER_GID}]
      volumes:
        - {{ name: dev-dri, hostPath: {{ path: /dev/dri, type: Directory }} }}
        - {{ name: aicity-dataset, hostPath: {{ path: {DATASET_ROOT}, type: Directory }} }}
      container:
        image: {REGISTRY}/wl-vemcmt-decode:latest
        imagePullPolicy: Always
        command: ["python3", "argo_decode_task.py"]
        env:
          - {{ name: VEMCMT_CAMERA, value: "{{{{inputs.parameters.camera}}}}" }}
          - {{ name: VEMCMT_CLIP_PATH, value: "{{{{inputs.parameters.clip}}}}" }}
          - {{ name: VEMCMT_FPS, value: "5" }}
          - {{ name: VEMCMT_OUT, value: /out/output }}
        volumeMounts:
          - {{ name: dev-dri, mountPath: /dev/dri }}
          - {{ name: aicity-dataset, mountPath: /dataset, readOnly: true }}
        resources:
          requests: {{ cpu: "1",    memory: "1Gi" }}
          limits:   {{ cpu: "1",    memory: "1Gi" }}
      outputs:
        artifacts:
          - {{ name: output, path: /out/output }}

    - name: preprocess
      inputs:
        parameters: [{{ name: node }}]
        artifacts: [{{ name: input, path: /in/decode/output }}]
      nodeSelector: {{ kubernetes.io/hostname: "{{{{inputs.parameters.node}}}}" }}
      securityContext: {{ supplementalGroups: [{RENDER_GID}] }}
      volumes: [{{ name: dev-dri, hostPath: {{ path: /dev/dri, type: Directory }} }}]
      container:
        image: {REGISTRY}/wl-vemcmt-preprocess:latest
        imagePullPolicy: Always
        command: ["python", "argo_preprocess_task.py"]
        env:
          - {{ name: VEMCMT_IN, value: /in/decode/output }}
          - {{ name: VEMCMT_OUT, value: /out/output }}
          - {{ name: VEMCMT_FMT, value: "{preprocess_fmt}" }}
        volumeMounts: [{{ name: dev-dri, mountPath: /dev/dri }}]
        resources:
          requests: {{ cpu: "1",    memory: "1Gi" }}
          limits:   {{ cpu: "1",    memory: "1Gi" }}
      outputs:
        artifacts: [{{ name: output, path: /out/output }}]

    - name: detect-embed
      inputs:
        parameters: [{{ name: node }}]
        artifacts: [{{ name: input, path: /in/preprocess/output }}]
      nodeSelector: {{ kubernetes.io/hostname: "{{{{inputs.parameters.node}}}}" }}
      securityContext: {{ supplementalGroups: [{RENDER_GID}] }}
      volumes: [{{ name: dev-dri, hostPath: {{ path: /dev/dri, type: Directory }} }}]
      container:
        image: {REGISTRY}/wl-vemcmt-detect-embed:latest
        imagePullPolicy: Always
        command: ["python3", "argo_detect_embed_task.py"]
        env:
          - {{ name: VEMCMT_IN, value: /in/preprocess/output }}
          - {{ name: VEMCMT_OUT, value: /out/output }}
          - {{ name: VEMCMT_DEVICE, value: GPU }}
          - {{ name: VEMCMT_DET_MODEL, value: /models/yolov8n.xml }}
          - {{ name: VEMCMT_REID_MODEL, value: /models/osnet_x0_25.xml }}
        volumeMounts: [{{ name: dev-dri, mountPath: /dev/dri }}]
        resources:
          requests: {{ cpu: "2",    memory: "2Gi" }}
          limits:   {{ cpu: "2",    memory: "2Gi" }}
      outputs:
        artifacts: [{{ name: output, path: /out/output }}]

    - name: track
      inputs:
        parameters: [{{ name: node }}]
        artifacts: [{{ name: input, path: /in/detect-embed/output }}]
      nodeSelector: {{ kubernetes.io/hostname: "{{{{inputs.parameters.node}}}}" }}
      container:
        image: {REGISTRY}/wl-vemcmt-track:latest
        imagePullPolicy: Always
        command: ["python", "argo_track_task.py"]
        env:
          - {{ name: VEMCMT_IN, value: /in/detect-embed/output }}
          - {{ name: VEMCMT_OUT, value: /out/output }}
        resources:
          requests: {{ cpu: "500m", memory: "512Mi" }}
          limits:   {{ cpu: "500m", memory: "512Mi" }}
      outputs:
        artifacts: [{{ name: output, path: /out/output }}]

    - name: cross-camera-match
      inputs:
        parameters: [{{ name: node }}]
        artifacts:
{chr(10).join(f"          - {{ name: track-{i}, path: /in/track-{i}/output }}" for i in range(1, n_cameras + 1))}
      nodeSelector: {{ kubernetes.io/hostname: "{{{{inputs.parameters.node}}}}" }}
      container:
        image: {REGISTRY}/wl-vemcmt-cross-camera-match:latest
        imagePullPolicy: Always
        command: ["python", "argo_cross_camera_match_task.py"]
        env:
          - {{ name: VEMCMT_IN_ROOT, value: /in }}
          - {{ name: VEMCMT_OUT, value: /out/output }}
          - {{ name: VEMCMT_SIM_THRESH, value: "0.55" }}
        resources:
          requests: {{ cpu: "500m", memory: "512Mi" }}
          limits:   {{ cpu: "500m", memory: "512Mi" }}
      outputs:
        artifacts: [{{ name: output, path: /out/output }}]

    - name: report
      inputs:
        parameters: [{{ name: node }}]
        artifacts: [{{ name: input, path: /in/cross-camera-match/output }}]
      nodeSelector: {{ kubernetes.io/hostname: "{{{{inputs.parameters.node}}}}" }}
      volumes: [{{ name: reports, hostPath: {{ path: {REPORT_ROOT}, type: DirectoryOrCreate }} }}]
      container:
        image: {REGISTRY}/wl-vemcmt-report:latest
        imagePullPolicy: Always
        command: ["python", "argo_report_task.py"]
        env:
          - {{ name: VEMCMT_IN, value: /in/cross-camera-match/output }}
          - {{ name: VEMCMT_REPORT_ROOT, value: /reports }}
        volumeMounts: [{{ name: reports, mountPath: /reports }}]
        resources:
          requests: {{ cpu: "100m", memory: "128Mi" }}
          limits:   {{ cpu: "100m", memory: "128Mi" }}
"""

    return head + "".join(dag_lines) + container_templates


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cameras", type=int, default=4)
    ap.add_argument("--duration", type=int, default=60, choices=[30, 60, 120])
    ap.add_argument("--preprocess-fmt", default="png", choices=["png", "jpg"])
    ap.add_argument("--name", default=None)
    ap.add_argument("--artifact-repo", default="e0bench-minio",
                    help="key in the argo artifact-repositories ConfigMap")
    ap.add_argument("-o", "--output", default="-")
    args = ap.parse_args()

    name = args.name or f"vemcmt-n{args.cameras}-d{args.duration}-{args.preprocess_fmt}-argo"
    yaml = render(args.cameras, args.duration, name, args.artifact_repo,
                  preprocess_fmt=args.preprocess_fmt)
    if args.output == "-":
        sys.stdout.write(yaml)
    else:
        Path(args.output).write_text(yaml)
        print(f"wrote {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
