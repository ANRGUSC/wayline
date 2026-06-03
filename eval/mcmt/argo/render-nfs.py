#!/usr/bin/env python3
"""
Render an Argo WorkflowTemplate for mcmt that passes intermediate
data through a *shared RWX filesystem* (NFS) instead of the MinIO/S3 artifact
store. Same tier pinning, fan-out/fan-in, CPU limits, and compute placement as
argo/render.py -- the ONLY difference is the data plane: every task mounts the
RWX PVC `mcmt-nfs-pvc` at /shared and reads/writes
  /shared/<workflow.name>/<task-instance>/output
via the tasks' existing VEMCMT_IN / VEMCMT_OUT / VEMCMT_IN_ROOT env contract.
Dependencies still enforce ordering; there is no S3 artifact staging.

This answers the reviewer question "is Wayline faster than a shared filesystem,
or just faster than S3?" -- a shared FS still routes every edge through the
central server (anrg-9), so under the tc matrix it pays the same two-hop
cross-tier cost MinIO does, which Wayline's direct push avoids.
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from render import (REGISTRY, COMPUTE_NODES, AGGREGATION_NODE, DATASET_ROOT,  # noqa: E402
                    REPORT_ROOT, RENDER_GID, _sensor_node_for, _compute_node_for)

WF = "{{workflow.name}}"
SHARED = "/shared"
PVC = "mcmt-nfs-pvc"


def _out(inst):  # this task instance's output file path on the shared FS (YAML-quoted)
    return f'"{SHARED}/{WF}/{inst}/output"'


def render(n_cameras: int, clip_duration: int, name: str, preprocess_fmt: str = "png") -> str:
    head = (
        f"apiVersion: argoproj.io/v1alpha1\nkind: WorkflowTemplate\n"
        f"metadata:\n  name: {name}\n  namespace: argo\n"
        f"spec:\n  entrypoint: dag\n  serviceAccountName: argo\n"
        f"  volumes:\n    - {{ name: shared, persistentVolumeClaim: {{ claimName: {PVC} }} }}\n"
        f"  templates:\n\n    - name: dag\n      dag:\n        tasks:\n"
    )
    dag = []
    for i in range(1, n_cameras + 1):
        clip = f"/dataset/cam-{i}/clip_{clip_duration}s.mp4"
        dag.append(
            f"          - name: decode-{i}\n            template: decode\n            arguments:\n"
            f"              parameters:\n"
            f"                - {{ name: camera, value: cam-{i} }}\n"
            f"                - {{ name: clip,   value: \"{clip}\" }}\n"
            f"                - {{ name: node,   value: {_sensor_node_for(i)} }}\n"
            f"                - {{ name: outname, value: decode-{i} }}\n")
        dag.append(
            f"          - name: preprocess-{i}\n            template: preprocess\n"
            f"            dependencies: [decode-{i}]\n            arguments:\n              parameters:\n"
            f"                - {{ name: node,    value: {_sensor_node_for(i)} }}\n"
            f"                - {{ name: inname,  value: decode-{i} }}\n"
            f"                - {{ name: outname, value: preprocess-{i} }}\n")
        dag.append(
            f"          - name: detect-embed-{i}\n            template: detect-embed\n"
            f"            dependencies: [preprocess-{i}]\n            arguments:\n              parameters:\n"
            f"                - {{ name: node,    value: {_compute_node_for(i)} }}\n"
            f"                - {{ name: inname,  value: preprocess-{i} }}\n"
            f"                - {{ name: outname, value: detect-embed-{i} }}\n")
        dag.append(
            f"          - name: track-{i}\n            template: track\n"
            f"            dependencies: [detect-embed-{i}]\n            arguments:\n              parameters:\n"
            f"                - {{ name: node,    value: {_compute_node_for(i)} }}\n"
            f"                - {{ name: inname,  value: detect-embed-{i} }}\n"
            f"                - {{ name: outname, value: track-{i} }}\n")
    track_deps = "[" + ",".join(f"track-{i}" for i in range(1, n_cameras + 1)) + "]"
    dag.append(
        f"          - name: cross-camera-match\n            template: cross-camera-match\n"
        f"            dependencies: {track_deps}\n            arguments:\n              parameters:\n"
        f"                - {{ name: node,    value: {AGGREGATION_NODE} }}\n"
        f"                - {{ name: outname, value: cross-camera-match }}\n")
    dag.append(
        f"          - name: report\n            template: report\n"
        f"            dependencies: [cross-camera-match]\n            arguments:\n              parameters:\n"
        f"                - {{ name: node,   value: {AGGREGATION_NODE} }}\n"
        f"                - {{ name: inname, value: cross-camera-match }}\n")

    # mkdir wrapper so the per-instance output dir exists before the task writes.
    def cmd(entry):
        return f'["sh","-c","mkdir -p \\"$(dirname $VEMCMT_OUT)\\" && exec python3 {entry}"]'
    sm = "          - { name: shared, mountPath: /shared }\n"
    ct = f"""
    - name: decode
      inputs:
        parameters: [{{ name: camera }}, {{ name: clip }}, {{ name: node }}, {{ name: outname }}]
      nodeSelector: {{ kubernetes.io/hostname: "{{{{inputs.parameters.node}}}}" }}
      securityContext: {{ supplementalGroups: [{RENDER_GID}] }}
      volumes:
        - {{ name: dev-dri, hostPath: {{ path: /dev/dri, type: Directory }} }}
        - {{ name: aicity-dataset, hostPath: {{ path: {DATASET_ROOT}, type: Directory }} }}
        - {{ name: shared, persistentVolumeClaim: {{ claimName: {PVC} }} }}
      container:
        image: {REGISTRY}/vemcmt-decode:latest
        imagePullPolicy: Always
        command: {cmd("argo_decode_task.py")}
        env:
          - {{ name: VEMCMT_CAMERA, value: "{{{{inputs.parameters.camera}}}}" }}
          - {{ name: VEMCMT_CLIP_PATH, value: "{{{{inputs.parameters.clip}}}}" }}
          - {{ name: VEMCMT_FPS, value: "5" }}
          - {{ name: VEMCMT_OUT, value: {_out("{{inputs.parameters.outname}}")} }}
        volumeMounts:
          - {{ name: dev-dri, mountPath: /dev/dri }}
          - {{ name: aicity-dataset, mountPath: /dataset, readOnly: true }}
{sm}        resources:
          requests: {{ cpu: "1", memory: "1Gi" }}
          limits:   {{ cpu: "1", memory: "1Gi" }}

    - name: preprocess
      inputs:
        parameters: [{{ name: node }}, {{ name: inname }}, {{ name: outname }}]
      nodeSelector: {{ kubernetes.io/hostname: "{{{{inputs.parameters.node}}}}" }}
      securityContext: {{ supplementalGroups: [{RENDER_GID}] }}
      volumes:
        - {{ name: dev-dri, hostPath: {{ path: /dev/dri, type: Directory }} }}
        - {{ name: shared, persistentVolumeClaim: {{ claimName: {PVC} }} }}
      container:
        image: {REGISTRY}/vemcmt-preprocess:latest
        imagePullPolicy: Always
        command: {cmd("argo_preprocess_task.py")}
        env:
          - {{ name: VEMCMT_IN,  value: {_out("{{inputs.parameters.inname}}")} }}
          - {{ name: VEMCMT_OUT, value: {_out("{{inputs.parameters.outname}}")} }}
          - {{ name: VEMCMT_FMT, value: "{preprocess_fmt}" }}
        volumeMounts:
          - {{ name: dev-dri, mountPath: /dev/dri }}
{sm}        resources:
          requests: {{ cpu: "1", memory: "1Gi" }}
          limits:   {{ cpu: "1", memory: "1Gi" }}

    - name: detect-embed
      inputs:
        parameters: [{{ name: node }}, {{ name: inname }}, {{ name: outname }}]
      nodeSelector: {{ kubernetes.io/hostname: "{{{{inputs.parameters.node}}}}" }}
      securityContext: {{ supplementalGroups: [{RENDER_GID}] }}
      volumes:
        - {{ name: dev-dri, hostPath: {{ path: /dev/dri, type: Directory }} }}
        - {{ name: shared, persistentVolumeClaim: {{ claimName: {PVC} }} }}
      container:
        image: {REGISTRY}/vemcmt-detect-embed:latest
        imagePullPolicy: Always
        command: {cmd("argo_detect_embed_task.py")}
        env:
          - {{ name: VEMCMT_IN,  value: {_out("{{inputs.parameters.inname}}")} }}
          - {{ name: VEMCMT_OUT, value: {_out("{{inputs.parameters.outname}}")} }}
          - {{ name: VEMCMT_DEVICE, value: GPU }}
          - {{ name: VEMCMT_DET_MODEL, value: /models/yolov8n.xml }}
          - {{ name: VEMCMT_REID_MODEL, value: /models/osnet_x0_25.xml }}
        volumeMounts:
          - {{ name: dev-dri, mountPath: /dev/dri }}
{sm}        resources:
          requests: {{ cpu: "2", memory: "2Gi" }}
          limits:   {{ cpu: "2", memory: "2Gi" }}

    - name: track
      inputs:
        parameters: [{{ name: node }}, {{ name: inname }}, {{ name: outname }}]
      nodeSelector: {{ kubernetes.io/hostname: "{{{{inputs.parameters.node}}}}" }}
      volumes:
        - {{ name: shared, persistentVolumeClaim: {{ claimName: {PVC} }} }}
      container:
        image: {REGISTRY}/vemcmt-track:latest
        imagePullPolicy: Always
        command: {cmd("argo_track_task.py")}
        env:
          - {{ name: VEMCMT_IN,  value: {_out("{{inputs.parameters.inname}}")} }}
          - {{ name: VEMCMT_OUT, value: {_out("{{inputs.parameters.outname}}")} }}
        volumeMounts:
{sm}        resources:
          requests: {{ cpu: "500m", memory: "512Mi" }}
          limits:   {{ cpu: "500m", memory: "512Mi" }}

    - name: cross-camera-match
      inputs:
        parameters: [{{ name: node }}, {{ name: outname }}]
      nodeSelector: {{ kubernetes.io/hostname: "{{{{inputs.parameters.node}}}}" }}
      volumes:
        - {{ name: shared, persistentVolumeClaim: {{ claimName: {PVC} }} }}
      container:
        image: {REGISTRY}/vemcmt-cross-camera-match:latest
        imagePullPolicy: Always
        command: {cmd("argo_cross_camera_match_task.py")}
        env:
          - {{ name: VEMCMT_IN_ROOT, value: "{SHARED}/{WF}" }}
          - {{ name: VEMCMT_OUT, value: {_out("{{inputs.parameters.outname}}")} }}
          - {{ name: VEMCMT_SIM_THRESH, value: "0.55" }}
        volumeMounts:
{sm}        resources:
          requests: {{ cpu: "500m", memory: "512Mi" }}
          limits:   {{ cpu: "500m", memory: "512Mi" }}

    - name: report
      inputs:
        parameters: [{{ name: node }}, {{ name: inname }}]
      nodeSelector: {{ kubernetes.io/hostname: "{{{{inputs.parameters.node}}}}" }}
      volumes:
        - {{ name: reports, hostPath: {{ path: {REPORT_ROOT}, type: DirectoryOrCreate }} }}
        - {{ name: shared, persistentVolumeClaim: {{ claimName: {PVC} }} }}
      container:
        image: {REGISTRY}/vemcmt-report:latest
        imagePullPolicy: Always
        command: ["python", "argo_report_task.py"]
        env:
          - {{ name: VEMCMT_IN, value: {_out("{{inputs.parameters.inname}}")} }}
          - {{ name: VEMCMT_REPORT_ROOT, value: /reports }}
        volumeMounts:
          - {{ name: reports, mountPath: /reports }}
{sm}        resources:
          requests: {{ cpu: "100m", memory: "128Mi" }}
          limits:   {{ cpu: "100m", memory: "128Mi" }}
"""
    return head + "".join(dag) + ct


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cameras", type=int, default=4)
    ap.add_argument("--duration", type=int, default=120, choices=[30, 60, 120])
    ap.add_argument("--preprocess-fmt", default="png", choices=["png", "jpg"])
    ap.add_argument("--name", default=None)
    ap.add_argument("-o", "--output", default="-")
    a = ap.parse_args()
    name = a.name or f"vemcmt-n{a.cameras}-d{a.duration}-{a.preprocess_fmt}-argo-nfs"
    y = render(a.cameras, a.duration, name, preprocess_fmt=a.preprocess_fmt)
    if a.output == "-":
        sys.stdout.write(y)
    else:
        Path(a.output).write_text(y); print(f"wrote {a.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
