#!/usr/bin/env python3
"""
IoBT fuse-tracks — Argo port. Fan-in: reads all /in/*/output JSON
(detections from each infer-i), groups by class, averages confidences,
keeps top-5 per class. Writes /out/output as JSON.
"""
import glob
import json
import os
import time

TASK_NAME = os.environ["E1_TASK_NAME"]
NODE_NAME = os.environ.get("NODE_NAME", "?")
print(f"[{TASK_NAME}] node={NODE_NAME}", flush=True)

t0 = time.perf_counter()
inputs = {}
for path in sorted(glob.glob("/in/*/output")):
    dep = path.split("/")[2]   # /in/<dep>/output
    with open(path) as f:
        inputs[dep] = json.load(f)
print(f"[{TASK_NAME}] read {len(inputs)} inputs in {time.perf_counter()-t0:.3f}s", flush=True)

# Argo does not propagate the upstream pod's nodeName cheaply; record
# whatever was logged at the source pod's JSON output.
provenance = {dep: data.get("node", "?") for dep, data in inputs.items()}

all_detections = []
per_source_counts = {}
class_groups = {}
for dep, data in inputs.items():
    dets = data.get("detections", [])
    per_source_counts[dep] = len(dets)
    for det in dets:
        all_detections.append(det)
        class_groups.setdefault(det["class"], []).append(det)

TOP_K = 5
fused_tracks = []
for cls, dets in sorted(class_groups.items()):
    dets_sorted = sorted(dets, key=lambda d: d["confidence"], reverse=True)
    top = dets_sorted[:TOP_K]
    avg_conf = sum(d["confidence"] for d in top) / len(top)
    fused_tracks.append({
        "class": cls,
        "count": len(dets),
        "avg_confidence": round(avg_conf, 3),
        "top_detections": top,
        "sensors": list({d["sensor"] for d in dets}),
    })

fused = {
    "source": TASK_NAME,
    "node": NODE_NAME,
    "total_detections": len(all_detections),
    "per_source_counts": per_source_counts,
    "num_classes": len(fused_tracks),
    "fused_tracks": fused_tracks,
    "provenance": provenance,
}

os.makedirs("/out", exist_ok=True)
with open("/out/output", "w") as f:
    json.dump(fused, f)
print(f"[{TASK_NAME}] wrote /out/output ({len(fused_tracks)} classes)", flush=True)
print(f"[{TASK_NAME}] done", flush=True)
