#!/usr/bin/env python3
"""
IoBT Mission Snapshot — fuse-tracks task.

Fan-in: receives detection JSON from infer-1 .. infer-4, fuses into
a unified track list grouped by class with averaged confidences,
and includes provenance (which node each inference ran on).
"""

import os
import time

from wl_sdk import WlTask

task = WlTask()

print(f"[{task.name}] node={task.node}", flush=True)
print(f"[{task.name}] dependencies={task.dependencies}", flush=True)

# --- receive all infer results ---
t0 = time.perf_counter()
inputs = task.recv_all()  # {"infer-1": {...}, "infer-2": {...}, ...}
elapsed_recv = time.perf_counter() - t0

print(f"[{task.name}] recv_all() completed in {elapsed_recv:.3f}s  ({len(inputs)} inputs)", flush=True)

# --- collect provenance ---
provenance = {}
for dep in task.dependencies:
    dep_node = task.dep_node(dep)
    provenance[dep] = dep_node
    print(f"[{task.name}] dep_node({dep}) -> {dep_node}", flush=True)

# --- fuse detections: group by class, average confidence, keep top-k ---
all_detections = []
per_source_counts = {}

for dep_name, data in inputs.items():
    dets = data.get("detections", [])
    per_source_counts[dep_name] = len(dets)
    for det in dets:
        det["origin_task"] = dep_name
        det["origin_node"] = provenance.get(dep_name, "unknown")
        all_detections.append(det)

print(f"[{task.name}] total detections across all sensors: {len(all_detections)}", flush=True)

# Group by class
class_groups = {}
for det in all_detections:
    cls = det["class"]
    if cls not in class_groups:
        class_groups[cls] = []
    class_groups[cls].append(det)

# Build fused tracks: one per class with top-5 highest-confidence detections
TOP_K = 5
fused_tracks = []
for cls, dets in sorted(class_groups.items()):
    dets_sorted = sorted(dets, key=lambda d: d["confidence"], reverse=True)
    top = dets_sorted[:TOP_K]
    avg_conf = sum(d["confidence"] for d in top) / len(top)
    track = {
        "class": cls,
        "count": len(dets),
        "avg_confidence": round(avg_conf, 3),
        "top_detections": top,
        "sensors": list(set(d["sensor"] for d in dets)),
    }
    fused_tracks.append(track)

fused_result = {
    "source": task.name,
    "node": task.node,
    "total_detections": len(all_detections),
    "per_source_counts": per_source_counts,
    "num_classes": len(fused_tracks),
    "fused_tracks": fused_tracks,
    "provenance": provenance,
}

for track in fused_tracks:
    print(f"[{task.name}]   class={track['class']}  count={track['count']}  avg_conf={track['avg_confidence']}", flush=True)

# --- send to generate-report ---
t1 = time.perf_counter()
task.send(fused_result)
elapsed_send = time.perf_counter() - t1

print(f"[{task.name}] send() completed in {elapsed_send:.3f}s", flush=True)
print(f"[{task.name}] done", flush=True)
task.close()
