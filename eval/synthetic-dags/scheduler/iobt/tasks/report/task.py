#!/usr/bin/env python3
"""
IoBT Mission Snapshot — generate-report task.

Receives fused track data from fuse-tracks and produces a final
mission report with summary counts, timestamps, provenance, and
run metadata.
"""

import json
import os
import time

from wl_sdk import WlTask

task = WlTask()

run_id = task.run_id or "unknown"

print(f"[{task.name}] node={task.node}  run_id={run_id}", flush=True)

# --- receive fused tracks ---
t0 = time.perf_counter()
fused = task.recv()
elapsed_recv = time.perf_counter() - t0

print(f"[{task.name}] recv() completed in {elapsed_recv:.3f}s", flush=True)

# --- build provenance chain ---
# fused already has provenance for infer nodes; add fuse node
stage_nodes = {"generate-report": task.node}
fuse_node = task.dep_node("fuse-tracks")
stage_nodes["fuse-tracks"] = fuse_node

# Propagate infer provenance from fused data
infer_provenance = fused.get("provenance", {})
for infer_name, node in infer_provenance.items():
    stage_nodes[infer_name] = node

# --- build final report ---
tracks = fused.get("fused_tracks", [])
report = {
    "title": "IoBT Rapid ISR Snapshot - Mission Report",
    "run_id": run_id,
    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "summary": {
        "total_detections": fused.get("total_detections", 0),
        "per_source_counts": fused.get("per_source_counts", {}),
        "num_classes_detected": len(tracks),
        "classes": [t["class"] for t in tracks],
    },
    "tracks": tracks,
    "execution_provenance": {
        "stage_nodes": stage_nodes,
        "fuse_node": fuse_node,
        "report_node": task.node,
    },
}

# --- print report ---
print("", flush=True)
print("=" * 72, flush=True)
print(f"  {report['title']}", flush=True)
print(f"  Run: {report['run_id']}   Time: {report['timestamp']}", flush=True)
print("=" * 72, flush=True)
print(f"  Total detections:  {report['summary']['total_detections']}", flush=True)
print(f"  Classes detected:  {report['summary']['num_classes_detected']}", flush=True)

for src, count in report["summary"]["per_source_counts"].items():
    print(f"    {src}: {count} detections", flush=True)

print("", flush=True)
print("  Fused tracks:", flush=True)
for track in tracks:
    print(f"    [{track['class']}]  count={track['count']}  "
          f"avg_conf={track['avg_confidence']}  "
          f"sensors={track['sensors']}", flush=True)

print("", flush=True)
print("  Execution provenance:", flush=True)
for stage, node in sorted(stage_nodes.items()):
    print(f"    {stage:20s} -> {node}", flush=True)

print("=" * 72, flush=True)
print("", flush=True)

# --- send report as final artifact ---
t1 = time.perf_counter()
task.send(report)
elapsed_send = time.perf_counter() - t1

print(f"[{task.name}] send() completed in {elapsed_send:.3f}s", flush=True)
print(f"[{task.name}] done", flush=True)
task.close()
