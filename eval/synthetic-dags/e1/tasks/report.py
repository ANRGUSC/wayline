#!/usr/bin/env python3
"""
IoBT generate-report — Argo port. Reads the fused detections JSON,
prints a formatted mission report, writes the final JSON to /out/output.
"""
import glob
import json
import os
import time

TASK_NAME = os.environ["E1_TASK_NAME"]
NODE_NAME = os.environ.get("NODE_NAME", "?")
print(f"[{TASK_NAME}] node={NODE_NAME}", flush=True)

input_files = sorted(glob.glob("/in/*/output"))
assert len(input_files) == 1
with open(input_files[0]) as f:
    fused = json.load(f)

tracks = fused.get("fused_tracks", [])
provenance = fused.get("provenance", {})

# Derive a stage-node map from upstream provenance (best-effort under Argo;
# Wayline's dep_node provides this directly, but here we only get what the
# source pod logged into its own JSON).
stage_nodes = {dep: node for dep, node in provenance.items()}
fuse_node = fused.get("node", "?")

report = {
    "title": "IoBT Mission Snapshot Report",
    "run_id": os.environ.get("HOSTNAME", "?"),
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
        "report_node": NODE_NAME,
    },
}

print("", flush=True)
print("=" * 72, flush=True)
print(f"  {report['title']}", flush=True)
print(f"  Run: {report['run_id']}", flush=True)
print("=" * 72, flush=True)
print(f"  Total detections: {report['summary']['total_detections']}", flush=True)
print(f"  Classes:          {report['summary']['num_classes_detected']}", flush=True)
print("=" * 72, flush=True)

os.makedirs("/out", exist_ok=True)
with open("/out/output", "w") as f:
    json.dump(report, f)
print(f"[{TASK_NAME}] done", flush=True)
