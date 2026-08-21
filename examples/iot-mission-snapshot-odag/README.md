# IoT Rapid ISR Snapshot - Mission Report (ODAGTemplate)

A realistic Internet of Battlefield Things (IoT) example demonstrating a
one-shot DAG template that simulates a rapid ISR (Intelligence, Surveillance,
Reconnaissance) snapshot pipeline across distributed sensor and compute nodes.

Uses `ODAGTemplate` for profiling, retention, and repeated runs via the
controller.

## DAG Structure

```
capture-1 -> preprocess-1 -> infer-1 \
capture-2 -> preprocess-2 -> infer-2  \
capture-3 -> preprocess-3 -> infer-3   --> fuse-tracks --> generate-report
capture-4 -> preprocess-4 -> infer-4  /
```

**14 tasks across 5 stages:**

| Stage | Tasks | Description | Data Size | Placement |
|-------|-------|-------------|-----------|-----------|
| Capture | capture-1..4 | Raw ISR image burst | 80-150 MB | Pinned to sensor nodes |
| Preprocess | preprocess-1..4 | Feature extraction (SHA-256 chunked) | 15-30 MB | Co-located with capture |
| Infer | infer-1..4 | Object detection (CPU busy loop) | ~1 MB JSON | Compute-capable subset |
| Fuse | fuse-tracks | Fan-in track fusion | ~1 MB JSON | Near inference tier |
| Report | generate-report | Final mission report | terminal | Gateway node |

## Key Properties

- **Sensor locality**: capture and preprocess are pinned to the same node (same-node file read, no network transfer)
- **Compute constraints**: inference tasks are restricted to nodes with sufficient resources
- **Fan-in materialization**: fuse-tracks waits for all 4 inference results before running
- **Gateway egress**: final report is pinned to a single gateway node
- **Profiling**: EMA-smoothed runtime/data profiling across repeated runs

## Quick Start

### 1. Generate template with real node names

```bash
cd examples/iot-mission-snapshot-odag
python gen_odag.py --registry 192.168.1.163:5000
```

This queries your cluster, assigns nodes to roles, and writes `template.yml`.

Or manually edit `template.yml` and replace node names with your `kubectl get nodes` output.

### 2. Build and push images

From the **repo root**:

```bash
REGISTRY=192.168.1.163:5000

for task in capture preprocess infer fuse report; do
  docker build \
    -f examples/iot-mission-snapshot-odag/tasks/$task/Dockerfile \
    -t $REGISTRY/wl-iot-$task:latest . \
  && docker push $REGISTRY/wl-iot-$task:latest
done
```

### 3. Apply the ODAGTemplate

```bash
kubectl apply -f examples/iot-mission-snapshot-odag/template.yml
```

### 4. Trigger a run

```bash
wayline run iot-mission-snapshot
```

### 5. Monitor

```bash
wayline status iot-mission-snapshot-run-1
wayline logs iot-mission-snapshot-run-1 capture-1
wayline logs iot-mission-snapshot-run-1 generate-report
```

Or view in the UI at `http://localhost:8080`.

### 6. Re-run

Submit again to trigger another profiled run:

```bash
wayline run iot-mission-snapshot
```

The controller increments the run number automatically. Old runs are garbage-collected
per the `retention.maxRuns` setting (default: 10).

## Task Details

### capture (capture-1..4)
Generates a deterministic raw blob (MD5-seeded repeating blocks) of `WL_DATA_SIZE` bytes.
Simulates sensor capture delay via `sleep(WL_RUNTIME)`. Sends raw bytes to preprocess.

### preprocess (preprocess-1..4)
Receives raw bytes, computes SHA-256 over 64 KB chunks (simulating feature extraction),
then produces a smaller feature blob. Co-located with capture for zero-network-hop read.

### infer (infer-1..4)
Receives feature bytes, runs a CPU busy loop for `WL_RUNTIME` seconds (SHA-256 hashing
in a tight loop), then outputs a JSON dict of 5-15 fake detections with class, confidence,
and bounding box. Classes: vehicle, person, uav, building, antenna.

### fuse-tracks
Fan-in from all 4 infer tasks via `recv_all()`. Groups detections by class, averages
confidence, keeps top-5 per class. Includes provenance (which node each infer ran on).

### generate-report
Produces a formatted mission report with detection summary, per-sensor counts, fused
tracks, and full execution provenance chain. Printed to stdout and stored as final artifact.
