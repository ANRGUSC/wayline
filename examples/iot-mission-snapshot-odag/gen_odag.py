#!/usr/bin/env python3
"""
Auto-generate template.yml (ODAGTemplate) for the IoT Mission Snapshot example.

Queries the cluster for schedulable nodes and assigns them to roles:
  - First 4 schedulable nodes  -> sensor (capture + preprocess pinned)
  - Last 4 schedulable nodes   -> inference-eligible
  - Last 2 of inference nodes  -> fuse-tracks eligible
  - Last schedulable node      -> gateway (generate-report)

Usage:
  python gen_odag.py                                   # uses default registry
  python gen_odag.py --registry 192.168.1.163:5000     # custom registry
  python gen_odag.py --output template-generated.yml   # custom output path
  python gen_odag.py --name my-snapshot                 # custom template name
"""

import argparse
import json
import subprocess
import sys

# ── task definitions (sizes in bytes, runtime in seconds) ───────────────────

CAPTURE_SIZES = [100_000_000, 120_000_000, 80_000_000, 150_000_000]
PREPROCESS_SIZES = [20_000_000, 25_000_000, 15_000_000, 30_000_000]

TEMPLATE_HEADER = """\
apiVersion: wl.io/v1
kind: ODAGTemplate
metadata:
  name: {name}
  namespace: wl-system
spec:
  description: "IoT rapid ISR snapshot: 4 sensors -> preprocess -> infer -> fuse -> report"
  scheduler: heft
  profiling:
    enabled: true
    warmupRuns: 0
    minSamples: 2
    emaAlpha: 0.3
    maxSamples: 50
  defaults:
    runtime: 5
    dataSize: "20MB"
  retention:
    maxRuns: 10
  tasks:
    # ====================================================================
    # Layer 0 - Capture (sensor nodes, no dependencies)
    # ====================================================================
{capture_tasks}
    # ====================================================================
    # Layer 1 - Preprocess (co-located with capture for sensor locality)
    # ====================================================================
{preprocess_tasks}
    # ====================================================================
    # Layer 2 - Inference (constrained to compute-capable nodes)
    # ====================================================================
{infer_tasks}
    # ====================================================================
    # Layer 3 - Fusion (fan-in from all 4 infer tasks)
    # ====================================================================
{fuse_task}
    # ====================================================================
    # Layer 4 - Report (gateway node)
    # ====================================================================
{report_task}"""


def get_schedulable_nodes():
    """Return list of schedulable node names from the cluster."""
    try:
        result = subprocess.run(
            ["kubectl", "get", "nodes", "-o", "json"],
            capture_output=True, text=True, check=True,
        )
    except FileNotFoundError:
        print("error: kubectl not found in PATH", file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"error: kubectl failed: {e.stderr}", file=sys.stderr)
        sys.exit(1)

    data = json.loads(result.stdout)
    nodes = []
    for item in data.get("items", []):
        name = item["metadata"]["name"]
        # Skip nodes with NoSchedule taint or SchedulingDisabled
        spec = item.get("spec", {})
        taints = spec.get("taints", [])
        unschedulable = spec.get("unschedulable", False)
        has_noschedule = any(
            t.get("effect") == "NoSchedule" for t in taints
        )
        if not has_noschedule and not unschedulable:
            nodes.append(name)

    nodes.sort()
    return nodes


def make_task_block(name, image, deps, data_size, runtime, cpu, memory, node_names):
    """Generate a single YAML task block."""
    deps_str = "[" + ", ".join(f'"{d}"' for d in deps) + "]" if deps else "[]"
    nodes_str = "[" + ", ".join(node_names) + "]"
    data_label = f'"{data_size // 1_000_000}MB"' if data_size >= 1_000_000 else f'"{data_size}"'

    return f"""\
    - name: {name}
      image: {image}
      command: ["python", "task.py"]
      dependencies: {deps_str}
      dataSize: {data_label}
      runtime: {runtime}
      resources:
        cpu: "{cpu}"
        memory: "{memory}"
      constraints:
        nodeNames: {nodes_str}
"""


def generate(nodes, registry, template_name):
    """Generate the full ODAGTemplate YAML string."""
    if len(nodes) < 4:
        print(f"error: need at least 4 schedulable nodes, found {len(nodes)}: {nodes}",
              file=sys.stderr)
        sys.exit(1)

    # Assign roles
    sensor_nodes = nodes[:4]
    infer_nodes = nodes[-4:] if len(nodes) >= 8 else nodes[-min(4, len(nodes)):]
    fuse_nodes = infer_nodes[-2:]
    gateway_node = nodes[-1]

    print(f"Sensor nodes (capture+preprocess): {sensor_nodes}", file=sys.stderr)
    print(f"Inference nodes:                   {infer_nodes}", file=sys.stderr)
    print(f"Fuse nodes:                        {fuse_nodes}", file=sys.stderr)
    print(f"Gateway node (report):             {gateway_node}", file=sys.stderr)

    # Build task blocks
    capture_blocks = []
    preprocess_blocks = []
    infer_blocks = []

    for i in range(4):
        idx = i + 1
        capture_blocks.append(make_task_block(
            name=f"capture-{idx}",
            image=f"{registry}/wl-iot-capture:latest",
            deps=[],
            data_size=CAPTURE_SIZES[i],
            runtime=3,
            cpu="200m", memory="512Mi",
            node_names=[sensor_nodes[i]],
        ))
        preprocess_blocks.append(make_task_block(
            name=f"preprocess-{idx}",
            image=f"{registry}/wl-iot-preprocess:latest",
            deps=[f"capture-{idx}"],
            data_size=PREPROCESS_SIZES[i],
            runtime=4,
            cpu="300m", memory="512Mi",
            node_names=[sensor_nodes[i]],  # co-located with capture
        ))
        infer_blocks.append(make_task_block(
            name=f"infer-{idx}",
            image=f"{registry}/wl-iot-infer:latest",
            deps=[f"preprocess-{idx}"],
            data_size=1_000_000,
            runtime=10,
            cpu="500m", memory="512Mi",
            node_names=infer_nodes,
        ))

    fuse_block = make_task_block(
        name="fuse-tracks",
        image=f"{registry}/wl-iot-fuse:latest",
        deps=["infer-1", "infer-2", "infer-3", "infer-4"],
        data_size=1_000_000,
        runtime=3,
        cpu="300m", memory="256Mi",
        node_names=fuse_nodes,
    )

    report_block = make_task_block(
        name="generate-report",
        image=f"{registry}/wl-iot-report:latest",
        deps=["fuse-tracks"],
        data_size=0,
        runtime=2,
        cpu="200m", memory="128Mi",
        node_names=[gateway_node],
    )

    return TEMPLATE_HEADER.format(
        name=template_name,
        capture_tasks="".join(capture_blocks),
        preprocess_tasks="".join(preprocess_blocks),
        infer_tasks="".join(infer_blocks),
        fuse_task=fuse_block,
        report_task=report_block,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Generate IoT Mission Snapshot ODAGTemplate YAML with real node names",
    )
    parser.add_argument(
        "--registry", default="192.168.1.163:5000",
        help="Container registry prefix (default: 192.168.1.163:5000)",
    )
    parser.add_argument(
        "--output", default="template.yml",
        help="Output file path (default: template.yml)",
    )
    parser.add_argument(
        "--name", default="iot-mission-snapshot",
        help="ODAGTemplate metadata.name (default: iot-mission-snapshot)",
    )
    args = parser.parse_args()

    nodes = get_schedulable_nodes()
    print(f"Found {len(nodes)} schedulable nodes: {nodes}", file=sys.stderr)

    yaml_str = generate(nodes, args.registry, args.name)

    with open(args.output, "w") as f:
        f.write(yaml_str)

    print(f"\nWrote {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
