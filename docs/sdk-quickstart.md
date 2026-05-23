# Wayline SDK Quick Start

This guide walks you through building and running a one-shot DAG (ODAG) on Wayline — from writing task code to seeing results in the cluster.

## Overview

An ODAG is a directed acyclic graph of tasks that runs once to completion. Each task is a container that reads from upstream tasks and writes to downstream tasks using the Wayline Python SDK. The `odag-controller` schedules tasks layer by layer: a task starts only after all its dependencies have finished and their data has arrived on the task's node.

---

## 1. Write your tasks

Each task is a Python script that imports `WlTask`. The pattern is always:

```
init → recv() → process → send() → close()
```

### Root task (no dependencies, produces output)

```python
from wl import WlTask

task = WlTask()

data = {"values": [1, 2, 3, 4, 5], "count": 5}

task.send(data)   # routes to all downstream successors automatically
task.close()
```

### Middle task (reads one upstream, writes downstream)

```python
from wl import WlTask

task = WlTask()

upstream = task.recv("source")   # name must match the dependency task name

result = {
    "values": [v * 2 for v in upstream["values"]],
    "count":  upstream["count"],
}

task.send(result)
task.close()
```

### Task with multiple inputs

Use `recv_all()` to read all dependencies at once:

```python
from wl import WlTask

task = WlTask()

inputs = task.recv_all()   # {"dep-a": ..., "dep-b": ...}

combined = inputs["dep-a"]["values"] + inputs["dep-b"]["values"]

task.send({"values": combined, "count": len(combined)})
task.close()
```

### Leaf task (reads upstream, produces no output)

```python
from wl import WlTask

task = WlTask()

data = task.recv("transform")

print(f"[{task.name}] result: {data['values']}", flush=True)

task.close()   # no send() — this is a terminal node
```

### SDK API reference

**Attributes** (read-only, set at init from injected env vars):


| Attribute                 | Type        | Description                                                       |
| ------------------------- | ----------- | ----------------------------------------------------------------- |
| `task.name`               | `str`       | This task's name.                                                 |
| `task.node`               | `str`       | Cluster node this pod is running on.                              |
| `task.dependencies`       | `list[str]` | Names of upstream tasks this task reads from.                     |
| `task.successors`         | `list[str]` | Names of downstream tasks that read from this task.               |
| `task.is_root`            | `bool`      | `True` if this task has no dependencies.                          |
| `task.is_leaf`            | `bool`      | `True` if this task has no successors (no need to call `send()`). |
| `task.expected_runtime`   | `float`     | Expected wall-clock runtime in seconds from the ODAG spec.        |
| `task.expected_data_size` | `int`       | Expected output size in bytes from the ODAG spec.                 |


**Methods:**


| Method               | Description                                                                                                         |
| -------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `task.dep_node(dep)` | Returns the node name where dependency `dep` ran. Useful for logging whether a transfer was local or cross-node.    |
| `task.recv(peer)`    | Read one upstream dependency's output. `peer` is the dependency task name. Omit if there is exactly one dependency. |
| `task.recv_all()`    | Read all upstream dependencies. Returns `{name: data}`.                                                             |
| `task.send(data)`    | Send output to all downstream successors. `data` must be JSON-serializable.                                         |
| `task.close()`       | Signal completion and exit cleanly. Always call at the end.                                                         |


`send()` is non-blocking — the controller handles routing data to each successor based on node placement. Same-node successors read the file directly; cross-node successors receive it via the data-agent.

---

## 2. Package each task as a Docker image

Each task needs its own `Dockerfile`. The SDK is bundled by copying it from the repo:

```dockerfile
# Build from the repo root:
#   docker build -f examples/my-dag/tasks/source/Dockerfile -t <registry>/my-dag-source:latest .
FROM python:3.11-slim
WORKDIR /app
COPY sdk/python/wl ./wl
COPY examples/my-dag/tasks/source/task.py .
CMD ["python", "task.py"]
```

Build and push (always build from the repo root so the SDK path resolves):

```bash
docker build -f examples/my-dag/tasks/source/Dockerfile \
             -t 192.168.1.163:5000/my-dag-source:latest .
docker push 192.168.1.163:5000/my-dag-source:latest
```

---

## 3. Write the ODAG spec

Create an `odag.yml` describing the task graph:

```yaml
apiVersion: wl.io/v1
kind: ODAG
metadata:
  name: my-dag
  namespace: default
spec:
  scheduler: heft      # heft or random
  tasks:

    - name: source
      image: 192.168.1.163:5000/my-dag-source:latest
      command: ["python", "task.py"]
      dependencies: []
      dataSize: "10MB"   # expected output size — used by HEFT for scheduling
      runtime: 3         # expected wall-clock time in seconds
      resources:
        cpu: "300m"
        memory: "256Mi"

    - name: transform
      image: 192.168.1.163:5000/my-dag-transform:latest
      command: ["python", "task.py"]
      dependencies: ["source"]
      dataSize: "20MB"
      runtime: 5
      resources:
        cpu: "300m"
        memory: "256Mi"

    - name: sink
      image: 192.168.1.163:5000/my-dag-sink:latest
      command: ["python", "task.py"]
      dependencies: ["transform"]
      dataSize: "0"
      runtime: 4
      resources:
        cpu: "300m"
        memory: "256Mi"
```

### Spec fields


| Field                   | Required | Description                                                             |
| ----------------------- | -------- | ----------------------------------------------------------------------- |
| `name`                  | yes      | Unique task name within the ODAG. Used as the peer name in `recv()`.    |
| `image`                 | yes      | Docker image to run.                                                    |
| `command`               | yes      | Entrypoint command.                                                     |
| `dependencies`          | yes      | List of task names this task reads from. Empty list for root tasks.     |
| `dataSize`              | yes      | Expected output size (e.g. `"30MB"`, `"1GiB"`). Used by HEFT scheduler. |
| `runtime`               | yes      | Expected wall-clock runtime in seconds. Used by HEFT scheduler.         |
| `resources.cpu`         | no       | CPU request (Kubernetes format, e.g. `"500m"`).                         |
| `resources.memory`      | no       | Memory request (e.g. `"512Mi"`).                                        |
| `constraints.nodeNames` | no       | Pin task to specific nodes (e.g. `[anrg-1, anrg-3]`).                   |
| `args`                  | no       | Additional arguments passed after `command`.                            |
| `env`                   | no       | Extra environment variables: `[{name: X, value: Y}]`.                   |


### Schedulers

- `**heft**` — Heterogeneous Earliest Finish Time. Uses `runtime` and `dataSize` to compute an optimal task-to-node mapping that minimises makespan. Recommended when you have profiled values.
- `**random**` — Picks a random schedulable node per task. Useful for quick tests.

### Node constraints

Pin a task to a subset of nodes:

```yaml
constraints:
  nodeNames: [anrg-1, anrg-3]
```

The controller picks from the intersection of the allowed nodes and schedulable cluster nodes.

---

## 4. Submit and monitor

```bash
# Submit
kubectl apply -f examples/my-dag/odag.yml

# Watch pod status
kubectl get pods -l wl-odag=my-dag -w

# Check task status and node placement
kubectl get odag my-dag -o yaml

# Stream logs from a specific task
kubectl logs -f my-dag-transform

# Delete when done
kubectl delete odag my-dag
```

The UI at `http://192.168.1.163:30080` shows a live graph of the ODAG with task states, node placement, and a Gantt chart of actual vs. predicted makespan.

---

## 5. Complete example — linear pipeline

Directory layout:

```
examples/my-dag/
├── odag.yml
└── tasks/
    ├── source/
    │   ├── Dockerfile
    │   └── task.py
    ├── transform/
    │   ├── Dockerfile
    │   └── task.py
    └── sink/
        ├── Dockerfile
        └── task.py
```

`**tasks/source/task.py**`

```python
from wl import WlTask

task = WlTask()

data = {"values": list(range(100)), "count": 100}
task.send(data)
task.close()
```

`**tasks/transform/task.py**`

```python
from wl import WlTask

task = WlTask()

upstream = task.recv("source")
squared  = [v ** 2 for v in upstream["values"]]

task.send({"values": squared, "count": len(squared)})
task.close()
```

`**tasks/sink/task.py**`

```python
from wl import WlTask

task = WlTask()

data = task.recv("transform")
print(f"[{task.name}] received {data['count']} values, sum={sum(data['values'])}", flush=True)

task.close()
```

**Build and run:**

```bash
# Build all images from repo root
for t in source transform sink; do
  docker build -f examples/my-dag/tasks/$t/Dockerfile \
               -t 192.168.1.163:5000/my-dag-$t:latest . && \
  docker push 192.168.1.163:5000/my-dag-$t:latest
done

# Submit
kubectl apply -f examples/my-dag/odag.yml

# Follow progress
kubectl get pods -l wl-odag=my-dag -w
```

---

## Tips

- Always build Docker images from the **repo root** — the `Dockerfile` copies `sdk/python/wl` relative to that path.
- `dataSize` and `runtime` don't have to be exact, but better estimates give HEFT a better schedule. Profile a few runs and update the values.
- If you resubmit an ODAG with the same name, the controller clears stale state from the previous run automatically — no manual cleanup needed.
- Use `print(..., flush=True)` in task code so logs appear immediately in `kubectl logs`.
- Leaf tasks (no successors) don't need to call `send()`. Just process and `close()`.

