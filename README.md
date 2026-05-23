# Wayline

**A data-aware DAG scheduling framework for Kubernetes.**

> Status: **v0.1 / beta.** Wayline is the public release of the system described in
> our ATC 2026 paper. The API is stable enough to run real workloads; expect rough
> edges in tooling.

Kubernetes-native workflow engines (Argo, Tekton, Kubeflow Pipelines) pass
intermediate results between tasks through a shared **artifact store** (S3/MinIO,
a PVC, …). That conflates two distinct events — *a task finished computing* and
*its output is available to the next task* — and forces every intermediate
through a central round-trip. On bandwidth-asymmetric edge clusters that
round-trip is the dominant cost.

**Wayline decouples those events.** A per-node **data-agent** moves a task's output
*directly* to the nodes that need it (peer-to-peer, content-addressed, atomic),
and exposes data readiness as **scheduler-visible runtime state**. A task pod is
started only once its inputs are already present on its node, so a downstream
`recv()` is always a local file read — no central store on the critical path.

On a real AI City multi-camera workload, holding CPU and task placement identical
to Argo+MinIO, Wayline cuts makespan **1.6–2.2×**; neither a distributed MinIO nor
a shared NFS filesystem closes the gap. See [`eval/`](eval/) to reproduce.

This release covers **one-shot DAGs (ODAGs)**. Continuous/streaming DAGs (CDAGs)
are future work and are not included.

---

## Table of contents

1. [How it works](#how-it-works)
2. [Repository layout](#repository-layout)
3. [Prerequisites](#prerequisites)
4. [Quick start](#quick-start)
5. [Writing tasks](#writing-tasks)
6. [ODAG reference](#odag-reference)
7. [CLI reference](#cli-reference)
8. [Web UI](#web-ui)
9. [Build & deploy reference](#build--deploy-reference)
10. [Cluster setup](#cluster-setup)
11. [Reproducing the paper](#reproducing-the-paper)
12. [Troubleshooting](#troubleshooting)
13. [Citation](#citation)

---

## How it works

```
            wayline apply -f odag.yml
                      │  ODAG custom resource (wl.io/v1)
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│  wl-system namespace (control plane, on the master node)             │
│                                                                      │
│   odag-controller                          ui-server :8080          │
│   • HEFT placement (runtime/dataSize        • K8s watch cache        │
│     profiling, EMA, spread-aware)           • SQLite run history     │
│   • starts a task pod only when its         • REST /api/* + SSE      │
│     inputs are .wl-ready on its node        • React frontend         │
│   • injects the WL_* task env contract                              │
└─────────────────────────────────────────────────────────────────────┘
                      │ creates task pods
                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│  task pods + per-node data-agent DaemonSet (hostPort 8082)           │
│                                                                      │
│   ┌──────────┐  1. PUT output → LOCAL agent (atomic, .wl-ready)      │
│   │ producer │  2. POST /push → agent ships to successor nodes       │
│   │  (WlTask)│  3. return (pod may exit after local handoff)         │
│   └────┬─────┘                                                       │
│        │ agent-to-agent install (content-addressed, idempotent)      │
│        ▼                                                             │
│   ┌──────────┐                                                       │
│   │ consumer │  recv() = local file read (inputs already on node)    │
│   │  (WlTask)│                                                       │
│   └──────────┘                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

**State model.** The data-agent tracks two independent signals per task, both
visible to the controller/scheduler:

- **Task lifecycle** — `Pending → Running → ComputeDone → Failed`.
- **Per-successor data readiness** — `Pending → Transferring → ReadyRemote`, plus a
  node-local `ReadyLocal` (`.wl-ready`) marker.

The agent is the *only* writer of `.wl-ready` (for both local installs and remote
pushes from peer agents), installs are atomic (temp → fsync → rename → fsync dir)
and content-addressed (`.wl-sha256`), and remote receives are idempotent. The wire
protocol and on-disk layout are documented in [`docs/architecture.md`](docs/architecture.md).

---

## Repository layout

```
wayline/
├── api/v1/                       # CRDs: odags.wl.io, odagtemplates.wl.io
├── cmd/
│   ├── odag-controller/          # one-shot DAG controller + HEFT scheduler
│   ├── data-agent/               # per-node DaemonSet: p2p data plane
│   ├── ui-server/                # REST + SSE + embedded React UI
│   └── cli/                      # `wayline` CLI (cobra, kubectl-style)
├── pkg/scheduler/                # HEFT scheduling interface
├── sdk/python/wl/                # Python SDK: `from wl import WlTask`
├── ui/                           # React + Vite frontend
├── deployments/                  # namespace, RBAC, Deployments, DaemonSet
├── examples/                     # 10 ODAG examples (dag-pipeline, rag-refresh, …)
├── eval/                         # full paper evaluation + benchmark suite
├── docs/                         # architecture, local-dev, SDK quickstart
└── Makefile                      # build / image / deploy targets
```

---

## Prerequisites

| Tool | Purpose |
|---|---|
| `kubectl` | Cluster access |
| `docker` | Building images |
| `go` ≥ 1.23 | Building the Go binaries |
| `node` ≥ 20, `npm` | Building the React UI |
| a **k3s** cluster | Wayline targets k3s; `~/.kube/config` configured |

---

## Quick start

```bash
# 1. Install CRDs, namespace, and RBAC
make install

# 2. Build all images and push to the local registry
make push-all

# 3. Deploy the data-agent, controller, and UI
make deploy

# 4. Build the CLI
make build        # produces bin/wayline
```

Run the bundled example pipeline (`generate → transform → output`):

```bash
make example-odag                       # or: bin/wayline apply -f examples/dag-pipeline/odag.yml
bin/wayline get    odags
bin/wayline status dag-pipeline
bin/wayline logs   dag-pipeline generate
bin/wayline delete dag-pipeline
```

The UI is available at `http://<master-ip>:30080`.

---

## Writing tasks

Tasks are ordinary container images. Inside, use the `wl` SDK — the controller
injects all peer/topology configuration as `WL_*` environment variables.

```python
from wl import WlTask

task = WlTask()                       # reads WL_* env vars

inputs = task.recv_all()              # dict: {dep_name: payload}; local file reads
result = process(inputs)
task.send(result)                     # routes to all successors via the data-agent
```

`send` / `recv` accept any JSON-serialisable value (`send_raw`/`recv_raw` for bytes).

### Dockerfile template

```dockerfile
# Build from the repo root:
#   docker build -f examples/my-dag/tasks/my-task/Dockerfile -t <registry>/my-task:latest .
FROM python:3.11-slim
WORKDIR /app
COPY sdk/python/wl ./wl
COPY examples/my-dag/tasks/my-task/task.py .
CMD ["python", "task.py"]
```

---

## ODAG reference

```yaml
apiVersion: wl.io/v1
kind: ODAG
metadata:
  name: my-dag
  namespace: default
spec:
  scheduler: heft
  schedulerConfig:
    spreadEpsilon: 0          # HEFT tie-break: spread parallel layers (0 = off)
  retryPolicy:
    maxRetries: 2
  tasks:
    - name: generate
      image: 192.168.1.163:5000/my-generate:latest
      command: ["python", "task.py"]
      dependencies: []
      resources: { cpu: "200m", memory: "128Mi" }
      dataSize: "1MB"         # used by the HEFT data-transfer cost model
      runtime: 10             # seed estimate; refined by the EMA profiler
    - name: transform
      image: 192.168.1.163:5000/my-transform:latest
      command: ["python", "task.py"]
      dependencies: ["generate"]
      resources: { cpu: "500m", memory: "256Mi" }
      constraints:
        nodeNames: [anrg-4, anrg-6, anrg-8]   # restrict placement to these nodes
```

| Status field | Description |
|---|---|
| `status.phase` | `Pending → Scheduling → Running → Succeeded / Failed` |
| `status.makespan` | Wall-clock makespan in seconds (set on completion) |
| `status.tasks[].phase` | Per-task phase |
| `status.tasks[].node` | Node the task ran on |

An **ODAGTemplate** is a reusable spec; `wayline run <template>` creates a new run
and the EMA profiler refines per-`(task, node)` runtime estimates across runs.

---

## CLI reference

```
wayline apply  -f <file>                Create/update an ODAG or ODAGTemplate (kind auto-detected)
wayline get    [odags|templates] [-n]   List resources
wayline status <name> [-n]              Detailed ODAG status (per-task phase, node, timing)
wayline logs   <odag> <task> [-n]       Stream logs from a task pod
wayline delete <name> [-n]              Delete an ODAG + its pods/services
wayline delete template <name> [-n]     Delete an ODAGTemplate
wayline run    <template> [-n]          Create a new run from an ODAGTemplate
wayline runs   <template> [-n]          List runs of a template
wayline show   <template> [-n]          Template detail + profile summary

Global flag:  --kubeconfig <path>   (default: $KUBECONFIG or ~/.kube/config)
```

The legacy verb groups `wayline odag …` and `wayline template …` remain available
as hidden aliases for backward compatibility.

---

## Web UI

Served by `ui-server` on NodePort **30080**.

| Page | URL | Description |
|---|---|---|
| ODAG list | `/` | All ODAGs with phase, makespan, age |
| ODAG detail | `/odags/{ns}/{name}` | Graph view, tasks table, run-history chart |
| Templates | `/templates` | ODAGTemplates and their runs |
| Batch | `/batch` | Multi-ODAG submission with a combined Gantt chart |
| Cluster | `/cluster` | Per-node task counts and utilization |

Live updates arrive via **Server-Sent Events** (`/api/events`) — no polling.
For local UI development see [`docs/local-dev.md`](docs/local-dev.md).

---

## Build & deploy reference

```bash
make build               # all Go binaries into bin/ (incl. bin/wayline)
make ui-build            # React UI into ui/dist/
make test                # Go unit tests

make push-all            # build + push control plane + example images
make push-controllers    # build + push only control-plane images

make install             # CRDs + namespace + RBAC
make deploy              # data-agent DaemonSet + odag-controller + ui-server
make rollout             # restart control-plane deployments (pick up :latest)

make example-odag        # submit the dag-pipeline example
make clean-deploy        # remove control-plane resources (keeps CRDs)
make clean-all           # remove everything incl. CRDs and namespace
```

Override the registry/namespace per invocation: `REGISTRY=myreg:5000 make push-all`.

---

## Cluster setup

One-time setup for a fresh k3s cluster (master + workers).

```bash
# 1. Local registry on the master
docker run -d -p 5000:5000 --restart=always --name registry registry:2

# 2. Trust it from Docker on the master
echo '{"insecure-registries":["<master-ip>:5000"]}' | sudo tee /etc/docker/daemon.json
sudo systemctl restart docker

# 3. Mirror it from k3s on EVERY node, then restart k3s
#    write to /etc/rancher/k3s/registries.yaml:
#      mirrors:
#        "<master-ip>:5000":
#          endpoint: ["http://<master-ip>:5000"]
sudo systemctl restart k3s          # master
sudo systemctl restart k3s-agent    # each worker
```

The data-agent DaemonSet binds **hostPort 8082** on every node (chosen so a
Wayline cluster can coexist with a legacy DSF deployment on 8081). It writes
node-local intermediates under `/data/wl-outputs`.

---

## Reproducing the paper

The complete evaluation lives in [`eval/`](eval/); start with
[`eval/EXPERIMENTS.md`](eval/EXPERIMENTS.md). Highlights:

| Directory | Experiment |
|---|---|
| `eval/videoedge-mcmt/` | AI City multi-camera tracking head-to-head (Wayline vs Argo+MinIO, distributed MinIO, NFS) |
| `eval/network-aware/` | HEFT placement under bandwidth-asymmetric `tc` topologies |
| `eval/two-hop/` | E0 data-plane microbenchmark (same-node vs cross-node, by payload size) |
| `eval/argo-headtohead/`, `eval/networkoverhead-headtohead/` | Argo/MinIO baselines |
| `eval/ray-microbench/`, `eval/scalability/`, `eval/overhead-stress/` | Ray comparison, scaling, overhead |

> The eval harnesses are preserved verbatim as they were run, and therefore still
> reference the project's original `dsf`/`dsf-system` identifiers.

---

## Troubleshooting

**Controller pod is Pending.** The control plane targets the master node, which
is often `SchedulingDisabled`. The Deployment carries a toleration for
`node.kubernetes.io/unschedulable:NoSchedule` and a `nodeSelector` for the master
hostname — adjust both to your cluster (`deployments/odag-controller/deployment.yml`).

**Task image pull fails.** Confirm the registry container is up
(`docker ps | grep registry`) and that `/etc/rancher/k3s/registries.yaml` exists on
the worker and k3s-agent was restarted after writing it.

**An ODAG hangs / a task never starts.** A task pod starts only when all upstream
`.wl-ready` markers are present on its node. Check the controller and the relevant
data-agent:

```bash
kubectl logs -n wl-system deployment/odag-controller --tail=40
kubectl logs -n wl-system -l app=data-agent --tail=40
kubectl get pods -l wl-odag=<name>
```

**Coexisting with a legacy DSF cluster.** Wayline uses namespace `wl-system`, CRD
group `wl.io`, label `wl-odag`, data dir `/data/wl-outputs`, and data-agent port
`8082` — all distinct from DSF's — so the two can run side by side.

---

## Citation

If you use Wayline in academic work, please cite the ATC 2026 paper:

```bibtex
@inproceedings{wayline2026,
  title     = {Wayline: Decoupling Task Completion from Data Availability
               for DAG Workflows on Kubernetes},
  author    = {Khodabandehlou, Mohammadali and Coleman, Jared and
               Krishnamachari, Bhaskar},
  booktitle = {Proceedings of the 2026 USENIX/ACM Annual Technical Conference (ATC)},
  year      = {2026}
}
```

## License

MIT — see [LICENSE](LICENSE).
