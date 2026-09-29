# Wayline

**Programmable data realization for DAG workflows on Kubernetes.**

> Status: research prototype. The API is stable enough to run real workloads on a
> k3s cluster; expect rough edges in tooling. This release covers one-shot DAGs
> (ODAGs); streaming DAGs are future work.

A DAG edge `A → B` says that `B` needs an output of `A`. It says nothing about
*how* that output gets there: whether it goes through a shared store or directly,
which node serves it, how many copies exist, or how long they live. Workflow
engines such as Argo, Tekton, and Kubeflow Pipelines fix all of that up front by
routing every intermediate through an artifact store and tying its lifetime to
the tasks that produce and consume it.

Wayline separates the two. Every intermediate output is a named **object**, and
each object has a **physical realization**: its copies, its serving point, its
movement, and its lifetime. The realization is live, revisable state that an
external policy can patch while the run executes, without changing the DAG or
moving any task. A per-node **data agent** holds objects and moves them peer to
peer; the **controller** reconciles the data plane toward the desired
realization and starts a task pod only once its inputs are already on its node.

On an 8-worker edge testbed with links that degrade, disconnect, or fail:

- revising an object's serving point after a producer's uplink degrades cuts
  traffic through the choked link by 55–63% and completion time by up to 43%,
  with one policy patch and no task restart;
- retaining an object on a relay node that runs no task completes every workflow
  across two contacts 20 s apart, where fixed direct delivery completes none;
- risk-aware replication gives the survival of always-on replication at 16–77%
  less replica storage-time;
- with task placement and per-node order held fixed, changing only the data path
  moves an 18-task AI City pipeline from 195.5 s (store-mediated) to 163 s
  (direct), and costs seven scientific-workflow structures 1.03–1.45×.

The evaluation lives in [`eval/experiments`](eval/experiments/); see
[Reproducing the evaluation](#reproducing-the-evaluation).

---

## Table of contents

1. [How it works](#how-it-works)
2. [Repository layout](#repository-layout)
3. [Prerequisites](#prerequisites)
4. [Quick start](#quick-start)
5. [Writing tasks](#writing-tasks)
6. [Warm runners](#warm-runners)
7. [ODAG reference](#odag-reference)
8. [Revising a realization at runtime](#revising-a-realization-at-runtime)
9. [Schedulers and policies](#schedulers-and-policies)
10. [CLI reference](#cli-reference)
11. [Web UI](#web-ui)
12. [Build & deploy reference](#build--deploy-reference)
13. [Cluster setup](#cluster-setup)
14. [Reproducing the evaluation](#reproducing-the-evaluation)
15. [Troubleshooting](#troubleshooting)
16. [License](#license)

---

## How it works

```
   wayline apply -f template.yml            kubectl patch odag <run> ...
   wayline run <template>                   (external policy: spec.realization)
              │                                        │
              ▼                                        ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  wl-system namespace (control plane)                                     │
│                                                                          │
│   odag-controller                                    ui-server :8080     │
│   • scheduler: heft | random | saga/<algo> | http://…  • K8s watch cache │
│   • dispatch: a task pod starts only when every       • SQLite history   │
│     input object is installed on its node             • REST + SSE       │
│   • reconciler: converges copies / serving point /    • React frontend   │
│     eviction toward spec.realization (live)                              │
│   • profiler: EMA runtime + dataSize per (task, node)                    │
└──────────────────────────────────────────────────────────────────────────┘
              │ creates task pods; asks agents to push / alias / evict
              ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  workers: task pods + data-agent DaemonSet (hostPort 8082)               │
│                                                                          │
│   ┌──────────┐  1. PUT output → LOCAL agent (temp → fsync → rename)      │
│   │ producer │  2. agent pushes to each consumer's node (digest-checked) │
│   │  (WlTask)│  3. pod may exit; the object outlives it on the agent     │
│   └────┬─────┘                                                           │
│        │ agent → agent install (content-addressed, idempotent)           │
│        ▼            ┌───────┐                                            │
│   ┌──────────┐      │ relay │  a node that runs NO task can hold a copy  │
│   │ consumer │      │ agent │  (temporal relay, replica, cache) and      │
│   │  (WlTask)│      └───────┘  serve consumers from it                   │
│   └──────────┘  recv() = local file read                                 │
└──────────────────────────────────────────────────────────────────────────┘
```

**Objects.** Within a run, an output's identity is `<run, producer, name>`; the
data-plane key is `producer` for the default output or `producer.name` for a
named one. A task can emit several named outputs, each with its own consumers
and its own realization.

**State the controller sees.** Each data agent tracks two signals per object:

- task lifecycle: `Pending → Running → ComputeDone → Failed`;
- per-destination transfer state: `Pending → Transferring → ReadyRemote | Failed`,
  plus a node-local installed marker (`.wl-ready`, with `.wl-sha256`).

The agent is the only writer of `.wl-ready`; installs are atomic and
digest-verified, and every agent verb (install, push, alias, evict, cancel) is
idempotent, so the controller can retry any step after a crash. The wire protocol
and on-disk layout are in [`docs/architecture.md`](docs/architecture.md).

**Safety rules the reconciler enforces.** A consumer runs only when every input
object is installed on its node, regardless of how the bytes arrived. A node may
not appear in both `copies` and `evict` of one entry. The last installed copy of
an object is never evicted while a consumer may still need it. A newer revision
supersedes a pending one; a partial object is never exposed.

---

## Repository layout

```
wayline/
├── api/v1/                       # CRDs: odags.wl.io, odagtemplates.wl.io
├── cmd/
│   ├── odag-controller/          # controller: scheduling, dispatch, realization reconciler
│   │   ├── realization.go        #   spec.realization → copies / servingCopy / evict
│   │   ├── datavertex.go         #   pod-less data vertices and serving-point rebinding
│   │   ├── saga.go, heft.go      #   SAGA bridge and built-in HEFT
│   │   └── profiler.go, cache.go #   EMA profiler, cross-run reuse (cacheKey)
│   ├── data-agent/               # per-node DaemonSet: object store + peer-to-peer transfers
│   ├── ui-server/                # REST + SSE + embedded React UI
│   └── cli/                      # `wayline` CLI (cobra, kubectl-style)
├── pkg/scheduler/                # scheduler interface
├── sdk/python/wl/                # Python SDK: `from wl import WlTask`
├── ui/                           # React + Vite frontend
├── deployments/                  # namespace, RBAC, Deployments, DaemonSet
├── examples/                     # ODAG examples (dag-pipeline, named-outputs, wide-pipeline, …)
├── eval/
│   ├── experiments/E0…E8/        # the paper's evaluation: scripts, policies, committed results
│   ├── policies/                 # scheduling-policy comparison (HEFT, CPoP, MinMin, OLB, …)
│   └── …                         # earlier microbenchmarks and baselines
├── docs/                         # architecture, getting started, SDK, bring-your-own-scheduler
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
| `python3` | Policies, evaluation scripts, and the SAGA scheduler sidecar |

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

Templates are the usual way to run: `wayline apply -f examples/named-outputs/template.yml`
registers an `ODAGTemplate`, and `wayline run named-outputs` creates a run from it.
The UI is at `http://<master-ip>:30080`. A longer walkthrough is in
[`docs/getting-started.md`](docs/getting-started.md).

---

## Writing tasks

Tasks are ordinary container images. Inside, use the `wl` SDK; the controller
injects all peer/topology configuration as `WL_*` environment variables.

```python
from wl import WlTask

task = WlTask()                       # reads WL_* env vars

inputs = task.recv_all()              # {dep_name: payload}; local file reads
result = process(inputs)
task.send(result)                     # default output, delivered to every successor
```

A task may emit several **named outputs**, each an independent object with its
own consumers and its own realization. Names must be declared in the task's
`spec.outputs`, and consumers pick one in `spec.inputs`:

```python
task.send(result)                     # default output
task.send("alert", alert)             # named output <run, task, alert>
task.send_raw("features", features)   # bytes, no JSON copy

alert = task.recv("infer.alert")      # a named object of an upstream task
```

`send` / `recv` accept any JSON-serializable value; `send_raw` / `recv_raw`
move bytes. See [`docs/sdk-quickstart.md`](docs/sdk-quickstart.md) and
[`examples/named-outputs`](examples/named-outputs/).

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

## Warm runners

A task normally runs in its own pod, which costs about a second of pod
startup per task (E9). For short tasks that overhead dominates. A task can
instead run as a call on a **warm runner**: a long-lived process on each
node that executes invocations without creating a pod. Add one field:

```yaml
  - name: resize
    image: <registry>/my-runner:latest   # the runner's image
    runner: images                       # run on the "images" runner
    function: resize                     # optional; defaults to the task name
    dependencies: [capture]
```

Nothing else changes. The scheduler places the task as usual, the
controller invokes it on the runner of the chosen node with the same
environment the pod would have had, and the task uses the same SDK and data
agent (inputs, named outputs, pushes, timings). Pod and warm tasks can be
mixed in one DAG.

A runner is any pod labeled `wl.io/runner=<name>` that serves the runner
API on port 8090 and mounts `/data/wl-outputs` from the host. The SDK
provides one. Either register functions:

```python
import wl

@wl.function
def resize(task):              # receives a ready WlTask; close() is automatic
    img = task.recv_raw()
    task.send_raw(shrink(img))

wl.serve()
```

or serve an existing task script unchanged:

```bash
python -m wl.runner --script task.py --preload numpy --cpus 7.4
```

The runner forks a zygote once all imports are done. The zygote forks one
child per invocation, so a call starts in milliseconds, CPU-bound calls run
in parallel, and a crash fails only that task. Calls are admitted by their
CPU requests, as the kubelet admits pods: a call starts once its request
fits beside the running ones within `--cpus`, and the rest queue. A
DaemonSet is the usual way to run one per node; see
`eval/experiments/E12/runner.yml`.

`--preload` imports run with one OpenMP/BLAS thread: OpenMP (torch's CPU
kernels) is not fork-safe, and a thread pool started while preloading
(building a model runs parallel kernels) would leave every forked call
waiting on it forever. Each call then computes with as many threads as CPUs
it requested, unless it sets its own.

Limits today: a task's `command` is ignored in warm mode.

---

## CPU requests and concurrency

Every task has a CPU request: its own `resources.cpu`, else the template's
`defaults.resources.cpu`, else 1 CPU. The pod requests exactly that, and
schedulers plan with it: a node's capacity is its free CPU (allocatable
minus what non-Wayline pods request), and tasks share a node while their
requests fit. The controller admits pods and warm calls against one account
per node, and a task's CPU is released when the task reports it has
finished, not when the kubelet later marks its pod Succeeded. Wayline's built-in `heft` and every SAGA scheduler plan this
way, so a 1-CPU task and a 3-CPU task can run side by side on a 4-CPU node,
and a schedule's times assume they do.

That keeps each task's runtime known only if a node runs as fast with many
tasks as with one. See `docs/limitations-and-future-work/cpu-clock-under-load.md`.

---

## ODAG reference

An `ODAGTemplate` is a reusable spec; `wayline run <template>` creates an `ODAG`
run from it. Both share the task schema below.

```yaml
apiVersion: wl.io/v1
kind: ODAGTemplate
metadata:
  name: my-dag
  namespace: wl-system
spec:
  scheduler: heft                 # random | heft | saga/<algo> | saga/<pkg.Class> | http://host:port
  schedulerConfig:
    spreadEpsilon: 0              # HEFT tie-break: spread parallel layers (0 = off)
  retryPolicy:
    maxRetries: 2
  defaults:                       # fallbacks for tasks that omit the hints
    runtime: 3
    dataSize: 1MB
  retention:                      # garbage collection of old runs and their data
    maxRuns: 10
    data:
      policy: keepLatest          # immediate | delayed | keepLatest | none
      keepRuns: 2
  tasks:
    - name: produce
      image: 192.168.1.163:5000/my-produce:latest
      command: [python, task.py]
      dependencies: []
      runtime: 30                 # seed estimate; refined by the EMA profiler
      dataSize: 300MB             # default output size, for the scheduler's cost model
      outputs:                    # optional named outputs
        - { name: alert,    dataSize: 1KB }
        - { name: features, dataSize: 50MB }
      constraints:
        nodeNames: [anrg-3]       # restrict placement
    - name: actuator
      image: 192.168.1.163:5000/my-actuator:latest
      command: [python, task.py]
      dependencies: [produce]
      inputs:                     # which object of the producer; default output if omitted
        - { producer: produce, object: alert }
      resources: { cpu: "200m", memory: "128Mi" }
    - name: stage                 # a pod-less data vertex: holds / forwards bytes, runs no container
      type: data
      dependencies: [produce]
    - name: analyze
      image: 192.168.1.163:5000/my-analyze:latest
      command: [python, task.py]
      dependencies: [stage]
      cacheKey: analyze-v3        # opt-in cross-run reuse of this task's output
```

| Task field | Meaning |
|---|---|
| `type` | `compute` (default) runs the container; `data` is a vertex the controller realizes through the agent (alias + push) with no pod. A data vertex has exactly one dependency and at least one successor. |
| `outputs[]` / `inputs[]` | Named outputs and which one a consumer takes. Producers listed in `dependencies` without an `inputs` entry supply their default output. |
| `runtime`, `dataSize` | Scheduler hints; the profiler refines them per `(task, node)` across runs of a template. |
| `constraints.nodeNames` | Placement restriction. |
| `cacheKey` | If an earlier run produced this task's output under the same key and the copy is still installed, the task does not execute: the controller pins it to the node holding the copy, aliases the bytes under this run's name, and serves consumers from there. Requires a retention policy that keeps run data. |

| Status field | Description |
|---|---|
| `status.phase` | `Pending → Scheduling → Running → Succeeded / Failed` |
| `status.makespan` | Wall-clock makespan in seconds |
| `status.tasks[]` | Per-task `phase`, agent-reported `state`, `node`, `podName`, `startTime`, `completionTime`, `retries`, `cachedFrom` |
| `status.objects[]` | Per revised object: `copies[{node, state}]` (`Transferring` / `Installed` / `Evicted`) and `servingCopy`, as actually reconciled |

The full schema, with every field's description, is in
[`api/v1/odag-crd.yml`](api/v1/odag-crd.yml) and
[`api/v1/odagtemplate-crd.yml`](api/v1/odagtemplate-crd.yml).

---

## Revising a realization at runtime

`spec.realization` on a **live run** is the policy interface. Each entry names an
object and states the desired copies, serving point, and evictions; the
controller converges the data plane toward it using only agent verbs, and reports
the result in `status.objects`. The DAG, the pods, and task placements are never
touched.

```bash
kubectl -n wl-system patch odag my-dag-run-abc12 --type merge -p '{
  "spec": {"realization": [
    {"object": "produce",          "copies": ["anrg-7"],          "servingCopy": "anrg-7"},
    {"object": "produce.features", "copies": ["anrg-7","anrg-8"], "evict": ["anrg-3"]}
  ]}}'
```

| Field | Semantics |
|---|---|
| `object` | Producing task, or `task.output` for a named output |
| `copies` | Nodes that must hold a valid copy. Additive: copies elsewhere are left alone unless named in `evict`. |
| `servingCopy` | Node whose copy serves future consumer installs and data-vertex execution. Empty keeps the producer's copy. |
| `evict` | Nodes whose copy must be removed. Refused for the last copy while consumers may need it. |

What the controller does with it:

1. **Copies.** For each missing copy it picks a source (the serving copy if its
   bytes are valid, else any valid desired copy, else wherever the object lives)
   and asks that node's agent to push. A pending or active transfer is polled, not
   re-posted; a failed one is retried on a later pass, which is how a copy lands
   when a contact window reopens.
2. **Serving point.** A data vertex whose input names a `servingCopy` executes
   from that node instead of its assigned one, and the old node's outbound
   transfers of the object are cancelled, so a revised path does not compete with
   the flows it replaces. Consumers are gated on any valid copy on their node, so
   a copy that arrived from the new serving point satisfies them.
3. **Eviction.** Per-object delete on the named agents, subject to the last-copy
   rule.

A revision can be issued before the producer runs, while transfers are in
flight, or after the producer has exited. Sample policies (a few dozen lines of
Python each, driven by measurements or signals) are in
`eval/experiments/E1/policy.py` (serving-point rebinding on a degradation
signal), `E2` (temporal relay across contacts), and `E3/policy.py` (risk-aware
replication on a health signal).

---


### Durability

Each object is either durable (fsynced to storage on every node that holds
it, so it survives a crash) or not (written atomically, so no reader ever
sees a partial object, but a power loss can lose it and its task reruns).
Syncing every object is expensive on edge storage: on the testbed's eMMC,
one sync can wait for other files' large writes, and E13 measured 7 to 11 s
of stalls on a run's critical path.

`spec.durability` sets the policy:

| value | durable objects |
|---|---|
| `auto` (default) | a run's final outputs (objects nothing consumes) and outputs with a `cacheKey` |
| `all` | every object |
| `none` | none |
| `node` | each data agent's `--sync` default decides |

`tasks[].durable` and `tasks[].outputs[].durable` override the policy for
one task or one output. A running policy can also make an object durable
after the fact:

```yaml
spec:
  realization:
  - object: fuse.tracks
    durable: true      # sync every installed copy now; later copies inherit it
```

The decision travels with the object: the SDK marks each install, and the
agents forward the mark on every copy they push.

## Schedulers and policies

`spec.scheduler` accepts four forms:

| Value | Behaviour |
|---|---|
| `heft`, `random` | Compiled-in schedulers |
| `saga/<name>` | A built-in [SAGA](https://github.com/ANRGUSC/saga) algorithm (`heft`, `cpop`, `peft`, `minmin`, `maxmin`, `sufferage`, …) run in the SAGA sidecar |
| `saga/<pkg.Class>` | Any importable `saga.Scheduler` subclass, so a scheduler validated in simulation runs here unchanged (`WL_SAGA_PATH` / `WL_SAGA_EXTRA_PACKAGES` on the sidecar) |
| `http(s)://host:port` | Any service implementing the scheduler contract, in any language |

The controller passes the scheduler task and node properties, profiled costs,
output sizes, network rates, and current data locations, and enacts the returned
placement and per-node order. On an external scheduler's failure it falls back to
built-in HEFT. See [`docs/bring-your-own-scheduler.md`](docs/bring-your-own-scheduler.md).

Placement (which node runs a task) and realization (how its outputs are served)
are separate decisions in separate spec fields; a policy may control either or
both.

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
as hidden aliases. Realization revisions are plain `kubectl patch` calls on the
run object (see above); the CLI does not wrap them.

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

Live updates arrive via Server-Sent Events (`/api/events`).
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

The data-agent DaemonSet binds **hostPort 8082** on every node and writes
node-local objects under `/data/wl-outputs`. The prototype data-agent image is
x86-64 only.

---

## Reproducing the evaluation

Every experiment in the paper is a directory under
[`eval/experiments`](eval/experiments/) with its harness (`*_paper.py` or
`*_pilot.py`, run from the control node), the network treatment it applies
(`*_net.sh`, `contacts.sh`: `tc` shaping on the sender's egress), the policy it
exercises, an analysis script, and the committed per-run results (`runs.csv`,
run objects, flow records, controller logs, provenance).

| Directory | Capability shown | Results |
|---|---|---|
| `E0/` | Clean-testbed characterization: goodput and RTT for every node pair | `results/` |
| `E1/` | Live serving-point revision under a degraded producer uplink (fixed, adaptive-late, adaptive-early, static oracle; B/2 … B/16) | `results-paper/` |
| `E2/` | Temporal relay across disjoint contacts; the relay runs no task | `results-paper/` |
| `E3/` | Risk-aware replication before source-copy loss | `results-paper/` |
| `E4/` | Per-object control with named outputs: equal-sized objects respond differently to rebinding | `results-paper/` |
| `E5/` | Policy enactment: HEFT, MaxTP, and OLB placements executed with per-node order fixed, direct vs store-mediated; the broader six-policy sweep and the Argo + MinIO referents are in `eval/policies` | `results-paper/` |
| `E6/` | Applications: AI City multi-camera pipeline (Part B) and seven WfChef scientific-workflow structures (Part A), schedule-matched direct vs store-mediated | `results-paper/`, `partA/results-paper/` |
| `E7/` | Reconciliation under faults: controller and agent restarts, superseding and conflicting revisions, last-copy eviction | `results-paper/` |
| `E8/` | Control-plane overhead and scaling: idle cost, object and copy count, concurrent transfers, concurrent runs | `results-campaign/` |

All measurements need the 8-worker x86 k3s testbed (an AI City clip manifest for
E6 Part B). Analysis scripts (`analyze_*.py`, `E8/plot.py`) regenerate the tables
and figures from the committed results without a cluster.
[`eval/policies`](eval/policies/) holds the scheduling-policy comparison, and the
older `eval/e0-microbench`, `eval/mcmt`, `eval/synthetic-dags`, and `eval/stress`
directories hold the earlier data-plane microbenchmarks and baselines
(`make repro-figures` regenerates those).

---

## Troubleshooting

**Controller pod is Pending.** The control plane targets the master node, which
is often `SchedulingDisabled`. The Deployment carries a toleration for
`node.kubernetes.io/unschedulable:NoSchedule` and a `nodeSelector` for the master
hostname; adjust both to your cluster (`deployments/odag-controller/deployment.yml`).

**Task image pull fails.** Confirm the registry container is up
(`docker ps | grep registry`) and that `/etc/rancher/k3s/registries.yaml` exists on
the worker and k3s-agent was restarted after writing it.

**An ODAG hangs / a task never starts.** A task pod starts only when every input
object is installed on its node. Check the controller, the relevant data agent,
and, if a revision is in flight, `status.objects`:

```bash
kubectl logs -n wl-system deployment/odag-controller --tail=40 | grep -e realize -e vertex
kubectl logs -n wl-system -l app=data-agent --tail=40
kubectl -n wl-system get odag <run> -o jsonpath='{.status.objects}'
kubectl get pods -l wl-odag=<name>
```

**A revision does not converge.** `status.objects` shows a copy stuck in
`Transferring`: the push toward that node is failing (no route, agent down). The
reconciler keeps retrying a failed transfer every 5 s and logs
`refusing to evict last copy` or `node … in both copies and evict` when it
declines an entry.

---

## License

MIT — see [LICENSE](LICENSE).
