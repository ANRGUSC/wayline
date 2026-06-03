# Wayline Architecture Reference

This document describes Wayline's internal design. For a quick start see
[../README.md](../README.md); for a hands-on walkthrough see
[getting-started.md](getting-started.md).

Wayline is a thin scheduling layer on top of Kubernetes that adds a **decoupled
data plane**: it separates *task completion* from *data availability* and exposes
the separation as scheduler-visible runtime state. This release covers one-shot
DAGs (ODAGs); continuous DAGs are future work.

---

## 1. Components

| Component | Kind | Role |
|---|---|---|
| `odag-controller` | Deployment (master) | Watches ODAG/ODAGTemplate CRs, schedules tasks (HEFT), creates task pods, advances status. |
| `data-agent` | DaemonSet (all nodes) | The data plane. Stores node-local intermediates, ships them peer-to-peer, tracks readiness. HTTP API on hostPort **8082**. |
| `ui-server` | Deployment (master) | K8s watch cache + SQLite run history + REST/SSE + embedded React UI. NodePort 30080. |
| `wayline` | CLI | kubectl-style client (`apply`/`get`/`status`/`logs`/`delete`/`run`). |
| `wl` (Python) | SDK | `WlTask` — the in-task `send`/`recv` API. |

CRDs live in `api/v1/` under group **`wl.io/v1`**: `ODAG` and `ODAGTemplate`.
The control plane runs in the **`wl-system`** namespace.

---

## 2. The data plane (data-agent)

The core idea: a task's output never transits a central artifact store. Instead
the per-node data-agent owns node-local storage under `/data/wl-outputs` and moves
data **directly** between the nodes that produce and consume it.

### Producer side (from the SDK `FileTransport`)

1. **Local install.** The task `PUT`s its output to the *local* agent at
   `PUT /<odag>/<task>/output`. The agent installs it atomically — write to a
   temp file → `fsync` → `rename` → `fsync` parent dir → write the `.wl-sha256`
   content hash → set the `.wl-ready` marker last. The marker is therefore never
   visible before the bytes are durable.
2. **Remote handoff.** The task `POST`s the list of successors+hosts to
   `POST /push/<odag>/<task>`; the local agent streams the output to each
   successor node's agent (agent-to-agent install, same atomic + content-addressed
   path, idempotent on the receiver).
3. **Return.** The task returns immediately and the pod may exit — completion is
   defined by *local handoff*, not by remote delivery.

### Consumer side

`recv()` / `recv_all()` is **always a local file read.** The controller starts a
task pod only once every upstream dependency's `.wl-ready` marker is present on
that pod's node, so the inputs are guaranteed local before the container runs.

### Data-agent HTTP API

| Endpoint | Purpose |
|---|---|
| `PUT /<odag>/<task>/output` | Install output on this node (atomic, content-addressed). |
| `GET /<odag>/<task>/output` | Read output (requires `.wl-ready` unless `?unsafe=1`). |
| `POST /push/<odag>/<task>` | Push local output to listed successor nodes. |
| `PUT /state/<odag>/<task>` | Report task lifecycle state. |
| `PUT /ready/<odag>/<task>` | Presence-only local-ready marker. |
| `GET /healthz` | Liveness probe. |

Wire headers: `X-Wayline-Content-SHA256`, `X-Wayline-Uncompressed-Length`
(payloads are gzip-framed). The agent is the **only writer** of `.wl-ready`.

---

## 3. State model

Two independent signals, both tracked by the data-agent and surfaced to the
controller:

- **Task lifecycle:** `Pending → Running → ComputeDone → Failed`.
- **Per-successor transfer:** `Pending → Transferring → ReadyRemote`, with a
  node-local `ReadyLocal` (`.wl-ready`) marker for same-node consumers.

Separating these is what lets the scheduler reason about *where data already is*
rather than only *whether a task finished*. The SDK never writes `Failed` for a
transfer error — only the agent owns transfer/readiness state.

---

## 4. Controller

`odag-controller` runs a watch + reconcile loop:

1. **Schedule.** Build the task DAG, run HEFT placement (§5), honoring any
   per-task `constraints.nodeNames`.
2. **Gate on data.** Create a task pod only when all upstream `.wl-ready` markers
   are present on its assigned node.
3. **Inject the env contract.** Each task pod receives the `WL_*` variables:
   `WL_ODAG_NAME`, `WL_TASK_NAME`, `WL_OUTPUT_DIR`, `WL_DEPS`, `WL_SUCCESSORS`,
   `WL_NODE_IP`, `WL_DEP_<DEP>_NODE`, `WL_SUCC_<SUCC>_NODE`, `WL_SUCC_<SUCC>_HOST`,
   `WL_RUNTIME`, `WL_DATA_SIZE`, and (for template runs) `WL_TEMPLATE_NAME` /
   `WL_RUN_ID`.
4. **Advance status.** Update per-task phase/node and the ODAG `makespan` on
   completion.

Task pods carry the label `wl-odag=<name>` (and `wl-task=<task>`) so the CLI and
UI can find their pods/services.

---

## 5. Scheduling (HEFT)

Placement is HEFT (Heterogeneous Earliest Finish Time): rank tasks by upward
rank, then assign each to the node minimizing estimated finish time given a
node-availability timeline and a data-transfer cost derived from `dataSize` and a
per-edge bandwidth profile (`wl-network-profile` ConfigMap).

- **Profiling.** ODAGTemplates record actual per-`(task, node)` runtimes after
  each run and refine estimates with an EMA. Early runs use the spec's `runtime`
  seed.
- **Spread-aware tie-break.** `spec.schedulerConfig.spreadEpsilon` breaks ties to
  avoid over-packing a parallel layer onto the single fastest-profiled node
  (`0` = plain HEFT). A fully contention-aware completion-time model is future
  work.

`pkg/scheduler` holds the scheduling interface; the controller's implementation
lives in `cmd/odag-controller/{heft,profiler,schedule,bandwidth}.go`.

---

## 6. UI server

`ui-server` keeps an in-memory watch cache of ODAGs/ODAGTemplates, persists run
history to SQLite (`wl-history.db`), and serves a REST API under `/api/*` plus a
Server-Sent-Events stream at `/api/events` that invalidates the React Query cache
on any resource change. The compiled React app (`ui/dist`) is embedded into the
binary. See [local-dev.md](local-dev.md) for running it as a local binary.

---

## 7. Key design decisions

- **No central store on the critical path.** Intermediates move node-to-node;
  the artifact store (if any) is only a run-boundary archive, not a transfer hop.
- **Completion = local handoff.** A producer can exit as soon as its output is
  installed locally and remote pushes are queued, overlapping transfer with the
  rest of the DAG.
- **Atomicity + idempotence over coordination.** Content-addressed atomic installs
  and idempotent receivers make retries safe without a distributed lock.
- **Trusted single-domain edge clusters.** The agent is privileged and validates
  every URL-derived path component before constructing a filesystem path; this is
  not a hostile multi-tenant isolation model.
- **Distinct identifiers from Wayline.** `wl-system` / `wl.io` / `wl-odag` /
  `/data/wl-outputs` / port 8082 let Wayline coexist with a legacy Wayline cluster.
