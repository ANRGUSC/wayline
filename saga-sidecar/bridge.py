"""
SAGA <-> Wayline bridge core.

Converts a Wayline scheduling request (the JSON contract from
sdk/python/wl/scheduler.py: {"dag": ..., "clusterState": ...}) into SAGA's
Network/TaskGraph model, runs the named SAGA scheduler, and returns a
Wayline-shaped response: {"assignments": [...], "estimatedMakespan": ...}.

Only the task -> node mapping is load-bearing on the Wayline side (dispatch
is data-readiness-driven), mirroring ncsim's saga_adapter, which also keeps
only the placement and discards SAGA's predicted times.

Model-conversion rules (each guards a known SAGA trap):

  * Runtimes. Wayline sends a per-(task, node) runtime matrix. It goes to
    SAGA as each task's exact per-node runtimes (TaskGraphNode.runtimes),
    so every scheduler plans with the true runtime on every node, separable
    or not. Each task's cost and each node's speed are also set to the
    best rank-1 fit in log space, used only where a heuristic reads a cost
    or speed on its own; "costModelFitRMSE" reports that fit's residual.

  * Network completeness. A missing SAGA edge defaults to speed 0.0 and
    comm time = size/speed divides by zero. We always emit every unordered
    pair, plus explicit self-loops with a large finite speed (1e12 B/s) --
    finite, not math.inf, because inf produces NaNs in SAGA's stochastic
    paths.

  * Symmetry. SAGA networks are undirected; Wayline bandwidth matrices may
    be asymmetric. We take min(bw(u,v), bw(v,u)) -- conservative.
    LIMITATION: SAGA's Network is undirected, so a directed matrix cannot
    be represented faithfully. Experiments that compare SAGA algorithms
    should therefore use symmetric link treatments, and any asymmetric
    result must be reported as scheduling under a conservative
    lower-bound model rather than under the true fabric.

  * Super nodes. TaskGraph.create() silently injects __super_source__ /
    __super_sink__ for multi-source/multi-sink DAGs. They are stripped from
    the returned mapping.

  * Capacity. A node's capacity is its free CPU in cores (allocatable
    minus what non-Wayline pods request) and a task's demand is its CPU
    request in cores, so tasks share a node while their requests fit, as the
    kubelet admits them. Every task has a request (the controller defaults
    it). A request without node CPU figures falls back to the classic model:
    capacity 1, demand 1, one task at a time per node.

  * Node model (request "model", optional; absent = everything above).
    {"nodeModel": "cores"} makes each machine C SAGA nodes of capacity 1
    named "<machine>/c<i>" (C = model.cores[machine], else floor(cpuMillis
    / 1000)), each task demand 1: the classic one-task-per-processor model
    of DAG scheduling. Assignments still name the machine (what the
    controller places on) and add "core". model.selfBandwidth (a SAGA
    node's self-loop: consecutive tasks on one core / one node) and
    model.intraBandwidth (between cores of one machine), each a number or
    {machine: bytes/s}, replace the 1e12 default. With the PyPI SAGA
    (no capacity model) only "cores" mode is accepted, and only when the
    runtimes fit cost/speed exactly, since SAGA then plans with cost/speed.

  * Constraints. A task's allowed nodes go to SAGA as
    TaskGraphNode.allowed_nodes, and every scheduler chooses only among
    them. The result is verified anyway; a violation fails the request.
    Placements are never moved after scheduling, so the estimate returned
    is the schedule that runs.
"""

from __future__ import annotations

import collections
import importlib
import importlib.metadata
import logging
from typing import Any, Dict, List, Optional, Tuple

import math

import numpy as np

from saga import Network, NetworkNode, Schedule, Scheduler, TaskGraph, TaskGraphNode

logger = logging.getLogger("saga-sidecar")

SUPER_NODES = ("__super_source__", "__super_sink__")
LOCAL_SPEED = 1e12  # bytes/sec for self-loops; large finite, never math.inf
MIN_BANDWIDTH = 1.0  # bytes/sec floor so comm cost stays finite
MIN_RUNTIME = 1e-6  # seconds floor so log() stays finite
CORE_SEP = "/c"  # "<machine>/c<i>"; "/" cannot occur in a Kubernetes node name
EXACT_FIT_RMSE = 1e-9

# SAGA's capacity model (demand, exact per-node runtimes, allowed nodes,
# node capacity) is on branch feature/capacity. Older SAGA silently ignores
# those fields, so the bridge must know whether they take effect.
SAGA_HAS_CAPACITY = (
    all(f in TaskGraphNode.model_fields for f in ("demand", "runtimes", "allowed_nodes"))
    and "capacity" in NetworkNode.model_fields
)


# ---------------------------------------------------------------------------
# Scheduler resolution
# ---------------------------------------------------------------------------
# Two ways to name a scheduler:
#
#   "heft"                        a short name from the built-in registry below
#   "mypkg.schedulers.MyHeft"     any importable saga.Scheduler subclass
#
# The second form is what makes porting a scheduler zero-effort: a researcher
# writes and validates a Scheduler subclass against SAGA, makes it importable
# in the sidecar (see WL_SAGA_PATH / WL_SAGA_EXTRA_PACKAGES in server.py), and
# names its dotted path in spec.scheduler. No entry here, no image rebuild, no
# Go code.
#
# SECURITY: resolving a dotted path imports and executes that module inside
# the sidecar. The sidecar is a trusted component of the control plane, on the
# same footing as the data-agent, and only code the cluster operator has
# installed is importable. This is not a sandbox for untrusted schedulers.
#
# The built-in registry is a convenience alias table, not a gate: every entry
# is a static batch scheduler constructible with no arguments.

def _builtin_registry() -> Dict[str, "Scheduler"]:
    from saga.schedulers import (
        CpopScheduler,
        DuplexScheduler,
        ETFScheduler,
        FastestNodeScheduler,
        HeftScheduler,
        MaxMinScheduler,
        MCTScheduler,
        METScheduler,
        MinMinScheduler,
        OLBScheduler,
        PEFTScheduler,
        SufferageScheduler,
        WBAScheduler,
    )

    registry = {
        "heft": HeftScheduler(),
        "cpop": CpopScheduler(),
        "peft": PEFTScheduler(),
        "minmin": MinMinScheduler(),
        "maxmin": MaxMinScheduler(),
        "sufferage": SufferageScheduler(),
        "mct": MCTScheduler(),
        "met": METScheduler(),
        "olb": OLBScheduler(),
        "etf": ETFScheduler(),
        "duplex": DuplexScheduler(),
        "wba": WBAScheduler(),
        "fastest_node": FastestNodeScheduler(),
        "cpop_ranking": CpopScheduler(),  # alias kept for experiment scripts
    }
    try:
        from saga.schedulers.contention import ContentionHeftScheduler, RoutingHeftScheduler
    except ImportError:  # SAGA without feature/contention-aware: the rest still work
        logger.warning("saga.schedulers.contention not installed; contention_heft/routing_heft unavailable")
    else:
        # One transfer at a time per node interface; routing also relays via
        # intermediate nodes (SAGA feature/contention-aware, feature/routing-aware).
        registry["contention_heft"] = ContentionHeftScheduler()
        registry["routing_heft"] = RoutingHeftScheduler()
    return registry


_SCHEDULERS: Optional[Dict[str, Scheduler]] = None


def available_algorithms() -> List[str]:
    global _SCHEDULERS
    if _SCHEDULERS is None:
        _SCHEDULERS = _builtin_registry()
    return sorted(_SCHEDULERS.keys())


def _load_by_path(path: str, options: Optional[Dict[str, Any]] = None) -> Scheduler:
    """Import and instantiate a Scheduler subclass named by dotted path.

    "pkg.module.ClassName" -> instance. Constructor keyword arguments come
    from options, so a parameterised scheduler (e.g. one taking alpha or a
    lookahead depth) is configurable from the ODAG spec without code changes.
    """
    module_path, _, class_name = path.rpartition(".")
    if not module_path or not class_name:
        raise KeyError(
            f"{path!r} is not a dotted path to a class "
            "(expected e.g. 'mypkg.schedulers.MyScheduler')"
        )
    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        raise KeyError(
            f"cannot import {module_path!r} for scheduler {path!r}: {e}. "
            "Install it in the sidecar via WL_SAGA_EXTRA_PACKAGES or mount it "
            "on WL_SAGA_PATH."
        ) from e
    try:
        cls = getattr(module, class_name)
    except AttributeError as e:
        raise KeyError(f"module {module_path!r} has no attribute {class_name!r}") from e
    if not (isinstance(cls, type) and issubclass(cls, Scheduler)):
        raise KeyError(
            f"{path!r} is not a saga.Scheduler subclass (got {cls!r}); a "
            "scheduler must implement schedule(network, task_graph) -> Schedule"
        )
    try:
        return cls(**(options or {}))
    except TypeError as e:
        raise KeyError(f"cannot construct {path!r} with options {options!r}: {e}") from e


def get_scheduler(name: str, options: Optional[Dict[str, Any]] = None) -> Scheduler:
    """Resolve a scheduler by short name or dotted path.

    Short names come from the built-in registry and ignore options unless the
    class accepts them; a dotted path is imported on demand. Dotted paths are
    not cached, so redeploying a scheduler package takes effect on the next
    request without restarting the sidecar.
    """
    global _SCHEDULERS
    if _SCHEDULERS is None:
        _SCHEDULERS = _builtin_registry()
    if name in _SCHEDULERS and not options:
        return _SCHEDULERS[name]
    if "." in name:
        return _load_by_path(name, options)
    if name in _SCHEDULERS:
        # Known name with options: re-instantiate so the options apply.
        return _load_by_path(
            type(_SCHEDULERS[name]).__module__ + "." + type(_SCHEDULERS[name]).__name__,
            options,
        )
    raise KeyError(
        f"unknown algorithm {name!r}; built-ins: {', '.join(sorted(_SCHEDULERS))}. "
        "For a scheduler of your own, give its dotted path "
        "(e.g. 'mypkg.schedulers.MyScheduler')."
    )


def saga_version() -> str:
    """Installed anrg-saga version, reported with every schedule so a run
    records which SAGA planned it (offline replays must match it)."""
    try:
        return importlib.metadata.version("anrg-saga")
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


# ---------------------------------------------------------------------------
# Request parsing
# ---------------------------------------------------------------------------

def _parse_data_size(s: Any) -> float:
    """Parse '100MB' / '1GB' / bare numbers to bytes (same rules as the SDK)."""
    if s is None:
        return 0.0
    if isinstance(s, (int, float)):
        return float(s)
    s = str(s).strip().upper()
    if not s:
        return 0.0
    units = {"B": 1, "KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12}
    for suffix, mult in sorted(units.items(), key=lambda x: -len(x[0])):
        if s.endswith(suffix):
            return float(s[: -len(suffix)]) * mult
    return float(s)


def _runtime_matrix(
    tasks: List[dict], node_names: List[str]
) -> np.ndarray:
    """RT[i, j] = runtime of task i on node j (seconds), from runtimeProfile
    with fall-through to the scalar runtime hint, floored at MIN_RUNTIME."""
    rt = np.full((len(tasks), len(node_names)), 0.0)
    for i, t in enumerate(tasks):
        profile = t.get("runtimeProfile") or {}
        scalar = float(t.get("runtime") or 0.0)
        for j, n in enumerate(node_names):
            v = float(profile.get(n, scalar) or scalar)
            rt[i, j] = max(v, MIN_RUNTIME)
    return rt


def _rank1_fit(rt: np.ndarray) -> Tuple[np.ndarray, np.ndarray, float]:
    """Best separable fit runtime(t,n) ~= cost_t / speed_n, in log space.

    Returns (costs[t], speeds[n], rmse) where rmse is the log-space residual
    RMSE — 0.0 when the matrix is exactly separable (e.g. uniform runtimes).
    """
    L = np.log(rt)
    grand = float(L.mean())
    log_cost = L.mean(axis=1)  # per task
    log_speed = grand - L.mean(axis=0)  # per node
    pred = log_cost[:, None] - log_speed[None, :]
    rmse = float(np.sqrt(np.mean((L - pred) ** 2)))
    return np.exp(log_cost), np.exp(log_speed), rmse


def _per_machine(value: Any, machine: str, default: float) -> float:
    """A model setting given as a number or as {machine: number}."""
    if value is None:
        return default
    if isinstance(value, dict):
        return float(value.get(machine, default))
    return float(value)


def machine_of(saga_node: str) -> str:
    """SAGA node -> the machine it belongs to ("m/c3" -> "m"; "m" -> "m")."""
    return saga_node.rpartition(CORE_SEP)[0] if CORE_SEP in saga_node else saga_node


def build_saga_models(
    dag: dict, cluster_state: dict, model: Optional[dict] = None
) -> Tuple[TaskGraph, Network, List[str], float]:
    """Convert the Wayline request into SAGA TaskGraph + Network.

    Returns (task_graph, network, saga_node_names, cost_model_rmse). Without
    a model (or nodeModel "capacity") the SAGA nodes are the machines; with
    nodeModel "cores" they are "<machine>/c<i>" (see machine_of).
    """
    model = model or {}
    node_model = model.get("nodeModel", "capacity")
    if node_model not in ("capacity", "cores"):
        raise ValueError(f"unknown model.nodeModel {node_model!r}; use 'capacity' or 'cores'")
    cores_mode = node_model == "cores"
    tasks: List[dict] = dag["tasks"]
    nodes = [n for n in cluster_state["nodes"] if n.get("ready", True)]
    if not nodes:
        raise ValueError("no ready nodes in clusterState")
    machines = [n["name"] for n in nodes]

    rt = _runtime_matrix(tasks, machines)
    costs, speeds, rmse = _rank1_fit(rt)
    if not SAGA_HAS_CAPACITY:
        # Old SAGA ignores capacity/demand/runtimes: only a capacity-1 model
        # whose runtimes ARE cost/speed is planned faithfully.
        if not cores_mode:
            raise RuntimeError(
                "the installed SAGA has no capacity model (needs feature/capacity); "
                "request model.nodeModel 'cores' or install it")
        if rmse > EXACT_FIT_RMSE:
            raise RuntimeError(
                f"runtimes are not cost/speed (fit RMSE {rmse:.3g}) and the installed SAGA "
                "cannot take exact per-node runtimes (needs feature/capacity)")

    # SAGA nodes: (name, machine index, capacity)
    if cores_mode:
        saga_nodes = []
        for j, n in enumerate(nodes):
            default = math.floor(float(n.get("cpuMillis") or 0) / 1000.0)
            count = int(_per_machine(model.get("cores"), n["name"], default))
            if count < 1:
                raise ValueError(f"model.cores gives {n['name']!r} no cores")
            saga_nodes += [(f"{n['name']}{CORE_SEP}{i}", j, 1.0) for i in range(count)]
        capacity_known = True
    else:
        # Capacity in cores; classic model when node CPU is not reported.
        capacity_known = all(float(n.get("cpuMillis") or 0) > 0 for n in nodes)
        saga_nodes = [(n["name"], j, float(n["cpuMillis"]) / 1000.0 if capacity_known else 1.0)
                      for j, n in enumerate(nodes)]
    saga_names = [name for name, _, _ in saga_nodes]

    # --- TaskGraph ---------------------------------------------------------
    tg_nodes = []
    for i, t in enumerate(tasks):
        if cores_mode:
            demand = 1.0
        else:
            demand = float(t.get("cpuMillis") or 0) / 1000.0 if capacity_known else 1.0
            if capacity_known and demand <= 0:
                raise ValueError(f"task {t['name']!r} has no CPU request")
        allowed = _allowed_nodes(t, machines)
        tg_nodes.append(TaskGraphNode(
            name=t["name"],
            cost=float(costs[i]),
            demand=demand,
            runtimes={name: float(rt[i, j]) for name, j, _ in saga_nodes},
            allowed_nodes=({name for name, j, _ in saga_nodes if machines[j] in allowed}
                           if allowed is not None else None),
        ))
    tg_edges = []
    task_index = {t["name"]: i for i, t in enumerate(tasks)}
    for t in tasks:
        # Per-edge sizes when the consumer declares which named object it
        # takes from each producer; a producer of several named outputs
        # otherwise weighs every outgoing edge with its aggregate size.
        declared = {}
        for inp in t.get("inputs", []) or []:
            p = inp.get("producer")
            if p is None:
                continue
            declared.setdefault(p, 0.0)
            declared[p] += float(inp.get("bytes", 0) or 0)
        for dep in t.get("dependencies", []) or []:
            if dep not in task_index:
                raise ValueError(f"task {t['name']!r} depends on unknown task {dep!r}")
            if dep in declared:
                size = declared[dep]
            else:
                src = tasks[task_index[dep]]
                profile = src.get("dataSizeProfile") or {}
                if profile:
                    size = float(np.mean([float(v) for v in profile.values()]))
                else:
                    size = _parse_data_size(src.get("dataSize"))
            tg_edges.append((dep, t["name"], max(size, 0.0)))
    task_graph = TaskGraph.create(tasks=tg_nodes, dependencies=tg_edges)

    # --- Network -----------------------------------------------------------
    bw: Dict[Tuple[str, str], float] = {}
    for e in cluster_state.get("bandwidth", []) or []:
        bw[(e["from"], e["to"])] = float(e["bytesPerSec"])

    def inter(u: str, v: str) -> float:
        candidates = [x for x in (bw.get((u, v)), bw.get((v, u))) if x is not None and x > 0]
        return max(min(candidates) if candidates else MIN_BANDWIDTH, MIN_BANDWIDTH)

    net_nodes = [NetworkNode(name=name, speed=float(speeds[j]), capacity=cap)
                 for name, j, cap in saga_nodes]
    net_edges = []
    for k, (u, ju, _) in enumerate(saga_nodes):
        mu = machines[ju]
        # explicit self-loop, finite
        net_edges.append((u, u, _per_machine(model.get("selfBandwidth"), mu, LOCAL_SPEED)))
        for v, jv, _ in saga_nodes[k + 1 :]:
            if jv == ju:
                speed = _per_machine(model.get("intraBandwidth"), mu, LOCAL_SPEED)
            else:
                speed = inter(mu, machines[jv])
            net_edges.append((u, v, speed))
    for _, _, speed in net_edges:
        if not speed > 0 or math.isinf(speed):
            raise ValueError(f"link speeds must be finite and > 0, got {speed}")
    network = Network.create(nodes=net_nodes, edges=net_edges)

    return task_graph, network, saga_names, rmse


# ---------------------------------------------------------------------------
# Scheduling
# ---------------------------------------------------------------------------

def _allowed_nodes(task: dict, node_names: List[str]) -> Optional[List[str]]:
    constraints = task.get("constraints") or {}
    allowed = constraints.get("nodeNames")
    if not allowed:
        return None
    present = [n for n in allowed if n in node_names]
    return present or None


def schedule_request(request: dict) -> dict:
    """Handle one scheduling request. Raises on invalid input; the server
    turns exceptions into HTTP errors so the Go side can fall back."""
    algorithm = request.get("algorithm", "heft")
    options = request.get("options") or {}
    dag = request["dag"]
    cluster_state = request["clusterState"]
    tasks: List[dict] = dag["tasks"]
    if not tasks:
        return {"assignments": [], "estimatedMakespan": 0.0, "algorithm": algorithm}

    scheduler = get_scheduler(algorithm, options)
    model = request.get("model") or {}
    cores_mode = model.get("nodeModel") == "cores"
    task_graph, network, _, rmse = build_saga_models(dag, cluster_state, model)
    sched: Schedule = scheduler.schedule(network, task_graph)
    machine_names = [n["name"] for n in cluster_state["nodes"] if n.get("ready", True)]

    # mapping: node -> [ScheduledTask]; invert, strip super nodes. Placement
    # is by machine; in "cores" mode the SAGA node is one of its cores.
    placement: Dict[str, str] = {}
    saga_node: Dict[str, str] = {}
    times: Dict[str, Tuple[float, float]] = {}
    for node_name, scheduled in sched.mapping.items():
        for st in scheduled:
            if st.name in SUPER_NODES:
                continue
            placement[st.name] = machine_of(node_name)
            saga_node[st.name] = node_name
            times[st.name] = (float(st.start), float(st.end))

    missing = [t["name"] for t in tasks if t["name"] not in placement]
    if missing:
        raise RuntimeError(f"{algorithm} left tasks unassigned: {missing}")

    # Verify, never repair: moving a task after scheduling would make the
    # returned estimate describe a schedule that does not run.
    violations = []
    for t in tasks:
        allowed = _allowed_nodes(t, machine_names)
        if allowed is not None and placement[t["name"]] not in allowed:
            violations.append(f"{t['name']} on {placement[t['name']]} (allowed {sorted(allowed)})")
    if violations:
        raise RuntimeError(f"{algorithm} violated placement constraints: {violations}")

    makespan = float(sched.makespan) if placement else 0.0
    assignments = []
    for t in tasks:
        a = {
            "task": t["name"],
            "node": placement[t["name"]],
            "estimatedStart": times.get(t["name"], (0.0, 0.0))[0],
            "estimatedFinish": times.get(t["name"], (0.0, 0.0))[1],
        }
        if cores_mode:
            a["core"] = saga_node[t["name"]]
        assignments.append(a)
    result = {
        "assignments": assignments,
        "estimatedMakespan": makespan,
        "algorithm": algorithm,
        "costModelFitRMSE": rmse,
        "sagaVersion": saga_version(),
    }
    ready = [n for n in cluster_state["nodes"] if n.get("ready", True)]
    if cores_mode:
        result["capacityModel"] = "cores"
    else:
        result["capacityModel"] = "cpu" if all(float(n.get("cpuMillis") or 0) > 0 for n in ready) else "classic"
    return result
