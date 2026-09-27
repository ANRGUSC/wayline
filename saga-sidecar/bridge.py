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

  * Cost model. SAGA's schedulers compute a task's runtime on a node as
    task.cost / node.speed, a separable model that cannot represent an
    arbitrary per-(task, node) runtime matrix. Every scheduler goes through
    that one division, so the bridge makes it exact: each task's cost and
    each node's speed are float subclasses carrying their names, and the
    division returns the true runtime from Wayline's matrix. Their plain
    float values are the best rank-1 fit (log space), used wherever a
    scheduler reads a cost or speed on its own (ranking heuristics, sums).
    "costModelFitRMSE" reports that fit's log-space residual; it no longer
    affects the runtimes any scheduler plans with.

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

  * Slots. SAGA's machine model runs one task at a time per node. With
    "slots": "auto" a node becomes floor(allocatable CPU / task CPU)
    identical processors "<node>#<i>", joined by the local link, so the
    scheduler can overlap tasks on it; placements are folded back onto the
    real node. Exact only when every task requests the same CPU: mixed
    requests would need capacity-aware placement, which a node-splitting
    wrapper cannot give every SAGA scheduler, so they are rejected.

  * Constraints. Most SAGA schedulers do not read placement constraints.
    They are enforced through the same division: a forbidden (task, node)
    pair has a runtime longer than any feasible schedule, so every
    finish-time-driven scheduler avoids it by construction. The result is
    then verified; a scheduler that still places a task on a forbidden
    node fails the request loudly. Placements are never moved after
    scheduling, so the estimate returned is the schedule that runs.
"""

from __future__ import annotations

import collections
import importlib
import logging
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from saga import Network, Schedule, Scheduler, TaskGraph

logger = logging.getLogger("saga-sidecar")

SUPER_NODES = ("__super_source__", "__super_sink__")
LOCAL_SPEED = 1e12  # bytes/sec for self-loops; large finite, never math.inf
MIN_BANDWIDTH = 1.0  # bytes/sec floor so comm cost stays finite
MIN_RUNTIME = 1e-6  # seconds floor so log() stays finite
SLOT_SEP = "#"  # virtual processor "<node>#<i>"; '#' cannot occur in a k8s node name


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

    from constrained import ConstrainedETF, ConstrainedFastestNode, ConstrainedOLB

    return {
        "heft": HeftScheduler(),
        "cpop": CpopScheduler(),
        "peft": PEFTScheduler(),
        "minmin": MinMinScheduler(),
        "maxmin": MaxMinScheduler(),
        "sufferage": SufferageScheduler(),
        "mct": MCTScheduler(),
        "met": METScheduler(),
        "olb": ConstrainedOLB(),
        "etf": ConstrainedETF(),
        "duplex": DuplexScheduler(),
        "wba": WBAScheduler(),
        "fastest_node": ConstrainedFastestNode(),
        "cpop_ranking": CpopScheduler(),  # alias kept for experiment scripts
    }


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


def slot_counts(tasks: List[dict], nodes: List[dict]) -> Dict[str, int]:
    """Processors per node for slots=auto: floor(node CPU / task CPU).

    Every task must request the same, nonzero CPU; otherwise splitting a node
    into identical processors misstates what fits on it, so this raises.
    """
    demands = sorted({int(t.get("cpuMillis") or 0) for t in tasks})
    if len(demands) != 1 or demands[0] <= 0:
        raise ValueError(
            "slots=auto needs every task to request the same nonzero CPU "
            f"(resources.cpu); got millicores {demands}")
    d = demands[0]
    counts = {}
    for n in nodes:
        cap = int(n.get("cpuMillis") or 0)
        if cap <= 0:
            raise ValueError(f"slots=auto: node {n['name']!r} reports no allocatable CPU")
        counts[n["name"]] = max(1, cap // d)
    return counts


def real_node(name: str) -> str:
    """Fold a virtual processor name back onto its node."""
    return name.split(SLOT_SEP, 1)[0]


class _Cost(float):
    """A task's cost: a float (rank-1 fitted value) that, divided by a
    _Speed, returns the true runtime of this task on that node."""

    def __new__(cls, value: float, task: str, table: Dict[Tuple[str, str], float]):
        obj = float.__new__(cls, value)
        obj.task, obj.table = task, table
        return obj

    def __truediv__(self, other):
        if isinstance(other, _Speed):
            return self.table[(self.task, other.node)]
        return float.__truediv__(self, other)


class _Speed(float):
    """A node's speed: a float (rank-1 fitted value) naming its real node,
    so slot processors share their node's runtimes."""

    def __new__(cls, value: float, node: str):
        obj = float.__new__(cls, value)
        obj.node = node
        return obj

    def __rtruediv__(self, other):
        # Reached for a plain-float numerator (e.g. SAGA's super nodes).
        return float.__truediv__(float(other), float(self))


def forbidden_runtime(rt: np.ndarray, total_bytes: float, min_bw: float) -> float:
    """A runtime no feasible schedule can reach: every task run back to back
    at its slowest node, plus every edge over the slowest link, doubled."""
    return 2.0 * (float(rt.max(axis=1).sum()) + total_bytes / max(min_bw, MIN_BANDWIDTH)) + 1.0


def build_saga_models(
    dag: dict, cluster_state: dict, slots: Optional[Dict[str, int]] = None
) -> Tuple[TaskGraph, Network, List[str], float]:
    """Convert the Wayline request into SAGA TaskGraph + Network.

    With `slots`, node n becomes slots[n] identical processors (see the
    module docstring). Returns (task_graph, network, node_names,
    cost_model_rmse); node_names are the real nodes.
    """
    tasks: List[dict] = dag["tasks"]
    nodes = [n for n in cluster_state["nodes"] if n.get("ready", True)]
    if not nodes:
        raise ValueError("no ready nodes in clusterState")
    node_names = [n["name"] for n in nodes]

    rt = _runtime_matrix(tasks, node_names)
    costs, speeds, rmse = _rank1_fit(rt)

    # --- TaskGraph ---------------------------------------------------------
    tg_tasks = [(t["name"], float(costs[i])) for i, t in enumerate(tasks)]
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
    task_graph = TaskGraph.create(tasks=tg_tasks, dependencies=tg_edges)

    # --- Network -----------------------------------------------------------
    bw: Dict[Tuple[str, str], float] = {}
    for e in cluster_state.get("bandwidth", []) or []:
        bw[(e["from"], e["to"])] = float(e["bytesPerSec"])

    def link(u: str, v: str) -> float:
        fwd = bw.get((u, v))
        rev = bw.get((v, u))
        candidates = [x for x in (fwd, rev) if x is not None and x > 0]
        speed = min(candidates) if candidates else MIN_BANDWIDTH
        return max(speed, MIN_BANDWIDTH)

    procs = [
        (f"{n}{SLOT_SEP}{i}" if slots else n, n, float(speeds[j]))
        for j, n in enumerate(node_names)
        for i in range(slots[n] if slots else 1)
    ]
    net_nodes = [(p, sp) for p, _, sp in procs]
    net_edges = []
    for a, (p, pn, _) in enumerate(procs):
        net_edges.append((p, p, LOCAL_SPEED))  # explicit self-loop, finite
        for q, qn, _ in procs[a + 1 :]:
            net_edges.append((p, q, LOCAL_SPEED if pn == qn else link(pn, qn)))
    network = Network.create(nodes=net_nodes, edges=net_edges)

    # Exact runtimes and constraints through task.cost / node.speed.
    allowed = {t["name"]: set(_allowed_nodes(t, node_names) or node_names) for t in tasks}
    total_bytes = sum(e[2] for e in tg_edges)
    min_bw = min([e[2] for e in net_edges if e[0] != e[1]] or [LOCAL_SPEED])
    big = forbidden_runtime(rt, total_bytes, min_bw)
    table: Dict[Tuple[str, str], float] = {}
    for i, t in enumerate(tasks):
        for j, n in enumerate(node_names):
            table[(t["name"], n)] = float(rt[i, j]) if n in allowed[t["name"]] else big
    for i, t in enumerate(tasks):
        node = task_graph.get_task(t["name"])
        node.__dict__["cost"] = _Cost(float(costs[i]), t["name"], table)
    for p, pn, sp in procs:
        network.get_node(p).__dict__["speed"] = _Speed(sp, pn)

    return task_graph, network, node_names, rmse


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
    slots = None
    mode = request.get("slots") or ""
    if mode == "auto":
        ready = [n for n in cluster_state["nodes"] if n.get("ready", True)]
        slots = slot_counts(tasks, ready)
    elif mode not in ("", "none"):
        raise ValueError(f"unknown slots mode {mode!r}; use 'auto'")
    task_graph, network, node_names, rmse = build_saga_models(dag, cluster_state, slots)

    import constrained
    constrained.current.allowed = {
        t["name"]: {nn.name for nn in network.nodes if real_node(nn.name) in allowed}
        for t in tasks
        if (allowed := _allowed_nodes(t, node_names)) is not None
    }
    try:
        sched: Schedule = scheduler.schedule(network, task_graph)
    finally:
        constrained.current.allowed = None

    # mapping: node -> [ScheduledTask]; invert, strip super nodes.
    placement: Dict[str, str] = {}
    times: Dict[str, Tuple[float, float]] = {}
    for node_name, scheduled in sched.mapping.items():
        for st in scheduled:
            if st.name in SUPER_NODES:
                continue
            placement[st.name] = real_node(node_name)
            times[st.name] = (float(st.start), float(st.end))

    missing = [t["name"] for t in tasks if t["name"] not in placement]
    if missing:
        raise RuntimeError(f"{algorithm} left tasks unassigned: {missing}")

    # Verify, never repair: moving a task after scheduling would make the
    # returned estimate describe a schedule that does not run.
    violations = []
    for t in tasks:
        allowed = _allowed_nodes(t, node_names)
        if allowed is not None and placement[t["name"]] not in allowed:
            violations.append(f"{t['name']} on {placement[t['name']]} (allowed {sorted(allowed)})")
    if violations:
        raise RuntimeError(f"{algorithm} violated placement constraints: {violations}")

    makespan = float(sched.makespan) if placement else 0.0
    assignments = [
        {
            "task": t["name"],
            "node": placement[t["name"]],
            "estimatedStart": times.get(t["name"], (0.0, 0.0))[0],
            "estimatedFinish": times.get(t["name"], (0.0, 0.0))[1],
        }
        for t in tasks
    ]
    result = {
        "assignments": assignments,
        "estimatedMakespan": makespan,
        "algorithm": algorithm,
        "costModelFitRMSE": rmse,
    }
    if slots:
        result["slots"] = slots
    return result
