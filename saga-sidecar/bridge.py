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

  * Cost model. SAGA's heterogeneity is separable: runtime(t, n) = cost_t /
    speed_n. Wayline supplies a true per-(task, node) runtime matrix, which a
    separable model cannot represent exactly. We compute the best rank-1 fit
    in log space (two-way additive decomposition): with L = log RT,
    log cost_t = rowmean_t(L), log speed_n = mean(L) - colmean_n(L).
    The fit residual is reported per request as "costModelFitRMSE" (in log
    space) so callers can see how much heterogeneity the model loses.

  * Network completeness. A missing SAGA edge defaults to speed 0.0 and
    comm time = size/speed divides by zero. We always emit every unordered
    pair, plus explicit self-loops with a large finite speed (1e12 B/s) --
    finite, not math.inf, because inf produces NaNs in SAGA's stochastic
    paths.

  * Symmetry. SAGA networks are undirected; Wayline bandwidth matrices may
    be asymmetric. We take min(bw(u,v), bw(v,u)) -- conservative.

  * Super nodes. TaskGraph.create() silently injects __super_source__ /
    __super_sink__ for multi-source/multi-sink DAGs. They are stripped from
    the returned mapping.

  * Constraints. Most SAGA schedulers ignore node constraints (HEFT/PEFT
    raise rather than avoid). Constraints are enforced by post-override,
    the same pattern ncsim uses for pinned tasks: a task assigned outside
    its allowed set is moved to the allowed node with the highest fitted
    speed. Overrides are reported in the response.
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

    return {
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


def build_saga_models(
    dag: dict, cluster_state: dict
) -> Tuple[TaskGraph, Network, List[str], float]:
    """Convert the Wayline request into SAGA TaskGraph + Network.

    Returns (task_graph, network, node_names, cost_model_rmse).
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
        for dep in t.get("dependencies", []) or []:
            if dep not in task_index:
                raise ValueError(f"task {t['name']!r} depends on unknown task {dep!r}")
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

    net_nodes = [(n, float(speeds[j])) for j, n in enumerate(node_names)]
    net_edges = []
    for j, u in enumerate(node_names):
        net_edges.append((u, u, LOCAL_SPEED))  # explicit self-loop, finite
        for v in node_names[j + 1 :]:
            fwd = bw.get((u, v))
            rev = bw.get((v, u))
            candidates = [x for x in (fwd, rev) if x is not None and x > 0]
            speed = min(candidates) if candidates else MIN_BANDWIDTH
            net_edges.append((u, v, max(speed, MIN_BANDWIDTH)))
    network = Network.create(nodes=net_nodes, edges=net_edges)

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
    task_graph, network, node_names, rmse = build_saga_models(dag, cluster_state)

    sched: Schedule = scheduler.schedule(network, task_graph)

    # mapping: node -> [ScheduledTask]; invert, strip super nodes.
    placement: Dict[str, str] = {}
    times: Dict[str, Tuple[float, float]] = {}
    for node_name, scheduled in sched.mapping.items():
        for st in scheduled:
            if st.name in SUPER_NODES:
                continue
            placement[st.name] = node_name
            times[st.name] = (float(st.start), float(st.end))

    missing = [t["name"] for t in tasks if t["name"] not in placement]
    if missing:
        raise RuntimeError(f"{algorithm} left tasks unassigned: {missing}")

    # Constraint post-override (ncsim's pinning pattern), load-balanced:
    # a violating task moves to the allowed node currently holding the
    # fewest tasks (ties broken by fitted speed). Moving every violator
    # to the single "best" allowed node packs constrained siblings onto
    # one node and serializes parallel tiers — measured on the wpf
    # benchmark as SAGA arms landing below even random placement.
    node_speed = {nn.name: nn.speed for nn in network.nodes}
    load = collections.Counter(placement.values())
    overrides: List[dict] = []
    for t in tasks:
        allowed = _allowed_nodes(t, node_names)
        if allowed is None:
            continue
        if placement[t["name"]] not in allowed:
            target = min(allowed, key=lambda n: (load[n], -node_speed[n]))
            overrides.append(
                {"task": t["name"], "from": placement[t["name"]], "to": target}
            )
            load[placement[t["name"]]] -= 1
            load[target] += 1
            placement[t["name"]] = target

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
    if overrides:
        result["constraintOverrides"] = overrides
        logger.warning("constraint overrides applied: %s", overrides)
    return result
