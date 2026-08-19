"""Unit tests for the SAGA<->Wayline bridge core. Run: pytest test_bridge.py"""

import math

import numpy as np
import pytest

import bridge


def _request(algorithm="heft", tasks=None, nodes=None, bandwidth=None):
    if nodes is None:
        nodes = [{"name": f"n{i}", "ready": True} for i in range(3)]
    if tasks is None:
        tasks = [
            {"name": "a", "dependencies": [], "runtime": 10, "dataSize": "100MB"},
            {"name": "b", "dependencies": ["a"], "runtime": 5, "dataSize": "1MB"},
            {"name": "c", "dependencies": ["a"], "runtime": 5, "dataSize": "1MB"},
            {"name": "d", "dependencies": ["b", "c"], "runtime": 2, "dataSize": "0"},
        ]
    if bandwidth is None:
        bandwidth = [
            {"from": u["name"], "to": v["name"], "bytesPerSec": 125e6}
            for u in nodes
            for v in nodes
            if u["name"] != v["name"]
        ]
    return {
        "algorithm": algorithm,
        "dag": {"tasks": tasks},
        "clusterState": {"nodes": nodes, "bandwidth": bandwidth},
    }


def test_all_tasks_assigned_to_real_nodes():
    result = bridge.schedule_request(_request())
    names = {a["task"] for a in result["assignments"]}
    assert names == {"a", "b", "c", "d"}
    node_names = {"n0", "n1", "n2"}
    for a in result["assignments"]:
        assert a["node"] in node_names
    assert result["estimatedMakespan"] > 0


def test_super_nodes_stripped_multi_source_multi_sink():
    # Two sources, two sinks -> SAGA injects __super_source__/__super_sink__.
    tasks = [
        {"name": "s1", "dependencies": [], "runtime": 1, "dataSize": "1MB"},
        {"name": "s2", "dependencies": [], "runtime": 1, "dataSize": "1MB"},
        {"name": "t1", "dependencies": ["s1"], "runtime": 1, "dataSize": "0"},
        {"name": "t2", "dependencies": ["s2"], "runtime": 1, "dataSize": "0"},
    ]
    result = bridge.schedule_request(_request(tasks=tasks))
    names = {a["task"] for a in result["assignments"]}
    assert names == {"s1", "s2", "t1", "t2"}
    for a in result["assignments"]:
        assert not a["task"].startswith("__super")


def test_constraint_post_override():
    tasks = [
        {
            "name": "pinned",
            "dependencies": [],
            "runtime": 1,
            "dataSize": "1MB",
            # Slow node so no scheduler would choose it voluntarily.
            "constraints": {"nodeNames": ["slow"]},
            "runtimeProfile": {"fast": 1, "slow": 100},
        },
        {
            "name": "free",
            "dependencies": ["pinned"],
            "runtime": 1,
            "dataSize": "0",
            "runtimeProfile": {"fast": 1, "slow": 100},
        },
    ]
    nodes = [{"name": "fast"}, {"name": "slow"}]
    result = bridge.schedule_request(_request(tasks=tasks, nodes=nodes))
    placement = {a["task"]: a["node"] for a in result["assignments"]}
    assert placement["pinned"] == "slow"


def test_heterogeneous_runtime_profile_drives_placement():
    # One node is 10x faster for every task: HEFT must use it heavily.
    tasks = [
        {
            "name": f"t{i}",
            "dependencies": [] if i == 0 else ["t0"],
            "runtime": 10,
            "dataSize": "1KB",  # negligible comm so compute dominates
            "runtimeProfile": {"fast": 1, "slow1": 10, "slow2": 10},
        }
        for i in range(4)
    ]
    nodes = [{"name": "fast"}, {"name": "slow1"}, {"name": "slow2"}]
    result = bridge.schedule_request(_request(tasks=tasks, nodes=nodes))
    placement = {a["task"]: a["node"] for a in result["assignments"]}
    assert placement["t0"] == "fast"


def test_asymmetric_bandwidth_symmetrized_to_min():
    nodes = [{"name": "u"}, {"name": "v"}]
    bandwidth = [
        {"from": "u", "to": "v", "bytesPerSec": 100e6},
        {"from": "v", "to": "u", "bytesPerSec": 10e6},
    ]
    tg, net, node_names, _ = bridge.build_saga_models(
        {"tasks": [{"name": "a", "dependencies": [], "runtime": 1}]},
        {"nodes": nodes, "bandwidth": bandwidth},
    )
    edge = net.get_edge("u", "v")
    assert edge.speed == pytest.approx(10e6)


def test_missing_bandwidth_pairs_get_floor_not_zero():
    nodes = [{"name": "u"}, {"name": "v"}, {"name": "w"}]
    bandwidth = [{"from": "u", "to": "v", "bytesPerSec": 1e6}]  # v-w, u-w missing
    tg, net, node_names, _ = bridge.build_saga_models(
        {"tasks": [{"name": "a", "dependencies": [], "runtime": 1}]},
        {"nodes": nodes, "bandwidth": bandwidth},
    )
    assert net.get_edge("v", "w").speed >= bridge.MIN_BANDWIDTH
    assert net.get_edge("u", "w").speed >= bridge.MIN_BANDWIDTH
    # Self-loops are finite (inf breaks SAGA's stochastic paths).
    assert math.isfinite(net.get_edge("u", "u").speed)


def test_rank1_fit_exact_on_separable_matrix():
    rt = np.outer([2.0, 4.0, 8.0], [1.0, 0.5])  # cost_t / speed_n exactly
    costs, speeds, rmse = bridge._rank1_fit(rt)
    assert rmse == pytest.approx(0.0, abs=1e-9)
    predicted = np.outer(costs, 1.0 / speeds)
    assert np.allclose(predicted, rt)


def test_rank1_fit_reports_loss_on_nonseparable_matrix():
    # Task A fast on n0 / slow on n1; task B the reverse -> not separable.
    rt = np.array([[1.0, 10.0], [10.0, 1.0]])
    _, _, rmse = bridge._rank1_fit(rt)
    assert rmse > 0.5


def test_every_registered_algorithm_produces_valid_placement():
    req = _request()
    node_names = {"n0", "n1", "n2"}
    for algo in bridge.available_algorithms():
        result = bridge.schedule_request(_request(algorithm=algo))
        placement = {a["task"]: a["node"] for a in result["assignments"]}
        assert set(placement) == {"a", "b", "c", "d"}, algo
        assert set(placement.values()) <= node_names, algo


def test_unknown_algorithm_raises_keyerror():
    with pytest.raises(KeyError):
        bridge.schedule_request(_request(algorithm="definitely-not-real"))


def test_empty_dag():
    result = bridge.schedule_request(_request(tasks=[]))
    assert result["assignments"] == []
    assert result["estimatedMakespan"] == 0.0


def test_constrained_siblings_spread_not_packed():
    # Three parallel siblings constrained to a 3-node tier none of which
    # SAGA would pick on its own (their tier nodes are slow). The override
    # must spread them, not pack all three onto one node.
    tier = ["t1", "t2", "t3"]
    tasks = [
        {"name": "src", "dependencies": [], "runtime": 1, "dataSize": "1MB",
         "constraints": {"nodeNames": ["fast"]}},
    ] + [
        {"name": f"par-{i}", "dependencies": ["src"], "runtime": 10,
         "dataSize": "0", "constraints": {"nodeNames": tier}}
        for i in range(3)
    ]
    nodes = [{"name": "fast"}] + [{"name": n} for n in tier]
    result = bridge.schedule_request(_request(tasks=tasks, nodes=nodes))
    placement = {a["task"]: a["node"] for a in result["assignments"]}
    par_nodes = {placement[f"par-{i}"] for i in range(3)}
    assert len(par_nodes) == 3, f"siblings packed: {placement}"


# --------------------------------------------------------------------------
# Bring-your-own-scheduler: loading a Scheduler subclass by dotted path
# --------------------------------------------------------------------------

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "testdata"))


def test_external_scheduler_loads_by_dotted_path():
    # The whole point: a scheduler that is NOT in the built-in registry,
    # living outside the sidecar image, runs with no code change here.
    assert "mysched.PinFirstNodeScheduler" not in bridge.available_algorithms()
    result = bridge.schedule_request(_request(algorithm="mysched.PinFirstNodeScheduler"))
    placement = {a["task"]: a["node"] for a in result["assignments"]}
    assert set(placement) == {"a", "b", "c", "d"}
    # This scheduler pins everything to the alphabetically-first node.
    assert set(placement.values()) == {"n0"}


def test_external_scheduler_receives_constructor_options():
    req = _request(algorithm="mysched.ParamScheduler")
    req["options"] = {"which": 2}
    result = bridge.schedule_request(req)
    placement = {a["task"]: a["node"] for a in result["assignments"]}
    assert set(placement.values()) == {"n2"}, placement


def test_builtin_name_still_wins_over_path_lookup():
    result = bridge.schedule_request(_request(algorithm="heft"))
    assert len(result["assignments"]) == 4


def test_non_scheduler_class_rejected_with_useful_message():
    with pytest.raises(KeyError) as e:
        bridge.schedule_request(_request(algorithm="mysched.NotAScheduler"))
    assert "Scheduler subclass" in str(e.value)


def test_missing_module_names_the_remedy():
    with pytest.raises(KeyError) as e:
        bridge.schedule_request(_request(algorithm="nosuchpkg.Sched"))
    msg = str(e.value)
    assert "cannot import" in msg and "WL_SAGA_EXTRA_PACKAGES" in msg


def test_bare_unknown_name_suggests_dotted_path():
    with pytest.raises(KeyError) as e:
        bridge.schedule_request(_request(algorithm="totally-unknown"))
    assert "dotted path" in str(e.value)
