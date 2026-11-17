"""The opt-in "cores" node model and configurable local links. Runs with any SAGA
(PyPI 2.1.0 included). Run: pytest test_bridge_cores.py"""

import importlib.metadata

import pytest

import bridge


def _request(model=None, speed=None, cpu=4000):
    speed = speed or {"n1": 1.0, "n2": 1.0}
    base = {"a": 10, "b": 5, "c": 5, "d": 2}
    deps = {"a": [], "b": ["a"], "c": ["a"], "d": ["b", "c"]}
    tasks = [{"name": t, "dependencies": deps[t], "runtime": c, "cpuMillis": 1000,
              "runtimeProfile": {n: c / s for n, s in speed.items()},
              "inputs": [{"producer": p, "object": "o", "bytes": 1_000_000} for p in deps[t]]}
             for t, c in base.items()]
    req = {"algorithm": "heft", "dag": {"tasks": tasks},
           "clusterState": {"nodes": [{"name": n, "ready": True, "cpuMillis": cpu} for n in speed],
                            "bandwidth": [{"from": u, "to": v, "bytesPerSec": 1e6}
                                          for u in speed for v in speed if u != v]}}
    if model is not None:
        req["model"] = model
    return req


def test_cores_mode_assigns_machines_and_cores():
    res = bridge.schedule_request(_request({"nodeModel": "cores", "cores": {"n1": 2, "n2": 3}}))
    assert res["capacityModel"] == "cores"
    assert res["sagaVersion"] == importlib.metadata.version("anrg-saga")
    for a in res["assignments"]:
        assert a["node"] in ("n1", "n2")
        machine, _, i = a["core"].rpartition("/c")
        assert machine == a["node"] and int(i) < {"n1": 2, "n2": 3}[machine]
    # b and c run in parallel on two cores of a's machine (inter-machine links are slow)
    by = {a["task"]: a for a in res["assignments"]}
    assert by["b"]["core"] != by["c"]["core"]
    assert res["estimatedMakespan"] == pytest.approx(17.0, abs=0.01)


def test_cores_default_count_from_cpu():
    tg, net, names, _ = bridge.build_saga_models(_request(cpu=3500)["dag"], _request(cpu=3500)["clusterState"],
                                                 {"nodeModel": "cores"})
    assert sorted(names) == sorted(f"{m}/c{i}" for m in ("n1", "n2") for i in range(3))


def test_link_speeds_are_configurable():
    req = _request()
    model = {"nodeModel": "cores", "cores": 2, "selfBandwidth": {"n1": 5e8}, "intraBandwidth": 2e8}
    _, net, _, _ = bridge.build_saga_models(req["dag"], req["clusterState"], model)
    assert net.get_edge("n1/c0", "n1/c0").speed == 5e8
    assert net.get_edge("n2/c0", "n2/c0").speed == bridge.LOCAL_SPEED      # default where unspecified
    assert net.get_edge("n1/c0", "n1/c1").speed == 2e8
    assert net.get_edge("n1/c0", "n2/c1").speed == 1e6
    # slower self/intra links cost time on the critical path
    slow = bridge.schedule_request({**req, "model": {**model, "selfBandwidth": 1e5, "intraBandwidth": 1e5}})
    fast = bridge.schedule_request({**req, "model": {"nodeModel": "cores", "cores": 2}})
    assert slow["estimatedMakespan"] > fast["estimatedMakespan"]


def test_speeds_follow_related_machines():
    res = bridge.schedule_request(_request({"nodeModel": "cores", "cores": 1}, speed={"n1": 2.0, "n2": 1.0}))
    assert res["costModelFitRMSE"] == pytest.approx(0, abs=1e-12)
    by = {a["task"]: a for a in res["assignments"]}
    assert by["a"]["node"] == "n1"
    assert by["a"]["estimatedFinish"] - by["a"]["estimatedStart"] == pytest.approx(5.0)


def test_bad_models_are_rejected():
    with pytest.raises(ValueError, match="nodeModel"):
        bridge.schedule_request(_request({"nodeModel": "threads"}))
    with pytest.raises(ValueError, match="> 0"):
        bridge.schedule_request(_request({"nodeModel": "cores", "intraBandwidth": 0}))


@pytest.mark.skipif(bridge.SAGA_HAS_CAPACITY, reason="SAGA has the capacity model")
def test_old_saga_refuses_what_it_would_mis_model():
    with pytest.raises(RuntimeError, match="capacity model"):
        bridge.schedule_request(_request())                                   # default model
    req = _request({"nodeModel": "cores"})
    req["dag"]["tasks"][0]["runtimeProfile"] = {"n1": 10, "n2": 1}            # not cost/speed
    with pytest.raises(RuntimeError, match="not cost/speed"):
        bridge.schedule_request(req)
