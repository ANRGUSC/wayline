"""Unit tests for the logical->physical translation. Run: pytest"""
import copy

import pytest

from wl.augment import AugmentError, augment, parse_edges


def tpl():
    return {
        "apiVersion": "wl.io/v1", "kind": "ODAGTemplate",
        "metadata": {"name": "toy", "namespace": "wl-system"},
        "spec": {"tasks": [
            {"name": "a", "image": "img:1", "command": ["python", "task.py"],
             "dependencies": [], "dataSize": "100MB", "runtime": 3},
            {"name": "b", "image": "img:1", "command": ["python", "task.py"],
             "dependencies": ["a"], "dataSize": "10MB", "runtime": 2},
            {"name": "c", "image": "img:2", "command": ["python", "task.py"],
             "dependencies": ["a"], "dataSize": "10MB", "runtime": 2,
             "constraints": {"nodeNames": ["n2"]}},
            {"name": "d", "image": "img:1", "command": ["python", "task.py"],
             "dependencies": ["b", "c"], "dataSize": "0", "runtime": 1},
        ]},
    }


def names(t):
    return [x["name"] for x in t["spec"]["tasks"]]


def deps(t, name):
    return next(x for x in t["spec"]["tasks"] if x["name"] == name)[
        "dependencies"]


def test_all_edges_one_vertex_per_producer():
    out = augment(tpl(), store_node="s")
    # a, b, c have successors -> three data vertices; sink d has none
    assert sorted(n for n in names(out) if n.startswith("store-")) == [
        "store-a", "store-b", "store-c"]
    assert deps(out, "b") == ["store-a"]
    assert deps(out, "c") == ["store-a"]
    assert deps(out, "d") == ["store-b", "store-c"]
    assert deps(out, "store-a") == ["a"]


def test_fan_out_is_one_upload():
    out = augment(tpl(), store_node="s")
    # a->{b,c} becomes ONE store-a consumed twice, not two uploads
    assert names(out).count("store-a") == 1


def test_vertex_shape():
    out = augment(tpl(), store_node="s9")
    v = next(x for x in out["spec"]["tasks"] if x["name"] == "store-a")
    assert v["type"] == "data"
    assert v["runtime"] == 0
    assert v["dataSize"] == "100MB"           # re-emits producer's payload
    assert v["image"] == "img:1"              # defaults to producer's image
    assert v["constraints"] == {"nodeNames": ["s9"]}


def test_subset_edges_coexist_with_direct():
    out = augment(tpl(), store_node="s", edges="a>b")
    assert deps(out, "b") == ["store-a"]      # selected: through the store
    assert deps(out, "c") == ["a"]            # unselected: stays direct
    assert deps(out, "d") == ["b", "c"]
    assert sorted(n for n in names(out) if n.startswith("store-")) == [
        "store-a"]


def test_producer_selection_takes_all_outgoing():
    out = augment(tpl(), store_node="s", edges="b,c")
    assert deps(out, "d") == ["store-b", "store-c"]
    assert deps(out, "b") == ["a"]            # a's edges untouched


def test_input_untouched_and_app_tasks_unchanged():
    t = tpl()
    frozen = copy.deepcopy(t)
    out = augment(t, store_node="s")
    assert t == frozen                        # pure function
    for name in ("a", "b", "c", "d"):
        orig = next(x for x in frozen["spec"]["tasks"] if x["name"] == name)
        got = next(x for x in out["spec"]["tasks"] if x["name"] == name)
        same = {k: v for k, v in got.items() if k != "dependencies"}
        assert same == {k: v for k, v in orig.items() if k != "dependencies"}


def test_bad_selections_rejected():
    with pytest.raises(AugmentError):
        augment(tpl(), store_node="s", edges="a>d")     # no such edge
    with pytest.raises(AugmentError):
        augment(tpl(), store_node="s", edges="nope")    # no such task
    with pytest.raises(AugmentError):
        augment(tpl(), store_node="s", edges="d")       # sink: no out-edges


def test_collision_rejected():
    t = tpl()
    t["spec"]["tasks"].append({"name": "store-a", "image": "x",
                               "dependencies": [], "runtime": 1})
    with pytest.raises(AugmentError):
        augment(t, store_node="s")


def test_parse_edges_forms():
    tasks = tpl()["spec"]["tasks"]
    assert parse_edges("all", tasks) == {("a", "b"), ("a", "c"),
                                         ("b", "d"), ("c", "d")}
    assert parse_edges("a>b, c>d", tasks) == {("a", "b"), ("c", "d")}
    assert parse_edges("a", tasks) == {("a", "b"), ("a", "c")}


def test_pod_realized_omits_type():
    out = augment(tpl(), store_node="s", pod_realized=True)
    v = next(x for x in out["spec"]["tasks"] if x["name"] == "store-a")
    assert "type" not in v          # runs as a passthrough container
    assert v["runtime"] == 0
