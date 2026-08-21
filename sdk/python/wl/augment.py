"""Logical -> physical DAG translation: the policy layer.

A DAG edge specifies that data produced by one task is required by another;
it does not prescribe where that data is stored or how it moves. This module
binds that free variable: it rewrites an ODAG/ODAGTemplate spec so selected
edges are realized through an intermediary data vertex instead of direct
agent-to-agent handoff, while preserving the application's dependency
semantics. The application containers are untouched; only the graph changes.

Realizations expressible here (the AND-shaped ones):

  store       A->B            becomes  A->S_A->B      S_A pinned to a store
                                                      node; one upload per
                                                      producer, one download
                                                      per consumer -- the
                                                      traffic of a
                                                      centralized store.
  checkpoint  same as store, applied to a subset of edges: the checkpointed
              output survives its producer on the store node while every
              other edge stays direct.
  relay       S_A pinned to an intermediate node with no compute role.

Replica failover (OR-shaped) and cross-run caching need runtime semantics,
not graph structure, and are deliberately not expressible here.

The data vertex is a real task in the physical DAG: the scheduler sees it,
places it (within its constraints), and its transfers appear in the same
per-edge state as any other. The prototype realizes it as a passthrough
container; agent-native execution (no pod) removes the dispatch overhead
without changing this translation.
"""
from __future__ import annotations

import copy
from typing import Any, Iterable

STORE_PREFIX = "store-"


class AugmentError(ValueError):
    pass


def _tasks(spec: dict[str, Any]) -> list[dict[str, Any]]:
    try:
        tasks = spec["spec"]["tasks"]
    except (KeyError, TypeError):
        raise AugmentError("not an ODAG/ODAGTemplate: missing spec.tasks")
    if not isinstance(tasks, list) or not tasks:
        raise AugmentError("spec.tasks is empty")
    return tasks


def parse_edges(sel: str, tasks: list[dict[str, Any]]) -> set[tuple[str, str]]:
    """Parse an edge selection: 'all', 'a>b,c>d', or 'a,c' (all of a's and
    c's outgoing edges). Names are validated against the DAG."""
    names = {t["name"] for t in tasks}
    edges = {(d, t["name"]) for t in tasks for d in t.get("dependencies", [])}
    if sel == "all":
        return set(edges)
    out: set[tuple[str, str]] = set()
    for part in filter(None, (p.strip() for p in sel.split(","))):
        if ">" in part:
            a, b = (x.strip() for x in part.split(">", 1))
            if (a, b) not in edges:
                raise AugmentError(f"no edge {a}->{b} in the DAG")
            out.add((a, b))
        else:
            if part not in names:
                raise AugmentError(f"no task named {part!r}")
            got = {e for e in edges if e[0] == part}
            if not got:
                raise AugmentError(f"task {part!r} has no outgoing edges")
            out |= got
    return out


def augment(
    template: dict[str, Any],
    *,
    store_node: str,
    edges: str | Iterable[tuple[str, str]] = "all",
    image: str | None = None,
    command: list[str] | None = None,
    cpu: str = "100m",
    memory: str = "256Mi",
) -> dict[str, Any]:
    """Return a deep-copied template with `edges` realized through a data
    vertex on `store_node`.

    One vertex per producer with at least one selected edge: A->{B,C} with
    both edges selected becomes one upload A->S_A and two downloads -- the
    same traffic a shared store carries. Unselected edges stay direct, so
    store-mediated and direct handoff coexist in one DAG.

    `image`/`command` default to the producer's own image and command: the
    generic task contract (receive deps, sleep runtime, emit dataSize,
    push successors) is a passthrough at runtime 0.
    """
    out = copy.deepcopy(template)
    tasks = _tasks(out)
    sel = parse_edges(edges, tasks) if isinstance(edges, str) else {
        (a, b) for a, b in edges}
    if not sel:
        return out

    by_name = {t["name"]: t for t in tasks}
    known = {(d, t["name"]) for t in tasks for d in t.get("dependencies", [])}
    bad = sel - known
    if bad:
        raise AugmentError(f"edges not in the DAG: {sorted(bad)}")

    producers = sorted({a for a, _ in sel})
    for a in producers:
        src = by_name[a]
        store_name = STORE_PREFIX + a
        if store_name in by_name:
            raise AugmentError(f"name collision: {store_name} already exists")
        vertex = {
            "name": store_name,
            "image": image or src["image"],
            "command": command or src.get("command", ["python", "task.py"]),
            "dependencies": [a],
            # the vertex re-emits the producer's payload unchanged
            "dataSize": src.get("dataSize", "0"),
            "runtime": 0,
            "resources": {"cpu": cpu, "memory": memory},
            "constraints": {"nodeNames": [store_node]},
        }
        tasks.append(vertex)
        by_name[store_name] = vertex

    for t in tasks:
        if t["name"].startswith(STORE_PREFIX) and t["name"] in (
                STORE_PREFIX + a for a in producers):
            continue
        deps = t.get("dependencies", [])
        t["dependencies"] = [
            STORE_PREFIX + d if (d, t["name"]) in sel else d for d in deps]
    return out


def main(argv: list[str] | None = None) -> int:
    import argparse
    import sys

    import yaml

    ap = argparse.ArgumentParser(
        prog="wl-augment",
        description="Rewrite an ODAG template so selected edges are realized "
                    "through a data vertex (store/checkpoint/relay) instead "
                    "of direct handoff.")
    ap.add_argument("template", help="ODAG/ODAGTemplate YAML (- for stdin)")
    ap.add_argument("--store-node", required=True,
                    help="node the data vertex is pinned to")
    ap.add_argument("--edges", default="all",
                    help="'all', 'a>b,c>d', or 'a,c' (default: all)")
    ap.add_argument("--image", default=None,
                    help="data-vertex image (default: producer's image)")
    ap.add_argument("--cpu", default="100m")
    ap.add_argument("--memory", default="256Mi")
    ap.add_argument("--suffix", default=None,
                    help="append to metadata.name (default: '-store' when "
                    "edges=all, '-ckpt' otherwise)")
    args = ap.parse_args(argv)

    text = sys.stdin.read() if args.template == "-" else open(args.template).read()
    tpl = yaml.safe_load(text)
    out = augment(tpl, store_node=args.store_node, edges=args.edges,
                  image=args.image, cpu=args.cpu, memory=args.memory)
    suffix = args.suffix or ("-store" if args.edges == "all" else "-ckpt")
    if out.get("metadata", {}).get("name"):
        out["metadata"]["name"] += suffix
    yaml.safe_dump(out, sys.stdout, sort_keys=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
