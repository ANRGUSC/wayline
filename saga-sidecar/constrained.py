"""Constraint-aware variants of the SAGA schedulers that choose nodes
without looking at runtime.

The bridge enforces placement constraints for every scheduler through the
runtime of a forbidden (task, node) pair (see bridge.py). That steers any
scheduler that compares finish times, but not the three below, which pick a
node by availability (OLB), by start time (ETF) or by speed alone
(FastestNode). These subclasses restrict each task's candidate nodes to its
allowed set and are otherwise the originals.

The allowed sets come from the request being served, published by the
bridge through `current` (thread-local; names are SAGA processor names, so
slot processors are included).
"""

import threading
from typing import Dict, Optional, Set

import numpy as np
from saga import Schedule, ScheduledTask
from saga.schedulers import ETFScheduler, FastestNodeScheduler, OLBScheduler

current = threading.local()


def allowed(task: str) -> Optional[Set[str]]:
    table: Optional[Dict[str, Set[str]]] = getattr(current, "allowed", None)
    return None if table is None else table.get(task)


class ConstrainedOLB(OLBScheduler):
    def schedule(self, network, task_graph, schedule=None, min_start_time=0.0):
        comp = Schedule(task_graph, network) if schedule is None else schedule.model_copy()
        done: Dict[str, ScheduledTask] = {t.name: t for _, ts in comp.items() for t in ts}
        names = [n.name for n in network.nodes]
        for task in task_graph.topological_sort():
            if task.name in done:
                continue
            ok = allowed(task.name)
            cands = [n for n in names if ok is None or n in ok]
            free = lambda n: comp[n][-1].end if comp[n] else min_start_time
            node = min(cands, key=free)
            start = max([free(node)] + [
                done[e.source].end + e.size / network.get_edge(done[e.source].node, node).speed
                for e in task_graph.in_edges(task.name)])
            new = ScheduledTask(name=task.name, node=node, start=start,
                                end=start + task.cost / network.get_node(node).speed)
            comp.add_task(new)
            done[task.name] = new
        return comp


class ConstrainedETF(ETFScheduler):
    def _get_start_times(self, task_map, ready_tasks, ready_nodes, task_graph, network,
                         min_start_time=0.0):
        out = {}
        for t in ready_tasks:
            ok = allowed(t)
            nodes = {n for n in ready_nodes if ok is None or n in ok}
            if nodes:
                out.update(super()._get_start_times(task_map, {t}, nodes, task_graph,
                                                    network, min_start_time))
        if not out and ready_tasks:
            # No ready task may use a free node yet: an unreachable start time
            # makes ETF advance to the next finishing task. (With nothing
            # running every node is free, so some task always fits.)
            out[next(iter(ready_tasks))] = ("", np.inf)
        return out


class ConstrainedFastestNode(FastestNodeScheduler):
    def schedule(self, network, task_graph, schedule=None, min_start_time=0.0,
                 node_constraints=None):
        table = getattr(current, "allowed", None)
        return super().schedule(network, task_graph, schedule, min_start_time,
                                node_constraints=node_constraints if node_constraints is not None
                                else ({t: set(s) for t, s in table.items()} if table else None))
