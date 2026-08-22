"""Data-gravity scheduler: place each task where its input bytes already are.

The demonstration scheduler for the open-scheduling claim: a communication-
minimizing policy the SAGA library does not contain, written against SAGA's
interface, deployed by putting this file in the wl-user-schedulers ConfigMap
and setting spec.scheduler to

    saga/datagravity.DataGravityScheduler

No controller changes, no image rebuild, no Go.
"""
from saga import Network, Schedule, ScheduledTask, Scheduler, TaskGraph


class DataGravityScheduler(Scheduler):
    def schedule(self, network: Network, task_graph: TaskGraph,
                 schedule: Schedule = None, min_start_time: float = 0.0) -> Schedule:
        nodes = [n.name for n in network.nodes]
        speed = {n.name: n.speed for n in network.nodes}
        mapping = {n: [] for n in nodes}
        placed, end = {}, {}
        for task in task_graph.topological_sort():
            deps = list(task_graph.in_edges(task.name))
            # bytes of input already resident per candidate node
            pull = {n: sum(e.size for e in deps if placed.get(e.source) == n)
                    for n in nodes}
            load = {n: sum(t.end - t.start for t in mapping[n]) for n in nodes}
            best = max(nodes, key=lambda n: (pull[n], -load[n]))
            start = max([min_start_time] +
                        [end[e.source] for e in deps if e.source in end])
            finish = start + task.cost / speed[best]
            mapping[best].append(ScheduledTask(node=best, name=task.name,
                                               start=start, end=finish))
            placed[task.name], end[task.name] = best, finish
        return Schedule(task_graph=task_graph, network=network, mapping=mapping)
