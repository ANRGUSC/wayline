"""A stand-in for a researcher's own scheduler package.

Deliberately not part of the sidecar image: the loading tests put this
directory on sys.path the way WL_SAGA_PATH would for a mounted volume, so
the test exercises the same path a real external scheduler takes.
"""

from saga import Network, Schedule, ScheduledTask, Scheduler, TaskGraph


class PinFirstNodeScheduler(Scheduler):
    """Places every task on the alphabetically-first node, in series.

    Trivial on purpose: the point is that an arbitrary Scheduler subclass
    loads and runs, not that it schedules well.
    """

    def schedule(self, network: Network, task_graph: TaskGraph,
                 schedule=None, min_start_time: float = 0.0) -> Schedule:
        return _serial_on(sorted(n.name for n in network.nodes)[0],
                          network, task_graph, min_start_time)


class ParamScheduler(Scheduler):
    """Same, but picks the node by index so option passthrough is testable."""

    which: int = 0

    def schedule(self, network: Network, task_graph: TaskGraph,
                 schedule=None, min_start_time: float = 0.0) -> Schedule:
        names = sorted(n.name for n in network.nodes)
        return _serial_on(names[self.which % len(names)],
                          network, task_graph, min_start_time)


def _serial_on(node: str, network: Network, task_graph: TaskGraph,
               t: float) -> Schedule:
    sched = Schedule(task_graph, network)
    speed = next(n.speed for n in network.nodes if n.name == node)
    for entry in task_graph.topological_sort():
        # topological_sort yields TaskGraphNode objects; accept a bare name too
        # so this fixture is not coupled to that detail.
        name = getattr(entry, "name", entry)
        cost = getattr(entry, "cost", None)
        if cost is None:
            cost = task_graph.get_task(name).cost
        dur = cost / speed
        sched.add_task(ScheduledTask(node=node, name=name, start=t, end=t + dur))
        t += dur
    return sched


class NotAScheduler:
    """Importable but not a Scheduler — must be rejected with a clear error."""
