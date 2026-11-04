#!/usr/bin/env python3
"""E17 simulator ablations: the paper's simulator with one modeling choice at
a time changed to what the testbed's data plane does, to test whether that
choice explains a testbed/simulation disagreement.

  ip-reroute  Rerouted transfers follow the routing table, as IP forwarding
              does: when a link recovers, an in-flight transfer on a detour
              moves to the new (direct) path and keeps its progress. The
              simulator pins a transfer to the path it started on.
  fix-dup     A simulator bug fixed: when a link goes down, aborting one
              multi-hop transfer recalculates the others on its path, which
              aborts (and parks) those crossing the downed link; the abort
              loop, iterating a stale copy of the link's transfer list, then
              parks them again. Each extra parked entry resumes as a duplicate
              concurrent transfer of the same edge (up to 16 copies of one
              edge), so multi-hop routing (reroute) is charged for traffic it
              never needed.
  keep        In-flight inputs survive a move: when a task moves, transfers
              already on their way to its old node are not aborted; they
              finish and leave a copy there (counted by retain), and a task
              that moves back adopts the one still in flight. The simulator
              aborts them; Wayline cancels only the producer's own pending
              push, and copies already ordered keep landing. (Implies
              retain.)
  jitterS_sN  Execution noise, not a modeling change: every task runs
              exp(N(0, S)) times its nominal duration (seed N), while the
              scheduler keeps planning with nominal costs, as on the testbed.
              It measures how sensitive one simulated trajectory is to small
              timing differences of the kind a real system always has.
  retain      Delivered inputs stay where they landed: a task moved back to
              a node that already received one of its inputs does not need
              it re-sent (Wayline's any-valid-copy gate). The simulator
              re-sends every input on every move.

The engine is patched around (no change to ncsim). Run on eclipse in the
paper environment:

  ablation.py --scenarios DIR --out FILE [--variants fix-dup fix-dup+ip-reroute ...] [--only ID ...]
"""
import argparse
import copy
import glob
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from ncsim.core.execution_engine import ExecutionEngine  # noqa: E402
from ncsim.core.simulation import Simulation  # noqa: E402
from ncsim.models.dag import SingleDAGSource  # noqa: E402
from ncsim.models.disruptions import DisruptionEvent, DisruptionType, ScheduledDisruptions  # noqa: E402
from ncsim.models.routing import DirectLinkRouting, WidestPathRouting  # noqa: E402
from ncsim.scheduler.repair import RepairingSagaScheduler  # noqa: E402

from driver import Scenario  # noqa: E402

T = {"down": DisruptionType.LINK_DOWN, "degrade": DisruptionType.LINK_DEGRADE,
     "recovery": DisruptionType.RECOVERY}
_orig = {}


def _migrate(engine):
    """Move in-flight multi-hop transfers onto the current routes."""
    routable = engine._routable_network()
    seen, touched = set(), set()
    for ls in list(engine.link_states.values()):
        for tr in list(ls.active_transfers):
            key = (tr.dag_id, tr.from_task, tr.to_task)
            if key in seen or len(tr.link_ids) <= 1:
                continue
            seen.add(key)
            src = engine.network.links[tr.link_ids[0]].from_node
            dst = engine.network.links[tr.link_ids[-1]].to_node
            new = engine.routing_model.get_path(src, dst, routable)
            if not new or list(new) == list(tr.link_ids):
                continue
            elapsed = engine.sim_time - tr.started_at
            if elapsed > 0 and tr.current_effective_rate > 0:
                tr.data_remaining = max(0.0, tr.data_remaining - tr.current_effective_rate * elapsed)
            tr.started_at = engine.sim_time
            for lid in tr.link_ids:
                if tr in engine.link_states[lid].active_transfers:
                    engine.link_states[lid].active_transfers.remove(tr)
            touched.update(tr.link_ids)
            tr.link_ids = list(new)
            tr.link_id = new[0]
            tr.total_latency = sum(engine.network.links[lid].latency for lid in new)
            for lid in new:
                engine.link_states[lid].active_transfers.append(tr)
            touched.update(new)
            engine.disruption_stats["transfers_migrated"] = engine.disruption_stats.get("transfers_migrated", 0) + 1
    for lid in sorted(touched):
        engine._recalculate_link_transfers(lid)


def _abort_once(self, link_id):
    """ExecutionEngine._abort_transfers_on_link, skipping transfers an
    earlier iteration already removed (and parked) via recalculation."""
    from ncsim.core.execution_engine import StalledTransfer
    from ncsim.models.dag import Edge
    link_state = self.link_states[link_id]
    orphans = getattr(self, "_e17_orphans", {})
    for transfer in list(link_state.active_transfers):
        if transfer not in link_state.active_transfers:
            self.disruption_stats["duplicate_aborts_skipped"] = self.disruption_stats.get("duplicate_aborts_skipped", 0) + 1
            continue
        self.event_queue.cancel(transfer.event_id)
        path = transfer.link_ids if transfer.link_ids else [transfer.link_id]
        for lid in path:
            other_state = self.link_states.get(lid)
            if other_state and transfer in other_state.active_transfers:
                other_state.active_transfers.remove(transfer)
        if id(transfer) in orphans:
            del orphans[id(transfer)]      # a copy for a node the task left: dropped
            for lid in path:
                if lid != link_id:
                    self._recalculate_link_transfers(lid)
            continue
        self._stalled_transfers.append(StalledTransfer(
            dag_id=transfer.dag_id,
            edge=Edge(from_task=transfer.from_task, to_task=transfer.to_task, data_size=transfer.data_size),
            stalled_at=self.sim_time, was_in_flight=True))
        self.disruption_stats["transfers_aborted"] += 1
        for lid in path:
            if lid != link_id:
                self._recalculate_link_transfers(lid)


def enable(ablations):
    import math
    import random
    from ncsim.models.task import Task
    from ncsim.scheduler.repair import RepairingSagaScheduler as _R
    for name in ("_handle_link_state_change", "_schedule_transfer_start", "_complete_local_transfer",
                 "_handle_transfer_complete", "_abort_transfers_on_link"):
        _orig.setdefault(name, getattr(ExecutionEngine, name))
        setattr(ExecutionEngine, name, _orig[name])
    if "_abort_transfers_to_task" in _orig:
        ExecutionEngine._abort_transfers_to_task = _orig["_abort_transfers_to_task"]
    _orig.setdefault("Task.execution_time", Task.execution_time)
    Task.execution_time = _orig["Task.execution_time"]
    _orig.setdefault("R._execution_window", _R._execution_window)
    _R._execution_window = _orig["R._execution_window"]
    for ab in ablations:
        if ab.startswith("jitter"):
            sig, seed = ab[len("jitter"):].split("s")
            sig = float(sig)

            def jittered(self, cap, _sig=sig, _seed=seed):
                f = math.exp(random.Random(f"{_seed}:{self.id}").gauss(0.0, _sig))
                return self.compute_cost / cap * f

            def nominal_window(self, task_state, dag, snapshot, _o=_orig["R._execution_window"]):
                Task.execution_time = _orig["Task.execution_time"]
                try:
                    return _o(self, task_state, dag, snapshot)
                finally:
                    Task.execution_time = jittered
            Task.execution_time = jittered
            _R._execution_window = nominal_window
    if "fix-dup" in ablations:
        ExecutionEngine._abort_transfers_on_link = _abort_once
    if "ip-reroute" in ablations:
        def link_change(self, event, _o=_orig["_handle_link_state_change"]):
            _o(self, event)
            if event.data.get("change") == "recovery" and isinstance(self.routing_model, WidestPathRouting):
                _migrate(self)
        ExecutionEngine._handle_link_state_change = link_change
    if "keep" in ablations and "retain" not in ablations:
        ablations = list(ablations) + ["retain"]
    if "keep" in ablations:
        _orig.setdefault("_abort_transfers_to_task", ExecutionEngine._abort_transfers_to_task)
        ExecutionEngine._abort_transfers_on_link = _abort_once

        def orphan_on_move(self, dag_id, task_id):
            """Leave in-flight inputs of a moved task running, as orphans."""
            if not hasattr(self, "_e17_orphans"):
                self._e17_orphans = {}
            for ls in self.link_states.values():
                for tr in ls.active_transfers:
                    if tr.dag_id == dag_id and tr.to_task == task_id:
                        dst = self.network.links[(tr.link_ids or [tr.link_id])[-1]].to_node
                        self._e17_orphans[id(tr)] = (tr, dst)
        ExecutionEngine._abort_transfers_to_task = orphan_on_move
    elif "_abort_transfers_to_task" in _orig:
        ExecutionEngine._abort_transfers_to_task = _orig["_abort_transfers_to_task"]
    if "retain" in ablations:
        def delivered(self):
            if not hasattr(self, "_e17_delivered"):
                self._e17_delivered = {}
            return self._e17_delivered

        def sched(self, dag_id, edge, _o=_orig["_schedule_transfer_start"], _l=_orig["_complete_local_transfer"]):
            to = self.get_task_state(dag_id, edge.to_task)
            if to is not None and to.assigned_node in delivered(self).get((dag_id, edge.from_task, edge.to_task), ()):
                self.disruption_stats["inputs_reused"] = self.disruption_stats.get("inputs_reused", 0) + 1
                return _l(self, dag_id, edge.from_task, edge.to_task)
            orphans = getattr(self, "_e17_orphans", {})
            for oid, (tr, dst) in list(orphans.items()):
                if (tr.dag_id, tr.from_task, tr.to_task) == (dag_id, edge.from_task, edge.to_task) and \
                        to is not None and dst == to.assigned_node:
                    del orphans[oid]           # the task came back: adopt the copy in flight
                    self.disruption_stats["orphans_adopted"] = self.disruption_stats.get("orphans_adopted", 0) + 1
                    return None
            return _o(self, dag_id, edge)

        def local(self, dag_id, f, t, _o=_orig["_complete_local_transfer"]):
            to = self.get_task_state(dag_id, t)
            if to is not None:
                delivered(self).setdefault((dag_id, f, t), set()).add(to.assigned_node)
            return _o(self, dag_id, f, t)

        def done(self, event, _o=_orig["_handle_transfer_complete"]):
            path = event.data.get("path", [event.link_id])
            orphans = getattr(self, "_e17_orphans", {})
            for oid, (tr, dst) in list(orphans.items()):
                if tr.event_id == event.event_id:
                    # A copy landing on a node its task left: keep it, wake nobody.
                    del orphans[oid]
                    for lid in (tr.link_ids or [tr.link_id]):
                        if tr in self.link_states[lid].active_transfers:
                            self.link_states[lid].active_transfers.remove(tr)
                    for lid in (tr.link_ids or [tr.link_id]):
                        self._recalculate_link_transfers(lid)
                    delivered(self).setdefault((tr.dag_id, tr.from_task, tr.to_task), set()).add(dst)
                    self.disruption_stats["orphans_landed"] = self.disruption_stats.get("orphans_landed", 0) + 1
                    return None
            live = any(tr.dag_id == event.dag_id and tr.from_task == event.from_task and tr.to_task == event.to_task
                       for tr in self.link_states[path[0]].active_transfers) if path and path[0] in self.link_states else False
            if live:
                dst = self.network.links[path[-1]].to_node
                delivered(self).setdefault((event.dag_id, event.from_task, event.to_task), set()).add(dst)
            return _o(self, event)
        ExecutionEngine._schedule_transfer_start = sched
        ExecutionEngine._complete_local_transfer = local
        ExecutionEngine._handle_transfer_complete = done


def trace_events(doc):
    out = []
    for e in doc["trace"]:
        out.append(DisruptionEvent(time=e["t"], target_type="link", target_id=e["link"],
                                   disruption_type=T[e["type"]],
                                   parameters={"bandwidth_factor": e["factor"]} if e["type"] == "degrade" else {}))
    return out


def simulate(sc, label):
    """One policy run; label as in the scenario file (wait@0, local@0.02, ...)."""
    pol, _, dl = label.partition("@")
    delta = float(dl or 0)
    scope, routing = {"wait": ("none", DirectLinkRouting()), "reroute": ("none", WidestPathRouting()),
                      "local": ("local", DirectLinkRouting()), "global": ("global", DirectLinkRouting())}[pol]
    sim = Simulation(network=copy.deepcopy(sc.net), scheduler=RepairingSagaScheduler(algorithm="heft", scope=scope),
                     dag_source=SingleDAGSource(copy.deepcopy(sc.dag)), routing_model=routing,
                     disruption_model=ScheduledDisruptions(trace_events(sc.doc)),
                     repair_cost_per_task=delta * sc.doc["m0"])
    res = sim.run()
    return res.makespan, dict(sim.engine.disruption_stats)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenarios", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--variants", nargs="*", default=["fix-dup", "fix-dup+ip-reroute", "fix-dup+ip-reroute+retain"],
                    help="ablation combinations, each a +-joined set")
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--labels", nargs="*")
    a = ap.parse_args()
    import logging
    logging.disable(logging.WARNING)
    variants = [v.split("+") for v in a.variants]
    out = {}
    if os.path.exists(a.out):
        out = json.load(open(a.out))
    for f in sorted(glob.glob(os.path.join(a.scenarios, "*.json"))):
        sc = Scenario(f)
        sid = sc.doc["id"]
        if a.only and sid not in a.only:
            continue
        rec = out.setdefault(sid, {})
        for label in (a.labels or list(sc.doc["sim"])):
            if label not in sc.doc["sim"]:
                continue
            for v in variants:
                key = f"{label}|{'+'.join(v)}"
                if key in rec:
                    continue
                enable(v)
                ms, stats = simulate(sc, label)
                rec[key] = {"makespan": ms, "base": sc.doc["sim"][label]["makespan"], "stats": stats}
        enable([])
        json.dump(out, open(a.out, "w"))
        print(sid, {k: round(v["makespan"] / sc.doc["m0"], 3) for k, v in rec.items()}, flush=True)


if __name__ == "__main__":
    main()
