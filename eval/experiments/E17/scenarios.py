#!/usr/bin/env python3
"""E17 scenario export: the MILCOM 2026 schedule-repair study, one JSON file
per testbed scenario, with the simulator's own results for that scenario.

Runs in the paper's environment (milcom2026/ncsim at 509abe3, saga at
ca5c664, PYTHONHASHSEED=0):

  PYTHONHASHSEED=0 PYTHONPATH=$M/ncsim:$M/saga/src python scenarios.py --out DIR [--quick]

A scenario is (structure, instance, CCR, disruption trace). For each one the
file holds everything the testbed run needs and everything it is compared
against:

  dag         tasks {id: cost}, edges [[from, to, size]] (simulation units)
  nodes       {node: speed}; bandwidth: the uniform link bandwidth b
  m0          the simulated undisrupted makespan
  plan        the initial HEFT placement {task: node}, shared by every policy
  trace       [{t, link, type: down|degrade|recovery, factor}] in simulation time
  sim         per policy and decision latency delta (fraction of M0): makespan,
              ratio to the oracle, and the per-task schedule {task: [node, start, end]}
  oracle      the winning candidate's label, makespan and placement

Disruption generators and parameters are the paper's (runner.build_trace and
the single-failure sweep). Single failures use one biting cell per variant:
the median, by wait/oracle ratio, of the cells where waiting is slowed
(wait makespan > 1.001 M0), as the paper's single-failure statistics do.
"""
import argparse
import copy
import json
import statistics
from pathlib import Path

from ncsim.core.simulation import Simulation
from ncsim.experiments.oracle import FixedPlacementScheduler, oracle_run, phase_networks
from ncsim.experiments.runner import _run_policy, build_trace, undisrupted_makespan
from ncsim.experiments.workloads import make_workload, scale_network_to_ccr, spatial_network
from ncsim.models.dag import SingleDAGSource
from ncsim.models.disruptions import DisruptionType, ScheduledDisruptions
from ncsim.models.routing import DirectLinkRouting
from ncsim.scheduler.base import NetworkSnapshot
from ncsim.scheduler.repair import RepairingSagaScheduler
from ncsim.scheduler.saga_adapter import SagaScheduler

STRUCTURES = ["out_tree", "in_tree", "parallel_chains", "etl", "stats", "train", "predict",
              "epigenomics", "montage", "seismology"]
GENERATORS = ["single", "flapping", "mobility", "jamming"]
POLICIES = ["wait", "reroute", "local", "global"]
DELTAS = [0.0, 0.02, 0.1, 0.5]           # decision latency per replanned task, x M0
SEED = 42
TYPES = {DisruptionType.LINK_DOWN: "down", DisruptionType.LINK_DEGRADE: "degrade",
         DisruptionType.RECOVERY: "recovery"}


# Tier 2: a lane of real, mixed hardware (n0 an UP board, n1-n2 Pi 4s, n3-n4
# Pi 3s; two identical lanes). Compute speeds are the measured rates of the
# task kernel
# (zlib level 6 on 256 KB, iterations/s inside the runner image), normalized
# to mean 1 as the paper's U(0.5, 1.5) speeds are. The UP board is wired to
# both Pi 4s (unshaped switch links, priced at wired_factor x the radio
# bandwidth); every other pair is radio, the only kind a disruption reaches.
TIER2 = {"lanes": [["anrg-7", "rpi4-1", "rpi4-2", "rpi51", "rpi52"],     # rpi4-1 capped at 1.5 GHz
                   ["anrg-8", "rpi4-3", "rpi4-4", "rpi49", "rpi50"]],
         "rates": [98.2, 29.2, 29.2, 15.7, 15.7],
         "wired": [["n0", "n1"], ["n0", "n2"]],
         "wired_factor": 100.0,
         "netem": "delay 10ms 2ms distribution normal loss 0.2%",   # + an fq_codel child (emulator.py)
         "radio_queue": "netem>fq_codel",
         "tcp_efficiency": 0.945}


def build_network(seed, dag, ccr, tier2=False):
    """The instance's network: Tier 1 is the paper's; Tier 2 replaces the
    speeds with the lane's hardware and makes the configured pairs wired."""
    net = spatial_network(5, seed=seed)
    if tier2:
        rates = TIER2["rates"]
        ref = sum(rates) / len(rates)
        for i, nid in enumerate(sorted(net.nodes)):
            net.nodes[nid].compute_capacity = rates[i] / ref
    scale_network_to_ccr(net, dag, ccr)
    if tier2:
        for link in net.links.values():
            if is_wired(link.from_node, link.to_node):
                link.bandwidth *= TIER2["wired_factor"]
    return net


def is_wired(a, b):
    return sorted((a, b)) in [sorted(p) for p in TIER2["wired"]]


def radio_only(net, trace):
    """Drop the events on wired pairs: jamming, mobility and radio flapping
    do not reach a cable."""
    return [e for e in trace if not is_wired(net.links[e.target_id].from_node, net.links[e.target_id].to_node)]


def schedule_of(sim):
    out = {}
    for key, st in sim.engine.task_states.items():
        out[key.split(":", 1)[1]] = [st.assigned_node, st.started_at, st.completed_at]
    return out


def run(policy, net, dag, trace, delta_abs):
    """_run_policy, keeping the Simulation for its per-task schedule."""
    if policy == "oracle":
        res, extra = _run_policy("oracle", net, dag, trace, 0.0)
        return res, extra, None
    from ncsim.models.routing import WidestPathRouting
    scope, routing = {"wait": ("none", DirectLinkRouting()), "reroute": ("none", WidestPathRouting()),
                      "local": ("local", DirectLinkRouting()), "global": ("global", DirectLinkRouting())}[policy]
    sim = Simulation(network=copy.deepcopy(net), scheduler=RepairingSagaScheduler(algorithm="heft", scope=scope),
                     dag_source=SingleDAGSource(copy.deepcopy(dag)), routing_model=routing,
                     disruption_model=ScheduledDisruptions(list(trace)), repair_cost_per_task=delta_abs)
    res = sim.run()
    return res, {}, schedule_of(sim)


def trace_json(trace):
    """The trace as ordered link steps, implicit recoveries materialized
    (an event with a duration recovers at time + duration), as the engine
    and oracle.phase_networks expand it. Same-time steps keep trace order:
    the engine processes them one by one, and under local repair the last
    one's plan is the one that applies."""
    steps = []
    for seq, e in enumerate(trace):
        kind = TYPES[e.disruption_type]
        factor = 0.0 if kind == "down" else float(e.parameters.get("bandwidth_factor", 1.0))
        steps.append((e.time, seq, e.target_id, kind, factor))
        if e.duration is not None and kind in ("down", "degrade"):
            steps.append((e.time + e.duration, len(trace) + seq, e.target_id, "recovery", 1.0))
    steps.sort(key=lambda s: (s[0], s[1]))
    return [{"t": t, "link": lid, "type": kind, "factor": f} for t, _, lid, kind, f in steps]


def oracle_placement(net, dag, trace, label):
    """The placement of the oracle's winning candidate (None for reactive-global)."""
    if not label.startswith("phase["):
        return None
    planner = SagaScheduler(algorithm="heft")
    for start_time, view in phase_networks(net, trace):
        if f"phase[t={start_time:g}]" == label:
            snap = NetworkSnapshot.from_network(view, timestamp=start_time)
            return dict(planner.on_dag_inject(dag, snap).assignments)
    return None


def make_single_trace(net, pair, start, duration):
    from ncsim.models.disruptions import DisruptionEvent
    a, b = pair
    ev = []
    for src, dst in ((a, b), (b, a)):
        lid = f"{src}->{dst}" if f"{src}->{dst}" in net.links else next(
            k for k, l in net.links.items() if l.from_node == src and l.to_node == dst)
        ev.append(DisruptionEvent(time=start, target_type="link", target_id=lid,
                                  disruption_type=DisruptionType.LINK_DOWN, duration=duration))
    return ev


def scenario(structure, index, ccr, gen, out, deltas, cell=None, tier2=False):
    seed = SEED * 10_000 + index
    dag = make_workload(structure, seed=seed)
    net = build_network(seed, dag, ccr, tier2)
    m0 = undisrupted_makespan(net, dag)
    if gen == "single":
        pair, frac = cell
        trace = make_single_trace(net, pair, frac * m0, 0.5 * m0)
    elif gen == "episode":
        pair, t = cell
        trace = make_single_trace(net, pair, t, 0.5 * m0)
    else:
        trace = build_trace(gen, net, m0, seed)
        if tier2:
            trace = radio_only(net, trace)
    sid = ("t2-" if tier2 else "") + f"{structure}-{index}-ccr{ccr:g}-{gen}"
    if gen == "single":
        sid += f"-{pair[0]}{pair[1]}-t{frac:g}"
    # Initial plan: the no-repair run's placement (HEFT on the pristine network).
    base, _, base_sched = run("wait", net, dag, [], 0.0)
    plan = {t: v[0] for t, v in base_sched.items()}
    sims = {}
    orc, extra, _ = run("oracle", net, dag, trace, 0.0)
    oracle = {"label": extra["oracle_label"], "makespan": orc.makespan,
              "placement": oracle_placement(net, dag, trace, extra["oracle_label"])}
    for p in POLICIES:
        for d in (deltas if p in ("local", "global") else [0.0]):
            res, _, sched = run(p, net, dag, trace, d * m0)
            sims[f"{p}@{d:g}"] = {"makespan": res.makespan, "ratio": res.makespan / orc.makespan,
                                  "status": res.status, "schedule": sched}
    doc = {
        "id": sid, "structure": structure, "index": index, "seed": seed, "ccr": ccr, "generator": gen,
        "cell": list(cell) if cell else None,
        "dag": {"id": dag.id, "tasks": {t: v.compute_cost for t, v in dag.tasks.items()},
                "edges": [[e.from_task, e.to_task, e.data_size] for e in dag.edges]},
        "nodes": {n: v.compute_capacity for n, v in net.nodes.items()},
        "links": {k: [l.from_node, l.to_node] for k, l in net.links.items()},
        "bandwidth": min(l.bandwidth for l in net.links.values()),
        "link_bw": {k: l.bandwidth for k, l in net.links.items()},
        "m0": m0, "plan": plan, "trace": trace_json(trace), "oracle": oracle, "sim": sims,
    }
    if tier2:
        rates = TIER2["rates"]
        doc["tier"] = 2
        doc["tier2"] = {**TIER2, "r_ref": sum(rates) / len(rates)}
    (out / f"{sid}.json").write_text(json.dumps(doc))
    return doc


def biting_cell(structure, index, ccr, tier2=False):
    """Median biting single-failure cell, by wait/oracle ratio (radio pairs)."""
    seed = SEED * 10_000 + index
    dag = make_workload(structure, seed=seed)
    net = build_network(seed, dag, ccr, tier2)
    m0 = undisrupted_makespan(net, dag)
    nodes = sorted(net.nodes)
    pairs = [(a, b) for i, a in enumerate(nodes) for b in nodes[i + 1:] if not (tier2 and is_wired(a, b))]
    cells = []
    for pair in pairs:
        for k in range(1, 10):
            frac = k / 10
            trace = make_single_trace(net, pair, frac * m0, 0.5 * m0)
            w, _, _ = run("wait", net, dag, trace, 0.0)
            if w.makespan > 1.001 * m0:
                cells.append((w.makespan / m0, pair, frac))
    if not cells:
        return None
    cells.sort()
    return cells[len(cells) // 2][1:]


def episode_cell():
    """Fig. 2 of the paper: out_tree-0, CCR 1; the pair carrying the plan's
    largest cross-node transfer fails 0.01 before that transfer starts."""
    seed = SEED * 10_000
    dag = make_workload("out_tree", seed=seed)
    net = spatial_network(5, seed=seed)
    scale_network_to_ccr(net, dag, 1.0)
    _, _, sched = run("wait", net, dag, [], 0.0)
    best = None
    for e in dag.edges:
        src, dst = sched[e.from_task][0], sched[e.to_task][0]
        if src != dst and (best is None or e.data_size > best[0]):
            best = (e.data_size, tuple(sorted((src, dst))), sched[e.from_task][2])
    return best[1], best[2] - 0.01


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--structures", nargs="*", default=STRUCTURES)
    ap.add_argument("--ccrs", nargs="*", type=float, default=[0.5, 1.0, 5.0])
    ap.add_argument("--generators", nargs="*", default=GENERATORS)
    ap.add_argument("--index", type=int, default=0)
    ap.add_argument("--quick", action="store_true", help="only the Fig. 2 episode")
    ap.add_argument("--tier2", action="store_true", help="the mixed-hardware lane (TIER2)")
    ap.add_argument("--deltas", nargs="*", type=float, default=DELTAS)
    a = ap.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    if not a.tier2:
        pair, t = episode_cell()
        d = scenario("out_tree", 0, 1.0, "episode", out, a.deltas, cell=(pair, t))
        print(f"{d['id']}: M0={d['m0']:.4f} pair={pair} t={t:.3f} oracle={d['oracle']['makespan']:.4f} "
              + " ".join(f"{k}={v['ratio']:.3f}" for k, v in d["sim"].items() if k.endswith("@0")), flush=True)
    if a.quick:
        return
    for s in a.structures:
        for ccr in a.ccrs:
            for g in a.generators:
                cell = None
                if g == "single":
                    cell = biting_cell(s, a.index, ccr, a.tier2)
                    if cell is None:
                        print(f"{s} ccr={ccr:g}: no biting single-failure cell", flush=True)
                        continue
                d = scenario(s, a.index, ccr, g, out, a.deltas, cell=cell, tier2=a.tier2)
                print(f"{d['id']}: M0={d['m0']:.4g} oracle={d['oracle']['label']} "
                      + " ".join(f"{k}={v['ratio']:.3f}" for k, v in d["sim"].items() if k.endswith("@0")),
                      flush=True)


if __name__ == "__main__":
    main()
