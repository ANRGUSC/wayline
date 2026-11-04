#!/usr/bin/env python3
"""E17 driver: the MILCOM 2026 schedule-repair study, executed on the testbed.

One scenario (scenarios.py) runs on one lane: five testbed nodes standing in
for the simulator's n0..n4, their links emulated by emulator.Lane. Every
policy executes the scenario's DAG through Wayline, from the same initial
HEFT plan, against the same disruption trace:

  m0        no disruption: the testbed's undisrupted makespan M0_tb
  wait      no repair; transfers on a downed link stall until it recovers
  reroute   no repair; host routes detour around downed links (widest path)
  local@d   repair the tasks the changed link reached (and their movable
            descendants), decision latency d x M0 per replanned task
  global@d  repair every movable task, same latency model
  oracle    the simulator oracle's winning fixed placement, no repair

Mapping to the testbed
  time      one simulated second is sigma testbed seconds. Task runtimes and
            transfer sizes are generated at the nominal S = T / M0_sim; the
            disruption trace and decision latencies are scaled by the
            measured sigma = M0_tb / M0_sim, so events land at the same point
            of the execution as in the simulation.
  compute   each task sleeps cost / speed(node) x S on whichever node runs
            it (runtimeProfile for all five lane nodes).
  data      each DAG edge sends size / b x S x GOODPUT incompressible bytes,
            so a transfer alone on an undisrupted link takes size / b x S.
  nodes     one task at a time per node (cpu "64" exceeds every node), in
            readiness order, as the simulator's FIFO node queue.
  repair    the simulator's own RepairingSagaScheduler decides; its input is
            the live run (task states from the ODAG status, delivered inputs
            from the data agents) and the emulated link state. Plans reach
            Wayline as spec.placement; the controller moves tasks that have
            not started and re-sends their inputs. A newer plan supersedes a
            pending one, the last non-empty plan of an instant wins, as in
            the simulator's engine.

Run on eclipse in the paper environment (PYTHONHASHSEED=0, PYTHONPATH with
milcom/ncsim and milcom/saga/src); the sudo password is the first line of
stdin.

  driver.py --lane rpi1,rpi2,rpi3,rpi4,rpi5 --scenario S.json --out DIR
  driver.py --lane ... --queue QDIR --out DIR      (claim scenarios until empty)
"""
import argparse
import calendar
import csv
import glob
import json
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from emulator import Lane  # noqa: E402

from ncsim.core.execution_engine import NetworkState  # noqa: E402
from ncsim.experiments.workloads import make_workload  # noqa: E402
from ncsim.models.task import TaskState, TaskStatus  # noqa: E402
from ncsim.scheduler.base import NetworkSnapshot  # noqa: E402
from ncsim.scheduler.repair import RepairingSagaScheduler  # noqa: E402
from scenarios import build_network  # noqa: E402

NS = "wl-system"
ODAGS = f"/apis/wl.io/v1/namespaces/{NS}/odags"
IMAGE = "10.10.0.1:5000/wl-runner-e17:milcom1"
AGENT_PORT = 8082
INVENTORY = os.path.expanduser("~/testbed/inventory.csv")
RATE_MBIT = 2.0         # emulated per-link rate (HTB, wire bytes)
TCP_EFFICIENCY = 0.955  # payload / wire rate, measured at 2, 4 and 10 Mbit/s
GOODPUT = RATE_MBIT * 1e6 / 8 * TCP_EFFICIENCY   # payload B/s through one undisrupted link
CHANGE = {"down": "link_down", "degrade": "link_degrade", "recovery": "recovery"}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def rfc3339(s):
    """Unix seconds from an RFC 3339 UTC timestamp with up to ns precision."""
    if not s:
        return None
    s = s.rstrip("Z")
    base, _, frac = s.partition(".")
    t = calendar.timegm(time.strptime(base, "%Y-%m-%dT%H:%M:%S"))
    return t + (float("0." + frac) if frac else 0.0)


def k8s_name(t):
    return re.sub(r"[^a-z0-9-]+", "-", t.lower()).strip("-") or "t"


class BadScenario(Exception):
    """A scenario file this driver cannot run (not a lane fault)."""


# ---------------------------------------------------------------- k8s api
class Api:
    """The API server through a private `kubectl proxy`."""

    def __init__(self):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        self.port = s.getsockname()[1]
        s.close()
        self.proxy = subprocess.Popen(["kubectl", "proxy", f"--port={self.port}"],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(100):
            try:
                self.req("GET", "/version")
                return
            except Exception:  # noqa: BLE001 - proxy still starting
                time.sleep(0.1)
        raise RuntimeError("kubectl proxy did not start")

    def req(self, method, path, body=None, ctype="application/json"):
        data = json.dumps(body).encode() if body is not None else None
        r = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                   headers={"Content-Type": ctype})
        try:
            with urllib.request.urlopen(r, timeout=15) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as e:
            if e.code != 404:
                e.msg = f"{e.msg}: {e.read().decode()[:2000]}"
            raise

    def get(self, name):
        return self.req("GET", f"{ODAGS}/{name}")

    def create(self, obj):
        return self.req("POST", ODAGS, obj)

    def patch(self, name, body):
        return self.req("PATCH", f"{ODAGS}/{name}", body, "application/merge-patch+json")

    def delete(self, name):
        try:
            self.req("DELETE", f"{ODAGS}/{name}")
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
        for _ in range(300):
            try:
                self.get(name)
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return
            time.sleep(0.2)

    def close(self):
        self.proxy.terminate()


def http_text(url, timeout=2.0):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read().decode().strip()


# --------------------------------------------------------------- scenario
class Scenario:
    """A scenario file plus the simulator objects it was generated from."""

    def __init__(self, path):
        self.path = path
        self.doc = json.load(open(path))
        d = self.doc
        self.tier2 = d.get("tier") == 2
        self.dag = make_workload(d["structure"], seed=d["seed"])
        self.net = build_network(d["seed"], self.dag, d["ccr"], self.tier2)
        # The rebuilt instance must be the exported one. Costs can differ in
        # the last bit across Python/numpy builds (riotbench workloads); the
        # exported values are the simulator's, so they are the ones used.
        assert set(self.dag.tasks) == set(d["dag"]["tasks"]), "dag mismatch"
        for t, c in d["dag"]["tasks"].items():
            assert abs(self.dag.tasks[t].compute_cost - c) <= 1e-12 * max(1.0, abs(c)), f"cost of {t} differs"
            self.dag.tasks[t].compute_cost = c
        edges = [[e.from_task, e.to_task, e.data_size] for e in self.dag.edges]
        assert len(edges) == len(d["dag"]["edges"]) and all(
            a[:2] == b[:2] and abs(a[2] - b[2]) <= 1e-12 * max(1.0, abs(b[2])) for a, b in zip(edges, d["dag"]["edges"])
        ), "edges differ"
        for e, (_, _, size) in zip(self.dag.edges, d["dag"]["edges"]):
            e.data_size = size
        for lid, l in self.net.links.items():
            want = d.get("link_bw", {}).get(lid, d["bandwidth"])
            assert abs(l.bandwidth - want) <= 1e-9 * want, f"bandwidth of {lid} differs"
        self.tn = {t: k8s_name(t) for t in self.dag.tasks}
        assert len(set(self.tn.values())) == len(self.tn), "task names collide after sanitizing"
        self.back = {v: k for k, v in self.tn.items()}
        self.children = defaultdict(list)
        self.parents = defaultdict(list)
        for e in self.dag.edges:
            self.children[e.from_task].append((e.to_task, e.data_size))
            self.parents[e.to_task].append(e.from_task)
        groups = []
        for e in d["trace"]:
            if groups and abs(groups[-1][0] - e["t"]) < 1e-9:
                groups[-1][1].append(e)
            else:
                groups.append((e["t"], [e]))
        self.groups = groups


# ------------------------------------------------------------------ lane
def lane_nodes(names):
    inv = {r["name"]: r for r in csv.DictReader(open(INVENTORY))}
    out = []
    for n in names:
        r = inv[n]
        out.append({"name": n, "ctl_ip": r["ctl_ip"], "data_ip": r["data_ip"], "mac": r["data_mac"],
                    "dif": "eth0.20" if r["kind"].startswith("rpi") else "enp1s0.20"})
    return out


class Runner:
    def __init__(self, lane_names, out_dir, T, password, tier2=None):
        """tier2: a Tier 2 scenario's "tier2" block, which fixes the lane's
        wired pairs and radio impairments (every scenario a Tier 2 lane runs
        shares them); None for Tier 1."""
        self.names = lane_names
        self.nodes = lane_nodes(lane_names)
        self.idx = {n: i for i, n in enumerate(lane_names)}
        self.lane_id = lane_names[0]
        self.out = out_dir
        self.T = T
        self.api = Api()
        self.t2 = tier2
        kw = {}
        if tier2:
            assert lane_names in tier2["lanes"], f"Tier 2 scenarios are for lanes {tier2['lanes']}"
            wired = [(int(a[1:]), int(b[1:])) for a, b in tier2["wired"]]
            bw = {(i, j): (tier2["wired_factor"] if frozenset((i, j)) in {frozenset(p) for p in wired} else 1.0)
                  for i in range(5) for j in range(5) if i != j}
            kw = {"wired": wired, "netem": tier2["netem"], "bw": bw}
        self.goodput = RATE_MBIT * 1e6 / 8 * (tier2["tcp_efficiency"] if tier2 else TCP_EFFICIENCY)
        self.lane = Lane(self.nodes, RATE_MBIT, password=password, log=log, **kw)
        self.lane.setup()
        self.pool = ThreadPoolExecutor(16)
        self.seq = 0

    def close(self):
        try:
            self.lane.teardown()
        finally:
            self.lane.close()
            self.api.close()

    # ------------------------------------------------------------- odag
    def odag(self, name, sc, placement, S):
        d = sc.doc
        speeds = d["nodes"]
        b = d["bandwidth"]
        tasks = []
        for t in sc.dag.tasks:
            cost = sc.dag.tasks[t].compute_cost
            prof = {self.names[k]: round(cost / speeds[f"n{k}"] * S, 4) for k in range(5)}
            node = self.names[int(placement[t][1:])]
            spec = {"name": sc.tn[t], "image": IMAGE, "command": ["python", "/app/stage.py"],
                    "runner": "e17t2" if sc.tier2 else "e17", "runtime": int(round(prof[node])),
                    "runtimeProfile": prof, "resources": {"cpu": "64", "memory": "64Mi"},
                    "constraints": {"nodeNames": [node]}}
            env = []
            if sc.tier2:
                # Real work: kernel iterations that take cost x S seconds at
                # the reference rate, so cost / speed x S on each node.
                env.append({"name": "WL_WORK", "value": str(max(0, int(round(cost * S * d["tier2"]["r_ref"]))))})
            outs = []
            for c, size in sc.children[t]:
                nbytes = max(1, int(round(size / b * S * self.goodput)))
                outs.append({"name": f"to-{sc.tn[c]}", "dataSize": str(nbytes)})
            if outs:
                spec["outputs"] = outs
                env.append({"name": "WL_EDGE_SIZES",
                            "value": ",".join(f"{o['name']}:{o['dataSize']}" for o in outs)})
            if env:
                spec["env"] = env
            if sc.parents[t]:
                spec["dependencies"] = [sc.tn[p] for p in sc.parents[t]]
                spec["inputs"] = [{"producer": sc.tn[p], "object": f"to-{sc.tn[t]}"} for p in sc.parents[t]]
            tasks.append(spec)
        return {"apiVersion": "wl.io/v1", "kind": "ODAG",
                "metadata": {"name": name, "namespace": NS, "labels": {"wl.io/experiment": "e17"}},
                "spec": {"scheduler": "random", "executor": {"order": "readiness"},
                         "realizationPolicy": {"mode": "none", "revise": False}, "tasks": tasks}}

    # ------------------------------------------------------- live state
    def task_states(self, sc, obj, t0, sigma, sim_now, plan0):
        """The live run as the simulator's task states (dag-qualified keys)."""
        dag = sc.dag
        entries = {e["name"]: e for e in obj.get("status", {}).get("tasks", []) or []}
        placement = obj["spec"].get("placement", {}) or {}
        states, node_of = {}, {}
        for t in dag.tasks:
            n = sc.tn[t]
            st = TaskState(task_id=t, dag_id=dag.id)
            e = entries.get(n)
            if e and e.get("node") in self.idx:
                st.assigned_node = f"n{self.idx[e['node']]}"
                start = rfc3339(e.get("taskStartTime") or e.get("startTime"))
                st.started_at = (start - t0) / sigma if start else sim_now
                close = rfc3339(e.get("taskCloseTime")) or (
                    rfc3339(e.get("completionTime")) if e.get("phase") == "Succeeded" else None)
                if close is not None and e.get("phase") == "Succeeded":
                    st.status = TaskStatus.COMPLETED
                    st.completed_at = (close - t0) / sigma
                else:
                    st.status = TaskStatus.RUNNING
            else:
                tgt = placement.get(n)
                st.assigned_node = f"n{self.idx[tgt]}" if tgt in self.idx else plan0[t]
                st.status = TaskStatus.PENDING
            states[t] = st
            node_of[t] = st.assigned_node
        # Inputs delivered to not-started tasks: a completed producer on the
        # same node has handed off; across nodes, ask the consumer's agent.
        queries = []
        for t, st in states.items():
            if st.status != TaskStatus.PENDING:
                continue
            remaining = set()
            for p in sc.parents[t]:
                ps = states[p]
                if ps.status != TaskStatus.COMPLETED:
                    remaining.add(p)
                elif ps.assigned_node != st.assigned_node:
                    queries.append((t, p))
            st.predecessors_remaining = remaining

        def ready(tp):
            t, p = tp
            ip = self.nodes[int(states[t].assigned_node[1:])]["data_ip"]
            key = f"{sc.tn[p]}.to-{sc.tn[t]}"
            try:
                return http_text(f"http://{ip}:{AGENT_PORT}/ready/{obj['metadata']['name']}/{key}") == "true"
            except Exception:  # noqa: BLE001 - unreachable agent: not delivered
                return False
        for (t, p), ok in zip(queries, self.pool.map(ready, queries)):
            if not ok:
                states[t].predecessors_remaining.add(p)
        for st in states.values():
            if st.status == TaskStatus.PENDING and not st.predecessors_remaining:
                st.status = TaskStatus.QUEUED   # inputs all here, waiting for its node
        return NetworkState(sim_time=sim_now, node_states={}, link_states={},
                            task_states={f"{dag.id}:{t}": s for t, s in states.items()})

    def snapshot(self, sc, view, sim_now):
        snap = NetworkSnapshot.from_network(sc.net, timestamp=sim_now)
        for lid, ls in snap.links.items():
            kind, f = view[lid]
            ls.bandwidth = 0.0 if kind == "down" else ls.bandwidth * f
        return snap

    # -------------------------------------------------------------- run
    def run(self, sc, label, policy, delta, placement, sigma, S, m0_tb):
        """One execution; returns its record."""
        self.seq += 1
        name = f"e17-{self.lane_id}-{os.getpid() % 10000}-{self.seq}"
        repair = policy in ("local", "global")
        sched = None
        if repair:
            sched = RepairingSagaScheduler(algorithm="heft", scope=policy)
            pristine = NetworkSnapshot.from_network(sc.net, timestamp=0.0)
            inj = dict(sched.on_dag_inject(sc.dag, pristine).assignments)
            assert inj == sc.doc["plan"], "HEFT initial plan differs from the scenario's (hash seed?)"
        try:
            self.lane.reset()
        except Exception as e:  # noqa: BLE001 - a node lost its emulation state (link reset, reboot)
            log(f"lane reset failed ({e}); setting the lane up again")
            self.lane.setup()
            self.lane.reset()
        self.api.delete(name)
        created = time.time()
        self.api.create(self.odag(name, sc, placement, S))
        events = sc.groups if policy != "m0" else []
        view = {lid: ("up", 1.0) for lid in sc.net.links}
        rec = {"scenario": sc.doc["id"], "label": label, "policy": policy, "delta": delta,
               "lane": self.names, "odag": name, "T": self.T, "S": S, "sigma": sigma, "m0_sim": sc.doc["m0"],
               "m0_tb": m0_tb, "events": [], "plans": [], "status": None}
        # t0: the first task start, as the DAG's injection in the simulator.
        t0, obj = None, None
        deadline = created + 120
        while t0 is None:
            obj = self.api.get(name)
            starts = [rfc3339(e.get("taskStartTime") or e.get("startTime"))
                      for e in obj.get("status", {}).get("tasks", []) or []]
            starts = [s for s in starts if s]
            if starts:
                t0 = min(starts)
            elif time.time() > deadline:
                rec["status"] = "no-start"
                self.api.delete(name)
                return rec
            else:
                time.sleep(0.05)
        rec["t0"] = t0
        rec["start_lag"] = t0 - created
        expect = (m0_tb or sc.doc["m0"] * S)
        limit = t0 + max(15 * expect, 300)
        gi, pending, next_check = 0, None, 0.0
        plan0 = dict(sc.doc["plan"])
        while True:
            now = time.time()
            if now >= next_check:
                obj = self.api.get(name)
                ph = obj.get("status", {}).get("phase")
                if ph in ("Succeeded", "Failed"):
                    rec["status"] = ph
                    break
                if now > limit:
                    rec["status"] = "timeout"
                    break
                next_check = now + 0.5
            t_ev = t0 + events[gi][0] * sigma if gi < len(events) else float("inf")
            t_pl = pending["apply_at"] if pending else float("inf")
            wake = min(t_ev, t_pl, next_check)
            if wake > now:
                time.sleep(min(wake - now, 0.05))
                continue
            if t_pl <= now and t_pl <= t_ev:
                self.apply_plan(sc, name, pending, rec)
                pending = None
                continue
            # A disruption instant: every directed-link change at this time.
            t_sim, group = events[gi]
            gi += 1
            changes = []
            for e in group:
                a, b = sc.net.links[e["link"]].from_node, sc.net.links[e["link"]].to_node
                changes.append((int(a[1:]), int(b[1:]), e["type"], e["factor"]))
            self.lane.apply(changes, reroute=(policy == "reroute"))
            applied = time.time()
            rec["events"].append({"t_sim": t_sim, "due": t_ev - t0, "applied": applied - t0,
                                  "n": len(group), "kinds": sorted({e["type"] for e in group})})
            if not repair:
                for e in group:
                    view[e["link"]] = (e["type"] if e["type"] != "recovery" else "up", e["factor"])
                continue
            # The simulator consults the scheduler once per directed-link
            # event, in trace order; each non-empty plan supersedes the
            # pending one.
            c0 = time.time()
            sim_now = (t_ev - t0) / sigma
            obj = self.api.get(name)
            nstate = self.task_states(sc, obj, t0, sigma, sim_now, plan0)
            newest = None
            for e in group:
                view[e["link"]] = (e["type"] if e["type"] != "recovery" else "up", e["factor"])
                snap = self.snapshot(sc, view, sim_now)
                p = sched.on_network_change(CHANGE[e["type"]], e["link"], snap, network_state=nstate)
                if p is not None:
                    newest = p
            compute = time.time() - c0
            if newest is not None:
                k = len(newest.new_assignments)
                if pending is not None:
                    rec["plans"].append({**pending["rec"], "superseded": time.time() - t0})
                movable = {t for t, s in nstate.task_states.items()
                           if s.status in (TaskStatus.PENDING, TaskStatus.QUEUED)}
                pending = {"apply_at": max(t_ev + delta * k * m0_tb, time.time()),
                           "assign": {key.split(":", 1)[1]: node for key, node in newest.new_assignments.items()},
                           "rec": {"t_sim": t_sim, "k": k, "compute": compute, "decided": time.time() - t0,
                                   "due": t_ev - t0 + delta * k * m0_tb, "movable": len(movable),
                                   "moves": {key.split(":", 1)[1]: [nstate.task_states[key].assigned_node, node]
                                             for key, node in newest.new_assignments.items()
                                             if nstate.task_states[key].assigned_node != node}}}
        if pending is not None:
            rec["plans"].append({**pending["rec"], "unapplied": True})
        rec["final"] = self.collect(sc, obj, t0, sigma)
        rec["wall_end"] = time.time() - t0
        if rec["status"] == "timeout" and os.environ.get("E17_KEEP_ON_TIMEOUT"):
            log(f"{name} kept for inspection (E17_KEEP_ON_TIMEOUT)")
            return rec
        self.api.delete(name)
        return rec

    def apply_plan(self, sc, name, pending, rec):
        body = {"spec": {"placement": {sc.tn[t]: self.names[int(n[1:])] for t, n in pending["assign"].items()}}}
        self.api.patch(name, body)
        rec["plans"].append({**pending["rec"], "applied": time.time() - rec["t0"]})

    def collect(self, sc, obj, t0, sigma):
        st = obj.get("status", {})
        tasks = {}
        ends = []
        for e in st.get("tasks", []) or []:
            start = rfc3339(e.get("taskStartTime") or e.get("startTime"))
            close = rfc3339(e.get("taskCloseTime") or e.get("completionTime"))
            if close:
                ends.append(close)
            tasks[sc.back.get(e["name"], e["name"])] = {
                "node": f"n{self.idx[e['node']]}" if e.get("node") in self.idx else e.get("node"),
                "start": start - t0 if start else None, "end": close - t0 if close else None,
                "compute": e.get("computeSeconds"), "handoff": e.get("handoffSeconds")}
        makespan = (max(ends) - t0) if ends and len(tasks) == len(sc.dag.tasks) else None
        return {"phase": st.get("phase"), "message": st.get("message"), "makespan": makespan,
                "ctl_makespan": st.get("makespan"), "tasks": tasks,
                "flows": st.get("actualNetworkFlows"), "recovery": st.get("recovery")}

    # --------------------------------------------------------- scenario
    def scenario(self, path, policies=None, repeat=0):
        try:
            sc = Scenario(path)
        except Exception as e:  # noqa: BLE001 - the file, not the lane
            raise BadScenario(f"{type(e).__name__}: {e}") from e
        d = sc.doc
        if sc.tier2 != (self.t2 is not None):
            raise ValueError(f"{d['id']}: a Tier {2 if sc.tier2 else 1} scenario on a Tier {2 if self.t2 else 1} lane")
        if sc.tier2:
            for k in ("lanes", "rates", "wired", "wired_factor", "netem", "tcp_efficiency", "radio_queue"):
                assert d["tier2"][k] == self.t2[k], f"{d['id']}: tier2 {k} differs from this lane's"
        S = self.T / d["m0"]
        out_dir = os.path.join(self.out, d["id"])
        os.makedirs(out_dir, exist_ok=True)
        plan0 = d["plan"]
        runs = [("m0", "m0", 0.0, plan0)]
        for p in (policies or ["wait", "reroute", "local@0", "local@0.02", "local@0.1", "local@0.5",
                               "global@0", "global@0.02", "global@0.1", "global@0.5", "oracle"]):
            if p == "oracle":
                # The oracle's winning fixed placement, run without repair. When
                # it is the initial plan itself, that run is the wait run.
                if d["oracle"]["placement"] is not None and d["oracle"]["placement"] != plan0:
                    runs.append(("oracle", "wait", 0.0, d["oracle"]["placement"]))
                continue
            pol, _, dl = p.partition("@")
            runs.append((p, pol, float(dl or 0), plan0))
        m0_tb, sigma = None, S
        for label, pol, dl, placement in runs:
            t = time.time()
            rec = self.run(sc, label, pol, dl, placement, sigma, S, m0_tb)
            rec["repeat"] = repeat
            ms = rec.get("final", {}).get("makespan")
            rec["rate_mbit"] = RATE_MBIT
            if label == "m0":
                if ms is None:
                    log(f"{d['id']}: m0 run {rec['status']}; skipping scenario")
                    json.dump(rec, open(os.path.join(out_dir, f"r{repeat}-{self.lane_id}-m0.json"), "w"))
                    return
                m0_tb, sigma = ms, ms / d["m0"]
            skey = label if "@" in label else f"{label}@0"
            sim_ms = d["oracle"]["makespan"] if label == "oracle" else d["sim"].get(skey, {}).get("makespan")
            rec["sim_makespan"] = sim_ms
            log(f"{d['id']} {label:11s} {rec['status']:9s} makespan={ms if ms is None else round(ms, 2)} "
                f"tb/M0={ms / m0_tb if ms and m0_tb else float('nan'):.3f} "
                f"sim/M0={sim_ms / d['m0'] if sim_ms else float('nan'):.3f} "
                f"events={len(rec['events'])} plans={len(rec['plans'])} ({time.time() - t:.0f}s)")
            json.dump(rec, open(os.path.join(out_dir, f"r{repeat}-{self.lane_id}-{label}.json"), "w"))


def main():
    # Stop cleanly (lane teardown in the finally below) on SIGTERM, and on
    # SIGINT even when started in the background, where it arrives ignored.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    signal.signal(signal.SIGINT, signal.default_int_handler)
    ap = argparse.ArgumentParser()
    ap.add_argument("--lane", required=True)
    ap.add_argument("--scenario", nargs="*", default=[])
    ap.add_argument("--queue", help="claim scenario files from this directory until it is empty")
    ap.add_argument("--policies", nargs="*")
    ap.add_argument("--T", type=float, default=60.0, help="target undisrupted makespan, seconds")
    ap.add_argument("--out", required=True)
    ap.add_argument("--repeat", type=int, default=0)
    ap.add_argument("--tier2", action="store_true", help="a Tier 2 lane (scenarios.TIER2)")
    a = ap.parse_args()
    pw = sys.stdin.readline().rstrip("\n")
    t2 = None
    if a.tier2:
        from scenarios import TIER2
        rates = TIER2["rates"]
        t2 = {**TIER2, "r_ref": sum(rates) / len(rates)}
    r = Runner(a.lane.split(","), a.out, a.T, pw, tier2=t2)
    try:
        for p in a.scenario:
            r.scenario(p, a.policies, a.repeat)
        if a.queue:
            mine = os.path.join(a.queue, "claimed", r.lane_id)
            os.makedirs(mine, exist_ok=True)
            failures = 0
            while True:
                todo = sorted(f for f in os.listdir(a.queue) if f.endswith(".json"))
                if not todo:
                    break
                got = None
                for f in todo:
                    try:
                        os.rename(os.path.join(a.queue, f), os.path.join(mine, f))
                        got = os.path.join(mine, f)
                        break
                    except FileNotFoundError:
                        continue
                if got:
                    try:
                        r.scenario(got, a.policies, a.repeat)
                        failures = 0
                    except BadScenario as e:
                        log(f"{got}: cannot run it ({e}); set aside in {a.queue}/bad")
                        os.makedirs(os.path.join(a.queue, "bad"), exist_ok=True)
                        os.rename(got, os.path.join(a.queue, "bad", os.path.basename(got)))
                        continue
                    except Exception as e:  # noqa: BLE001 - keep the lane going while it is healthy
                        log(f"{got}: {type(e).__name__}: {e}")
                        failures += 1
                        # Set the scenario aside for a whole re-run elsewhere
                        # and drop this lane's partial results for it.
                        sid = json.load(open(got))["id"]
                        for f in glob.glob(os.path.join(a.out, sid, f"r{a.repeat}-{r.lane_id}-*.json")):
                            os.remove(f)
                        os.makedirs(os.path.join(a.queue, "retry"), exist_ok=True)
                        os.rename(got, os.path.join(a.queue, "retry", os.path.basename(got)))
                        try:
                            r.lane.setup()
                        except Exception as e2:  # noqa: BLE001
                            log(f"lane setup failed ({e2}); this lane stops claiming scenarios")
                            break
                        if failures >= 2:
                            log("two scenarios in a row failed; this lane stops claiming scenarios")
                            break
    finally:
        r.close()


if __name__ == "__main__":
    main()
