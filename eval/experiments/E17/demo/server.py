#!/usr/bin/env python3
"""Repair Lab: one DAG on one Tier 2 lane, run undisrupted, then again
against a disruption the user draws (link cuts, jammers), under one repair
strategy. A thin web server around E17's driver (Runner.run) and emulator.

  echo <sudo password> | ./run-demo.sh [--lane anrg-7,...] [--port 8095] [--T 60]

The UI and a JSON API are served on 127.0.0.1; `tailscale serve` publishes
them. Runs execute one at a time (one lane); the rest wait in a queue.

Which pairs are wired is the lab's topology, set from the map: every other
pair is an emulated radio link. A topology gets its own network model (a
wired pair is wired_factor x the radio bandwidth, as in Tier 2), its own HEFT
plan and undisrupted makespan, and its own baselines; the lane's emulation is
rebuilt for it (wired pairs ride the switch unshaped).

Disruptions are drawn in testbed seconds from the baseline's first task
start. They become the driver's trace (simulated seconds, t / sigma) with
the paper's semantics: a radio pair goes down, both directions, while any
of its windows is active (a cut on that pair, or a jammer with one of the
pair's endpoints within its radius, on the lab's map, LAYOUT); overlapping
windows merge into one outage. Wired pairs are never disrupted.
"""
import argparse
import collections
import copy
import glob
import json
import math
import os
import queue
import signal
import sys
import re
import threading
import time
import traceback
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import driver  # noqa: E402  (E17)
from emulator import RootShell  # noqa: E402
import scenarios  # noqa: E402  (E17)
from scenarios import TIER2  # noqa: E402
from ncsim.experiments.runner import undisrupted_makespan  # noqa: E402
from ncsim.experiments.workloads import scale_network_to_ccr, spatial_network  # noqa: E402

STATIC = os.path.join(HERE, "static")
PREFIX = "/repair"          # the path tailscale serve mounts the app under
STRATEGIES = ("wait", "reroute", "local", "global")
FAMILIES = {"in_tree": "Synthetic", "out_tree": "Synthetic", "parallel_chains": "Synthetic",
            "etl": "RIoTBench", "predict": "RIoTBench", "stats": "RIoTBench", "train": "RIoTBench",
            "epigenomics": "WfCommons", "montage": "WfCommons", "seismology": "WfCommons"}
GENERATORS = ("-flapping", "-jamming", "-mobility", "-single")
T2_WIRED = frozenset(tuple(sorted(p)) for p in TIER2["wired"])   # the E17 Tier 2 wiring
# The lane's map, in a 100 x 100 field: a convex pentagon (no link passes
# through a node), the UP board at the left with its two cabled Pi 4s, the
# radio-only Pi 3s on the right. One layout for every DAG (the lane is the
# same five machines); jammers act on it.
LAYOUT = {"n0": (18.0, 52.0), "n1": (42.0, 22.0), "n2": (42.0, 82.0), "n3": (82.0, 30.0), "n4": (82.0, 74.0)}


def hardware(name):
    if name.startswith("anrg"):
        return "UP board"
    return "Raspberry Pi 4" if name.startswith("rpi4-") else "Raspberry Pi 3"


def pair_key(a, b):
    return tuple(sorted((a, b)))


def topo_label(topo):
    return "all radio" if not topo else "wired " + ", ".join(f"{a}-{b}" for a, b in sorted(topo))


def variant(base, topo):
    """The scenario on the lab's topology: same DAG, compute speeds and radio
    bandwidth; wired pairs at wired_factor x; plan and M0 recomputed (HEFT on
    the pristine network, as scenarios.py exports them)."""
    d0 = base.doc
    net = spatial_network(5, seed=d0["seed"])
    rates = TIER2["rates"]
    ref = sum(rates) / len(rates)
    for i, nid in enumerate(sorted(net.nodes)):
        net.nodes[nid].compute_capacity = rates[i] / ref
    scale_network_to_ccr(net, base.dag, d0["ccr"])
    for link in net.links.values():
        if pair_key(link.from_node, link.to_node) in topo:
            link.bandwidth *= TIER2["wired_factor"]
    m0 = undisrupted_makespan(net, base.dag)
    _, _, sched = scenarios.run("wait", net, base.dag, [], 0.0)
    sc = copy.copy(base)
    sc.net = net
    sc.groups = []
    sc.doc = {**d0, "m0": m0, "plan": {t: v[0] for t, v in sched.items()},
              "bandwidth": min(l.bandwidth for l in net.links.values()),
              "link_bw": {k: l.bandwidth for k, l in net.links.items()},
              "tier2": {**d0["tier2"], "wired": [list(p) for p in sorted(topo)]}, "trace": []}
    return sc


def network_use(sc, topo):
    """How the scenario's plan uses the network on this topology: DAG edges
    whose ends are on different nodes, and those over a radio pair."""
    plan = sc.doc["plan"]
    cross = [(plan[a], plan[b]) for a, b, _ in sc.doc["dag"]["edges"] if plan[a] != plan[b]]
    return {"cross": len(cross), "radio": sum(1 for x, y in cross if pair_key(x, y) not in topo)}


def base_id(scenario_id):
    for g in GENERATORS:
        i = scenario_id.find(g)
        if i > 0:
            return scenario_id[:i]
    return scenario_id


# ------------------------------------------------------------ the meter
class LinkMeter:
    """What the emulated links actually carry, read twice a second.

    Each lane node gets a root shell of its own (the lane's are busy with
    link changes) and an nftables table with two kinds of counters:

      sent     postrouting, per next hop (rt ip nexthop): what the node put
               on the link to each neighbour, its own traffic and relayed
               traffic alike, wired and radio links alike;
      relayed  forward, per destination: what the node passed on for others.

    Each counter is split by what the packets carry. A TCP packet with no
    payload is at most 80 bytes (a 20-byte IP header and at most 60 bytes of
    TCP header), so "data" counts IP packets longer than 80 bytes and "acks"
    the rest: the bare acknowledgments a receiver sends back."""

    PERIOD = 0.5

    def __init__(self, nodes, password):
        self.nodes = nodes
        self.n = len(nodes)
        # (t, {(kind, peer, "data"|"acks"): bytes}); kind is "sent" or "relayed"
        self.samples = [collections.deque(maxlen=14400) for _ in nodes]
        self.ip_of = {n["data_ip"]: j for j, n in enumerate(nodes)}
        self.shells = [None] * self.n
        errs = []

        def open_shell(i):
            try:
                sh = RootShell(f"{nodes[i]['name']} meter", nodes[i]["ctl_ip"], password)
                d = nodes[i]["dif"]
                sh.run("nft delete table inet wlmeter 2>/dev/null; true")
                sh.run("nft add table inet wlmeter")
                sh.run("nft add chain inet wlmeter sent '{ type filter hook postrouting priority 0; policy accept; }'")
                sh.run("nft add chain inet wlmeter relayed '{ type filter hook forward priority -20; policy accept; }'")
                for j, peer in enumerate(nodes):
                    if j == i:
                        continue
                    ip = peer["data_ip"]
                    for cmp in ("gt", "le"):
                        sh.run(f'nft add rule inet wlmeter sent oifname "{d}" rt ip nexthop {ip} ip length {cmp} 80 counter')
                        sh.run(f'nft add rule inet wlmeter relayed iifname "{d}" oifname "{d}" ip daddr {ip} ip length {cmp} 80 counter')
                self.shells[i] = sh
            except Exception as e:  # noqa: BLE001 - reported below
                errs.append(e)
        ts = [threading.Thread(target=open_shell, args=(i,)) for i in range(self.n)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        if errs:
            self.close()
            raise errs[0]
        self.stopping = False
        for i in range(self.n):
            threading.Thread(target=self.loop, args=(i,), daemon=True).start()

    RULE = re.compile(r"(?:rt ip nexthop|ip daddr) (\S+) ip length (>|<=) 80 counter packets \d+ bytes (\d+)")

    def loop(self, i):
        sh = self.shells[i]
        while not self.stopping:
            t = time.time()
            try:
                out = sh.run("nft list table inet wlmeter")
            except Exception:  # noqa: BLE001 - a missed sample; try again shortly
                time.sleep(2)
                continue
            got, kind = {}, None
            for line in out.splitlines():
                if "chain sent" in line:
                    kind = "sent"
                elif "chain relayed" in line:
                    kind = "relayed"
                m = self.RULE.search(line)
                if m and kind and m.group(1) in self.ip_of:
                    got[(kind, self.ip_of[m.group(1)], "data" if m.group(2) == ">" else "acks")] = int(m.group(3))
            self.samples[i].append((t, got))
            time.sleep(max(0.0, self.PERIOD - (time.time() - t)))

    def window(self, t0, t_end):
        """Between t0 - 1 and t_end + 1, times relative to t0:
        links  {"n3>n1": {"series": [[t, data Mbit/s], ...], "mb": data MB, "ack_mb": MB}}
        relays {"n1>n0": same, for what n1 passed on toward n0}."""
        out = {"links": {}, "relays": {}}
        for i in range(self.n):
            pts = [s for s in list(self.samples[i]) if t0 - 1 <= s[0] <= t_end + 1]
            for kind, dest in (("sent", "links"), ("relayed", "relays")):
                for j in range(self.n):
                    if j == i:
                        continue
                    series, tot = [], {"data": 0, "acks": 0}
                    for a, b in zip(pts, pts[1:]):
                        if b[0] <= a[0]:
                            continue
                        for part in ("data", "acks"):
                            k = (kind, j, part)
                            if k in a[1] and k in b[1]:
                                dbytes = max(0, b[1][k] - a[1][k])   # a counter reset reads as 0
                                tot[part] += dbytes
                                if part == "data":
                                    series.append([round((a[0] + b[0]) / 2 - t0, 2), round(dbytes * 8 / (b[0] - a[0]) / 1e6, 3)])
                    if tot["data"] or tot["acks"]:
                        out[dest][f"n{i}>n{j}"] = {"series": series, "mb": round(tot["data"] / 1e6, 3),
                                                   "ack_mb": round(tot["acks"] / 1e6, 3)}
        return out

    def close(self):
        self.stopping = True
        for sh in self.shells:
            if sh is None:
                continue
            try:
                sh.run("nft delete table inet wlmeter 2>/dev/null; true")
            except Exception:  # noqa: BLE001
                pass
            sh.close()


# ------------------------------------------------------------- the lane
class DemoRunner(driver.Runner):
    """The E17 runner, exposing the run in progress to the live view."""

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.current = None          # name of the ODAG being run
        self.applied = []            # disruption instants applied, wall clock
        self.plans = []              # repair plans applied, wall clock
        apply = self.lane.apply

        def recorded(changes, reroute=False, **kw2):
            apply(changes, reroute=reroute, **kw2)
            # The emulator's host routes after the change: (i, j) -> relay.
            routes = {f"n{i}>n{j}": f"n{r}" for (i, j), r in self.lane.routes.items() if r is not None}
            self.applied.append({"at": time.time(), "changes": [list(c) for c in changes], "routes": routes})
        self.lane.apply = recorded

    def odag(self, name, sc, placement, S):
        self.current = name
        return super().odag(name, sc, placement, S)

    def apply_plan(self, sc, name, pending, rec):
        super().apply_plan(sc, name, pending, rec)
        self.plans.append({"at": time.time(), "moves": pending["rec"].get("moves", {}), "k": pending["rec"].get("k")})

    def collect(self, sc, obj, t0, sigma):
        fin = super().collect(sc, obj, t0, sigma)
        start = driver.rfc3339(obj.get("status", {}).get("startTime"))
        fin["odag_start"] = start - t0 if start else 0.0
        return fin


# ----------------------------------------------------------- the catalog
class Catalog:
    def __init__(self, directory):
        self.files = {}
        for f in sorted(glob.glob(os.path.join(directory, "*.json"))):
            d = json.load(open(f))
            if d.get("tier") != 2:
                continue
            self.files.setdefault(base_id(d["id"]), f)
        self.cache = {}
        self.variants = {}
        self.lock = threading.Lock()

    def scenario(self, base, topo=None):
        """The exported scenario (topo None) or its variant on a topology."""
        with self.lock:
            if base not in self.cache:
                if base not in self.files:
                    raise KeyError(base)
                self.cache[base] = driver.Scenario(self.files[base])
            if topo is None:
                return self.cache[base]
            key = (base, topo)
            if key not in self.variants:
                self.variants[key] = variant(self.cache[base], topo)
            return self.variants[key]

    def listing(self, topo):
        out = []
        for base, f in self.files.items():
            d = json.load(open(f))
            out.append({"id": base, "structure": d["structure"], "family": FAMILIES.get(d["structure"], "Other"),
                        "ccr": d["ccr"], "tasks": len(d["dag"]["tasks"]), "edges": len(d["dag"]["edges"]),
                        **network_use(self.scenario(base, topo), topo)})
        order = {"Synthetic": 0, "RIoTBench": 1, "WfCommons": 2}
        return sorted(out, key=lambda s: (order.get(s["family"], 9), s["structure"], s["ccr"]))


# ------------------------------------------------------------------ runs
class Run:
    seq = 0

    def __init__(self, base, kind, topo, strategy=None, disruptions=None):
        Run.seq += 1
        self.id = f"r{Run.seq}"
        self.base, self.kind, self.strategy = base, kind, strategy
        self.topo = topo
        self.disruptions = disruptions or []
        self.windows = []           # merged outages per pair, from the disruptions
        self.status = "queued"
        self.error = None
        self.created = time.time()
        self.started = self.finished = None
        self.live = None
        self.result = None

    def view(self):
        return {"id": self.id, "scenario": self.base, "kind": self.kind, "strategy": self.strategy,
                "topology": [list(p) for p in sorted(self.topo)], "topology_label": topo_label(self.topo),
                "disruptions": self.disruptions, "windows": self.windows, "status": self.status,
                "error": self.error, "created": self.created, "started": self.started,
                "finished": self.finished, "live": self.live if self.status == "running" else None,
                "result": self.result}


def outage_windows(topo, disruptions):
    """The disruptions as merged outages per radio pair: [{a, b, start, end, by}]."""
    pos = LAYOUT
    nodes = sorted(pos)
    radio = [(a, b) for i, a in enumerate(nodes) for b in nodes[i + 1:] if (a, b) not in topo]
    wins = {p: [] for p in radio}
    for k, d in enumerate(disruptions):
        s, e = float(d["start"]), float(d["end"])
        if not (0 <= s < e):
            raise ValueError(f"disruption {k + 1}: the window must start at or after 0 and end after it starts")
        if d["type"] == "link":
            a, b = sorted((d["a"], d["b"]))
            if (a, b) not in wins:
                raise ValueError(f"{a}-{b} is not a radio link (wired links are never disrupted)")
            wins[(a, b)].append((s, e, k))
        elif d["type"] == "jammer":
            x, y, r = float(d["x"]), float(d["y"]), float(d["r"])
            if r <= 0:
                raise ValueError(f"disruption {k + 1}: the jammer's radius must be positive")
            for a, b in radio:
                if math.hypot(pos[a][0] - x, pos[a][1] - y) <= r or math.hypot(pos[b][0] - x, pos[b][1] - y) <= r:
                    wins[(a, b)].append((s, e, k))
        else:
            raise ValueError(f"disruption {k + 1}: unknown type {d['type']!r}")
    out = []
    for (a, b), ws in wins.items():
        ws.sort()
        merged = []
        for s, e, k in ws:
            if merged and s <= merged[-1]["end"]:
                merged[-1]["end"] = max(merged[-1]["end"], e)
                merged[-1]["by"].append(k)
            else:
                merged.append({"a": a, "b": b, "start": s, "end": e, "by": [k]})
        out += merged
    return out


def annotate_paths(flows, routes):
    """Mark the stretches of each transfer the emulator relayed: "relayed":
    [{"from", "to", "path"}], path following each hop's route at the time."""
    cuts = [(-1e9, {})] + [(r["t"], r["via"]) for r in routes]
    for f in flows:
        f["relayed"] = []
        if not f.get("src") or not f.get("dst") or f.get("start") is None or f.get("end") is None:
            continue
        for k, (ta, via) in enumerate(cuts):
            tb = cuts[k + 1][0] if k + 1 < len(cuts) else 1e9
            a, b = max(f["start"], ta), min(f["end"], tb)
            if b <= a:
                continue
            path, cur = [f["src"]], f["src"]
            while cur != f["dst"] and len(path) < 6:
                cur = via.get(f"{cur}>{f['dst']}", f["dst"])
                path.append(cur)
            if len(path) > 2:
                f["relayed"].append({"from": round(a, 2), "to": round(b, 2), "path": path})


def trace_groups(windows, sigma):
    """Merged outages as the driver's event groups, in simulated seconds."""
    steps = []
    for w in windows:
        for x, y in ((w["a"], w["b"]), (w["b"], w["a"])):
            steps.append((w["start"], 0, {"link": f"l_{x}_{y}", "type": "down", "factor": 0.0}))
            steps.append((w["end"], 1, {"link": f"l_{x}_{y}", "type": "recovery", "factor": 1.0}))
    steps.sort(key=lambda s: (s[0], s[1], s[2]["link"]))
    groups = []
    for t, _, e in steps:
        ev = {"t": t / sigma, **e}
        if groups and abs(groups[-1][0] - ev["t"]) < 1e-9:
            groups[-1][1].append(ev)
        else:
            groups.append((ev["t"], [ev]))
    return groups


# ------------------------------------------------------------ the server
class Lab:
    def __init__(self, runner, catalog, T, store, meter):
        self.runner, self.catalog, self.T, self.meter = runner, catalog, T, meter
        self.runs = {}
        self.baselines = {}          # (scenario, topology) -> {"run", "m0_tb", "sigma"}
        self.reconfiguring = False
        self.store = store           # one JSON file per finished run
        os.makedirs(store, exist_ok=True)
        tf = os.path.join(store, "topology.json")
        self.topo = frozenset(tuple(p) for p in json.load(open(tf))) if os.path.exists(tf) else frozenset()
        self.apply_topology(self.topo)   # wired pairs; every other pair is radio
        self.load()
        self.todo = queue.Queue()
        self.lock = threading.Lock()
        self.active = None
        threading.Thread(target=self.worker, daemon=True).start()
        threading.Thread(target=self.poller, daemon=True).start()

    # ---------------------------------------------------------- persistence
    def load(self):
        """Earlier runs, so the history and baselines survive a restart."""
        for f in sorted(glob.glob(os.path.join(self.store, "r*.json")), key=os.path.getmtime):
            v = json.load(open(f))
            if v["scenario"] not in self.catalog.files:
                continue
            run = Run.__new__(Run)
            run.id, run.base, run.kind, run.strategy = v["id"], v["scenario"], v["kind"], v["strategy"]
            # Runs from before topologies were editable ran on the Tier 2 wiring.
            run.topo = frozenset(tuple(p) for p in v["topology"]) if "topology" in v else T2_WIRED
            run.disruptions, run.windows = v["disruptions"], v["windows"]
            run.status, run.error = v["status"], v["error"]
            if run.status in ("queued", "running"):
                run.status, run.error = "failed", "interrupted by a server restart"
            run.created, run.started, run.finished = v["created"], v["started"], v["finished"]
            run.live, run.result = None, v["result"]
            self.runs[run.id] = run
            Run.seq = max(Run.seq, int(run.id[1:]))
            if run.kind == "baseline" and run.status == "done":
                m0 = self.catalog.scenario(run.base, run.topo).doc["m0"]
                ms = run.result["makespan"]
                self.baselines[(run.base, run.topo)] = {"run": run.id, "m0_tb": ms, "sigma": ms / m0}

    def save(self, run):
        tmp = os.path.join(self.store, f".{run.id}.json")
        json.dump(run.view(), open(tmp, "w"))
        os.replace(tmp, os.path.join(self.store, f"{run.id}.json"))

    # ------------------------------------------------------------ topology
    def apply_topology(self, topo):
        """Rebuild the lane's emulation: wired pairs unshaped, the rest radio."""
        lane = self.runner.lane
        lane.wired = {frozenset((int(a[1:]), int(b[1:]))) for a, b in topo}
        lane.bw = {(i, j): (TIER2["wired_factor"] if frozenset((i, j)) in lane.wired else 1.0)
                   for i in range(5) for j in range(5) if i != j}
        lane.setup()

    def set_topology(self, pairs):
        topo = set()
        for p in pairs:
            a, b = pair_key(*p)
            if a not in LAYOUT or b not in LAYOUT or a == b:
                raise ValueError(f"no link {a}-{b}")
            topo.add((a, b))
        topo = frozenset(topo)
        with self.lock:
            if self.active is not None or self.todo.qsize():
                raise ValueError("wait for the runs in progress to finish before changing the links")
            if topo == self.topo:
                return
            self.reconfiguring = True
        try:
            self.apply_topology(topo)
            self.topo = topo
            json.dump([list(p) for p in sorted(topo)], open(os.path.join(self.store, "topology.json"), "w"))
            driver.log(f"topology: {topo_label(topo)}")
        finally:
            self.reconfiguring = False

    # ------------------------------------------------------------ requests
    def submit(self, body):
        base = body.get("scenario")
        topo = self.topo
        sc = self.catalog.scenario(base, topo)
        kind = body.get("kind", "baseline")
        if kind == "baseline":
            run = Run(base, "baseline", topo)
        elif kind == "disrupted":
            strategy = body.get("strategy")
            if strategy not in STRATEGIES:
                raise ValueError(f"strategy must be one of {', '.join(STRATEGIES)}")
            disruptions = body.get("disruptions") or []
            if not disruptions:
                raise ValueError("add at least one disruption")
            if (base, topo) not in self.baselines:
                raise ValueError("run the baseline for this DAG on these links first")
            run = Run(base, "disrupted", topo, strategy, disruptions)
            run.windows = outage_windows(topo, disruptions)
        else:
            raise ValueError("kind must be baseline or disrupted")
        with self.lock:
            self.runs[run.id] = run
        self.todo.put(run)
        self.save(run)
        return run

    def describe(self, base):
        sc = self.catalog.scenario(base, self.topo)
        d = sc.doc
        names = self.runner.names
        nodes = []
        for i, n in enumerate(sorted(sc.net.nodes)):
            nodes.append({"id": n, "name": names[i], "hw": hardware(names[i]), "speed": d["nodes"][n],
                          "x": LAYOUT[n][0], "y": LAYOUT[n][1]})
        ids = sorted(sc.net.nodes)
        pairs = [{"a": a, "b": b, "wired": (a, b) in self.topo} for i, a in enumerate(ids) for b in ids[i + 1:]]
        base_run = self.baselines.get((base, self.topo))
        return {"id": base, "structure": d["structure"], "family": FAMILIES.get(d["structure"], "Other"),
                "ccr": d["ccr"], "m0_sim": d["m0"], "T": self.T,
                "tasks": [{"id": t, "cost": c} for t, c in d["dag"]["tasks"].items()],
                "edges": [{"from": a, "to": b, "size": s} for a, b, s in d["dag"]["edges"]],
                "plan": d["plan"], "nodes": nodes, "pairs": pairs, "topology_label": topo_label(self.topo),
                **network_use(sc, self.topo),
                "baseline": base_run["run"] if base_run else None}

    # --------------------------------------------------------------- worker
    def worker(self):
        while True:
            run = self.todo.get()
            try:
                self.execute(run)
            except Exception as e:  # noqa: BLE001 - reported on the run
                run.status, run.error = "failed", f"{type(e).__name__}: {e}"
                driver.log(f"{run.id} failed: {traceback.format_exc()}")
            finally:
                run.finished = time.time()
                with self.lock:
                    self.active = None
                self.runner.current = None
                self.save(run)

    def execute(self, run):
        if run.topo != self.topo:
            raise RuntimeError("the links changed after this run was queued")
        sc = self.catalog.scenario(run.base, run.topo)
        d = sc.doc
        S = self.T / d["m0"]
        r = self.runner
        r.applied, r.plans = [], []
        with self.lock:
            self.active = run
        run.status, run.started = "running", time.time()
        run.live = {"t0": None, "tasks": [], "flows": [], "applied": [], "plans": []}
        if run.kind == "baseline":
            rec = r.run(sc, "m0", "m0", 0.0, d["plan"], S, S, None)
        else:
            b = self.baselines[(run.base, run.topo)]
            sc.groups = trace_groups(run.windows, b["sigma"])
            rec = r.run(sc, run.strategy, run.strategy, 0.0, d["plan"], b["sigma"], S, b["m0_tb"])
        run.result = self.normalize_record(sc, rec)
        t0 = rec.get("t0")
        if t0:
            routes = [{"t": a["at"] - t0, "via": a["routes"]} for a in self.runner.applied]
            run.result.update(self.meter.window(t0, t0 + (rec.get("wall_end") or 0)), routes=routes)
            annotate_paths(run.result["flows"], routes)
        if rec.get("status") != "Succeeded":
            run.status = "failed"
            run.error = f"the run ended {rec.get('status')}"
            return
        run.status = "done"
        if run.kind == "baseline":
            ms = rec["final"]["makespan"]
            self.baselines[(run.base, run.topo)] = {"run": run.id, "m0_tb": ms, "sigma": ms / d["m0"]}
        else:
            run.result["baseline_makespan"] = self.baselines[(run.base, run.topo)]["m0_tb"]

    # ---------------------------------------------------------------- views
    def flows_view(self, sc, flows, shift):
        out = []
        for f in flows or []:
            src = f.get("fromTask", "").split(".to-")[0]
            out.append({"from": sc.back.get(src, src), "to": sc.back.get(f.get("toTask"), f.get("toTask")),
                        "src": f"n{self.runner.idx[f['srcNode']]}" if f.get("srcNode") in self.runner.idx else None,
                        "dst": f"n{self.runner.idx[f['dstNode']]}" if f.get("dstNode") in self.runner.idx else None,
                        "start": f["start"] + shift if f.get("start") is not None else None,
                        "end": f["end"] + shift if f.get("end") is not None else None,
                        "bytes": f.get("dataSize"), "ok": f.get("ok", True)})
        return out

    def normalize_record(self, sc, rec):
        fin = rec.get("final") or {}
        tasks = [{"id": t, "node": v["node"], "start": v["start"], "end": v["end"], "phase": "Succeeded"}
                 for t, v in (fin.get("tasks") or {}).items()]
        plans = [{"t": p["applied"], "k": p.get("k"), "moves": p.get("moves", {})}
                 for p in rec.get("plans", []) if "applied" in p]
        # Moved: ran somewhere other than its initial plan (a task moved and
        # moved back is not).
        moved = {t["id"]: t["node"] for t in tasks if t["node"] and t["node"] != sc.doc["plan"].get(t["id"])}
        applied = [{"t": e["applied"], "kinds": e["kinds"], "n": e["n"]} for e in rec.get("events", [])]
        return {"status": rec.get("status"), "makespan": fin.get("makespan"), "tasks": tasks,
                "flows": self.flows_view(sc, fin.get("flows"), fin.get("odag_start", 0.0)),
                "plans": plans, "moved": moved, "applied": applied}

    def poller(self):
        """Snapshot the run in progress for the live view, about once a second."""
        while True:
            time.sleep(0.8)
            run, name = self.active, self.runner.current
            if run is None or name is None or run.status != "running":
                continue
            try:
                obj = self.runner.api.get(name)
            except (urllib.error.HTTPError, OSError):
                continue
            sc = self.catalog.scenario(run.base, run.topo)
            st = obj.get("status", {}) or {}
            entries = st.get("tasks", []) or []
            starts = [driver.rfc3339(e.get("taskStartTime") or e.get("startTime")) for e in entries]
            starts = [s for s in starts if s]
            if not starts:
                continue
            t0 = min(starts)
            tasks = []
            for e in entries:
                s = driver.rfc3339(e.get("taskStartTime") or e.get("startTime"))
                c = driver.rfc3339(e.get("taskCloseTime") or e.get("completionTime"))
                node = e.get("node")
                tasks.append({"id": sc.back.get(e["name"], e["name"]),
                              "node": f"n{self.runner.idx[node]}" if node in self.runner.idx else None,
                              "start": s - t0 if s else None, "end": c - t0 if c else None,
                              "phase": e.get("phase")})
            ostart = driver.rfc3339(st.get("startTime"))
            routes = [{"t": a["at"] - t0, "via": a["routes"]} for a in self.runner.applied]
            flows = self.flows_view(sc, st.get("actualNetworkFlows"), (ostart - t0) if ostart else 0.0)
            annotate_paths(flows, routes)
            run.live = {"t0": t0, "now": time.time() - t0, "tasks": tasks, "flows": flows, "routes": routes,
                        **self.meter.window(t0, time.time()),
                        "applied": [{"t": a["at"] - t0} for a in self.runner.applied],
                        "plans": [{"t": p["at"] - t0, "k": p["k"], "moves": p["moves"]} for p in self.runner.plans]}

    def state(self):
        with self.lock:
            active = self.active.id if self.active else None
        return {"lane": [{"id": f"n{i}", "name": n, "hw": hardware(n)} for i, n in enumerate(self.runner.names)],
                "T": self.T, "active": active, "queued": self.todo.qsize(),
                "topology": [list(p) for p in sorted(self.topo)], "topology_label": topo_label(self.topo),
                "reconfiguring": getattr(self, "reconfiguring", False)}


def make_handler(lab):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            pass

        def send(self, code, body, ctype="application/json"):
            data = body if isinstance(body, bytes) else json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def path_only(self):
            p = self.path.split("?", 1)[0]
            if p == PREFIX or p.startswith(PREFIX + "/"):
                p = p[len(PREFIX):] or "/"
            return p

        def do_GET(self):
            p = self.path_only()
            try:
                if p == "/api/state":
                    return self.send(200, lab.state())
                if p == "/api/scenarios":
                    return self.send(200, lab.catalog.listing(lab.topo))
                if p.startswith("/api/scenarios/"):
                    return self.send(200, lab.describe(p.rsplit("/", 1)[1]))
                if p == "/api/runs":
                    q = self.path.split("?", 1)[1] if "?" in self.path else ""
                    want = dict(kv.split("=", 1) for kv in q.split("&") if "=" in kv).get("scenario")
                    runs = [r.view() for r in sorted(lab.runs.values(), key=lambda r: -r.created)
                            if (want is None or r.base == want) and r.topo == lab.topo]
                    return self.send(200, runs)
                if p.startswith("/api/runs/"):
                    run = lab.runs.get(p.rsplit("/", 1)[1])
                    return self.send(200, run.view()) if run else self.send(404, {"error": "no such run"})
            except KeyError:
                return self.send(404, {"error": "no such scenario"})
            name = "index.html" if p in ("/", "") else p.lstrip("/")
            f = os.path.normpath(os.path.join(STATIC, name))
            if not f.startswith(STATIC) or not os.path.isfile(f):
                return self.send(404, {"error": "not found"})
            ctype = {".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8",
                     ".css": "text/css; charset=utf-8", ".svg": "image/svg+xml"}.get(os.path.splitext(f)[1],
                                                                                 "application/octet-stream")
            return self.send(200, open(f, "rb").read(), ctype)

        def do_POST(self):
            p = self.path_only()
            if p not in ("/api/runs", "/api/topology"):
                return self.send(404, {"error": "not found"})
            try:
                n = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(n) or b"{}")
                if p == "/api/topology":
                    lab.set_topology(body.get("wired") or [])
                    return self.send(200, lab.state())
                run = lab.submit(body)
                return self.send(201, run.view())
            except KeyError:
                return self.send(404, {"error": "no such scenario"})
            except (ValueError, TypeError) as e:
                return self.send(400, {"error": str(e)})

    return Handler


def main():
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    ap = argparse.ArgumentParser()
    ap.add_argument("--lane", default=",".join(TIER2["lanes"][0]))
    ap.add_argument("--scenarios", default=os.path.expanduser("~/testbed/e17/scenarios-t2"))
    ap.add_argument("--T", type=float, default=60.0, help="target undisrupted makespan, seconds")
    ap.add_argument("--port", type=int, default=8095)
    ap.add_argument("--out", default=os.path.expanduser("~/testbed/e17/results-demo"))
    ap.add_argument("--tag", help=argparse.SUPPRESS)   # marks the process for pkill
    a = ap.parse_args()
    pw = sys.stdin.readline().rstrip("\n")
    lane = a.lane.split(",")
    rates = TIER2["rates"]
    runner = DemoRunner(lane, a.out, a.T, pw, tier2={**TIER2, "r_ref": sum(rates) / len(rates)})
    meter = LinkMeter(runner.nodes, pw)
    lab = Lab(runner, Catalog(a.scenarios), a.T, os.path.join(a.out, "runs"), meter)
    srv = ThreadingHTTPServer(("127.0.0.1", a.port), make_handler(lab))
    driver.log(f"Repair Lab on 127.0.0.1:{a.port} (lane {','.join(lane)}, {len(lab.catalog.files)} DAGs)")
    try:
        srv.serve_forever()
    finally:
        meter.close()
        runner.close()


if __name__ == "__main__":
    main()
