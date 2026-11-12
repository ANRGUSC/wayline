"""E18 lanes: the workers a suite runs on, the roles workloads pin to, and
the emulated link rates between them.

A lane is a set of testbed workers given by names, globs or label selectors
(resolved against ~/testbed/inventory.csv and the live node list). Workloads
never name hosts: a pinned source or a fault victim is a role reference such
as "camera#4" (the lane's fourth camera node) or "victim" (its first victim
node), resolved against the lane. The same suite therefore runs on 8 UP
boards (the E16 lane, which reproduces E16's placements and link classes),
on 2 UP + 1 Pi 4 + 13 Pi 3, or on all 65 workers.

Links: every directed pair of lane nodes (and each node's link to eclipse,
where the Argo repository, the Ray head and the controller live) is one
emulated link on the data VLAN (netem.py). Its rate follows E13's link
classes: a node of class k has B/k, a pair runs at the slower end, capped by
both NICs' physical rate.

Lane spec (a dict in LANES, or a --lane string):
  nodes    ["anrg-1", "rpi1?", ...]       names or globs
  select   [{"label": "testbed.lab/class=rpi3", "count": 13, "skip": 0}, ...]
  roles    {"camera": [names] | {"label": ..., "count": n, "skip": k} | {"from": "lane", "count": n, "skip": k}}
  links    "uniform" | {node | "role:camera" | "label=value": class k, ...}   (default class 1)
  repo_class  link class of eclipse's links (default 1)
--lane string: comma-separated tokens; "k=v" or "k=v:N" (or "k=v:N+S", skip S)
selects by label, a token with * ? [ is a glob, anything else a node name.
"""
import csv
import fnmatch
import json
import os
import re

import kube

INVENTORY = os.environ.get("E18_INVENTORY", os.path.expanduser("~/testbed/inventory.csv"))
CALIBRATION = os.environ.get("E18_CALIBRATION", os.path.expanduser("~/E18-results/calibration.json"))
CLASS_LABEL = "testbed.lab/class"
MASTER = "eclipse"
MASTER_DATA_IP = "10.20.0.1"
DATA_IF = {"up": "enp1s0.20", "rpi3": "eth0.20", "rpi4": "eth0.20", "master": "enp4s0f0.20"}
SSH_USER = "anrg"

# E13's B: the measured TCP-level rate of one gigabit link on the old
# network. To re-measure on the data VLAN with `e18.py verify-links`.
B_MBIT = 942.0
# Physical per-class NIC limits (Mbit/s). UP and Pi 4: gigabit (taken as B,
# not yet measured on the data VLAN). Pi 3 Model B: 10/100 Ethernet over
# USB 2.0, 94 Mbit/s as the 100BASE-TX counterpart of B (unverified).
CAP_MBIT = {"up": B_MBIT, "rpi4": B_MBIT, "rpi3": 94.0, "master": B_MBIT}
# Relative CPU speed per class for the hash-bound chain tasks (E11 kernel),
# 1.0 = an UP board at full clock. Only UP is known; others come from
# `e18.py calibrate` (CALIBRATION file) or --speed.
SPEED = {"up": 1.0}

# E13's per-node link classes for the eight E16 workers.
E13_LINKS = {"anrg-1": 1, "anrg-6": 4, "anrg-7": 8, "anrg-3": 1, "anrg-5": 2, "anrg-8": 8, "anrg-4": 2, "anrg-9": 4}
E16_NODES = ["anrg-1", "anrg-3", "anrg-4", "anrg-5", "anrg-6", "anrg-7", "anrg-8", "anrg-9"]

LANES = {
    # E16 on the rebuilt testbed: the same eight UP boards, roles and link
    # classes as E16 (anrg-2, now a worker, stays out).
    "e16": {
        "nodes": E16_NODES,
        "roles": {"camera": ["anrg-1", "anrg-3", "anrg-4", "anrg-5"],   # iot capture-1..4 pins
                  "sensor": ["anrg-4", "anrg-5", "anrg-9"],             # GDTM source-n1..n3 pins
                  "source": ["anrg-4", "anrg-9"],                       # FOCAL seismic, audio pins
                  "victim": ["anrg-7"],     # chain: p and c fast here; chain-link/fan: p's node
                  "dst": ["anrg-6"],        # chain-link/fan: c fast here
                  "holder": ["anrg-1"]},    # fan: d fast here (holds a copy of p's output)
        "links": E13_LINKS,
    },
    # A three-board lane for a smoke test of the harness.
    "smoke": {
        "nodes": ["anrg-1", "anrg-3", "anrg-4"],
        "roles": {"camera": ["anrg-1", "anrg-3", "anrg-4", "anrg-1"], "sensor": ["anrg-1", "anrg-3", "anrg-4"],
                  "source": ["anrg-1", "anrg-4"], "victim": ["anrg-3"], "dst": ["anrg-4"], "holder": ["anrg-1"]},
        "links": {"anrg-3": 8, "anrg-4": 4},
    },
    # Heterogeneous: compute on UP, sensors on Pi 3, one Pi 4.
    "mixed16": {
        "select": [{"label": f"{CLASS_LABEL}=up", "count": 2}, {"label": f"{CLASS_LABEL}=rpi4", "count": 1},
                   {"label": f"{CLASS_LABEL}=rpi3", "count": 13}],
        "roles": {"camera": {"label": f"{CLASS_LABEL}=rpi3", "count": 4},
                  "sensor": {"label": f"{CLASS_LABEL}=rpi3", "count": 3, "skip": 4},
                  "source": {"label": f"{CLASS_LABEL}=rpi3", "count": 2, "skip": 7},
                  "victim": {"label": f"{CLASS_LABEL}=up", "count": 1},
                  "dst": {"label": f"{CLASS_LABEL}=up", "count": 1, "skip": 1},
                  "holder": {"label": f"{CLASS_LABEL}=rpi4", "count": 1}},
        "links": "uniform",
    },
    # Three disjoint mixed lanes for parallel suites (2026-11-08). They leave
    # out the Repair Lab demo's nodes (anrg-7, rpi4-1, rpi4-2, rpi51, rpi52)
    # and the Pis with under-voltage history (rpi3, rpi7, rpi19, rpi30,
    # rpi36, rpi37, rpi46). Sensors are Pi 3s; compute is UP and Pi 4.
    "mixA": {
        "nodes": ["anrg-1", "anrg-2", "rpi4-3"] + [f"rpi{i}" for i in (1, 2, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15)],
        "roles": {"camera": ["rpi1", "rpi2", "rpi4", "rpi5"], "sensor": ["rpi6", "rpi8", "rpi9"],
                  "source": ["rpi10", "rpi11"], "victim": ["anrg-1"], "dst": ["anrg-2"], "holder": ["rpi4-3"]},
        "links": "uniform",
    },
    "mixB": {
        "nodes": ["anrg-3", "anrg-4", "rpi4-4"] + [f"rpi{i}" for i in (16, 17, 18, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29)],
        "roles": {"camera": ["rpi16", "rpi17", "rpi18", "rpi20"], "sensor": ["rpi21", "rpi22", "rpi23"],
                  "source": ["rpi24", "rpi25"], "victim": ["anrg-3"], "dst": ["anrg-4"], "holder": ["rpi4-4"]},
        "links": "uniform",
    },
    "mixC": {
        "nodes": ["anrg-5", "anrg-6"] + [f"rpi{i}" for i in (31, 32, 33, 34, 35, 38, 39, 40, 41, 42, 43, 44, 45)],
        "roles": {"camera": ["rpi31", "rpi32", "rpi33", "rpi34"], "sensor": ["rpi35", "rpi38", "rpi39"],
                  "source": ["rpi40", "rpi41"], "victim": ["anrg-5"], "dst": ["anrg-6"], "holder": ["rpi42"]},
        "links": "uniform",
    },
    # Every worker (65).
    "all": {
        "select": [{"label": CLASS_LABEL}],
        "roles": {"camera": {"label": f"{CLASS_LABEL}=rpi3", "count": 4},
                  "sensor": {"label": f"{CLASS_LABEL}=rpi3", "count": 3, "skip": 4},
                  "source": {"label": f"{CLASS_LABEL}=rpi3", "count": 2, "skip": 7},
                  "victim": {"label": f"{CLASS_LABEL}=up", "count": 1},
                  "dst": {"label": f"{CLASS_LABEL}=up", "count": 1, "skip": 1},
                  "holder": {"label": f"{CLASS_LABEL}=rpi4", "count": 1}},
        "links": "uniform",
    },
}

# Roles a lane given without roles gets, drawn from the lane in natural order.
DEFAULT_ROLES = {"camera": (4, 0), "sensor": (3, 0), "source": (2, 0), "victim": (1, 0), "dst": (1, 1), "holder": (1, 2)}


def natural(s):
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", s)]


# ---------------------------------------------------------------- discovery
def discover(api=None):
    """Every testbed worker (a node with the class label), plus eclipse, as
    {name: node} with inventory addresses and live labels and state."""
    inv = {r["name"]: r for r in csv.DictReader(open(INVENTORY))}
    items = api.nodes() if api else kube.kubectl_json(["get", "nodes"], ns=None)["items"]
    out = {}
    for n in items:
        name = n["metadata"]["name"]
        labels = n["metadata"].get("labels") or {}
        r = inv.get(name)
        if r is None:
            continue
        kind = r["kind"]
        if kind != "master" and CLASS_LABEL not in labels:
            continue
        ready = next((c["status"] for c in n["status"].get("conditions", []) if c["type"] == "Ready"), "Unknown")
        cpu = str(n["status"].get("allocatable", {}).get("cpu", "1"))
        cores = max(1, int(cpu[:-1]) // 1000) if cpu.endswith("m") else int(cpu)
        out[name] = {
            "name": name, "kind": kind, "class": labels.get(CLASS_LABEL, kind),
            "arch": labels.get("kubernetes.io/arch", r.get("arch")),
            "ctl_ip": r["ctl_ip"], "data_ip": r["data_ip"], "mac": r["data_mac"].lower(),
            "dif": DATA_IF.get(kind, "eth0.20"), "labels": labels, "cores": cores,
            "ready": ready == "True", "unschedulable": bool(n["spec"].get("unschedulable")),
            "taints": [t for t in n["spec"].get("taints") or []],
            "flannel_ip": (n["metadata"].get("annotations") or {}).get("flannel.alpha.coreos.com/public-ip"),
        }
    return out


def workers(nodes):
    return sorted((n for n in nodes.values() if n["kind"] != "master"), key=lambda n: natural(n["name"]))


def _match_label(node, sel):
    k, _, v = sel.partition("=")
    return k in node["labels"] and (not v or node["labels"][k] == v)


def _select(pool, item):
    """Nodes of `pool` (natural order) an item selects."""
    if isinstance(item, str):
        if any(c in item for c in "*?["):
            return [n for n in pool if fnmatch.fnmatch(n["name"], item)]
        return [n for n in pool if n["name"] == item] or _missing(item)
    if "name" in item:
        return _select(pool, item["name"])
    if "glob" in item:
        return [n for n in pool if fnmatch.fnmatch(n["name"], item["glob"])]
    if "label" in item or "from" in item:
        hits = [n for n in pool if "label" not in item or _match_label(n, item["label"])]
        hits = [n for n in hits if n["name"] not in set(item.get("exclude", []))]
        skip = int(item.get("skip", 0))
        cnt = item.get("count")
        hits = hits[skip:skip + int(cnt)] if cnt is not None else hits[skip:]
        if cnt is not None and len(hits) < int(cnt):
            raise ValueError(f"selector {item} matches {len(hits)} nodes, needs {cnt}")
        return hits
    raise ValueError(f"bad selector {item!r}")


def _missing(name):
    raise ValueError(f"node {name!r} is not a testbed worker (inventory + class label)")


def parse_lane_string(s):
    """'anrg-1,anrg-3,testbed.lab/class=rpi3:13' -> a lane spec."""
    items = []
    for tok in [t.strip() for t in s.split(",") if t.strip()]:
        if "=" in tok:
            sel, _, rest = tok.partition(":")
            it = {"label": sel}
            if rest:
                cnt, _, skip = rest.partition("+")
                it["count"] = int(cnt)
                if skip:
                    it["skip"] = int(skip)
            items.append(it)
        else:
            items.append(tok)
    return {"select": items}


def parse_roles_string(s):
    """'camera=rpi1,rpi2;victim=anrg-1' -> {role: [names]}."""
    out = {}
    for part in [p for p in s.split(";") if p.strip()]:
        role, _, names = part.partition("=")
        out[role.strip()] = [n.strip() for n in names.split(",") if n.strip()]
    return out


def load_calibration():
    try:
        doc = json.load(open(CALIBRATION))
    except (OSError, ValueError):
        return {}
    return {c: v["factor"] for c, v in doc.get("classes", {}).items() if v.get("factor")}


# --------------------------------------------------------------------- lane
class Lane:
    def __init__(self, name, spec, nodes, role_overrides=None, speed_overrides=None):
        self.name = name
        self.spec = spec
        self.all_nodes = nodes
        pool = workers(nodes)
        chosen = []
        for item in list(spec.get("nodes", [])) + list(spec.get("select", [])):
            for n in _select(pool, item):
                if n["name"] not in {c["name"] for c in chosen}:
                    chosen.append(n)
        if not chosen:
            raise ValueError(f"lane {name}: selects no nodes")
        self.nodes = sorted(chosen, key=lambda n: natural(n["name"]))
        self.names = [n["name"] for n in self.nodes]
        self.by_name = {n["name"]: n for n in self.nodes}
        self.master = nodes.get(MASTER)
        self.schedulable_workers = [n["name"] for n in pool if not n["unschedulable"]]
        self.roles = self._roles(spec.get("roles") or {}, role_overrides or {})
        self.link_class = self._link_classes(spec.get("links", "uniform"))
        self.repo_class = int(spec.get("repo_class", 1))
        self.speed = dict(SPEED)
        self.speed.update(load_calibration())
        self.speed.update(speed_overrides or {})

    # ------------------------------------------------------------- roles
    def _roles(self, spec, overrides):
        out = {}
        for role, (cnt, skip) in DEFAULT_ROLES.items():
            if role not in spec:
                out[role] = [self.names[(skip + i) % len(self.names)] for i in range(cnt)]
        for role, rs in spec.items():
            if isinstance(rs, list):
                out[role] = list(rs)
            else:
                out[role] = [n["name"] for n in _select(self.nodes, rs)]
        out.update(overrides)
        for role, names in out.items():
            bad = [n for n in names if n not in self.by_name]
            if bad:
                raise ValueError(f"lane {self.name}: role {role} names nodes outside the lane: {bad}")
        return out

    def resolve(self, ref):
        """'camera#4' -> the 4th camera node; 'victim' -> the first victim
        node; a node name of the lane -> itself."""
        if ref in self.by_name:
            return ref
        role, _, k = ref.partition("#")
        if role not in self.roles:
            raise KeyError(f"lane {self.name}: no role {role!r} (roles: {sorted(self.roles)})")
        k = int(k or 1)
        if not 1 <= k <= len(self.roles[role]):
            raise KeyError(f"lane {self.name}: role {role} has {len(self.roles[role])} nodes, asked #{k}")
        return self.roles[role][k - 1]

    # ------------------------------------------------------------- links
    def _link_classes(self, links):
        cls = {n: 1 for n in self.names}
        if links == "uniform" or not links:
            return cls
        if links == "e13":
            links = E13_LINKS
        for key, k in links.items():
            if key.startswith("role:"):
                targets = self.roles[key[5:]]
            elif "=" in key:
                targets = [n["name"] for n in self.nodes if _match_label(n, key)]
            else:
                targets = [key]
            for t in targets:
                if t not in cls:
                    raise ValueError(f"lane {self.name}: link class for {t}, not a lane node")
                cls[t] = int(k)
        return cls

    def node_class(self, name):
        return self.repo_class if name == MASTER else self.link_class[name]

    def node_cap(self, name):
        n = self.master if name == MASTER else self.by_name[name]
        return CAP_MBIT.get(n["class"] if name != MASTER else "master", B_MBIT)

    def pair_mbit(self, u, v):
        """Emulated rate u -> v: the slower end's class, within both NICs."""
        return min(B_MBIT / max(self.node_class(u), self.node_class(v)), self.node_cap(u), self.node_cap(v))

    def bw_matrix(self):
        """{(u, v): bytes/s} for every ordered pair of lane nodes (the
        controller's wl-network-profile)."""
        return {(u, v): self.pair_mbit(u, v) * 1e6 / 8 for u in self.names for v in self.names if u != v}

    # ------------------------------------------------------------- speed
    def classes(self):
        return sorted({n["class"] for n in self.nodes})

    def archs(self):
        return sorted({n["arch"] for n in self.nodes})

    def uncalibrated(self):
        return [c for c in self.classes() if c not in self.speed]

    def speed_of(self, name):
        return self.speed.get(self.by_name[name]["class"])

    def is_subset(self):
        """Whether schedulable workers exist outside the lane (the
        controller plans over every schedulable worker unless fenced)."""
        return bool(set(self.schedulable_workers) - set(self.names))

    def allowed(self, archs):
        """Lane nodes whose CPU architecture is in `archs`, sorted
        byte-wise as the controller's HEFT sorts candidates."""
        return sorted(n["name"] for n in self.nodes if n["arch"] in archs)

    def describe(self):
        keep = ("name", "class", "arch", "ctl_ip", "data_ip", "mac", "dif", "cores")
        return {
            "name": self.name, "spec": self.spec,
            "nodes": [{k: n[k] for k in keep} for n in self.nodes],
            "master": {k: self.master[k] for k in keep} if self.master else None,
            "roles": self.roles,
            "links": {"B_mbit": B_MBIT, "caps_mbit": CAP_MBIT, "class": self.link_class, "repo_class": self.repo_class,
                      "pair_mbit": {f"{u}>{v}": round(self.pair_mbit(u, v), 4)
                                    for u in self.names + [MASTER] for v in self.names + [MASTER] if u != v}},
            "speed": {c: self.speed.get(c) for c in self.classes()},
            "subset": self.is_subset(),
        }


def resolve(lane_arg, nodes, role_overrides=None, speed_overrides=None):
    """A Lane from a LANES key or a --lane string."""
    if lane_arg in LANES:
        return Lane(lane_arg, LANES[lane_arg], nodes, role_overrides, speed_overrides)
    return Lane("adhoc", parse_lane_string(lane_arg), nodes, role_overrides, speed_overrides)
