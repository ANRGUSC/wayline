#!/usr/bin/env python3
"""E18: E16's campaign (plan-driven copies under loss and link degradation)
on the rebuilt testbed.

E16's workloads, faults, Wayline arms and Ray / Argo baselines, run on a lane
of testbed workers (lanes.py) with every link emulated on the data VLAN
(netem.py). Cells come from suites.py; arms from arms.py. Results:

  ~/E18-results/<date>_<suite>/manifest.json            builds, lane, links, cells, sessions
  ~/E18-results/<date>_<suite>/<cell>/rep<k>.json        one valid run
  ~/E18-results/<date>_<suite>/<cell>/rep<k>.attempt<n>.invalid.json
  ~/E18-results/<date>_<suite>/plans/<workload>.json     the plan Ray and Argo follow

Every repetition is a block: the systems (Wayline, Ray per image, Argo) in
random order, the cells of each in random order (seeded per repetition, so a
resumed suite replays the same order). A cell's repetition is done when its
rep<k>.json exists; an invalid run is kept aside and retried up to
--max-attempts times.

Run on eclipse in ~/testbed/venv; the sudo password of the workers is the
first line of stdin (run.sh):

  echo "$PW" | ./run.sh run --suite smoke                 # or: ./run.sh run --suite smoke < pwfile
  ./run.sh run --suite e16 --reps 3 --dry-run             # lane, preflight, cells, order; touches nothing
  ./run.sh preflight --suite mixed16
  echo "$PW" | ./run.sh verify-links --lane e16
  echo "$PW" | ./run.sh calibrate --lane mixed16 --host
  echo "$PW" | ./run.sh cleanup --lane e16 [--out DIR]
"""
import argparse
import datetime
import glob
import hashlib
import importlib.util
import json
import os
import random
import re
import shlex
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import arms  # noqa: E402
from hostlock import HostLock  # noqa: E402
import faults  # noqa: E402
import images  # noqa: E402
import kube  # noqa: E402
import lanes  # noqa: E402
import netem  # noqa: E402
import suites  # noqa: E402
import workloads  # noqa: E402


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


rayrun = _load("rayrun", os.path.join(HERE, "ray", "rayrun.py"))
argorun = _load("argorun", os.path.join(HERE, "argo", "argorun.py"))
daskrun = _load("daskrun", os.path.join(HERE, "dask", "daskrun.py"))
workloads._examples()   # gen_gdtm / gen_focal importable (Argo tasks, GDTM pins)

NS = kube.NS
RES = os.environ.get("E18_RESULTS", os.path.expanduser("~/E18-results"))
RUN_TIMEOUT_S = 900


def run_timeout(flt):
    """A run's limit: one recovery's worth per node loss, so a second loss gets two."""
    return 2 * RUN_TIMEOUT_S if flt and flt[0] == "crash2" else RUN_TIMEOUT_S
ARGO_HANG_TIMEOUT_S = 300     # plain Argo on a crash: hangs (E16); cut here
# Link evidence lives in the controller for 60 s (linkstate.go linkObsTTL):
# after a link fault or a partition, every Wayline run waits it out, or it
# could react to the previous run's degraded link (E16 round 2).
LINK_COOLDOWN_S = 65
CM = "wl-network-profile"
NODE_LOSS_FAULTS = ("crash", "crash2", "partition")


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def now_iso():
    return datetime.datetime.now().astimezone().isoformat(timespec="seconds")


def http_text(url, timeout=3.0):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read().decode().strip()


# ===================================================================== context
class Ctx:
    def __init__(self, a, api, lane, cells, out=None):
        self.a = a
        self.api = api
        self.lane = lane
        self.cells = cells
        self.out = out
        self.shells = {}
        self.emu = None
        self.ray = None
        self.ray_image = None
        self.argo_up = False
        self.dask = None
        self.state = {}
        self.plans = {}

    # ---------------------------------------------------------------- state
    def state_path(self):
        return os.path.join(self.out, "state.json")

    def load_state(self):
        try:
            self.state = json.load(open(self.state_path()))
        except (OSError, ValueError):
            self.state = {}

    def save_state(self):
        tmp = self.state_path() + ".tmp"
        json.dump(self.state, open(tmp, "w"), indent=1)
        os.replace(tmp, self.state_path())

    def node(self, name):
        return self.lane.master if name == lanes.MASTER else self.lane.by_name[name]


# ================================================================== manifest
def git_info():
    repo = workloads.REPO
    try:
        commit = subprocess.run(["git", "-C", repo, "rev-parse", "HEAD"], capture_output=True, text=True,
                                timeout=10).stdout.strip()
        if commit:
            dirty = subprocess.run(["git", "-C", repo, "status", "--porcelain", "--", "eval/experiments/E18"],
                                   capture_output=True, text=True, timeout=10).stdout.strip()
            return {"commit": commit, "e18Dirty": bool(dirty), "source": "git"}
    except (OSError, subprocess.TimeoutExpired):
        pass
    try:
        return dict(json.load(open(os.path.join(repo, "GIT_COMMIT.json"))), source="sync.sh")
    except (OSError, ValueError):
        return {"commit": "unknown", "source": "none"}


def harness_hashes():
    out = {}
    for p in sorted(glob.glob(os.path.join(HERE, "**", "*.py"), recursive=True)) + [os.path.join(HERE, "iot-base.json")]:
        out[os.path.relpath(p, HERE)] = hashlib.sha256(open(p, "rb").read()).hexdigest()[:16]
    return out


def fingerprint(lane, cells, a):
    doc = {"lane": lane.names, "roles": lane.roles, "links": lane.describe()["links"]["pair_mbit"],
           "speed": lane.describe()["speed"], "cells": sorted(c["id"] for c in cells),
           "fence": a.fence, "arch": a.arch, "seed": a.seed}
    if images.SET != "e16":     # E16's images: the fingerprints of suites begun before images.py
        doc["images"] = images.SET
    return hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest()[:16]


def results_dir(a, suite, fp):
    """--out; else the latest results directory of this suite with the same
    configuration (resume); else a new <date>_<suite>[-k]."""
    if a.out:
        return a.out
    mine = []
    for d in glob.glob(os.path.join(RES, f"*_{suite}*")):
        try:
            m = json.load(open(os.path.join(d, "manifest.json")))
        except (OSError, ValueError):
            continue
        if m.get("suite") == suite and m.get("fingerprint") == fp:
            mine.append((m.get("created", ""), d))
    if mine and not a.new:
        return max(mine)[1]
    base = os.path.join(RES, f"{datetime.date.today().isoformat()}_{suite}")
    d, k = base, 1
    while os.path.exists(d):
        k += 1
        d = f"{base}-{k}"
    return d


def session_info(ctx):
    dep = kube.wayline_deployment()
    ctl = (dep.get("controller") or {}).get("image")
    agent = (dep.get("dataAgent") or {}).get("image")
    return {"start": now_iso(), "argv": sys.argv[1:], "git": git_info(), "harness": harness_hashes(),
            "controllerImage": ctl, "controllerTag": kube.tag_of(ctl), "agentImage": agent,
            "agentTag": kube.tag_of(agent), "wayline": dep, "host": socket.gethostname(),
            "k3s": kube.kubectl(["version", "-o", "json"], ns=None).stdout[:2000]}


def write_manifest(ctx, suite, fp):
    path = os.path.join(ctx.out, "manifest.json")
    sess = session_info(ctx)
    used = {"ray": {k: f"{kube.REG}/{v}" for k, v in suites.RAY_IMAGES.items()},
            "argo": {"task": argorun.TASK_IMAGE, "s3": argorun.S3_IMAGE, **argorun.IMAGES},
            "dask": daskrun.IMAGE, "runners": workloads.RUNNER_IMAGES,
            "set": images.SET, "tags": images.TAGS}
    if os.path.exists(path):
        m = json.load(open(path))
        if m.get("fingerprint") != fp and not ctx.a.force:
            raise SystemExit(f"{ctx.out} holds another configuration (fingerprint {m.get('fingerprint')} != {fp}); "
                             f"use --new, another --out, or --force")
        first = m["sessions"][0]
        for key in ("controllerTag", "agentTag"):
            if sess[key] != first[key] and not ctx.a.allow_mixed_builds:
                raise SystemExit(f"{key} changed since this suite began ({first[key]} -> {sess[key]}); "
                                 f"finish it on the same build or pass --allow-mixed-builds")
        m["sessions"].append(sess)
    else:
        first = sess
        m = {"suite": suite, "created": now_iso(), "fingerprint": fp, "registry": kube.REG,
             "controllerTag": sess["controllerTag"], "agentTag": sess["agentTag"], "git": sess["git"]["commit"],
             "lane": ctx.lane.describe(), "linkClasses": ctx.lane.link_class, "reps": ctx.a.reps,
             "seed": ctx.a.seed, "fence": ctx.a.fence, "arch": ctx.a.arch,
             "timeouts": {"run": RUN_TIMEOUT_S, "argoHang": ARGO_HANG_TIMEOUT_S, "linkCooldown": LINK_COOLDOWN_S},
             "arms": {"wayline": arms.WAYLINE, "ray": arms.RAY, "argo": arms.ARGO, "dask": arms.DASK},
             "images": used, "cells": ctx.cells, "blocks": {}, "sessions": [sess]}
    m["reps"] = max(m.get("reps", 0), ctx.a.reps)
    json.dump(m, open(path, "w"), indent=1)
    return m


def update_manifest(ctx, fn):
    path = os.path.join(ctx.out, "manifest.json")
    m = json.load(open(path))
    fn(m)
    tmp = path + ".tmp"
    json.dump(m, open(tmp, "w"), indent=1)
    os.replace(tmp, path)


# ===================================================================== order
def block_order(cells, rep, seed):
    """[(group, [cells])]: systems shuffled, cells within each shuffled."""
    rng = random.Random(f"e18:{seed}:{rep}")
    groups = {}
    for c in cells:
        groups.setdefault(c["group"], []).append(c)
    keys = sorted(groups)
    rng.shuffle(keys)
    out = []
    for k in keys:
        cs = sorted(groups[k], key=lambda c: c["id"])
        rng.shuffle(cs)
        out.append((k, cs))
    return out


def rep_path(out, cell, rep):
    return os.path.join(out, cell["id"], f"rep{rep}.json")


def invalid_attempts(out, cell, rep):
    return sorted(glob.glob(os.path.join(out, cell["id"], f"rep{rep}.attempt*.invalid.json")))


# ================================================================= preflight
def preflight(ctx):
    """(errors, warnings) for the selected cells on this lane; read-only."""
    lane, api, cells, a = ctx.lane, ctx.api, ctx.cells, ctx.a
    errors, warnings = [], []
    nodes = {n["metadata"]["name"]: n for n in api.nodes()}
    for name in lane.names:
        n = nodes.get(name)
        if not n:
            errors.append(f"{name}: not a cluster node")
            continue
        ready = next((c["status"] for c in n["status"].get("conditions", []) if c["type"] == "Ready"), "")
        if ready != "True":
            errors.append(f"{name}: not Ready")
        if n["spec"].get("unschedulable"):
            errors.append(f"{name}: cordoned")
        bad = [t["key"] for t in n["spec"].get("taints") or [] if t.get("effect") in ("NoSchedule", "NoExecute")]
        if bad:
            errors.append(f"{name}: taints {bad}")
        if lane.by_name[name]["flannel_ip"] != lane.by_name[name]["data_ip"]:
            errors.append(f"{name}: flannel public-ip {lane.by_name[name]['flannel_ip']} is not its data IP "
                          f"{lane.by_name[name]['data_ip']} (Wayline would push over another network)")
    dep = kube.wayline_deployment()
    env = (dep.get("controller") or {}).get("env", {})
    if any(c["fault"] and c["fault"][0] in NODE_LOSS_FAULTS for c in cells if c["system"] == "wayline") \
            and env.get("WL_NODE_LOSS", "").lower() == "off":
        errors.append("the controller runs with WL_NODE_LOSS=off (E17's setting): crash and partition cells need "
                      "node-loss detection (see README, 'Controller settings')")
    agent_env = (dep.get("dataAgent") or {}).get("env", {})
    warnings.append(f"data agent: WL_PUSH_RETRIES={agent_env.get('WL_PUSH_RETRIES', '(default 5)')}, "
                    f"sync={agent_env.get('WL_AGENT_SYNC', '(default)')}; controller poll "
                    f"{env.get('WL_POLL_INTERVAL_MS', '(default)')} ms (E16 ran on the defaults)")
    if lane.uncalibrated():
        warnings.append(f"no speed factor for {lane.uncalibrated()} (chain workloads refuse; `calibrate`)")
    # Wayline templates: images, archs, pins, speed factors.
    seen = set()
    runners_needed = set()
    for c in cells:
        if c["system"] != "wayline" or (c["workload"], c["arm"]) in seen:
            continue
        seen.add((c["workload"], c["arm"]))
        try:
            _, notes = workloads.build(c["workload"], c["arm"], lane, a.arch, fence=True)
            if any(nt["why"] == "arch" for nt in notes):
                warnings.append(f"{c['workload']}: tasks narrowed to the nodes their images run on: "
                                f"{[nt['task'] for nt in notes if nt['why'] == 'arch']}")
            runners_needed |= set(workloads.runners_of(c["workload"], lane))
        except workloads.WorkloadError as e:
            errors.append(f"{c['id']}: {e}")
        if c["arm"] == "W-static" and lane.is_subset() and a.fence == "constraints":
            errors.append(f"{c['id']}: W-static places copies over every schedulable worker (rendezvous hash over "
                          f"the cluster), outside this lane; run with --fence cordon (or nodes)")
    if a.fence == "nodes" and "nodes" not in api.crd_spec_fields():
        errors.append("--fence nodes: the installed ODAG CRD has no spec.nodes (the deployed controller predates it)")
    for r in sorted(runners_needed):
        want = set(lane.allowed(kube.image_archs(workloads.RUNNER_IMAGES[r])))
        have = {p["spec"].get("nodeName") for p in api.pods(NS, f"wl.io/runner={r}")}
        if want - have:
            errors.append(f"runner {r}: no runner pod on {sorted(want - have)[:8]}{'...' if len(want - have) > 8 else ''} "
                          f"(deploy/runners.py)")
    # Baselines.
    if any(c["system"] == "ray" for c in cells):
        for kind in sorted({suites.ray_image_kind(c["workload"]) for c in cells if c["system"] == "ray"}):
            img = f"{kube.REG}/{suites.RAY_IMAGES[kind]}"
            archs = kube.image_archs(img)
            if not archs:
                errors.append(f"Ray image {img} is not in the registry")
            elif set(lane.archs()) - archs:
                (errors if a.arch == "strict" else warnings).append(
                    f"Ray image {img} is {sorted(archs)} only; lane has {lane.archs()}")
        busy = ports_in_use(rayrun.HEAD_TCP_PORTS)
        if busy and not ray_head_running():
            errors.append(f"eclipse ports {busy} in use: the Ray head needs them")
        if "rpi3" in lane.classes():
            warnings.append("Ray workers on Pi 3 (1 GB): object store and workers may not fit (README)")
    if any(c["system"] == "argo" for c in cells):
        if not argorun.installed():
            errors.append("Argo Workflows is not installed (argo/install.sh)")
        for img in sorted({i for c in cells if c["system"] == "argo" for i in argorun.images_of(c["workload"])}
                          | {argorun.S3_IMAGE}):
            archs = kube.image_archs(img)
            if not archs:
                errors.append(f"Argo image {img} is not in the registry")
            elif img != argorun.S3_IMAGE and set(lane.archs()) - archs:
                (errors if a.arch == "strict" else warnings).append(f"Argo image {img} is {sorted(archs)} only")
        if ports_in_use([argorun.S3_PORT]) and not argo_repo_running():
            errors.append(f"eclipse port {argorun.S3_PORT} in use: the S3 repository needs it")
    if any(c["system"] == "dask" for c in cells):
        archs = kube.image_archs(daskrun.IMAGE)
        if not archs:
            errors.append(f"Dask image {daskrun.IMAGE} is not in the registry (build.sh dask)")
        elif set(lane.archs()) - archs:
            (errors if a.arch == "strict" else warnings).append(f"Dask image {daskrun.IMAGE} is {sorted(archs)} only")
        busy = ports_in_use(daskrun.SCHED_TCP_PORTS)
        if busy and not dask_scheduler_running():
            errors.append(f"eclipse ports {busy} in use: the Dask scheduler needs them")
    # A Docker swarm service publishing a port takes it on every interface of eclipse (ingress DNAT)
    # without anything listening: ss shows it free, but connections never reach the baseline.
    swarm = swarm_published_ports()
    for system, ports in (("ray", rayrun.HEAD_TCP_PORTS), ("argo", [argorun.S3_PORT]),
                          ("dask", daskrun.SCHED_TCP_PORTS)):
        taken = sorted(set(ports) & swarm)
        if taken and any(c["system"] == system for c in cells):
            errors.append(f"eclipse ports {taken} are published by a Docker swarm service: {system} cannot use them")
    mon = [d["metadata"]["name"] for d in kube.kubectl_json(["get", "ds"], ns="monitoring").get("items", [])] \
        if kube.kubectl(["get", "ns", "monitoring"], ns=None).returncode == 0 else []
    if mon:
        warnings.append(f"monitoring DaemonSets {mon} run on the workers (pause them for timing-sensitive suites)")
    if a.fence == "cordon" and lane.is_subset():
        warnings.append(f"--fence cordon: {len(set(lane.schedulable_workers) - set(lane.names))} workers outside the "
                        f"lane will be cordoned for the suite and uncordoned after")
    return errors, warnings


def ports_in_use(ports):
    """Ports of `ports` something listens on here (eclipse)."""
    used = set()
    for line in subprocess.run(["ss", "-ltnH"], capture_output=True, text=True).stdout.splitlines():
        cols = line.split()
        if len(cols) >= 4 and cols[3].rsplit(":", 1)[-1].isdigit():
            used.add(int(cols[3].rsplit(":", 1)[-1]))
    return sorted(set(ports) & used)


def swarm_published_ports():
    """Ports Docker swarm services publish on eclipse (ingress mesh: every interface)."""
    try:
        r = subprocess.run(["docker", "service", "ls", "--format", "{{.Ports}}"], capture_output=True,
                           text=True, timeout=20)
    except (OSError, subprocess.TimeoutExpired):
        return set()
    out = set()
    for part in r.stdout.replace(",", " ").split():
        if "->" in part:
            port = part.split("->")[0].rsplit(":", 1)[-1]
            if port.isdigit():
                out.add(int(port))
    return out


def dask_scheduler_running():
    return kube.kubectl(["get", "pod", daskrun.SCHED, "-o", "name"], ns=daskrun.NS).returncode == 0


def ray_head_running():
    return kube.kubectl(["get", "pod", rayrun.HEAD, "-o", "name"], ns=rayrun.NS).returncode == 0


def argo_repo_running():
    return kube.kubectl(["get", "deploy", argorun.NAME, "-o", "name"], ns=argorun.NS).returncode == 0


# ==================================================================== health
def health(ctx, runners):
    """Problems that keep the lane from running a cell: its nodes, their
    data agents, the runners the cell calls (on the lane's nodes only)."""
    lane, api = ctx.lane, ctx.api
    probs = []
    nodes = {n["metadata"]["name"]: n for n in api.nodes()}
    for name in lane.names:
        n = nodes.get(name, {})
        ready = next((c["status"] for c in n.get("status", {}).get("conditions", []) if c["type"] == "Ready"), "")
        if ready != "True":
            probs.append(f"{name} not Ready")
        if n.get("spec", {}).get("unschedulable"):
            probs.append(f"{name} cordoned")
        taints = [t["key"] for t in n.get("spec", {}).get("taints") or [] if t.get("effect") in ("NoSchedule", "NoExecute")]
        if taints:
            probs.append(f"{name} taints {taints}")
    agents = {p["spec"].get("nodeName"): p for p in api.pods(NS, "app=data-agent")}
    down = []
    with ThreadPoolExecutor(32) as ex:
        oks = dict(zip(lane.names, ex.map(lambda nm: faults.agent_ok(lane.by_name[nm]["data_ip"]), lane.names)))
    for name in lane.names:
        if (agents.get(name) or {}).get("status", {}).get("phase") != "Running" or not oks[name]:
            down.append(name)
    if down:
        probs.append(f"data agent down on {down}")
    for r in runners:
        want = set(lane.allowed(kube.image_archs(workloads.RUNNER_IMAGES[r])))
        pods = {p["spec"].get("nodeName"): p for p in api.pods(NS, f"wl.io/runner={r}")}
        bad = []
        for name in sorted(want):
            p = pods.get(name)
            ip = (p or {}).get("status", {}).get("podIP")
            ready = any(c["type"] == "Ready" and c["status"] == "True" for c in (p or {}).get("status", {}).get("conditions", []))
            if not (p and ready and ip and _runner_ok(ip)):
                bad.append(name)
        if bad:
            probs.append(f"runner {r} not answering on {bad}")
    # Runs of an interrupted session hold nodes and data: delete this lane's
    # (other lanes may be running in parallel).
    for o in api.odags(f"{kube.EXPERIMENT_LABEL}={kube.EXPERIMENT},wl.io/e18-lane={lane.name.lower()}"):
        if (o.get("status") or {}).get("phase") not in ("Succeeded", "Failed"):
            log(f"deleting a leftover E18 run {o['metadata']['name']}")
            api.delete_odag(o["metadata"]["name"])
    return probs


def _runner_ok(ip):
    try:
        http_text(f"http://{ip}:8090/healthz", timeout=2)
        return True
    except Exception:  # noqa: BLE001
        return False


def wait_healthy(ctx, runners, timeout=900):
    t0, last = time.time(), 0.0
    while True:
        probs = health(ctx, runners)
        if not probs:
            return
        if time.time() - last > 30:
            log("waiting for the lane: " + "; ".join(probs))
            last = time.time()
        if time.time() - t0 > timeout:
            raise RuntimeError("lane not healthy after %ds: %s" % (timeout, "; ".join(probs)))
        time.sleep(5)


# ================================================================ run pieces
def cooldown(ctx):
    last = ctx.state.get("lastLinkFault", 0)
    wait = LINK_COOLDOWN_S - (time.time() - last)
    if wait > 0:
        log(f"  waiting {wait:.0f}s for the previous link fault's evidence to expire")
        time.sleep(wait)


def pre_run(ctx):
    ctx.emu.reset()
    rb = ctx.emu.verify()
    if not rb["ok"]:
        log(f"  link readback problems: {rb['problems'][:5]}")
    return {"links": rb, "counters": ctx.emu.counters(), "metrics": agent_metrics(ctx)}


def agent_metrics(ctx):
    names = ctx.lane.names
    with ThreadPoolExecutor(32) as ex:
        vals = list(ex.map(lambda n: faults.agent_metrics(ctx.lane.by_name[n]["data_ip"]), names))
    return {n: v for n, v in zip(names, vals)}


def metric_delta(before, after):
    out = {}
    for n, a in after.items():
        b = before.get(n)
        if not a or not b:
            out[n] = None
            continue
        out[n] = {f"{g}.{k}": a[g][k] - b[g][k] for g in ("transfers", "push") for k in a.get(g, {})
                  if k in b.get(g, {}) and a[g][k] != b[g][k] and k != "inflight"}
        if a.get("uptime_seconds", 0) < b.get("uptime_seconds", 0):
            out[n]["restarted"] = True
    return out


def restore_all(ctx, victims, rec):
    """Restore crashed nodes in parallel; record when each came back."""
    rec.setdefault("restoredAt", {})
    errs = []

    def one(v):
        try:
            rec["restoredAt"][v] = faults.restore(ctx.shells[v], v, ctx.lane.by_name[v]["data_ip"], ctx.api)
        except Exception as e:  # noqa: BLE001 - reported
            errs.append(f"{v}: {e}")
    ts = [threading.Thread(target=one, args=(v,)) for v in dict.fromkeys(victims)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    if errs:
        rec["restoreErrors"] = errs
        raise RuntimeError("restore failed: " + "; ".join(errs))


def wait_planned(api, run, timeout=120):
    t0 = time.time()
    while time.time() - t0 < timeout:
        st = api.status(run)
        if st.get("predictedTasks") or st.get("phase") in ("Succeeded", "Failed"):
            return st
        time.sleep(0.1)
    return api.status(run)


def wait_trigger(api, run, task, t0, timeout=300):
    while time.time() - t0 < timeout:
        st = api.status(run)
        if kube.task_phase(st, task) == "Succeeded":
            return True, st
        if st.get("phase") in ("Succeeded", "Failed"):
            return False, st
        time.sleep(0.2)
    return False, api.status(run)


def wait_done(api, run, timeout):
    t0 = time.time()
    while time.time() - t0 < timeout:
        st = api.status(run)
        if st.get("phase") in ("Succeeded", "Failed"):
            return st, st["phase"]
        time.sleep(0.5)
    return api.status(run), "Timeout"


def timings(ctx, run, st):
    jobs = [(t["name"], t["node"]) for t in st.get("tasks") or [] if t.get("node") in ctx.lane.by_name]

    def one(j):
        name, node = j
        try:
            return name, json.loads(http_text(f"http://{ctx.lane.by_name[node]['data_ip']}:8082/timings/{run}/{name}"))
        except Exception:  # noqa: BLE001 - no timings (lost node, pod task without SDK timings)
            return name, {}
    with ThreadPoolExecutor(16) as ex:
        return dict(ex.map(one, jobs))


def digests(ctx, run, st):
    """SHA-256 of every installed copy of every object, from the data agents
    (/digest), grouped by object: {obj: {node: hex | "absent" | "unreachable"}}."""
    producer = {t["name"]: t.get("node") for t in st.get("tasks") or []}
    holders = {}
    for f in st.get("actualNetworkFlows") or []:
        if f.get("ok") and f.get("fromTask"):
            holders.setdefault(f["fromTask"], set()).update(x for x in (f.get("srcNode"), f.get("dstNode")) if x)
    for o in st.get("objects") or []:
        for c in o.get("copies") or []:
            if c.get("state") == "Installed":
                holders.setdefault(o["object"], set()).add(c["node"])
    for obj in list(holders):
        p = producer.get(obj.split(".")[0])
        if p:
            holders[obj].add(p)
    jobs = [(obj, n) for obj, ns in holders.items() for n in sorted(ns) if n in ctx.lane.by_name]

    def one(j):
        obj, n = j
        try:
            return obj, n, http_text(f"http://{ctx.lane.by_name[n]['data_ip']}:8082/digest/{run}/{obj}")
        except urllib.error.HTTPError as e:
            return obj, n, "absent" if e.code == 404 else f"http {e.code}"
        except Exception:  # noqa: BLE001
            return obj, n, "unreachable"
    out = {}
    with ThreadPoolExecutor(32) as ex:
        for obj, n, d in ex.map(one, jobs):
            out.setdefault(obj, {})[n] = d
    return out


def digest_verdict(dig):
    """mismatch: two copies of an object differ. ok: every object held on
    two or more nodes was read on at least two, all equal. unverified:
    otherwise (single holders cannot be cross-checked; they are not
    counted against the run)."""
    verdict = "ok"
    for obj, vals in dig.items():
        hexes = {v for v in vals.values() if len(v) == 64}
        if len(hexes) > 1:
            return "mismatch"
        if len(vals) >= 2 and sum(1 for v in vals.values() if len(v) == 64) < 2:
            verdict = "unverified"
    return verdict


def lane_escape(ctx, st):
    allowed = set(ctx.lane.names)
    used = {t.get("node") for t in st.get("tasks") or []}
    used |= {c["node"] for o in st.get("objects") or [] for c in o.get("copies") or []}
    used |= {f.get(k) for f in st.get("actualNetworkFlows") or [] for k in ("srcNode", "dstNode")}
    return sorted(n for n in used - allowed if n)


def counter_check(flows, delta):
    """Every ok transfer's payload must have crossed the shaped class of its
    pair: bytes sent through class u>v >= payload of ok flows u -> v (wire
    bytes are never fewer than payload bytes; fewer means the traffic took
    another path, unshaped)."""
    payload = {}
    for f in flows or []:
        if f.get("ok") and f.get("srcNode") and f.get("dstNode") and f["srcNode"] != f["dstNode"]:
            k = f"{f['srcNode']}>{f['dstNode']}"
            payload[k] = payload.get(k, 0) + int(f.get("dataSize") or 0)
    short = {k: {"payload": p, "classBytes": delta.get(k, 0)} for k, p in payload.items() if delta.get(k, 0) < p}
    return ("violations" if short else "ok") if payload else "n/a", short


def validity(ctx, cell, rec):
    out = rec.get("outcome")
    flt = rec.get("faultResolved")
    v = {"outcome": out,
         "terminal": out in ("Succeeded", "Failed") or (out == "Timeout" and cell["expect_hang"])}
    if flt:
        inj = bool(rec.get("faultAt"))
        if flt[0] == "crash2":
            inj = inj and (bool(rec.get("fault2At")) or out == "Failed")
        v["fault_injected"] = inj
    else:
        v["fault_injected"] = True
    lb = rec.get("linksBefore") or {}
    lf = rec.get("linksFault")
    v["links_config"] = bool(lb.get("ok")) and (lf is None or bool(lf.get("ok")))
    if cell["system"] == "wayline":
        st = rec.get("status") or {}
        v["links_counters"], v["links_short"] = counter_check(st.get("actualNetworkFlows"), rec.get("linkCounters") or {})
        v["digest"] = digest_verdict(rec.get("digests") or {})
        v["checksum_mismatches"] = sum((d or {}).get("transfers.put_checksum_mismatch", 0)
                                       for d in (rec.get("agentMetrics") or {}).values())
        v["lane_escape"] = lane_escape(ctx, st)
        v["bandwidth_model"] = rec.get("bandwidthModel", [])
        v["sut_stable"] = sut_stable(ctx, rec.get("sut"))
        v["realization_seen"] = realization_unseen(rec)
        plan = ctx.plans.get(cell["workload"])
        if plan:
            mine = {t["name"]: t["node"] for t in st.get("predictedTasks") or []}
            v["plan_matches_probe"] = mine == plan["plan"]
        digest_ok = v["digest"] == "ok" or (v["digest"] == "unverified" and out != "Succeeded")
        v["ok"] = (v["terminal"] and v["fault_injected"] and v["links_config"] and v["links_counters"] != "violations"
                   and digest_ok and v["checksum_mismatches"] == 0 and not v["lane_escape"] and not v["bandwidth_model"]
                   and v["sut_stable"] and not v["realization_seen"])
    else:
        v["digest"] = "n/a"
        used = set(rec.get("nodesUsed") or [])
        v["lane_escape"] = sorted(used - set(ctx.lane.names) - {""})
        v["ok"] = v["terminal"] and v["fault_injected"] and v["links_config"] and not v["lane_escape"]
    return v


def base_record(ctx, cell, rep, attempt):
    flt = faults.resolve(cell["fault"], ctx.lane)
    return {"suite": ctx.a.suite, "cell": cell["id"], "row": cell["row"], "claim": cell["claim"],
            "workload": cell["workload"], "arm": cell["arm"], "system": cell["system"], "rep": rep,
            "attempt": attempt, "fault": cell["fault"], "faultResolved": flt, "lane": ctx.lane.names,
            "linkState": None}


# ============================================================ Wayline cells
def run_wayline(ctx, cell, rep, attempt):
    api, emu, lane = ctx.api, ctx.emu, ctx.lane
    rec = base_record(ctx, cell, rep, attempt)
    flt = rec["faultResolved"]
    kind = flt[0] if flt else None
    cooldown(ctx)
    tmpl, notes = workloads.build(cell["workload"], cell["arm"], lane, ctx.a.arch, fence=True)
    # One template per lane: parallel lanes must not overwrite each other's pins.
    tmpl["metadata"]["name"] = f"{tmpl['metadata']['name']}-{lane.name.lower()}"[:63].rstrip("-.")
    api.put_template(tmpl)
    time.sleep(1)
    rec.update(policy=arms.policy(cell["arm"]), template=tmpl["metadata"]["name"], constraintNotes=notes)
    pre = pre_run(ctx)
    rec["linksBefore"] = pre["links"]
    rec["bandwidthModel"] = bandwidth_mismatches(ctx)
    t0 = time.time()
    run = api.run_from_template(tmpl["metadata"]["name"], {"wl.io/e18-cell": cell["id"][:63].rstrip("-."),
                                                            "wl.io/e18-lane": lane.name.lower()},
                                run_spec(ctx))
    rec.update(run=run, submitted=t0)
    if ctx.a.fence == "nodes" and not (api.odag(run).get("spec") or {}).get("nodes"):
        api.delete_odag(run)
        raise RuntimeError("spec.nodes did not persist on the run (pruned by the CRD)")
    log(f"  run {run}")
    victims, st, outcome = [], {}, "Error"
    link_fault = kind in ("link", "link-pair", "partition")
    try:
        st = wait_planned(api, run)
        rec["plannedAt"] = time.time()
        if kind == "link":
            emu.degrade_node(flt[1], flt[2])
            rec["faultAt"] = time.time()
        elif kind == "link-pair":
            emu.degrade_pair(flt[1], flt[2], flt[3])
            rec["faultAt"] = time.time()
        elif kind in ("crash", "crash2", "partition"):
            victim, trigger, delay = flt[1:4]
            ok, st = wait_trigger(api, run, trigger, t0)
            if ok:
                rec["triggerAt"] = time.time()
                time.sleep(delay)
                if kind == "partition":
                    emu.partition(victim)
                    rec["faultAt"] = time.time()
                else:
                    victims.append(victim)
                    rec["faultAt"] = faults.crash(ctx.shells[victim], victim)
                if kind == "crash2":
                    second_loss(ctx, run, flt, victims, rec)
            else:
                rec["faultSkipped"] = f"trigger {trigger} did not succeed (run {st.get('phase')})"
        if kind in ("link", "link-pair", "partition") and rec.get("faultAt"):
            rec["linksFault"] = emu.verify()
            rec["linkState"] = emu.snapshot_state()
        st, outcome = wait_done(api, run, run_timeout(flt))
        rec["doneSeen"] = time.time()
    finally:
        if link_fault:
            emu.reset()
            rec["unlinkAt"] = time.time()
            ctx.state["lastLinkFault"] = rec["unlinkAt"]
            ctx.save_state()
        if victims:
            restore_all(ctx, victims, rec)
    if kind == "partition" and rec.get("faultAt"):
        time.sleep(20)   # the healed node's agent answers again; the result must stand
        after = api.status(run)
        rec["afterHeal"] = {"phase": after.get("phase"), "recoveries": len(after.get("recovery") or [])}
    rec["outcome"] = outcome
    rec["wall"] = rec.get("doneSeen", time.time()) - t0
    rec["terminalStatus"] = {k: st.get(k) for k in ("phase", "message", "makespan", "startTime", "completionTime")}
    rec["status"] = api.status(run) or st          # re-read: late status patches (flows) after the terminal phase
    try:
        final = api.odag(run) or {}
    except urllib.error.HTTPError:
        final = {}
    rec["realizationSpec"] = ((final.get("spec") or {}).get("realization")) or []
    rec["timings"] = timings(ctx, run, rec["status"])
    rec["digests"] = digests(ctx, run, rec["status"])
    rec["linkCounters"] = netem.counter_delta(pre["counters"], emu.counters())
    rec["agentMetrics"] = metric_delta(pre["metrics"], agent_metrics(ctx))
    rp = rec["status"].get("realizationPolicy") or {}
    log(f"  {outcome} wall {rec['wall']:.1f}s copies {rp.get('copies')} recoveries {len(rec['status'].get('recovery') or [])}")
    if not api.delete_odag(run):
        log(f"  run {run} still present after delete")
    return rec


def second_loss(ctx, run, flt, victims, rec):
    """crash2: the node the consumer was moved to, delay2 s after it runs there (the run ends the wait)."""
    consumer, delay2 = flt[4:6]
    deadline = time.time() + run_timeout(flt)
    while time.time() < deadline:   # as long as the run is in progress: a slow recovery gets its second loss too
        st = ctx.api.status(run)
        if st.get("phase") in ("Succeeded", "Failed"):
            rec["fault2Skipped"] = f"run {st.get('phase')} before the consumer moved"
            return
        moved = {m["task"]: m["to"] for r in st.get("recovery") or [] for m in (r.get("plan") or {}).get("moved", [])}
        t = next((x for x in st.get("tasks") or [] if x.get("name") == consumer), {})
        if moved.get(consumer) and t.get("node") == moved[consumer] and t.get("phase") == "Running":
            time.sleep(delay2)
            v2 = moved[consumer]
            victims.append(v2)
            rec["victim2"] = v2
            rec["fault2At"] = faults.crash(ctx.shells[v2], v2)
            return
        time.sleep(0.2)
    rec["fault2Skipped"] = "run still in progress at its limit and the consumer had not moved"


# ================================================================ plan probe
def plan_for(ctx, workload):
    """The plan Ray and Argo follow: Wayline's HEFT placement of the
    workload on this lane and W's plan-time copy decisions, from a probe run
    (created, read once the policy has decided, deleted). One per suite."""
    if workload in ctx.plans:
        return ctx.plans[workload]
    path = os.path.join(ctx.out, "plans", f"{workload}.json")
    if os.path.exists(path):
        ctx.plans[workload] = json.load(open(path))
        return ctx.plans[workload]
    api = ctx.api
    wrong = bandwidth_mismatches(ctx)
    if wrong:
        raise RuntimeError(f"plan probe {workload}: the bandwidth ConfigMap does not hold this lane's rates "
                           f"({len(wrong)} pairs, e.g. {wrong[:3]})")
    name = f"e18-{workload}-probe"
    tmpl, notes = workloads.build(workload, "W", ctx.lane, ctx.a.arch, fence=True, name=name)
    api.put_template(tmpl)
    time.sleep(1)
    run = api.run_from_template(name, {"wl.io/e18-probe": "true"}, run_spec(ctx))
    t0, st = time.time(), {}
    while time.time() - t0 < 120:
        st = api.status(run)
        if st.get("predictedTasks") and "decisions" in (st.get("realizationPolicy") or {}):
            break
        time.sleep(0.2)
    api.delete_odag(run)
    if not st.get("predictedTasks"):
        raise RuntimeError(f"probe {run}: no plan within 120 s ({st.get('phase')}: {st.get('message')})")
    plan = {t["name"]: t["node"] for t in st["predictedTasks"]}
    copies = {}
    multi_output = workload in ("gdtm", "focal")
    for d in (st.get("realizationPolicy") or {}).get("decisions") or []:
        if not d.get("copy"):
            continue
        key = d["object"] if multi_output else d["object"].split(".")[0]
        for n in str(d.get("fallback", "")).split(","):
            if n and n not in copies.setdefault(key, []):
                copies[key].append(n)
    doc = {"workload": workload, "probeRun": run, "at": now_iso(), "plan": plan, "copies": copies,
           "decisions": (st.get("realizationPolicy") or {}).get("decisions"), "constraintNotes": notes}
    os.makedirs(os.path.dirname(path), exist_ok=True)
    json.dump(doc, open(path, "w"), indent=1)
    ctx.plans[workload] = doc
    log(f"  plan probe {workload}: {len(plan)} tasks, copies {copies}")
    return doc


def run_spec(ctx):
    """Fields set on every run: with --fence nodes, spec.nodes = the lane
    (a controller that knows spec.nodes plans, repairs and copies only there)."""
    return {"nodes": sorted(ctx.lane.names)} if ctx.a.fence == "nodes" else None


def pins_for(ctx, workload):
    if workload == "gdtm":
        return workloads.gdtm_pins(ctx.lane)
    if workload == "focal":
        return workloads.focal_pins(ctx.lane)
    return {}


def fault_fns(ctx, flt, victims):
    """(fn, trigger, delay, second) for a baseline run; crashes append to victims."""
    if not flt:
        return None, None, 0.0, None
    kind = flt[0]

    def crash(node):
        victims.append(node)
        faults.crash(ctx.shells[node], node)
    if kind in ("crash", "crash2"):
        second = (flt[4], flt[5], crash, flt[1]) if kind == "crash2" else None
        return (lambda: crash(flt[1])), flt[2], flt[3], second
    if kind == "partition":
        return (lambda: ctx.emu.partition(flt[1])), flt[2], flt[3], None
    if kind == "link":
        return (lambda: ctx.emu.degrade_node(flt[1], flt[2])), None, 0.0, None
    if kind == "link-pair":
        return (lambda: ctx.emu.degrade_pair(flt[1], flt[2], flt[3])), None, 0.0, None
    raise ValueError(kind)


# ============================================================ W's decisions
def policy_decisions(ctx, row):
    """Where W's repair policy re-ran the consumer after each loss, and where its copy policy re-protected the
    consumer's input before the next loss, from the most recent valid W record of `row` on this lane (any suite on
    the lane: mixA, mixA-iot, ...). The node the consumer moved to after loss k+1 held the re-protection copy made
    after loss k, so reprotect = nodes[1:]."""
    lane = ctx.lane.name
    best = None
    for f in glob.glob(os.path.join(RES, f"*_{lane}*", "*", "rep*.json")):
        if not re.fullmatch(r"rep\d+\.json", os.path.basename(f)):
            continue
        try:
            r = json.load(open(f))
        except (OSError, ValueError):
            continue
        if r.get("arm") != "W" or r.get("row") != row or r.get("outcome") != "Succeeded":
            continue
        moves = [m for x in (r.get("status") or {}).get("recovery") or []
                 for m in ((x.get("plan") or {}).get("moved") or []) if m.get("task") == "c"]
        if moves and (best is None or os.path.getmtime(f) > best[0]):
            best = (os.path.getmtime(f), [m["to"] for m in moves], f)
    if not best:
        raise SystemExit(f"no W record of {row} on lane {lane} to take the repair decisions from")
    nodes = best[1]
    return {"task": "c", "nodes": nodes, "reprotect": nodes[1:], "source": os.path.relpath(best[2], RES)}


# ==================================================================== Ray
def run_ray(ctx, cell, rep, attempt):
    rec = base_record(ctx, cell, rep, attempt)
    flt = rec["faultResolved"]
    probe = plan_for(ctx, cell["workload"])
    plan = probe["plan"]
    copies = probe["copies"] if arms.ray_copies(cell["arm"]) else {}
    repairs = policy_decisions(ctx, cell["row"]) if arms.baseline_policy(cell["arm"]) else None
    pins = pins_for(ctx, cell["workload"])
    rec.update(plan=plan, copies=copies, pins=pins, rayImage=ctx.ray_image, repairs=repairs)
    missing = sorted({n for n in list(plan.values()) + list(pins.values())} - set(ctx.ray.workers))
    if missing:
        rec.update(outcome="Error", message=f"plan uses nodes without a Ray worker (image arch): {missing}",
                   linksBefore={"ok": True})
        return rec
    ctx.ray.up()
    victims = []
    fn, trigger, delay, second = fault_fns(ctx, flt, victims)
    pre = pre_run(ctx)
    rec["linksBefore"] = pre["links"]
    log(f"  Ray, copies {copies}")
    res = {}
    try:
        wname, wbase = workloads.baseline_workload(cell["workload"], ctx.lane, plan)
        res = ctx.ray.run(wname, plan, copies, pins, wbase,
                          fn, trigger, delay, second, timeout=run_timeout(flt), repairs=repairs)
        if flt and flt[0] in ("link", "link-pair", "partition") and res.get("faultAt"):
            rec["linksFault"] = ctx.emu.verify()
            rec["linkState"] = ctx.emu.snapshot_state()
    finally:
        if flt and flt[0] in ("link", "link-pair", "partition"):
            ctx.emu.reset()
            rec["unlinkAt"] = time.time()
        if victims:
            restore_all(ctx, victims, rec)
    rec.update(res)
    ok = res.get("phase") == "Succeeded"
    rec["outcome"] = ("Timeout" if res.get("timedOut") else res.get("phase") if res.get("phase") in ("Succeeded", "Failed")
                      else "Error")
    rec["wall"] = res.get("completion") if ok else res.get("wall")
    ip2node = ctx.ray.ip2node
    rec["nodesUsed"] = sorted({ip2node.get(t.get("node"), t.get("node")) for t in (res.get("tasks") or {}).values()})
    rec["linkCounters"] = netem.counter_delta(pre["counters"], ctx.emu.counters())
    log(f"  {rec['outcome']} completion {res.get('completion') or 0:.1f}s errors {list((res.get('errors') or {}))}")
    return rec


# =================================================================== Argo
def run_argo(ctx, cell, rep, attempt):
    rec = base_record(ctx, cell, rep, attempt)
    flt = rec["faultResolved"]
    probe = plan_for(ctx, cell["workload"])
    plan, pins = probe["plan"], pins_for(ctx, cell["workload"])
    lane_nodes = {img: ctx.lane.allowed(kube.image_archs(img)) for img in argorun.images_of(cell["workload"])}
    wname, wbase = workloads.baseline_workload(cell["workload"], ctx.lane, plan)
    wf = argorun.workflow(wname, plan, wbase, pins, lane_nodes)
    crashed = bool(flt) and flt[0] in ("crash", "crash2")
    oos = faults.OutOfService() if (arms.argo_out_of_service(cell["arm"]) and crashed) else None
    victims = []
    fn, trigger, delay, second = fault_fns(ctx, flt, victims)
    rec.update(plan=plan, pins=pins)
    pre = pre_run(ctx)
    rec["linksBefore"] = pre["links"]
    res = {}
    try:
        res = argorun.run(wf, fn, trigger, delay,
                          timeout=ARGO_HANG_TIMEOUT_S if (crashed and cell["arm"] == "Argo") else run_timeout(flt),
                          oos=oos, oos_node=flt[1] if oos else None, second=second)
        if flt and flt[0] in ("link", "link-pair", "partition") and res.get("faultAt"):
            rec["linksFault"] = ctx.emu.verify()
            rec["linkState"] = ctx.emu.snapshot_state()
    finally:
        if oos:
            oos.close()
            rec["outOfServiceAt"] = dict(oos.marked)
        for v in dict.fromkeys(victims):
            faults.clear_out_of_service(v)
        if flt and flt[0] in ("link", "link-pair", "partition"):
            ctx.emu.reset()
            rec["unlinkAt"] = time.time()
        if victims:
            restore_all(ctx, victims, rec)
    rec.update(res)
    ph = res.get("phase")
    rec["outcome"] = "Timeout" if res.get("timedOut") else ("Succeeded" if ph == "Succeeded" else "Failed")
    rec["nodesUsed"] = sorted({t.get("node") for t in res.get("tasks") or [] if t.get("node")})
    rec["linkCounters"] = netem.counter_delta(pre["counters"], ctx.emu.counters())
    log(f"  {rec['outcome']} wall {res.get('wall', 0):.1f}s retries {res.get('retries')}")
    return rec


# =================================================================== Dask
def run_dask(ctx, cell, rep, attempt):
    """As run_ray: the plan of the probe, pinned sources hard, the rest soft;
    Dask+copies replicates exactly the objects W's policy copied, to the same
    nodes (client.replicate)."""
    rec = base_record(ctx, cell, rep, attempt)
    flt = rec["faultResolved"]
    probe = plan_for(ctx, cell["workload"])
    plan = probe["plan"]
    copies = probe["copies"] if arms.dask_copies(cell["arm"]) else {}
    repairs = policy_decisions(ctx, cell["row"]) if arms.baseline_policy(cell["arm"]) else None
    pins = pins_for(ctx, cell["workload"])
    rec.update(plan=plan, copies=copies, pins=pins, daskImage=ctx.dask.image, repairs=repairs)
    missing = sorted({n for n in list(plan.values()) + list(pins.values())} - set(ctx.dask.workers))
    if missing:
        rec.update(outcome="Error", message=f"plan uses nodes without a Dask worker (image arch): {missing}",
                   linksBefore={"ok": True})
        return rec
    ctx.dask.up()
    victims = []
    fn, trigger, delay, second = fault_fns(ctx, flt, victims)
    pre = pre_run(ctx)
    rec["linksBefore"] = pre["links"]
    log(f"  Dask, copies {copies}")
    res = {}
    try:
        wname, wbase = workloads.baseline_workload(cell["workload"], ctx.lane, plan)
        res = ctx.dask.run(wname, plan, copies, pins, wbase,
                           fn, trigger, delay, second, timeout=run_timeout(flt), repairs=repairs)
        if flt and flt[0] in ("link", "link-pair", "partition") and res.get("faultAt"):
            rec["linksFault"] = ctx.emu.verify()
            rec["linkState"] = ctx.emu.snapshot_state()
    finally:
        if flt and flt[0] in ("link", "link-pair", "partition"):
            ctx.emu.reset()
            rec["unlinkAt"] = time.time()
        if victims:
            restore_all(ctx, victims, rec)
    rec.update(res)
    ok = res.get("phase") == "Succeeded"
    rec["outcome"] = ("Timeout" if res.get("timedOut") else res.get("phase") if res.get("phase") in ("Succeeded", "Failed")
                      else "Error")
    rec["wall"] = res.get("completion") if ok else res.get("wall")
    ip2node = ctx.dask.ip2node
    rec["nodesUsed"] = sorted({ip2node.get(t.get("node"), t.get("node")) for t in (res.get("tasks") or {}).values()})
    rec["linkCounters"] = netem.counter_delta(pre["counters"], ctx.emu.counters())
    log(f"  {rec['outcome']} completion {res.get('completion') or 0:.1f}s errors {list((res.get('errors') or {}))}"
        f"{' no-worker ' + str(res.get('noWorker')) if res.get('noWorker') else ''}")
    return rec


# ============================================================ system groups
def group_up(ctx, group):
    if group.startswith("ray:"):
        kind = group.split(":", 1)[1]
        img = f"{kube.REG}/{suites.RAY_IMAGES[kind]}"
        workers = {n: ctx.lane.by_name[n] for n in ctx.lane.allowed(kube.image_archs(img))}
        if ctx.ray and ctx.ray_image != img:
            ctx.ray.down()
            time.sleep(10)
        ctx.ray, ctx.ray_image = rayrun.Cluster(img, workers, ctx.lane.master), img
        ctx.ray.up()
    elif group == "argo":
        argorun.cleanup_stale()
        ctx.argo_up = True
        argorun.repo_up(ctx.lane.master)
    elif group == "dask":
        workers = {n: ctx.lane.by_name[n] for n in ctx.lane.allowed(kube.image_archs(daskrun.IMAGE))}
        ctx.dask = daskrun.Cluster(daskrun.IMAGE, workers, ctx.lane.master)
        ctx.dask.up()


def group_down(ctx, group):
    if group.startswith("ray:") and ctx.ray:
        ctx.ray.down()
        ctx.ray = None
    elif group == "argo" and ctx.argo_up:
        argorun.repo_down()
        ctx.argo_up = False
    elif group == "dask" and ctx.dask:
        ctx.dask.down()
        ctx.dask = None


RUNNERS = {"wayline": run_wayline, "ray": run_ray, "argo": run_argo, "dask": run_dask}


def run_cell(ctx, cell, rep):
    """Run one repetition of a cell until it is valid or out of attempts."""
    path = rep_path(ctx.out, cell, rep)
    if os.path.exists(path):
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    while True:
        attempt = len(invalid_attempts(ctx.out, cell, rep)) + 1
        if attempt > ctx.a.max_attempts:
            log(f"{cell['id']} rep{rep}: {ctx.a.max_attempts} invalid attempts, giving up on it")
            return
        runners = workloads.runners_of(cell["workload"], ctx.lane) if cell["system"] == "wayline" else []
        wait_healthy(ctx, runners)
        log(f"{cell['id']} rep{rep} (attempt {attempt})")
        started = time.time()
        sut_before = sut_pods(ctx)
        try:
            rec = RUNNERS[cell["system"]](ctx, cell, rep, attempt)
        except Exception as e:  # noqa: BLE001 - one broken run must not end the suite
            rec = base_record(ctx, cell, rep, attempt)
            rec.update(outcome="Error", error=f"{type(e).__name__}: {e}", linksBefore={"ok": False})
            log(f"  error: {rec['error']}")
            try:
                ctx.emu.reset()
            except Exception as e2:  # noqa: BLE001
                log(f"  link reset failed after the error: {e2}")
        rec["harnessSeconds"] = time.time() - started
        rec["finishedAt"] = now_iso()
        rec["sut"] = {"before": sut_before, "after": sut_pods(ctx)}
        rec["validity"] = validity(ctx, cell, rec)
        if rec["validity"]["ok"]:
            tmp = path + ".tmp"
            json.dump(rec, open(tmp, "w"), indent=1, default=str)
            os.replace(tmp, path)
            return
        bad = {k: v for k, v in rec["validity"].items() if v in (False, "violations", "mismatch") or
               (k in ("lane_escape", "bandwidth_model", "realization_seen") and v) or (k == "sut_stable" and not v)}
        log(f"  invalid: {bad}")
        json.dump(rec, open(os.path.join(os.path.dirname(path), f"rep{rep}.attempt{attempt}.invalid.json"), "w"),
                  indent=1, default=str)


# ===================================================================== fence
def cordon_others(ctx):
    names = [n for n in ctx.lane.schedulable_workers if n not in ctx.lane.by_name]
    for n in names:
        ctx.api.patch_node(n, {"spec": {"unschedulable": True}})
    ctx.state["cordoned"] = sorted(set(ctx.state.get("cordoned", [])) | set(names))
    ctx.save_state()
    log(f"cordoned {len(names)} workers outside the lane")


def uncordon(ctx):
    for n in ctx.state.get("cordoned", []):
        try:
            ctx.api.patch_node(n, {"spec": {"unschedulable": False}})
        except Exception as e:  # noqa: BLE001
            log(f"uncordon {n}: {e}")
    if ctx.state.get("cordoned"):
        log(f"uncordoned {len(ctx.state['cordoned'])} workers")
    ctx.state["cordoned"] = []
    ctx.save_state()


# ================================================================ commands
def setup_lane(ctx, pw):
    ctx.shells = netem.open_shells(ctx.lane.nodes + [ctx.lane.master], pw, log)
    # eclipse's links are shaped only when data crosses eclipse (Argo's
    # repository); otherwise parallel lanes would fight over its qdisc.
    shape_master = any(c["system"] == "argo" for c in ctx.cells)
    ctx.emu = netem.Emulator(ctx.lane, ctx.shells, log, shape_master=shape_master)
    ctx.emu.setup()
    rb = ctx.emu.verify()
    if not rb["ok"]:
        raise RuntimeError(f"emulation readback after setup: {rb['problems'][:10]}")


def teardown_lane(ctx):
    if ctx.emu:
        try:
            ctx.emu.teardown()
        finally:
            ctx.emu.close()
    for s in ctx.shells.values():
        s.close()


def lane_pairs(ctx):
    """This lane's keys in the bandwidth ConfigMap: every ordered pair of its nodes."""
    return {f"{u}_to_{v}": str(int(b)) for (u, v), b in sorted(ctx.lane.bw_matrix().items())}


def apply_bandwidth(ctx):
    """The lane's pair rates in the controller's bandwidth model. Lanes run
    side by side and share the one ConfigMap, so a suite merges only its own
    pairs and never touches another lane's pairs or the default. The values
    its pairs had before are saved once per suite directory: a session that
    died before restoring them leaves ours in place, which must not become
    the 'before' of the next session."""
    ours = lane_pairs(ctx)
    path = ctx.state.get("configmapBefore")
    if not (path and os.path.exists(path)):
        cm = (ctx.api.configmap(NS, CM) or {}).get("data") or {}
        path = os.path.join(ctx.out, f"configmap-before-{int(time.time())}.json")
        json.dump({k: cm.get(k) for k in ours}, open(path, "w"), indent=1)
    ctx.api.merge_configmap(NS, CM, ours)
    ctx.state["configmapBefore"] = path
    ctx.save_state()


def restore_bandwidth(ctx):
    """Put this lane's pairs back as they were (absent ones removed). Only
    the lane's own keys are written, whatever the saved file holds."""
    path = ctx.state.get("configmapBefore")
    if path and os.path.exists(path):
        before = json.load(open(path))
        ctx.api.merge_configmap(NS, CM, {k: before.get(k) for k in lane_pairs(ctx)})
        log("bandwidth ConfigMap: lane pairs restored")
        ctx.state.pop("configmapBefore", None)
        ctx.save_state()


SUT_APPS = ("odag-controller", "wl-policy")


def sut_pods(ctx):
    """The controller's and the policy process's pods: {app: [(pod, node, start time), ...]}."""
    out = {}
    for app in SUT_APPS:
        try:
            pods = ctx.api.pods(NS, selector=f"app={app}")
        except Exception as e:  # noqa: BLE001 - recorded, and the run is then not counted
            out[app] = [("error", str(e), "")]
            continue
        out[app] = sorted((p["metadata"]["name"], p["spec"].get("nodeName", ""), (p.get("status") or {}).get("startTime", ""))
                          for p in pods if not p["metadata"].get("deletionTimestamp"))
    return out


def sut_stable(ctx, sut):
    """The system under test ran unchanged through the run: one pod per process, the same before and after, and
    none on a node of the lane (where a fault could take it down)."""
    if not sut:
        return False
    b, a = sut.get("before") or {}, sut.get("after") or {}
    for app in SUT_APPS:
        pb, pa = [tuple(x) for x in b.get(app) or []], [tuple(x) for x in a.get(app) or []]
        if len(pb) != 1 or pb != pa or pb[0][1] in ctx.lane.names:
            return False
    return True


def realization_unseen(rec):
    """Copies the run's final binding asked for that the controller never took up: absent from its object status
    (where every entry it reconciles appears, with each copy's state). Non-empty means the controller missed a
    revision, and the run does not count."""
    st = rec.get("status") or {}
    seen = {(o.get("object"), c.get("node")) for o in st.get("objects") or [] for c in o.get("copies") or []}
    lost = set()
    for t in st.get("tasks") or []:
        if t.get("lost") and t.get("node"):
            lost.add(t["node"])
    return sorted(f"{e.get('object')}@{n}" for e in rec.get("realizationSpec") or [] for n in e.get("copies") or []
                  if (e.get("object"), n) not in seen and n not in lost)


def bandwidth_mismatches(ctx):
    """This lane's pairs whose value in the bandwidth ConfigMap is not the
    lane's emulated rate: the controller would plan with another link model."""
    cm = (ctx.api.configmap(NS, CM) or {}).get("data") or {}
    return sorted(k for k, v in lane_pairs(ctx).items() if cm.get(k) != v)


def make_ctx(a, need_cells=True):
    api = kube.Api()
    nodes = lanes.discover(api)
    suite = suites.SUITES.get(getattr(a, "suite", None) or "", {})
    lane_arg = a.lane or suite.get("lane")
    if not lane_arg:
        raise SystemExit("no lane: give --lane or --suite")
    speed = {k: float(v) for k, _, v in (s.partition("=") for s in (a.speed or "").split(",") if s)}
    lane = lanes.resolve(lane_arg, nodes, lanes.parse_roles_string(a.roles or ""), speed)
    if hasattr(a, "fence") and a.fence is None:
        a.fence = suite.get("fence", "constraints")
    cells = []
    if need_cells:
        split = lambda s: [x for x in (s or "").split(",") if x] or None  # noqa: E731
        cells = suites.suite_cells(a.suite, rows=split(a.rows), systems=split(a.systems), arms_=split(a.arms),
                                   claims=split(a.claims), workloads=split(a.workloads))
    return Ctx(a, api, lane, cells)


def print_lane(lane):
    log(f"lane {lane.name}: {len(lane.names)} nodes {lane.classes()} {lane.archs()}"
        f"{' (subset of the workers)' if lane.is_subset() else ''}")
    log("  roles: " + ", ".join(f"{r}={v}" for r, v in sorted(lane.roles.items())))
    rates = sorted({round(lane.pair_mbit(u, v), 2) for u in lane.names for v in lane.names if u != v})
    log(f"  link classes: {lane.link_class}; pair rates {rates} Mbit/s; eclipse class {lane.repo_class}")


def cmd_run(a, pw_reader):
    ctx = make_ctx(a)
    try:
        if not a.dry_run:
            # One suite session per lane: a second one would reshape its links, delete its runs and crash its nodes.
            ctx.lane_lock = HostLock(f"e18-lane-{ctx.lane.name}")
            if not ctx.lane_lock.try_acquire():
                raise SystemExit(f"another E18 session holds lane {ctx.lane.name}")
        print_lane(ctx.lane)
        log(f"{len(ctx.cells)} cells x {a.reps} reps")
        errors, warnings = preflight(ctx)
        for w in warnings:
            log(f"warning: {w}")
        for e in errors:
            log(f"ERROR: {e}")
        fp = fingerprint(ctx.lane, ctx.cells, a)
        out = results_dir(a, a.suite, fp)
        if a.dry_run:
            log(f"results would go to {out} (fingerprint {fp})")
            for rep in range(1, a.reps + 1):
                for g, cs in block_order(ctx.cells, rep, a.seed):
                    done = sum(1 for c in cs if os.path.exists(rep_path(out, c, rep)))
                    log(f"rep{rep} {g}: {len(cs)} cells ({done} done): " + ", ".join(c["id"] for c in cs))
            return 1 if errors else 0
        if errors and not a.ignore_preflight:
            raise SystemExit("preflight failed (fix, or --ignore-preflight)")
        pw = pw_reader()
        ctx.out = out
        os.makedirs(out, exist_ok=True)
        ctx.load_state()
        write_manifest(ctx, a.suite, fp)
        log(f"results in {out}")
        signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
        signal.signal(signal.SIGINT, signal.default_int_handler)
        try:
            setup_lane(ctx, pw)
            apply_bandwidth(ctx)
            if a.fence == "cordon" and ctx.lane.is_subset():
                cordon_others(ctx)
            for rep in range(1, a.reps + 1):
                order = block_order(ctx.cells, rep, a.seed)
                update_manifest(ctx, lambda m: m["blocks"].__setitem__(str(rep), [[g, [c["id"] for c in cs]]
                                                                                   for g, cs in order]))
                for group, cs in order:
                    todo = [c for c in cs if not os.path.exists(rep_path(out, c, rep))]
                    if not todo:
                        continue
                    log(f"=== rep{rep} {group}: {len(todo)} cells")
                    try:
                        group_up(ctx, group)
                    except Exception as e:  # noqa: BLE001 - leave the group for a later session
                        log(f"{group} could not start ({type(e).__name__}: {e}); its cells stay to do")
                        group_down(ctx, group)
                        continue
                    try:
                        for c in todo:
                            run_cell(ctx, c, rep)
                    finally:
                        group_down(ctx, group)
        finally:
            for fn in (lambda: ctx.ray and ctx.ray.down(), lambda: ctx.argo_up and argorun.repo_down(),
                       lambda: ctx.dask and ctx.dask.down(),
                       lambda: teardown_lane(ctx), lambda: restore_bandwidth(ctx), lambda: uncordon(ctx)):
                try:
                    fn()
                except Exception as e:  # noqa: BLE001 - keep cleaning up
                    log(f"cleanup step failed: {e}")
        log("suite done")
        return 0
    finally:
        ctx.api.close()


def cmd_preflight(a):
    ctx = make_ctx(a)
    try:
        print_lane(ctx.lane)
        errors, warnings = preflight(ctx)
        for w in warnings:
            log(f"warning: {w}")
        for e in errors:
            log(f"ERROR: {e}")
        log("preflight " + ("failed" if errors else "ok"))
        return 1 if errors else 0
    finally:
        ctx.api.close()


PROBE_SERVER = """import socket, sys, time
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((sys.argv[1], int(sys.argv[2]))); s.listen(1); s.settimeout(30)
c, _ = s.accept(); n, t0, t1 = 0, None, None
while True:
    b = c.recv(1 << 16)
    if not b:
        break
    t1 = time.time(); t0 = t0 or t1; n += len(b)
c.sendall(f"{n} {(t1 or 0) - (t0 or 0)}".encode()); c.close()
"""
PROBE_CLIENT = """import socket, sys, time
c = socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=30); buf = bytes(65536); t = time.time()
while time.time() - t < float(sys.argv[3]):
    c.sendall(buf)
c.shutdown(socket.SHUT_WR); n, dt = c.recv(100).decode().split()
print("MBIT", int(n) * 8 / max(float(dt), 1e-9) / 1e6)
"""


def measure(ctx, u, v, secs=3.0, port=5299):
    """Goodput u -> v over the data VLAN, measured by the receiver (first to
    last byte), with a few lines of Python on both ends (no iperf)."""
    vip = ctx.node(v)["data_ip"]
    ctx.shells[v].run(f"(timeout {int(secs) + 25} python3 -c {shlex.quote(PROBE_SERVER)} {vip} {port} "
                      f">/tmp/e18-probe.log 2>&1 &)")
    time.sleep(1.0)
    out = ctx.shells[u].run(f"timeout {int(secs) + 20} python3 -c {shlex.quote(PROBE_CLIENT)} {vip} {port} {secs}",
                            timeout=secs + 30)
    return float(out.split("MBIT")[-1].split()[0])


def cmd_verify_links(a, pw_reader):
    """Measure a few emulated links (one per distinct pair rate, and one to
    eclipse) with the lane shaped as a suite would; then remove the shaping."""
    ctx = make_ctx(a, need_cells=False)
    try:
        print_lane(ctx.lane)
        setup_lane(ctx, pw_reader())
        try:
            pairs = [tuple(p.split(":")) for p in (a.pairs or "").split(",") if p]
            if not pairs:
                by_rate = {}
                for u in ctx.lane.names:
                    for v in ctx.lane.names:
                        if u != v:
                            by_rate.setdefault(round(ctx.lane.pair_mbit(u, v), 3), (u, v))
                pairs = list(by_rate.values()) + [(ctx.lane.names[0], lanes.MASTER)]
            for u, v in pairs:
                want = ctx.lane.pair_mbit(u, v)
                got = measure(ctx, u, v, secs=a.seconds)
                note = "  ABOVE the shaped rate: shaping not in effect" if got > want else ""
                log(f"{u} -> {v}: shaped {want:.2f} Mbit/s (wire), goodput {got:.2f} Mbit/s "
                    f"({got / want:.3f} of it){note}")
        finally:
            teardown_lane(ctx)
        return 0
    finally:
        ctx.api.close()


BENCH = """import hashlib, time
def spin(n):
    d = b"wl-e11"
    for _ in range(n):
        d = hashlib.sha256(d).digest()
spin(200000)
n, t0 = 0, time.time()
while time.time() - t0 < SECS:
    spin(20000); n += 20000
print("RATE", n / (time.time() - t0) / 1e6)
"""


def cmd_calibrate(a, pw_reader):
    """Speed factor per lane class for the hash-bound chain tasks: the E11
    kernel's rate (Mhash/s) on one node of each class, relative to an UP
    board. --host runs it with the node's python3 through the root shell;
    otherwise as a pod of E11's image (`task.py --bench`, the tasks' own
    Python), which needs that image for every class's architecture."""
    ctx = make_ctx(a, need_cells=False)
    try:
        classes = ctx.lane.classes()
        pick = {c: next(n for n in ctx.lane.names if ctx.lane.by_name[n]["class"] == c) for c in classes}
        if "up" not in pick:
            ups = [n for n in lanes.workers(ctx.lane.all_nodes) if n["class"] == "up"]
            if not ups:
                raise SystemExit("no UP board to calibrate against")
            pick["up"] = ups[0]["name"]
        rates = {}
        if a.host:
            nodes = [ctx.lane.all_nodes[n] for n in pick.values()]
            ctx.shells = netem.open_shells(nodes, pw_reader(), log)
            try:
                for c, n in pick.items():
                    code = BENCH.replace("SECS", str(a.seconds))
                    out = ctx.shells[n].run(f"python3 -c {shlex.quote(code)}", timeout=a.seconds + 60)
                    rates[c] = (n, float(out.split("RATE")[-1].split()[0]), "host python3")
            finally:
                for s in ctx.shells.values():
                    s.close()
        else:
            for c, n in pick.items():
                rates[c] = (n, bench_pod(n, a.seconds), f"pod {workloads.E11_IMAGE}")
        ref = rates["up"][1]
        doc = {"at": now_iso(), "reference": {"class": "up", "node": rates["up"][0], "mhash_s": ref},
               "harnessRATE_mhash_s": workloads.RATE / 1e6,
               "classes": {c: {"node": n, "mhash_s": r, "factor": round(r / ref, 4), "method": m}
                           for c, (n, r, m) in rates.items()}}
        for c, v in doc["classes"].items():
            log(f"{c}: {v['mhash_s']:.3f} Mhash/s on {v['node']} -> factor {v['factor']}")
        if abs(ref - workloads.RATE / 1e6) / (workloads.RATE / 1e6) > 0.05:
            log(f"note: the UP board hashes at {ref:.3f} Mhash/s, E11's RATE is {workloads.RATE / 1e6:.3f} "
                f"(clock settings differ from E11's full clock?)")
        os.makedirs(os.path.dirname(lanes.CALIBRATION), exist_ok=True)
        json.dump(doc, open(lanes.CALIBRATION, "w"), indent=1)
        log(f"written {lanes.CALIBRATION}")
        return 0
    finally:
        ctx.api.close()


def bench_pod(node, secs):
    name = f"e18-bench-{node}"
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "namespace": NS},
           "spec": {"nodeName": node, "restartPolicy": "Never",
                    "containers": [{"name": "bench", "image": workloads.E11_IMAGE,
                                    "command": ["python", "task.py", "--bench", str(secs)],
                                    "env": [{"name": "NODE_NAME", "value": node}]}]}}
    kube.kubectl(["delete", "pod", name, "--ignore-not-found", "--wait=true"])
    kube.apply(pod)
    try:
        for _ in range(int(secs) + 300):
            ph = kube.kubectl(["get", "pod", name, "-o", "jsonpath={.status.phase}"]).stdout.strip()
            if ph in ("Succeeded", "Failed"):
                break
            time.sleep(1)
        out = kube.kubectl(["logs", name]).stdout
        if "RATE" not in out:
            raise RuntimeError(f"bench on {node}: {ph} {out[-500:]}")
        return float(out.split("RATE")[-1].split()[1])
    finally:
        kube.kubectl(["delete", "pod", name, "--ignore-not-found", "--wait=false"])


def cmd_cleanup(a, pw_reader):
    """Undo what an interrupted suite left: shaping on the lane, the
    bandwidth ConfigMap (with --out), cordons, out-of-service taints, Ray
    pods, the Argo repository, leftover E18 runs and workflows."""
    ctx = make_ctx(a, need_cells=False)
    try:
        if a.out:
            ctx.out = a.out
            ctx.load_state()
        ctx.shells = netem.open_shells(ctx.lane.nodes + [ctx.lane.master], pw_reader(), log)
        ctx.emu = netem.Emulator(ctx.lane, ctx.shells, log)
        teardown_lane(ctx)
        if a.out:
            restore_bandwidth(ctx)
            uncordon(ctx)
        for n in ctx.lane.names:
            faults.clear_out_of_service(n)
        rayrun.Cluster(None, {}, ctx.lane.master).down()
        daskrun.Cluster(None, {}, ctx.lane.master).down()
        if argorun.installed():
            argorun.repo_down()
            argorun.cleanup_stale()
        for o in ctx.api.odags():
            ctx.api.delete_odag(o["metadata"]["name"])
        log("cleanup done")
        return 0
    finally:
        ctx.api.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def lane_args(p):
        p.add_argument("--lane", help="a lanes.LANES key or 'name,glob*,label=value:N,...' (default: the suite's)")
        p.add_argument("--roles", help="role overrides: 'camera=rpi1,rpi2,rpi3,rpi4;victim=anrg-1'")
        p.add_argument("--speed", help="speed factors per class: 'rpi4=0.4,rpi3=0.2'")

    def cell_args(p):
        p.add_argument("--suite", required=True, choices=sorted(suites.SUITES))
        p.add_argument("--rows", help="row ids (suites.ROWS), comma-separated")
        p.add_argument("--systems", help="wayline,ray,argo")
        p.add_argument("--arms", help="comma-separated arms")
        p.add_argument("--claims", help="comma-separated claims (cost, survival, ...)")
        p.add_argument("--workloads", help="comma-separated workloads")
        p.add_argument("--fence", choices=["constraints", "cordon", "nodes"], default=None,
                       help="keep runs in a subset lane by task constraints, also cordon the other workers, or "
                            "set spec.nodes (controllers that support it) (default: the suite's, else constraints)")
        p.add_argument("--arch", choices=["strict", "constrain"], default="strict",
                       help="refuse images lacking a lane architecture (default), or narrow tasks to the nodes they run on")

    p = sub.add_parser("run", help="run a suite (resumes the latest matching results directory)")
    lane_args(p)
    cell_args(p)
    p.add_argument("--reps", type=int, default=1)
    p.add_argument("--seed", type=int, default=18)
    p.add_argument("--out", help="results directory (default ~/E18-results/<date>_<suite>, or the latest matching one)")
    p.add_argument("--new", action="store_true", help="start a new results directory even if one matches")
    p.add_argument("--force", action="store_true", help="resume into --out although its configuration differs")
    p.add_argument("--allow-mixed-builds", action="store_true")
    p.add_argument("--max-attempts", type=int, default=2)
    p.add_argument("--dry-run", action="store_true", help="lane, preflight, cells and order; no changes anywhere")
    p.add_argument("--ignore-preflight", action="store_true")

    p = sub.add_parser("preflight", help="read-only checks for a suite")
    lane_args(p)
    cell_args(p)

    p = sub.add_parser("verify-links", help="shape a lane, measure a few links, unshape")
    lane_args(p)
    p.add_argument("--pairs", help="u:v,u:v (default: one pair per distinct rate, and one to eclipse)")
    p.add_argument("--seconds", type=float, default=3.0)

    p = sub.add_parser("calibrate", help="speed factor per class of the lane (chain tasks)")
    lane_args(p)
    p.add_argument("--host", action="store_true", help="the node's python3 via the root shell instead of a pod")
    p.add_argument("--seconds", type=float, default=5.0)

    p = sub.add_parser("cleanup", help="undo what an interrupted suite left")
    lane_args(p)
    p.add_argument("--out", help="the suite's results directory (restores its ConfigMap and cordons)")

    a = ap.parse_args()
    pw_box = []

    def pw_reader():
        if not pw_box:
            line = sys.stdin.readline()
            if not line:
                raise SystemExit("the workers' sudo password is read from the first line of stdin")
            pw_box.append(line.rstrip("\n"))
        return pw_box[0]

    if a.cmd == "run":
        return cmd_run(a, pw_reader)
    if a.cmd == "preflight":
        return cmd_preflight(a)
    if a.cmd == "verify-links":
        return cmd_verify_links(a, pw_reader)
    if a.cmd == "calibrate":
        return cmd_calibrate(a, pw_reader)
    if a.cmd == "cleanup":
        return cmd_cleanup(a, pw_reader)
    return 2


if __name__ == "__main__":
    sys.exit(main())
