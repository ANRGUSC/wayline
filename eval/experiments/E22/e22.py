#!/usr/bin/env python3
"""E22: realization under edge dynamics, on the heterogeneous testbed.

Three experiments, each an external policy that revises a live run's realization (spec.realization) while the
DAG and the task placement stay fixed. Every task is pinned; links are the lane's data-VLAN links (E18 netem).

  serve   A producer's uplink degrades while it serves one 300 MB output to three consumers (two UP boards and a
          Pi 3). Arms: fixed (no policy); adaptive-early (cap during production, the policy copies the output to
          a target node and serves the consumers from it); adaptive-late (cap once the fan-out has started);
          static-oracle (copy and serving copy declared at submission); plus clean runs without a cap. Caps:
          the producer's traffic to the three consumers shares B/2, B/4, B/8 or B/16 (B = 942 Mbit/s).
  relay   No end-to-end path at any instant: producer->relay is open for 8 s after the output exists, relay->
          consumer for 8 s starting 28 s later, producer->consumer never. Arms: adaptive-relay (copy to the
          relay when the first contact opens, then to the consumer at the second), static-relay (both declared
          at submission), fixed-direct (no policy; censored at 120 s), clean-direct (no contacts).
  risk    A warning precedes the producer leaving the data plane. Arms: single-copy (no policy), always-on (a
          backup declared at submission), on-warning (backup copied on the warning, served from after the loss),
          warning-clears (backup copied on the warning, evicted when it clears; no loss).

Run on eclipse, the workers' sudo password on stdin's first line:
  printf '%s\\n' "$PW" | ./run.sh serve --reps 5 --out ~/E22-results
"""
import argparse
import json
import os
import random
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "E18"))
import kube                       # noqa: E402
import lanes                      # noqa: E402
import netem                      # noqa: E402
import ctlshape                   # noqa: E402
import images                     # noqa: E402
from hostlock import HostLock     # noqa: E402

NS = "wl-system"
CM = "wl-network-profile"
IMAGE = images.ref("wl-e11")
RATE = 2.568e6                    # E11 hash iterations per second on an UP board
SHA_SPEED = {"up": 1.0, "rpi4": 0.1351, "rpi3": 0.0572}   # E11 kernel speed per node class (calibrated)
NODE_CLASS = {}                   # node -> class, filled from the lane at start
B = 942.0
LANE = ["anrg-1", "anrg-2", "anrg-3", "anrg-4", "rpi1", "rpi2", "rpi4", "rpi5"]
ROLES = {"safety": {"producer": "anrg-1", "target": "anrg-2", "alt": "anrg-4", "consumers": ["anrg-3", "rpi1", "rpi2"],
                    "report": "rpi5"},
         "reuse": {"prep": "rpi4", "consumer": "anrg-3", "report": "anrg-4"},
         "serve": {"producer": "anrg-1", "target": "anrg-2", "consumers": ["anrg-3", "anrg-4", "rpi1"], "report": "rpi2"},
         "relay": {"producer": "anrg-1", "relay": "anrg-2", "consumer": "anrg-3"},
         "risk": {"producer": "anrg-1", "backup": "anrg-2", "consumer": "rpi1"}}
RUN_TIMEOUT = 900


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ------------------------------------------------------------------ workload
def task(name, node, runtime, inputs=(), outputs=()):
    t = {"name": name, "image": IMAGE, "command": ["python", "task.py"], "runtime": runtime,
         "dependencies": sorted({p for p, _ in inputs}), "resources": {"cpu": "2", "memory": "512Mi"},
         "constraints": {"nodeNames": [node]},
         # runtime is the task's wall time on its node: the work is scaled by the node's kernel speed
         "env": [{"name": "E11_ITERS", "value": str(int(runtime * RATE * SHA_SPEED[NODE_CLASS.get(node, "up")]))},
                 {"name": "E11_INPUTS", "value": ",".join(f"{p}.{o}" for p, o in inputs)},
                 {"name": "E11_OUTPUTS", "value": ",".join(f"{o}:{mb * 1000000}" for o, mb in outputs)}]}
    if inputs:
        t["inputs"] = [{"producer": p, "object": o} for p, o in inputs]
    if outputs:
        t["outputs"] = [{"name": o, "dataSize": f"{mb}MB"} for o, mb in outputs]
    return t


def odag(name, tasks, realization=None, ablate=None):
    """ablate: the controller's experiment-only switch (annotation wl.io/ablate) that disables one enactment rule
    for this run: "repost" or "no-cancel" (cmd/odag-controller/ablation.go)."""
    spec = {"scheduler": "heft", "nodes": LANE, "tasks": tasks,
            "profiling": {"enabled": False, "runtimeSource": "manual", "bandwidthSource": "external"},
            "realizationPolicy": {"mode": "none", "repair": False, "revise": False}}
    if realization:
        spec["realization"] = realization
    meta = {"name": name, "namespace": NS, "labels": {"wl.io/experiment": "e22"}}
    if ablate:
        meta["annotations"] = {"wl.io/ablate": ablate}
    return {"apiVersion": "wl.io/v1", "kind": "ODAG", "metadata": meta, "spec": spec}


ABLATIONS = {"-repost": "repost", "-nocancel": "no-cancel"}   # arm suffix -> the rule switched off


def ablation_of(arm):
    """(arm without the ablation suffix, the controller switch or None)."""
    for suf, sw in ABLATIONS.items():
        if arm.endswith(suf):
            return arm[:-len(suf)], sw
    return arm, None


def serve_dag(r):
    """p -> serve (a data vertex on the producer's node: the serving point, no code) -> three consumers ->
    report. A realization entry naming a serving copy for p.out moves the vertex: the old node's fan-out is
    cancelled and the consumers are served from the new node once its copy has landed."""
    vertex = {"name": "serve", "type": "data", "image": IMAGE, "command": ["python", "task.py"], "runtime": 0,
              "dependencies": ["p"], "inputs": [{"producer": "p", "object": "out"}],
              "constraints": {"nodeNames": [r["producer"]]}}
    out = [task("p", r["producer"], 30, outputs=[("out", 300)]), vertex]
    for k, c in enumerate(r["consumers"], 1):
        t = task(f"c{k}", c, 4, outputs=[("res", 1)])
        t["dependencies"] = ["serve"]
        t["env"][1]["value"] = "serve"
        out.append(t)
    out.append(task("report", r["report"], 2, inputs=[(f"c{k}", "res") for k in range(1, len(r["consumers"]) + 1)]))
    return out


def relay_dag(r):
    return [task("p", r["producer"], 3, outputs=[("out", 300)]),
            task("c", r["consumer"], 4, inputs=[("p", "out")])]


def risk_dag(r):
    return [task("p", r["producer"], 3, outputs=[("out", 300)]),
            task("c", r["consumer"], 4, inputs=[("p", "out")])]


def reuse_dag(r, key):
    """prep (20 s) on a Pi 3 -> consume -> report. With a cache key, a later run reuses the output an earlier run
    retained on prep's node instead of running prep again. Cross-run reuse covers a task's default output (the
    controller checks <run>/<task>), so prep publishes its default output and consume reads it."""
    prep = task("prep", r["prep"], 20)
    if key:
        prep["cacheKey"] = key
    cons = task("consume", r["consumer"], 4, outputs=[("res", 1)])
    cons["dependencies"] = ["prep"]
    cons["env"][1]["value"] = "prep"          # E11_INPUTS: prep's default output
    return [prep, cons, task("report", r["report"], 2, inputs=[("consume", "res")])]


# ------------------------------------------------------------------ harness
class Ctx:
    def __init__(self, password, out):
        self.api = kube.Api()
        nodes = lanes.discover(self.api)
        self.lane = lanes.resolve(",".join(LANE), nodes, {}, {})
        NODE_CLASS.update({n["name"]: n["class"] for n in self.lane.nodes})
        self.shells = netem.open_shells(self.lane.nodes, password, log)
        self.password = password
        self.ctl_shells = None    # the control-plane experiment's shells (lane + control node), opened on first use
        self.emu = netem.Emulator(self.lane, self.shells, log, shape_master=False)
        self.out = out
        self.seq = 0

    def setup(self):
        self.emu.setup()
        pairs = {f"{u}_to_{v}": str(int(b)) for (u, v), b in sorted(self.lane.bw_matrix().items())}
        cm = (self.api.configmap(NS, CM) or {}).get("data") or {}
        self.cm_before = {k: cm.get(k) for k in pairs}
        self.api.merge_configmap(NS, CM, pairs)

    def teardown(self):
        try:
            if self.ctl_shells is not None:
                self.control(False)
                for sh in self.ctl_shells.values():
                    sh.close()
            self.emu.teardown()
        finally:
            self.api.merge_configmap(NS, CM, self.cm_before)
            self.emu.close()
            for s in self.shells.values():
                s.close()

    def ip(self, node):
        return self.lane.by_name[node]["data_ip"]

    def control(self, shaped):
        """Shape the lane's control traffic like a radio link (2 Mbit/s, 10+-2 ms, 0.2% loss; ssh exempt), or not."""
        ips = ctlshape.control_ips()
        if self.ctl_shells is None:
            self.ctl_shells = {n: netem.RootShell(n, ips[n], self.password) for n in LANE}
            self.ctl_shells[ctlshape.MASTER] = netem.RootShell(ctlshape.MASTER, ips.get(ctlshape.MASTER, "10.10.0.1"),
                                                               self.password, local=True)
        ctlshape.unshape(self.ctl_shells, ips)
        if shaped:
            ctlshape.shape(self.ctl_shells, LANE, ips)

    def create(self, obj):
        return self.api.req("POST", self.api.ODAGS, obj)

    def patch(self, run, realization):
        self.api.req("PATCH", f"{self.api.ODAGS}/{run}", {"spec": {"realization": realization}},
                     "application/merge-patch+json")

    def status(self, run):
        try:
            return self.api.status(run)
        except urllib.error.HTTPError:
            return {}

    def task(self, st, name):
        return next((t for t in st.get("tasks") or [] if t.get("name") == name), {})

    def http(self, url, method="GET", timeout=10):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, method=method), timeout=timeout) as r:
                return r.read().decode(errors="replace")
        except Exception as e:  # noqa: BLE001
            return f"ERR {e}"

    def digests(self, run, obj, nodes):
        return {n: self.http(f"http://{self.ip(n)}:8082/digest/{run}/{obj}").strip() for n in nodes}

    def purge(self, run):
        for n in LANE:
            self.http(f"http://{self.ip(n)}:8082/data/{run}", method="DELETE", timeout=30)

    def sut(self):
        out = {}
        for app in ("odag-controller", "wl-policy"):
            pods = self.api.pods(NS, selector=f"app={app}")
            out[app] = sorted((p["metadata"]["name"], p["spec"].get("nodeName", ""),
                               (p.get("status") or {}).get("startTime", "")) for p in pods
                              if not p["metadata"].get("deletionTimestamp"))
        return out


def makespan(st):
    from datetime import datetime as dt

    def ts(x):
        if not x:
            return None
        import re
        return dt.fromisoformat(re.sub(r"(\.\d{6})\d+", r"\1", x.replace("Z", "+00:00"))).timestamp()
    starts = [ts(t.get("taskStartTime") or t.get("startTime")) for t in st.get("tasks") or []]
    ends = [ts(t.get("taskCloseTime") or t.get("completionTime")) for t in st.get("tasks") or []]
    starts, ends = [s for s in starts if s], [e for e in ends if e]
    return (max(ends) - min(starts)) if starts and ends else None


def run_cell(ctx, exp, arm, cap, rep, body):
    """Submit, drive the arm's events (body: a generator of (when-predicate, action) steps), wait, record."""
    ctx.seq += 1
    run = f"e22-{exp}-{arm}-{ctx.seq}-{int(time.time()) % 100000}"
    ctx.emu.reset()
    rec = {"exp": exp, "arm": arm, "cap": cap, "rep": rep, "run": run, "events": [], "submitted": time.time()}
    sut_before = ctx.sut()
    counters_before = ctx.emu.counters()

    def ev(name):
        rec["events"].append([round(time.time() - rec["submitted"], 2), name])
        log(f"    {name}")
    rec["phase"] = body(ctx, run, rec, ev)
    st = ctx.status(run)
    rec["status"] = st
    rec["makespan"] = makespan(st)
    rec["wall"] = time.time() - rec["submitted"]
    rec["linkCounters"] = netem.counter_delta(counters_before, ctx.emu.counters())
    rec["linkState"] = ctx.emu.snapshot_state()
    rec["sut"] = {"before": sut_before, "after": ctx.sut()}
    rec["valid"] = (rec["sut"]["before"] == rec["sut"]["after"])
    fn = os.path.join(ctx.out, f"{exp}-{arm}-{cap or 'none'}-r{rep}.json")
    json.dump(rec, open(fn, "w"), indent=1, default=str)
    log(f"  {exp} {arm} cap={cap} rep={rep}: {rec['phase']} makespan "
        f"{rec['makespan'] and round(rec['makespan'], 1)} s{'' if rec['valid'] else ' (INVALID: controller/policy changed)'}")
    ctx.purge(run)
    ctx.api.delete_odag(run)
    ctx.emu.reset()
    time.sleep(3)
    return rec


def wait_until(ctx, run, pred, timeout):
    t0 = time.time()
    while time.time() - t0 < timeout:
        st = ctx.status(run)
        if st.get("phase") in ("Succeeded", "Failed"):
            return st, False
        if pred(st):
            return st, True
        time.sleep(0.2)
    return ctx.status(run), False


def wait_done(ctx, run, timeout):
    t0 = time.time()
    while time.time() - t0 < timeout:
        st = ctx.status(run)
        if st.get("phase") in ("Succeeded", "Failed"):
            return st.get("phase")
        time.sleep(0.5)
    return "Timeout"


# ------------------------------------------------------------------ serve
def serve_body(arm, cap):
    r = ROLES["serve"]
    arm, ablate = ablation_of(arm)     # adaptive-late-nocancel: the serving revision without cancellation

    def body(ctx, run, rec, ev):
        real = None
        if arm == "static-oracle":
            real = [{"object": "p.out", "copies": [r["target"]], "servingCopy": r["target"]}]
        rec["ablate"] = ablate
        ctx.create(odag(run, serve_dag(r), real, ablate=ablate))
        ev("submitted")

        def degrade():
            ctx.emu.cap_group(r["producer"], r["consumers"], cap)
            ev(f"cap {cap} Mbit/s on {r['producer']} -> consumers")
            if arm.startswith("adaptive"):
                ctx.patch(run, [{"object": "p.out", "copies": [r["target"]], "servingCopy": r["target"]}])
                ev("policy: copy to target, serve from it")
        if cap:
            if arm in ("adaptive-early", "static-oracle", "fixed-early"):
                time.sleep(15)
                degrade()
            else:   # late: once the output exists and the fan-out has started
                _, ok = wait_until(ctx, run, lambda st: ctx.task(st, "p").get("phase") == "Succeeded", RUN_TIMEOUT)
                if ok:
                    time.sleep(0.5)
                    degrade()
        ph = wait_done(ctx, run, RUN_TIMEOUT)
        rec["digests"] = {"p.out": ctx.digests(run, "p.out", [r["producer"]] + ([r["target"]] if arm != "fixed" else [])),
                          "serve": ctx.digests(run, "serve", r["consumers"])}
        return ph
    return body


def exp_serve(ctx, reps, seed):
    caps = [round(B / k, 1) for k in (2, 4, 8, 16)]
    cells = [(a, None) for a in ("fixed", "adaptive-late")] + \
            [(a, c) for c in caps for a in ("fixed", "adaptive-early", "adaptive-late", "static-oracle")]
    for rep in range(1, reps + 1):
        order = cells[:]
        random.Random(f"{seed}-{rep}").shuffle(order)
        for arm, cap in order:
            if os.path.exists(os.path.join(ctx.out, f"serve-{arm}-{cap or 'none'}-r{rep}.json")):
                continue
            log(f"serve {arm} cap={cap} rep={rep}")
            run_cell(ctx, "serve", arm, cap, rep, serve_body(arm, cap))


# ------------------------------------------------------------------ relay
def relay_body(arm, censor=120):
    r = ROLES["relay"]
    P, R, C = r["producer"], r["relay"], r["consumer"]

    def link(u, v, kind):
        ctx_ = link.ctx
        ctx_.emu.apply([(ctx_.emu.idx[u], ctx_.emu.idx[v], kind, None)])

    def body(ctx, run, rec, ev):
        link.ctx = ctx
        blocked = arm != "clean-direct"
        if blocked:   # no end-to-end path: producer->consumer never, the two contacts closed for now
            for u, v in ((P, C), (P, R), (R, C)):
                link(u, v, "stall")
            ev("contacts closed")
        real = None
        if arm == "static-relay":
            real = [{"object": "p.out", "copies": [R, C], "servingCopy": R}]
        ctx.create(odag(run, relay_dag(r), real))
        ev("submitted")
        if not blocked:
            return wait_done(ctx, run, RUN_TIMEOUT)
        # contact 1 opens when the output exists on the producer
        _, ok = wait_until(ctx, run, lambda st: ctx.task(st, "p").get("phase") == "Succeeded", RUN_TIMEOUT)
        if not ok:
            return wait_done(ctx, run, 1)
        t0 = time.time()
        link(P, R, "up")
        ev("contact 1 open (producer -> relay)")
        err = relay_error(arm)          # the policy's error on contact 2's opening (None: not a relay policy)
        if arm == "adaptive-relay" or err is not None:
            ctx.patch(run, [{"object": "p.out", "copies": [R], "servingCopy": R}])
            ev("policy: copy to the relay")
        second = lambda: (ctx.patch(run, [{"object": "p.out", "copies": [R, C], "servingCopy": R}]),
                          ev("policy: copy to the consumer from the relay"))
        plan = [(8, lambda: (link(P, R, "stall"), ev("contact 1 closed"))),
                (28, lambda: (link(R, C, "up"), ev("contact 2 open (relay -> consumer)"),
                              arm == "adaptive-relay" and second())),
                (36, lambda: (link(R, C, "stall"), ev("contact 2 closed")))]
        if err is not None:      # the policy acts at its predicted opening, early or late
            rec["predictionError"] = err
            plan.append((28 + err, second))
            plan.sort(key=lambda x: x[0])
        limit = t0 + (censor if arm == "fixed-direct" or err is not None else RUN_TIMEOUT)
        while time.time() < limit:
            st = ctx.status(run)
            if st.get("phase") in ("Succeeded", "Failed"):
                break
            while plan and time.time() - t0 >= plan[0][0]:
                plan.pop(0)[1]()
            time.sleep(0.2)
        st = ctx.status(run)
        rec["censoredAt"] = None if st.get("phase") in ("Succeeded", "Failed") else time.time() - t0
        rec["digests"] = ctx.digests(run, "p.out", [P, R, C])
        return st.get("phase") if st.get("phase") in ("Succeeded", "Failed") else "Timeout"
    return body


RELAY_ERRORS = {"relay-early8": -8, "relay-early4": -4, "relay-on-time": 0, "relay-late4": 4, "relay-late6": 6}
WARN_LEADS = {"warning-lead10": 10.0, "warning-lead4": 4.0, "warning-lead2": 2.0, "warning-lead1": 1.0,
              "warning-lead0": 0.0}


def relay_error(arm):
    return RELAY_ERRORS.get(arm)


def exp_relay(ctx, reps, seed):
    arms = ["adaptive-relay", "static-relay", "fixed-direct", "clean-direct"]
    for rep in range(1, reps + 1):
        order = arms[:]
        random.Random(f"{seed}-{rep}").shuffle(order)
        for arm in order:
            if os.path.exists(os.path.join(ctx.out, f"relay-{arm}-none-r{rep}.json")):
                continue
            log(f"relay {arm} rep={rep}")
            run_cell(ctx, "relay", arm, None, rep, relay_body(arm))


# ------------------------------------------------------------------ risk
WARN_S, LOSS_S, CLEAR_S = 5.0, 15.0, 15.0   # after the output exists


def risk_body(arm):
    r = ROLES["risk"]
    P, K, C = r["producer"], r["backup"], r["consumer"]

    def body(ctx, run, rec, ev):
        real = None
        if arm == "always-on":
            real = [{"object": "p.out", "copies": [K]}]
        # The consumer cannot read from the producer until the loss: the producer -> consumer link is cut, so the
        # object's only route is a copy; at the loss the producer's copy is gone.
        ctx.emu.apply([(ctx.emu.idx[P], ctx.emu.idx[C], "stall", None)])
        ctx.create(odag(run, risk_dag(r), real))
        ev("submitted; producer -> consumer held")
        _, ok = wait_until(ctx, run, lambda st: ctx.task(st, "p").get("phase") == "Succeeded", RUN_TIMEOUT)
        if not ok:
            return wait_done(ctx, run, 1)
        t0 = time.time()
        steps = []
        if arm in ("on-warning", "warning-clears") or arm in WARN_LEADS:
            at = LOSS_S - WARN_LEADS[arm] if arm in WARN_LEADS else WARN_S
            rec["warningLead"] = LOSS_S - at
            steps.append((at, "warning", lambda: ctx.patch(run, [{"object": "p.out", "copies": [K]}])))
        if arm == "warning-clears":
            steps.append((WARN_S + CLEAR_S, "warning clears: evict the backup",
                          lambda: ctx.patch(run, [{"object": "p.out", "copies": [], "evict": [K]}])))
            steps.append((WARN_S + CLEAR_S + 5, "release the consumer's link",
                          lambda: ctx.emu.apply([(ctx.emu.idx[P], ctx.emu.idx[C], "up", None)])))
        else:
            # The loss: the producer leaves the data plane (every link cut both ways); a backup, if any, serves.
            steps.append((LOSS_S, "loss: the producer is gone from the data plane", lambda: (
                ctx.emu.partition(P),
                arm != "single-copy" and ctx.patch(run, [{"object": "p.out", "copies": [K, C], "servingCopy": K}]))))
        steps.sort(key=lambda x: x[0])     # stable: a warning at the loss's instant comes first
        while time.time() < t0 + RUN_TIMEOUT:
            st = ctx.status(run)
            if st.get("phase") in ("Succeeded", "Failed"):
                break
            while steps and time.time() - t0 >= steps[0][0]:
                _, name, fn = steps.pop(0)
                fn()
                ev(name)
            if not steps and (arm == "single-copy" or arm in WARN_LEADS) and time.time() - t0 > LOSS_S + 60:
                break       # no copy survived the loss: the run cannot finish
            time.sleep(0.2)
        st = ctx.status(run)
        rec["objects"] = st.get("objects")
        ctx.emu.reset()      # heal the producer before reading digests
        rec["digests"] = ctx.digests(run, "p.out", [P, K, C])
        return st.get("phase") if st.get("phase") in ("Succeeded", "Failed") else "Timeout"
    return body


def exp_risk(ctx, reps, seed):
    arms = ["single-copy", "always-on", "on-warning", "warning-clears"]
    for rep in range(1, reps + 1):
        order = arms[:]
        random.Random(f"{seed}-{rep}").shuffle(order)
        for arm in order:
            if os.path.exists(os.path.join(ctx.out, f"risk-{arm}-none-r{rep}.json")):
                continue
            log(f"risk {arm} rep={rep}")
            run_cell(ctx, "risk", arm, None, rep, risk_body(arm))


# ------------------------------------------------------------------ reuse
REUSE_RUNS = 10


def exp_reuse(ctx, reps, seed):
    """Sequences of REUSE_RUNS runs of one workflow, with and without a cache key (a fresh key per sequence, so
    each cached sequence starts with a cold miss). A sequence's runs and data are kept until it ends: a later
    run can only reuse what an earlier one retained."""
    r = ROLES["reuse"]
    for rep in range(1, reps + 1):
        order = ["cache", "nocache"]
        random.Random(f"{seed}-{rep}").shuffle(order)
        for arm in order:
            done = os.path.join(ctx.out, f"reuse-{arm}-r{rep}.json")
            if os.path.exists(done):
                continue
            key = f"e22-prep-{rep}-{int(time.time())}" if arm == "cache" else None
            log(f"reuse {arm} rep={rep} key={key}")
            runs, recs = [], []
            for k in range(1, REUSE_RUNS + 1):
                ctx.seq += 1
                run = f"e22-reuse-{arm}-{ctx.seq}-{int(time.time()) % 100000}"
                t0 = time.time()
                ctx.create(odag(run, reuse_dag(r, key)))
                ph = wait_done(ctx, run, RUN_TIMEOUT)
                st = ctx.status(run)
                prep = ctx.task(st, "prep")
                recs.append({"run": run, "k": k, "phase": ph, "makespan": makespan(st), "wall": time.time() - t0,
                             "prepCompute": prep.get("computeSeconds"), "prepNode": prep.get("node"),
                             "cacheHit": prep.get("computeSeconds") in (None, 0) and ph == "Succeeded",
                             "status": st})
                runs.append(run)
                log(f"    run {k}: {ph} makespan {recs[-1]['makespan'] and round(recs[-1]['makespan'], 1)} s "
                    f"prep compute {prep.get('computeSeconds')}")
                time.sleep(2)
            json.dump({"exp": "reuse", "arm": arm, "rep": rep, "key": key, "runs": recs}, open(done, "w"),
                      indent=1, default=str)
            for run in runs:
                ctx.purge(run)
                ctx.api.delete_odag(run)


# ------------------------------------------------------------------ safety
SAFETY_ARMS = ["revision-control", "repeat-identical", "controller-restart", "source-agent-restart",
               "target-agent-restart", "superseding-revision", "conflicting-request", "last-copy-eviction"]


def copy_state(st, obj="p.out"):
    o = next((x for x in st.get("objects") or [] if x.get("object") == obj), {})
    return o.get("servingCopy", ""), {c.get("node"): c.get("state") for c in o.get("copies") or []}


def restart_pod(ctx, selector, node=None):
    pods = [p for p in ctx.api.pods(NS, selector=selector) if node is None or p["spec"].get("nodeName") == node]
    if not pods:
        return None, None
    old = pods[0]["metadata"]["uid"]
    ctx.api.req("DELETE", f"/api/v1/namespaces/{NS}/pods/{pods[0]['metadata']['name']}",
                {"gracePeriodSeconds": 0})
    t0 = time.time()
    while time.time() - t0 < 180:
        now = [p for p in ctx.api.pods(NS, selector=selector) if (node is None or p["spec"].get("nodeName") == node)
               and p["metadata"]["uid"] != old and (p.get("status") or {}).get("phase") == "Running"
               and all(c.get("ready") for c in (p.get("status") or {}).get("containerStatuses") or [])]
        if now:
            return old, now[0]["metadata"]["uid"]
        time.sleep(1)
    return old, None


COPY_S = 42.7      # an undisturbed 300 MB revision copy at B/16 (safety revision-control, median)


def safety_body(arm):
    """arm: a disturbance, or <restart kind>@<percent>: restart once the copy is that far along (by time, against
    COPY_S), to tell a transfer that continues (recovery ~ the remaining share of COPY_S) from one restarted from
    the first byte (recovery ~ COPY_S)."""
    r = ROLES["safety"]
    P, T, A = r["producer"], r["target"], r["alt"]
    base, ablate = ablation_of(arm)
    head, sep, pct = base.rpartition("-at")
    kind, frac = (head, int(pct) / 100) if sep and pct.isdigit() else (base, None)

    def body(ctx, run, rec, ev):
        # every transfer out of the producer at 59 Mbit/s, so a revision is still in flight when it is disturbed
        ctx.emu.apply([(ctx.emu.idx[P], ctx.emu.idx[n], "degrade", round(B / 16, 1)) for n in r["consumers"] + [T, A]])
        rec["ablate"] = ablate
        ctx.create(odag(run, serve_dag(r), ablate=ablate))
        ev("submitted")
        _, ok = wait_until(ctx, run, lambda st: ctx.task(st, "p").get("phase") == "Succeeded", RUN_TIMEOUT)
        if not ok:
            return wait_done(ctx, run, 1)
        time.sleep(1)
        rev = {"object": "p.out", "copies": [T], "servingCopy": T, "evict": []}

        def transferring(timeout=60):
            t1 = time.time()
            while time.time() - t1 < timeout:
                _, copies = copy_state(ctx.status(run))
                if T in copies:
                    return copies[T]
                time.sleep(0.5)
            return None
        rec["checks"] = {}
        if arm == "revision-control":
            ctx.patch(run, [rev]); ev("revision: copy to target, serve from it")
        elif arm == "repeat-identical":
            ctx.patch(run, [rev]); ev("revision")
            transferring()
            for _ in range(5):
                time.sleep(0.2)
                ctx.patch(run, [rev])
            ev("the same revision 5 more times")
        elif kind in ("controller-restart", "source-agent-restart", "target-agent-restart"):
            ctx.patch(run, [rev]); ev("revision")
            t_rev = time.time()
            rec["checks"]["stateAtFault"] = transferring()
            if frac is not None:
                while time.time() < t_rev + frac * COPY_S:
                    time.sleep(0.05)
                rec["checks"]["faultAtFraction"] = frac
            tf = time.time()
            rec["checks"]["faultAfterRevision"] = tf - t_rev
            ready = lambda: ctx.http(f"http://{ctx.ip(T)}:8082/ready/{run}/p.out", timeout=3).strip() == "true"
            installed_at = []

            def watch_target():   # from the fault on, while the restart is in progress too
                while not installed_at and time.time() - tf < RUN_TIMEOUT:
                    if ready():
                        installed_at.append(time.time())
                        return
                    time.sleep(0.1)
            threading.Thread(target=watch_target, daemon=True).start()
            if kind == "controller-restart":
                old, new = restart_pod(ctx, "app=odag-controller")
            else:
                old, new = restart_pod(ctx, "app=data-agent", P if kind == "source-agent-restart" else T)
            ev(f"{arm}: {old} -> {new}")
            rec["checks"]["restarted"] = bool(new)
            rec["checks"]["faultToRecovery"] = None
            rec["checks"]["faultToInstalled"] = None      # the target agent's own readiness, no status latency
            while time.time() - tf < RUN_TIMEOUT:
                if rec["checks"]["faultToInstalled"] is None and installed_at:
                    rec["checks"]["faultToInstalled"] = installed_at[0] - tf
                serving, copies = copy_state(ctx.status(run))
                if copies.get(T) == "Installed" and serving == T:
                    rec["checks"]["faultToRecovery"] = time.time() - tf
                    for _ in range(50):
                        if installed_at:
                            break
                        time.sleep(0.1)
                    rec["checks"]["faultToInstalled"] = (installed_at[0] - tf) if installed_at else None
                    break
                time.sleep(0.2)
        elif arm == "superseding-revision":
            ctx.patch(run, [rev]); ev("revision")
            transferring()
            ctx.patch(run, [{"object": "p.out", "copies": [A], "servingCopy": A, "evict": [T]}])
            ev("superseding revision: serve from the alternate, evict the first target")
        elif kind == "newer-requests":
            # four newer requests during the copy, each still wanting the copy to the target (the copy list alternates
            # with the producer's node, which already holds the object): a wanted transfer must not restart
            t_rev = time.time()
            ctx.patch(run, [rev]); ev("revision")
            ready = lambda: ctx.http(f"http://{ctx.ip(T)}:8082/ready/{run}/p.out", timeout=3).strip() == "true"
            installed_at = []

            def watch_target():
                while not installed_at and time.time() - t_rev < RUN_TIMEOUT:
                    if ready():
                        installed_at.append(time.time())
                        return
                    time.sleep(0.1)
            threading.Thread(target=watch_target, daemon=True).start()
            transferring()
            for k in range(1, 5):
                time.sleep(max(0.0, t_rev + 10 * k - time.time()))
                if installed_at:
                    break
                ctx.patch(run, [dict(rev, copies=[T, P] if k % 2 else [T])])
                ev(f"newer request {k}: still wants the copy to the target")
            t_end = time.time()
            while not installed_at and time.time() - t_end < RUN_TIMEOUT:
                time.sleep(0.2)
            rec["checks"]["revisionToInstalled"] = (installed_at[0] - t_rev) if installed_at else None
        elif arm == "conflicting-request":
            ctx.patch(run, [{"object": "p.out", "copies": [T], "servingCopy": "", "evict": [T]}])
            ev("conflicting request: copy to and evict from the same node")
        elif arm == "last-copy-eviction":
            ctx.patch(run, [{"object": "p.out", "copies": [], "servingCopy": "", "evict": [P]}])
            ev("evict the only copy")
        ph = wait_done(ctx, run, RUN_TIMEOUT)
        st = ctx.status(run)
        serving, copies = copy_state(st)
        rec["checks"].update({"finalServing": serving, "finalCopies": copies})
        holders = [n for n in [P, T, A] if n in copies or n == P]
        rec["digests"] = {"p.out": ctx.digests(run, "p.out", holders), "serve": ctx.digests(run, "serve", r["consumers"])}
        vals = {v for d in rec["digests"].values() for v in d.values() if v and not v.startswith("ERR")}
        rec["checks"]["digestsAgree"] = len(vals) == 1
        expect = {"revision-control": serving == T and copies.get(T) == "Installed",
                  "repeat-identical": serving == T and copies.get(T) == "Installed",
                  "controller-restart": serving == T and copies.get(T) == "Installed",
                  "source-agent-restart": serving == T and copies.get(T) == "Installed",
                  "target-agent-restart": serving == T and copies.get(T) == "Installed",
                  "superseding-revision": serving == A and copies.get(A) == "Installed" and T not in
                  {n for n, s_ in copies.items() if s_ == "Installed"},
                  "conflicting-request": T not in {n for n, s_ in copies.items() if s_ == "Installed"},
                  "last-copy-eviction": True,
                  "newer-requests": serving == T and copies.get(T) == "Installed"}[kind]
        rec["checks"]["expectedState"] = bool(expect)
        rec["checks"]["ok"] = ph == "Succeeded" and rec["checks"]["digestsAgree"] and bool(expect)
        return ph
    return body


def exp_safety(ctx, reps, seed):
    for rep in range(1, reps + 1):
        order = SAFETY_ARMS[:]
        random.Random(f"{seed}-{rep}").shuffle(order)
        for arm in order:
            if os.path.exists(os.path.join(ctx.out, f"safety-{arm}-none-r{rep}.json")):
                continue
            log(f"safety {arm} rep={rep}")
            rec = run_cell(ctx, "safety", arm, None, rep, safety_body(arm))
            log(f"    checks {rec.get('checks')}")


def exp_signals(ctx, reps, seed):
    """Imperfect signals: the relay policy acts on a predicted opening of contact 2 that is early or late; the
    replication policy's warning arrives with less lead time before the loss. Records go next to relay/risk ones
    under signals/ (exp names relay and risk, arms named by the error or lead)."""
    cells = [("relay", a, relay_body(a)) for a in RELAY_ERRORS] + [("risk", a, risk_body(a)) for a in WARN_LEADS]
    for rep in range(1, reps + 1):
        order = cells[:]
        random.Random(f"{seed}-signals-{rep}").shuffle(order)
        for exp, arm, body in order:
            if os.path.exists(os.path.join(ctx.out, f"{exp}-{arm}-none-r{rep}.json")):
                continue
            log(f"signals {exp} {arm} rep={rep}")
            run_cell(ctx, exp, arm, None, rep, body)


def exp_ablate(ctx, reps, seed):
    """Enactment rules switched off, one at a time, against the same arm with the rule on: newer requests during a
    copy (rule: a wanted transfer is never restarted; switch repost) and a serving revision after the fan-out has
    started at B/16 (rule: the obsolete fan-out is cancelled; switch no-cancel). Records under ablate/."""
    b16 = round(B / 16, 1)
    cells = [("safety", a, None, safety_body(a)) for a in ("newer-requests", "newer-requests-repost")] + \
            [("serve", a, b16, serve_body(a, b16)) for a in ("adaptive-late", "adaptive-late-nocancel")]
    for rep in range(1, reps + 1):
        order = cells[:]
        random.Random(f"{seed}-ablate-{rep}").shuffle(order)
        for exp, arm, cap, body in order:
            if os.path.exists(os.path.join(ctx.out, f"{exp}-{arm}-{cap or 'none'}-r{rep}.json")):
                continue
            log(f"ablate {exp} {arm} cap={cap} rep={rep}")
            rec = run_cell(ctx, exp, arm, cap, rep, body)
            if exp == "safety":
                log(f"    checks {rec.get('checks')}")


# ------------------------------------------------------------------ stress
STRESS_P = "anrg-1"
STRESS_TARGETS = ["anrg-2", "anrg-4", "rpi1", "rpi2"]     # not the consumers' nodes (rpi4, rpi5, anrg-3)
STRESS_OBJECTS = ["o1", "o2", "o3"]


def stress_dag():
    out = [task("p", STRESS_P, 3, outputs=[(o, 100) for o in STRESS_OBJECTS])]
    for k, (o, node) in enumerate(zip(STRESS_OBJECTS, ["rpi4", "rpi5", "anrg-3"]), 1):
        out.append(task(f"c{k}", node, 75, inputs=[("p", o)]))    # keeps the run live while the writers act
    return out


def stress_body(rep, seed):
    """One randomized run: two writers revise the realizations of three objects while copies are in flight, each
    by read-modify-write on the run object with optimistic concurrency (a stale write is refused and redone), and
    0 to 2 restarts (controller, source agent, a target agent) land at random times. Then, once quiet: does every
    agent hold exactly what the run's final spec asks for (convergence), with the producer's digest?"""
    rnd = random.Random(f"{seed}-stress-{rep}")

    def body(ctx, run, rec, ev):
        ctx.emu.apply([(ctx.emu.idx[STRESS_P], ctx.emu.idx[n], "degrade", round(B / 8, 1))
                       for n in STRESS_TARGETS + ["rpi4", "rpi5"]])
        ctx.create(odag(run, stress_dag()))
        ev("submitted")
        _, ok = wait_until(ctx, run, lambda st: ctx.task(st, "p").get("phase") == "Succeeded", RUN_TIMEOUT)
        if not ok:
            return wait_done(ctx, run, 1)
        path = f"{ctx.api.ODAGS}/{run}"
        objs = [f"p.{o}" for o in STRESS_OBJECTS]
        rec["ops"], rec["conflicts"], rec["writes"] = [], 0, []
        lock = threading.Lock()

        def write(who, op, change):
            for _ in range(20):
                cur = ctx.api.get(path)
                entries = {e["object"]: dict(e) for e in (cur.get("spec") or {}).get("realization") or []}
                new = change(entries)
                if new is None:
                    return
                body_ = {"metadata": {"resourceVersion": cur["metadata"]["resourceVersion"]},
                         "spec": {"realization": [new[o] for o in sorted(new)]}}
                try:
                    ctx.api.req("PATCH", path, body_, "application/merge-patch+json")
                    with lock:
                        rec["ops"].append([round(time.time() - t0, 2), who, op])
                        rec["writes"].append({o: {k: e.get(k) for k in ("copies", "evict", "servingCopy")}
                                              for o, e in new.items()})
                    return
                except urllib.error.HTTPError as e:
                    if e.code != 409:
                        raise
                    with lock:
                        rec["conflicts"] += 1

        def entry(entries, o):
            e = entries.get(o) or {"object": o, "copies": [], "evict": []}
            e.setdefault("copies", []); e.setdefault("evict", [])
            return e

        def op_copy(entries):
            o, n = rnd.choice(objs), rnd.choice(STRESS_TARGETS)
            e = entry(entries, o)
            e["copies"] = sorted(set(e["copies"]) | {n}); e["evict"] = [x for x in e["evict"] if x != n]
            entries[o] = e
            return entries

        def op_evict(entries):
            o = rnd.choice(objs)
            e = entry(entries, o)
            cand = [n for n in e["copies"] if n != e.get("servingCopy")]
            if not cand:
                return None
            n = rnd.choice(cand)
            e["copies"] = [x for x in e["copies"] if x != n]; e["evict"] = sorted(set(e["evict"]) | {n})
            entries[o] = e
            return entries

        def op_serve(entries):
            o = rnd.choice(objs)
            e = entry(entries, o)
            if not e["copies"]:
                return None
            e["servingCopy"] = rnd.choice(e["copies"])
            entries[o] = e
            return entries

        def op_newer(entries):      # a newer request with the same intent: the producer's node, which holds it
            o = rnd.choice(objs)
            e = entry(entries, o)
            e["copies"] = sorted(set(e["copies"]) ^ {STRESS_P})
            entries[o] = e
            return entries

        ops = {"A": [("copy", op_copy), ("evict", op_evict), ("newer", op_newer)],
               "B": [("serve", op_serve), ("copy", op_copy), ("newer", op_newer)]}
        span = 40.0
        plan = []
        for who in ("A", "B"):
            for _ in range(rnd.randint(4, 6)):
                name, fn = rnd.choice(ops[who])
                plan.append((rnd.uniform(0, span), "write", (who, name, fn)))
        kinds = ["controller-restart", "source-agent-restart", "target-agent-restart"]
        for _ in range(rnd.randint(0, 2)):
            plan.append((rnd.uniform(0, span), "restart", rnd.choice(kinds)))
        plan.sort(key=lambda x: x[0])
        rec["plan"] = [[round(t, 2), k, a if k == "restart" else f"{a[0]}:{a[1]}"] for t, k, a in plan]
        t0 = time.time()
        threads = []
        for at, kind, arg in plan:
            time.sleep(max(0.0, t0 + at - time.time()))
            if kind == "write":
                who, name, fn = arg
                th = threading.Thread(target=write, args=(who, name, fn), daemon=True)   # writers overlap
                th.start(); threads.append(th)
            else:
                if arg == "controller-restart":
                    old, new = restart_pod(ctx, "app=odag-controller")
                else:
                    node = STRESS_P if arg == "source-agent-restart" else rnd.choice(STRESS_TARGETS)
                    old, new = restart_pod(ctx, "app=data-agent", node)
                ev(f"{arg}: {old} -> {new}")
        for th in threads:
            th.join(timeout=60)
        t_quiet = time.time()
        ev(f"writers done ({len(rec['ops'])} writes, {rec['conflicts']} conflicts retried)")
        final = {e["object"]: e for e in (ctx.api.get(path).get("spec") or {}).get("realization") or []}
        rec["finalSpec"] = final
        # What each object must end as: its last ACCEPTED request. An entry that copies to and evicts from one node is
        # refused (the object stays as the previous request left it), so it is skipped.
        expect, refused = {}, 0
        for w in rec["writes"]:
            for o, e in w.items():
                if set(e.get("copies") or []) & set(e.get("evict") or []):
                    refused += 1
                    continue
                expect[o] = e
        rec["expected"], rec["refusedWrites"] = expect, refused
        lane = [STRESS_P] + STRESS_TARGETS

        def held(o):
            return {n for n in lane if ctx.http(f"http://{ctx.ip(n)}:8082/ready/{run}/{o}", timeout=3).strip() == "true"}

        def converged():
            for o in objs:
                e = expect.get(o)
                if not e:
                    continue
                h = held(o)
                if not set(e.get("copies") or []) <= h or set(e.get("evict") or []) & h:
                    return False
            return True
        conv_at = None
        while time.time() - t_quiet < RUN_TIMEOUT:
            if converged():
                conv_at = time.time()
                break
            time.sleep(1.0)
        rec["checks"] = {"converged": conv_at is not None,
                         "quietToConverged": (conv_at - t_quiet) if conv_at else None}
        ph = wait_done(ctx, run, RUN_TIMEOUT)
        st = ctx.status(run)
        serving = {x.get("object"): x.get("servingCopy", "") for x in st.get("objects") or []}
        rec["checks"]["servingAsRequested"] = all(
            not (expect.get(o) or {}).get("servingCopy") or serving.get(o) == expect[o]["servingCopy"] for o in objs)
        rec["digests"] = {o: ctx.digests(run, o, sorted(held(o))) for o in objs}
        # every copy that exists has the producer's bytes (an object no node holds any more has nothing to disagree)
        rec["checks"]["digestsAgree"] = all(
            len({v for v in d.values() if v and not v.startswith("ERR")}) <= 1 and
            not any(v.startswith("ERR") for v in d.values()) for d in rec["digests"].values())
        rec["checks"]["ok"] = (ph == "Succeeded" and rec["checks"]["converged"] and rec["checks"]["digestsAgree"]
                               and rec["checks"]["servingAsRequested"])
        return ph
    return body


def exp_stress(ctx, reps, seed):
    for rep in range(1, reps + 1):
        if os.path.exists(os.path.join(ctx.out, f"stress-random-none-r{rep}.json")):
            continue
        log(f"stress rep={rep}")
        rec = run_cell(ctx, "stress", "random", None, rep, stress_body(rep, seed))
        log(f"    checks {rec.get('checks')} plan {rec.get('plan')}")


def exp_ctlplane(ctx, reps, seed):
    """The control plane on a radio-like link: the same cells with the lane's control traffic clean and shaped like
    the radio lanes' data links (ctlshape.py), randomized together. Arms with -ctl ran shaped. Records under
    ctlplane/."""
    b16 = round(B / 16, 1)
    base = [("serve", "adaptive-late", b16, lambda a: serve_body(a, b16)), ("serve", "fixed", b16, lambda a: serve_body(a, b16)),
            ("relay", "adaptive-relay", None, relay_body), ("safety", "revision-control", None, safety_body)]
    cells = [(exp, arm, cap, mk, shaped) for exp, arm, cap, mk in base for shaped in (False, True)]
    for rep in range(1, reps + 1):
        order = cells[:]
        random.Random(f"{seed}-ctlplane-{rep}").shuffle(order)
        for exp, arm, cap, mk, shaped in order:
            name = arm + ("-ctl" if shaped else "")
            if os.path.exists(os.path.join(ctx.out, f"{exp}-{name}-{cap or 'none'}-r{rep}.json")):
                continue
            log(f"ctlplane {exp} {name} cap={cap} rep={rep}")
            ctx.control(shaped)
            try:
                rec = run_cell(ctx, exp, name, cap, rep, mk(arm))
            finally:
                ctx.control(False)
            if exp == "safety":
                log(f"    checks {rec.get('checks')}")


RESUME_ARMS = [f"{k}-at{p}" for k in ("controller-restart", "source-agent-restart", "target-agent-restart")
               for p in (5, 50, 90)]


def exp_resume(ctx, reps, seed):
    for rep in range(1, reps + 1):
        order = RESUME_ARMS[:]
        random.Random(f"{seed}-resume-{rep}").shuffle(order)
        for arm in order:
            if os.path.exists(os.path.join(ctx.out, f"safety-{arm}-none-r{rep}.json")):
                continue
            log(f"resume {arm} rep={rep}")
            rec = run_cell(ctx, "safety", arm, None, rep, safety_body(arm))
            log(f"    checks {rec.get('checks')}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("exp", choices=["serve", "relay", "risk", "reuse", "safety", "resume", "signals", "ablate", "stress", "ctlplane"])
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--seed", type=int, default=22)
    ap.add_argument("--out", required=True)
    ap.add_argument("--smoke", action="store_true", help="one quick cell per arm family, rep 0")
    a = ap.parse_args()
    password = sys.stdin.readline().rstrip("\n")
    lock = HostLock("e22-lane")
    if not lock.try_acquire():
        raise SystemExit("another E22 session holds its lane")
    out = os.path.join(a.out, a.exp)
    os.makedirs(out, exist_ok=True)
    ctx = Ctx(password, out)
    json.dump({"exp": a.exp, "lane": LANE, "roles": ROLES["safety"] if a.exp == "resume" else ({"relay": ROLES["relay"], "risk": ROLES["risk"]} if a.exp == "signals" else
                               {"safety": ROLES["safety"], "serve": ROLES["serve"]} if a.exp == "ablate" else
                               {"producer": STRESS_P, "targets": STRESS_TARGETS} if a.exp == "stress" else
                               {k: ROLES[k] for k in ("serve", "relay", "safety")} if a.exp == "ctlplane" else ROLES[a.exp]), "reps": a.reps, "seed": a.seed,
               "started": datetime.now(timezone.utc).isoformat(),
               "controller": kube.wayline_deployment().get("controller", {}).get("image")},
              open(os.path.join(out, f"manifest-{int(time.time())}.json"), "w"), indent=1)
    try:
        ctx.setup()
        if a.smoke:
            smoke = {"serve": [("fixed", None, serve_body), ("adaptive-late", round(B / 16, 1), serve_body)],
                     "relay": [("adaptive-relay", None, relay_body), ("fixed-direct", None, relay_body)],
                     "risk": [("on-warning", None, risk_body), ("single-copy", None, risk_body)],
                     "reuse": [], "safety": []}[a.exp]
            for arm, cap, mk in smoke:
                log(f"smoke {a.exp} {arm} cap={cap}")
                run_cell(ctx, a.exp, arm, cap, 0, mk(arm, cap) if mk is serve_body else mk(arm))
        else:
            {"serve": exp_serve, "relay": exp_relay, "risk": exp_risk, "reuse": exp_reuse,
             "safety": exp_safety, "resume": exp_resume, "signals": exp_signals, "ablate": exp_ablate, "stress": exp_stress, "ctlplane": exp_ctlplane}[a.exp](ctx, a.reps, a.seed)
    finally:
        ctx.teardown()
        log("done")


if __name__ == "__main__":
    main()
