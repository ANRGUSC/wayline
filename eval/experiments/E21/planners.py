#!/usr/bin/env python3
"""E21: planners plug in. The repair study's undisrupted DAG families, placed by Wayline's built-in HEFT and by
SAGA's schedulers through the sidecar, on a Tier 2 lane (one fast node, two mid-range, two slow; emulated radio
links, wired pairs; every task does its real compute, as in E17/E19).

Per run it records the planner requested and used (a SAGA error falls back to HEFT; such a run is not counted),
the placement, the controller's predicted makespan for that placement, and the measured makespan (first task
start to last task end, E17's collect()).

The lane's link rates are merged into the controller's bandwidth ConfigMap (only this lane's pairs, restored at
the end), so every planner prices transfers with the rates the lane emulates.

Work is a queue of (scenario, planner, rep) files that every lane process claims from, in a randomized order:
  printf '%s\\n' "$PW" | ./run.sh --lane anrg-7,rpi4-1,rpi4-2,rpi51,rpi52 --queue ~/E21-queue --out ~/E21-results --init
  printf '%s\\n' "$PW" | ./run.sh --lane anrg-8,rpi4-3,rpi4-4,rpi49,rpi50 --queue ~/E21-queue --out ~/E21-results
"""
import argparse
import json
import os
import random
import subprocess
import sys
import time
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
EXP = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(EXP, "E17"))
sys.path.insert(0, os.path.join(EXP, "E18"))
import driver as e17               # noqa: E402  (Scenario, Runner, lane emulation)
from hostlock import HostLock      # noqa: E402

NS = "wl-system"
CM = "wl-network-profile"
PLANNERS = ["heft", "saga/heft", "saga/cpop", "saga/peft", "saga/etf", "saga/minmin", "saga/maxmin", "saga/met",
            "saga/olb", "saga/fastest_node", "saga/saga.schedulers.throughput.maxtp.MaxTPScheduler"]


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def kubectl(args, stdin=None):
    return subprocess.run(["k3s", "kubectl", "-n", NS] + args, input=stdin, capture_output=True, text=True)


def lane_pairs(runner, tier2):
    """{u_to_v: bytes/s} for every ordered pair of the lane: the radio goodput, times the wired factor on wired
    pairs, as the lane emulates them."""
    wired = {frozenset((int(a[1:]), int(b[1:]))) for a, b in tier2["wired"]}
    out = {}
    for i, u in enumerate(runner.names):
        for j, v in enumerate(runner.names):
            if i != j:
                f = tier2["wired_factor"] if frozenset((i, j)) in wired else 1.0
                out[f"{u}_to_{v}"] = str(int(runner.goodput * f))
    return out


def merge_cm(data):
    r = kubectl(["patch", "configmap", CM, "--type", "merge", "-p", json.dumps({"data": data})])
    if r.returncode:
        raise RuntimeError(f"bandwidth ConfigMap: {r.stderr}")


def undisrupted(scen_dir, ccrs):
    """One scenario per (structure, seed, CCR) at the chosen CCRs: the undisrupted plan does not depend on the
    disruption."""
    seen, out = set(), []
    for f in sorted(os.listdir(scen_dir)):
        if not f.endswith(".json"):
            continue
        d = json.load(open(os.path.join(scen_dir, f)))
        k = (d["structure"], d["seed"], d["ccr"])
        if d["ccr"] in ccrs and k not in seen:
            seen.add(k)
            out.append(os.path.join(scen_dir, f))
    return out


def init_queue(qdir, paths, planners, reps, seed):
    os.makedirs(os.path.join(qdir, "claimed"), exist_ok=True)
    items = [(p, pl, r) for r in range(1, reps + 1) for p in paths for pl in planners]
    rng = random.Random(seed)
    # randomized blocks: every item of repetition r before any of r + 1
    order = []
    for r in range(1, reps + 1):
        block = [it for it in items if it[2] == r]
        rng.shuffle(block)
        order += block
    for k, (p, pl, r) in enumerate(order):
        json.dump({"scenario": p, "planner": pl, "rep": r},
                  open(os.path.join(qdir, f"{k:04d}.json"), "w"))
    log(f"queued {len(order)} runs")


def claim(qdir):
    for f in sorted(x for x in os.listdir(qdir) if x.endswith(".json")):
        try:
            os.rename(os.path.join(qdir, f), os.path.join(qdir, "claimed", f))
        except FileNotFoundError:
            continue
        return json.load(open(os.path.join(qdir, "claimed", f)))
    return None


def build(runner, name, sc, S, planner):
    """E17's ODAG for the scenario, unpinned and placed by the planner."""
    obj = runner.odag(name, sc, sc.doc["plan"], S)
    for t in obj["spec"]["tasks"]:
        t.pop("constraints", None)
    obj["spec"]["scheduler"] = planner
    obj["spec"].pop("executor", None)       # the planner's per-node order, not readiness
    obj["spec"]["nodes"] = list(runner.names)
    obj["metadata"]["labels"]["wl.io/experiment"] = "e21"
    return obj


def run_one(runner, item, out_dir, T):
    sc = e17.Scenario(item["scenario"])
    S = T / sc.doc["m0"]
    planner = item["planner"]
    runner.seq += 1
    name = f"e21-{runner.lane_id}-{os.getpid() % 10000}-{runner.seq}"
    rec = {"scenario": sc.doc["id"], "structure": sc.doc["structure"], "ccr": sc.doc["ccr"], "planner": planner,
           "rep": item["rep"], "lane": runner.names, "odag": name, "T": T, "S": S, "submitted": time.time()}
    try:
        runner.lane.reset()
    except Exception as e:  # noqa: BLE001 - a node lost its emulation state
        log(f"lane reset failed ({e}); setting the lane up again")
        runner.lane.setup()
        runner.lane.reset()
    runner.api.delete(name)
    runner.api.create(build(runner, name, sc, S, planner))
    t0, obj, deadline = None, None, time.time() + 120
    while t0 is None:
        obj = runner.api.get(name)
        starts = [e17.rfc3339(e.get("taskStartTime") or e.get("startTime"))
                  for e in obj.get("status", {}).get("tasks", []) or []]
        starts = [s for s in starts if s]
        if starts:
            t0 = min(starts)
        elif time.time() > deadline:
            rec["outcome"] = "no-start"
            break
        else:
            time.sleep(0.1)
    if t0 is not None:
        limit = t0 + max(15 * T, 300)
        while True:
            obj = runner.api.get(name)
            ph = obj.get("status", {}).get("phase")
            if ph in ("Succeeded", "Failed"):
                rec["outcome"] = ph
                break
            if time.time() > limit:
                rec["outcome"] = "timeout"
                break
            time.sleep(0.5)
        rec["final"] = runner.collect(sc, obj, t0, 1.0)
    st = (obj or {}).get("status", {})
    rec["scheduling"] = st.get("scheduling")
    pred = st.get("predictedTasks") or []
    rec["placement"] = {p["name"]: p["node"] for p in pred}
    rec["predicted"] = max((p.get("estEnd") or 0) for p in pred) if pred else None
    rec["predictedStart"] = min((p.get("estStart") or 0) for p in pred) if pred else None
    runner.api.delete(name)
    fn = os.path.join(out_dir, f"{sc.doc['id']}-{planner.split('.')[-1].replace('/', '_')}-r{item['rep']}.json")
    json.dump(rec, open(fn, "w"), indent=1, default=str)
    ms = (rec.get("final") or {}).get("makespan")
    log(f"rep {item['rep']} {planner:18s} {sc.doc['id']}: {rec.get('outcome')} makespan "
        f"{ms if ms is None else round(ms, 1)} predicted {rec['predicted']} "
        f"(used {(rec['scheduling'] or {}).get('used')})")
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lane", required=True)
    ap.add_argument("--scenarios", default=os.path.expanduser("~/testbed/e17/scenarios-t2"))
    ap.add_argument("--ccr", type=float, nargs="+", default=[1.0])
    ap.add_argument("--planners", nargs="+", default=PLANNERS)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--T", type=float, default=60.0)
    ap.add_argument("--queue", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--init", action="store_true", help="fill the queue (once, from one lane)")
    ap.add_argument("--seed", type=int, default=21)
    a = ap.parse_args()
    password = sys.stdin.readline().rstrip("\n")
    lane = a.lane.split(",")
    lock = HostLock(f"e21-lane-{lane[0]}")
    if not lock.try_acquire():
        raise SystemExit(f"another session holds lane {lane[0]}")
    paths = undisrupted(a.scenarios, set(a.ccr))
    if a.init:
        init_queue(a.queue, paths, a.planners, a.reps, a.seed)
    os.makedirs(a.out, exist_ok=True)
    tier2 = json.load(open(paths[0]))["tier2"]
    runner = e17.Runner(lane, a.out, a.T, password, tier2=tier2)
    pairs = lane_pairs(runner, tier2)
    before = (json.loads(kubectl(["get", "configmap", CM, "-o", "json"]).stdout or "{}").get("data") or {})
    merge_cm(pairs)
    json.dump({"lane": lane, "planners": a.planners, "ccr": a.ccr, "reps": a.reps, "T": a.T,
               "bandwidth": pairs, "started": datetime.now(timezone.utc).isoformat(),
               "controller": kubectl(["get", "deploy", "odag-controller", "-o",
                                      "jsonpath={.spec.template.spec.containers[0].image}"]).stdout},
              open(os.path.join(a.out, f"manifest-{lane[0]}.json"), "w"), indent=1)
    log(f"E21 on {lane}: {len(paths)} DAGs, planners {a.planners}")
    try:
        while True:
            item = claim(a.queue)
            if item is None:
                break
            try:
                run_one(runner, item, a.out, a.T)
            except Exception as e:  # noqa: BLE001 - one run, not the campaign
                log(f"{item}: {type(e).__name__}: {e}")
            time.sleep(3)
    finally:
        merge_cm({k: before.get(k) for k in pairs})
        runner.close()
        log("done")


if __name__ == "__main__":
    main()
