#!/usr/bin/env python3
"""E19: does a plan survive deployment? The same plans on Wayline, Ray, Dask and Argo.

Each E17 Tier 2 scenario (the MILCOM 2026 schedule-repair study on the
testbed) carries a DAG, the lane's measured node speeds, link bandwidths, the
initial HEFT placement and the simulator's undisrupted makespan M0. Scaled to
the testbed (S = T / M0, T = 60 s by default, as E17), the plan predicts a
makespan of T. E19 runs that plan, undisrupted, on the E17 Tier 2 lane (one
UP board, two Pi 4s, two Pi 3s; the UP board wired to the Pi 4s, every other
pair a 2 Mbit/s radio link with E17's netem impairments) with every task
pinned to its planned node, one task at a time per node, the same kernel
work per task and the same bytes per edge:

  wayline  E17's own m0 run (warm runners, external placement)
  wayline-pull  the same with spec.dataPlane "pull": a task's inputs move to its node only once it is ready
           (every producer done), as Ray and Dask fetch them; the push/pull ablation (2026-11-11)
  ray      Ray, hard node affinity, each task taking its node's CPUs
  dask     Dask, workers= pinning, each task taking its worker's CPU resource
  argo     Argo Workflows, node affinity, artifacts through an S3 repository
           on the lane's gateway (n0, the UP board), so store traffic crosses
           the lane's emulated links like any other

and records each system's makespan (first task start to last task end)
against the prediction. Blocks are randomized: per repetition, systems in
random order, scenarios in random order within each.

Run on eclipse in E17's paper environment (ncsim, saga); the sudo password
for the lane (link emulation) is the first line of stdin:
  printf '%s\\n' "$PW" | ./run.sh --lane anrg-7,rpi4-1,rpi4-2,rpi51,rpi52 --reps 3 --out ~/E19-results
"""
import argparse
import importlib.util
import json
import os
import random
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
EXP = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(EXP, "E17"))
sys.path.insert(0, os.path.join(EXP, "E18"))


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


import driver as e17       # noqa: E402  (E17: Scenario, Runner, lane emulation)
import kube                # noqa: E402  (E18)
import lanes               # noqa: E402  (E18)
import images              # noqa: E402  (E18)
rayrun = _load("rayrun", os.path.join(EXP, "E18", "ray", "rayrun.py"))
daskrun = _load("daskrun", os.path.join(EXP, "E18", "dask", "daskrun.py"))
argorun = _load("argorun", os.path.join(EXP, "E18", "argo", "argorun.py"))

BODY = os.path.join(HERE, "stage_body.py")
S3_NAME, S3_PORT = "e19-s3", 9501
SYSTEMS = ["wayline", "ray", "dask", "argo"]


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ------------------------------------------------------------------ the DAG as tasks
def dag_tasks(sc, S, goodput, lane, nodes):
    """Tasks with the same work and edge bytes as E17's Wayline run."""
    d = sc.doc
    out = []
    for t in sc.dag.tasks:
        cost = sc.dag.tasks[t].compute_cost
        node = lane[int(d["plan"][t][1:])]
        outs = {}
        for c, size in sc.children[t]:
            outs[sc.tn[c]] = max(1, int(round(size / d["bandwidth"] * S * goodput)))
        out.append({"name": sc.tn[t], "work": max(0, int(round(cost * S * d["tier2"]["r_ref"]))),
                    "node": node, "node_ip": nodes[node]["data_ip"],
                    "deps": [sc.tn[p] for p in sc.parents[t]], "outs": outs})
    return out


def makespan_of(times):
    return max(e for _, e in times.values()) - min(s for s, _ in times.values())


# ------------------------------------------------------------------ Ray
class RaySys:
    def __init__(self, nodes, lane, master):
        self.workers = {n: nodes[n] for n in lane}
        self.cl = rayrun.Cluster(images.ref("wl-ray-baseline"), self.workers, master)
        self.cpus = {n["data_ip"]: max(1, n["cores"] - 1) for n in self.workers.values()}

    def up(self):
        self.cl.up()
        self.cl.put("/tmp/stage_body.py", open(BODY).read())
        self.cl.put("/tmp/drive_ray.py", open(os.path.join(HERE, "drive_ray.py")).read())

    def down(self):
        self.cl.down()

    def run(self, tasks, timeout):
        self.cl.put("/tmp/dag.json", json.dumps({"tasks": tasks, "cpus": self.cpus}))
        r = kube.kubectl(["exec", rayrun.HEAD, "--", "python", "-u", "/tmp/drive_ray.py", "--dag", "/tmp/dag.json",
                          "--body", "/tmp/stage_body.py"], ns=rayrun.NS, timeout=timeout)
        return _result(r)


# ------------------------------------------------------------------ Dask
class DaskSys:
    def __init__(self, nodes, lane, master):
        self.workers = {n: nodes[n] for n in lane}
        self.master = master
        self.cl = daskrun.Cluster(daskrun.IMAGE, self.workers, master)
        self.cpus = {n["data_ip"]: max(1, n["cores"] - 1) for n in self.workers.values()}

    def up(self):
        self.cl.up()
        self.cl.put("/tmp/stage_body.py", open(BODY).read())
        self.cl.put("/tmp/drive_dask.py", open(os.path.join(HERE, "drive_dask.py")).read())

    def down(self):
        self.cl.down()

    def run(self, tasks, timeout):
        self.cl.put("/tmp/dag.json", json.dumps({"tasks": tasks, "cpus": self.cpus}))
        r = kube.kubectl(["exec", daskrun.SCHED, "--", "python", "-u", "/tmp/drive_dask.py", "--dag", "/tmp/dag.json",
                          "--body", "/tmp/stage_body.py", "--scheduler", f"tcp://{self.master['data_ip']}:8786"],
                         ns=daskrun.NS, timeout=timeout)
        return _result(r)


def _result(r):
    for line in reversed((r.stdout or "").splitlines()):
        if line.startswith("RESULT "):
            return json.loads(line[7:])
    return {"error": (r.stderr or r.stdout or "")[-2000:]}


# ------------------------------------------------------------------ Argo
ARGO_SCRIPT = r'''
import glob, json, os, sys, time, types
mod = types.ModuleType("stage_body"); exec(compile(BODY_SRC, "stage_body", "exec"), mod.__dict__)
work = int(sys.argv[1]); outs = json.loads(sys.argv[2])
inputs = [open(p, "rb").read() for p in sorted(glob.glob("/in/*"))]
t0, t1, res = mod.stage(work, outs, *inputs)
os.makedirs("/out", exist_ok=True)
for c, b in res.items():
    open(f"/out/to-{c}", "wb").write(b)
if not res:
    open("/out/done", "wb").write(b"1")
open("/tmp/timing.json", "w").write(json.dumps({"start": t0, "end": t1}))
print("TIMING " + json.dumps({"start": t0, "end": t1}), flush=True)
'''


class ArgoSys:
    def __init__(self, nodes, lane, master):
        self.nodes, self.lane = nodes, lane
        self.gateway = nodes[lane[0]]
        self.cpus = {n: max(1, nodes[n]["cores"] - 1) for n in lane}

    def up(self):
        if not argorun.installed():
            raise RuntimeError("Argo is not installed (E18/argo/install.sh)")
        endpoint = f"{self.gateway['data_ip']}:{S3_PORT}"
        objs = [
            {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": f"{S3_NAME}-cred", "namespace": argorun.NS},
             "stringData": {"accesskey": "e19admin", "secretkey": "e19adminpw"}},
            {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": S3_NAME, "namespace": argorun.NS},
             "spec": {"replicas": 1, "selector": {"matchLabels": {"app": S3_NAME}},
                      "template": {"metadata": {"labels": {"app": S3_NAME}},
                                   "spec": {"nodeName": self.gateway["name"], "hostNetwork": True,
                                            "containers": [{"name": "s3", "image": argorun.S3_IMAGE,
                                                            "command": ["sh", "-c",
                                                                        f"mkdir -p /data/e19 && exec rclone serve s3 /data "
                                                                        f"--addr {endpoint} --auth-key e19admin,e19adminpw"],
                                                            "volumeMounts": [{"name": "data", "mountPath": "/data"}]}],
                                            "volumes": [{"name": "data", "emptyDir": {"medium": "Memory",
                                                                                      "sizeLimit": "2Gi"}}]}}}},
            {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": f"{S3_NAME}-artifacts", "namespace": argorun.NS},
             "data": {"s3": f"s3:\n  bucket: e19\n  endpoint: {endpoint}\n"
                            "  insecure: true\n  createBucketIfNotPresent:\n    objectLocking: false\n"
                            "  keyFormat: '{{workflow.name}}/{{pod.name}}'\n"
                            f"  accessKeySecret:\n    name: {S3_NAME}-cred\n    key: accesskey\n"
                            f"  secretKeySecret:\n    name: {S3_NAME}-cred\n    key: secretkey\n"}},
        ]
        for o in objs:
            r = kube.kubectl(["apply", "-f", "-"], stdin=json.dumps(o), ns=argorun.NS)
            if r.returncode != 0:
                raise RuntimeError(f"e19 repository: {r.stderr}")
        kube.kubectl(["rollout", "status", f"deploy/{S3_NAME}", "--timeout=180s"], ns=argorun.NS, timeout=200)

    def down(self):
        kube.kubectl(["delete", f"deploy/{S3_NAME}", "--ignore-not-found"], ns=argorun.NS)

    def workflow(self, tasks):
        script = ARGO_SCRIPT.replace("BODY_SRC", repr(open(BODY).read()))
        host = "kubernetes.io/hostname"
        templates, dag = [], []
        for t in tasks:
            ins = [(p, f"to-{t['name']}") for p in t["deps"]]
            outs = [f"to-{c}" for c in t["outs"]] or ["done"]
            templates.append({
                "name": t["name"],
                "inputs": {"artifacts": [{"name": f"in{i}", "path": f"/in/{i:02d}-{p}"} for i, (p, _) in enumerate(ins)]},
                "outputs": {"artifacts": [{"name": o, "path": f"/out/{o}", "archive": {"none": {}}} for o in outs],
                            "parameters": [{"name": "timing", "valueFrom": {"path": "/tmp/timing.json"}}]},
                "container": {"image": argorun.TASK_IMAGE, "imagePullPolicy": "IfNotPresent",
                              "command": ["python", "-c", script], "args": [str(t["work"]), json.dumps(t["outs"])],
                              "resources": {"requests": {"cpu": str(self.cpus[t["node"]]), "memory": "128Mi"}}},
                "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
                    {"matchExpressions": [{"key": host, "operator": "In", "values": [t["node"]]}]}]}}},
            })
            dag.append({"name": t["name"], "template": t["name"], "dependencies": sorted(t["deps"]),
                        "arguments": {"artifacts": [{"name": f"in{i}", "from": f"{{{{tasks.{p}.outputs.artifacts.{o}}}}}"}
                                                    for i, (p, o) in enumerate(ins)]}})
        return {"apiVersion": "argoproj.io/v1alpha1", "kind": "Workflow",
                "metadata": {"generateName": "e19-", "namespace": argorun.NS},
                "spec": {"entrypoint": "dag", "serviceAccountName": "argo",
                         "artifactRepositoryRef": {"configMap": f"{S3_NAME}-artifacts", "key": "s3"},
                         "podGC": {"strategy": "OnWorkflowCompletion"},
                         "templates": [{"name": "dag", "dag": {"tasks": dag}}] + templates}}

    def run(self, tasks, timeout):
        wf = self.workflow(tasks)
        r = kube.kubectl(["create", "-f", "-", "-o", "jsonpath={.metadata.name}"], stdin=json.dumps(wf), ns=argorun.NS)
        if r.returncode != 0:
            return {"error": r.stderr}
        name = r.stdout.strip()
        t0 = time.time()
        st = {}
        while time.time() - t0 < timeout:
            st = argorun.status(name)
            if st.get("phase") in ("Succeeded", "Failed", "Error"):
                break
            time.sleep(1)
        times = {}
        for n in (st.get("nodes") or {}).values():
            if n.get("type") != "Pod" or n.get("phase") != "Succeeded":
                continue
            for prm in (n.get("outputs") or {}).get("parameters") or []:
                if prm.get("name") == "timing" and prm.get("value"):
                    v = json.loads(prm["value"])
                    times[n["displayName"].split("(")[0]] = (v["start"], v["end"])
        kube.kubectl(["delete", "wf", name, "--wait=false"], ns=argorun.NS)
        # The repository is a memory-backed volume: drop this run's artifacts.
        kube.kubectl(["exec", f"deploy/{S3_NAME}", "--", "rm", "-rf", f"/data/e19/{name}"], ns=argorun.NS, timeout=60)
        failed = {n.get("displayName"): n.get("message") for n in (st.get("nodes") or {}).values()
                  if n.get("phase") in ("Failed", "Error")}
        out = {"system": "argo", "workflow": name, "phase": st.get("phase"), "submitted": t0,
               "message": st.get("message"), "failedNodes": failed,
               "tasks": {k: {"start": s, "end": e} for k, (s, e) in times.items()}}
        if st.get("phase") == "Succeeded" and len(times) == len(tasks):
            out["makespan"] = makespan_of(times)
        return out


# ------------------------------------------------------------------ main
def unique_m0(paths):
    """One scenario file per (structure, seed, CCR): m0 does not depend on the
    disruption generator."""
    seen, out = set(), []
    for p in sorted(paths):
        d = json.load(open(p))
        k = (d["structure"], d["seed"], d["ccr"])
        if k not in seen:
            seen.add(k)
            out.append(p)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lane", required=True)
    ap.add_argument("--scenarios", default=os.path.expanduser("~/testbed/e17/scenarios-t2"))
    ap.add_argument("--systems", default=",".join(SYSTEMS))
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--T", type=float, default=60.0)
    ap.add_argument("--limit", type=int, default=0, help="first N scenarios only (pilot)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--resume", action="store_true",
                    help="--out is an existing results directory: run only the cells it has no record for")
    ap.add_argument("--seed", type=int, default=19)
    a = ap.parse_args()
    password = sys.stdin.readline().rstrip("\n")
    lane = a.lane.split(",")
    paths = unique_m0([os.path.join(a.scenarios, f) for f in os.listdir(a.scenarios) if f.endswith(".json")])
    if a.limit:
        paths = paths[:a.limit]
    systems = a.systems.split(",")
    allnodes = lanes.discover()
    master = allnodes[lanes.MASTER]
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")
    out = a.out if a.resume else os.path.join(a.out, stamp)
    os.makedirs(out, exist_ok=True)
    tier2 = json.load(open(paths[0]))["tier2"]
    runner = e17.Runner(lane, out, a.T, password, tier2=tier2)
    goodput = runner.goodput
    json.dump({"lane": lane, "systems": systems, "reps": a.reps, "T": a.T, "scenarios": [os.path.basename(p) for p in paths],
               "controller": kube.wayline_deployment().get("controller", {}).get("image"),
               "images": {"ray": images.ref("wl-ray-baseline"), "dask": daskrun.IMAGE, "argo": argorun.TASK_IMAGE},
               "args": vars(a)}, open(os.path.join(out, f"manifest-resume-{stamp}.json" if a.resume else "manifest.json"), "w"),
              indent=1)
    log(f"E19: {len(paths)} scenarios x {systems} x {a.reps} reps on {lane}; results in {out}")
    impls = {"ray": RaySys, "dask": DaskSys, "argo": ArgoSys}
    try:
        for rep in range(1, a.reps + 1):
            rng = random.Random(f"{a.seed}-{rep}")
            order = systems[:]
            rng.shuffle(order)
            for sysname in order:
                impl = None
                if sysname not in ("wayline", "wayline-pull"):
                    impl = impls[sysname](allnodes, lane, master)
                    log(f"rep {rep}: {sysname} up")
                    impl.up()
                sc_order = paths[:]
                rng.shuffle(sc_order)
                if rep == 1 and impl is not None:
                    # One discarded run per system: image pulls and first-use
                    # costs (Argo's executor and task images on the Pis, Ray and
                    # Dask worker imports) are not part of any measured run.
                    try:
                        sc = e17.Scenario(sc_order[0])
                        runner.lane.reset()
                        w = impl.run(dag_tasks(sc, a.T / sc.doc["m0"], goodput, lane, allnodes), timeout=max(15 * a.T, 900))
                        log(f"rep 1 {sysname}: warm-up run {w.get('makespan')}")
                    except Exception as e:  # noqa: BLE001
                        log(f"rep 1 {sysname}: warm-up failed: {e}")
                for p in sc_order:
                    fn = os.path.join(out, f"{os.path.basename(p)[:-5]}-{sysname}-r{rep}.json")
                    if os.path.exists(fn):
                        continue
                    try:
                        sc = e17.Scenario(p)
                        S = a.T / sc.doc["m0"]
                        runner.lane.reset()
                        if sysname in ("wayline", "wayline-pull"):
                            runner.data_plane = "pull" if sysname == "wayline-pull" else None
                            rec = runner.run(sc, "m0", "m0", 0.0, sc.doc["plan"], S, S, None)
                            res = {"system": sysname, "makespan": rec.get("final", {}).get("makespan"),
                                   "status": rec.get("status"), "e17": {k: rec.get(k) for k in ("t0", "start_lag", "final")}}
                        else:
                            tasks = dag_tasks(sc, S, goodput, lane, allnodes)
                            res = impl.run(tasks, timeout=max(15 * a.T, 600))
                        res.update({"scenario": sc.doc["id"], "rep": rep, "predicted": a.T, "S": S,
                                    "m0_sim": sc.doc["m0"], "lane": lane})
                        json.dump(res, open(fn, "w"))
                        ms = res.get("makespan")
                        log(f"rep {rep} {sysname:7s} {sc.doc['id']}: makespan {ms if ms is None else round(ms, 2)} "
                            f"vs predicted {a.T} ({'' if ms is None else f'{ms / a.T - 1:+.1%}'}) {res.get('error', '')[:200]}")
                    except Exception as e:  # noqa: BLE001 - one scenario, not the campaign
                        log(f"rep {rep} {sysname} {os.path.basename(p)}: {type(e).__name__}: {e}")
                        traceback.print_exc()
                    time.sleep(3)
                if impl is not None:
                    impl.down()
                    log(f"rep {rep}: {sysname} down")
    finally:
        runner.close()


if __name__ == "__main__":
    main()
