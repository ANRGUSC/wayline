#!/usr/bin/env python3
"""E23: cold (a pod per task) against warm (a call on a long-lived runner) on the heterogeneous cluster.

  e23.py runners                 print the warm-runner DaemonSets E23 needs (hostname-pinned); apply with kubectl
  e23.py overhead --reps 4       per-task dispatch cost, cold and warm, on each node class: a 5-task chain of E11's
                                 kernel pinned to one node, 1 s of work per task at that node's speed
  e23.py focal --reps 5          FOCAL in its three shapes (all pods, mixed, all warm): sources and FFTs on two
                                 Pi 3 sensor nodes, encoders and the fusion head on two UP boards; the all-pods
                                 runs have no warm runners on the nodes (each repetition removes and restores them)
  e23.py gdtm --reps 5           GDTM in its three shapes on four UP boards (its model stack is amd64-only)
  --smoke                        one run per cell

Every run is one JSON file in --out. A task's dispatch cost is what its time adds beyond its own work: on the
chain, (close of task i) - (close of task i-1) - (its compute, input read and output write), all on one node's
clock. The pipelines' outputs are compared across shapes (FOCAL's logits, GDTM's detections and track): the three
shapes must compute the same bytes.
"""
import argparse
import json
import os
import sys
import time
import urllib.request
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "E18"))
import kube  # noqa: E402
import images  # noqa: E402
import workloads  # noqa: E402
from hostlock import HostLock  # noqa: E402

NS = kube.NS
RATE = 2.568e6                                              # E11 hashes/s on an UP board
SHA_SPEED = {"up": 1.0, "rpi4": 0.1351, "rpi3": 0.0572}     # E11 kernel speed per node class (E22's calibration)
OVERHEAD_NODES = {"up": ["anrg-1", "anrg-2", "anrg-3"], "rpi4": ["rpi4-1", "rpi4-2", "rpi4-3"],
                  "rpi3": ["rpi5", "rpi6", "rpi8"]}
FOCAL_UP, FOCAL_SENSORS = ["anrg-4", "anrg-5"], ["rpi9", "rpi10"]
GDTM_NODES, GDTM_SENSORS = ["anrg-6", "anrg-7", "anrg-8", "anrg-9"], ["anrg-6", "anrg-7", "anrg-8"]
SHAPES = ["pods", "mixed", "warm"]
CLASS_OF = {n: c for c, ns in OVERHEAD_NODES.items() for n in ns}
CLASS_OF.update({n: "up" for n in FOCAL_UP + GDTM_NODES}, **{n: "rpi3" for n in FOCAL_SENSORS})


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def ts(x):
    if not x:
        return None
    import re
    return datetime.fromisoformat(re.sub(r"(\.\d{6})\d+", r"\1", x.replace("Z", "+00:00"))).timestamp()


# ------------------------------------------------------------------ warm runners
CORES = {"up": 8, "rpi4": 4, "rpi3": 4}
RUNNERS = {  # runner -> (image, script, preload, env, nodes)
    "focal": (images.ref("wl-focal"), "/app/stage.py", "torch,numpy,focal_stages", {"FOCAL_PRELOAD": "1"},
              FOCAL_UP + FOCAL_SENSORS),
    "gdtm": (images.ref("wl-gdtm"), "/app/stage.py", "numpy,cv2,h5py,torch,gdtm_glue,tracker", {}, GDTM_NODES),
    "gdtm-model": (images.ref("wl-gdtm-model"), "/app/stage.py", "numpy,torch,gdtm_model", {"GDTM_PRELOAD": "1"},
                   GDTM_NODES),
}


def runner_manifests():
    """One DaemonSet per (runner, node class), pinned to E23's nodes by hostname (the e11 runners of E18 already
    run on every node)."""
    items = []
    for r, (image, script, preload, env, nodes) in RUNNERS.items():
        for cls in sorted({CLASS_OF[n] for n in nodes}):
            hosts = sorted(n for n in nodes if CLASS_OF[n] == cls)
            labels = {"wl.io/runner": r, "e23.lab/runner": f"{r}-{cls}"}
            cmd = ["python", "-m", "wl.runner", "--script", script, "--preload", preload,
                   "--cpus", f"{CORES[cls] - 0.6:.1f}"]
            items.append({
                "apiVersion": "apps/v1", "kind": "DaemonSet",
                "metadata": {"name": f"wl-runner-e23-{r}-{cls}", "namespace": NS, "labels": labels},
                "spec": {"selector": {"matchLabels": labels},
                         "template": {"metadata": {"labels": labels}, "spec": {
                             "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
                                 "nodeSelectorTerms": [{"matchExpressions": [
                                     {"key": "kubernetes.io/hostname", "operator": "In", "values": hosts}]}]}}},
                             "containers": [{
                                 "name": "runner", "image": image, "imagePullPolicy": "IfNotPresent", "command": cmd,
                                 "env": [{"name": "NODE_NAME", "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}}]
                                 + [{"name": k, "value": v} for k, v in env.items()],
                                 "ports": [{"containerPort": 8090, "name": "invoke"}],
                                 "readinessProbe": {"httpGet": {"path": "/healthz", "port": 8090}, "periodSeconds": 2},
                                 "volumeMounts": [{"name": "o", "mountPath": "/data/wl-outputs"},
                                                  {"name": "s", "mountPath": "/shared/wl-outputs"}]}],
                             "volumes": [{"name": "o", "hostPath": {"path": "/data/wl-outputs", "type": "DirectoryOrCreate"}},
                                         {"name": "s", "hostPath": {"path": "/shared/wl-outputs", "type": "DirectoryOrCreate"}}],
                         }}}})
    return {"apiVersion": "v1", "kind": "List", "items": items}


# ------------------------------------------------------------------ runs
class Ctx:
    def __init__(self, out):
        self.api = kube.Api()
        self.out = out
        os.makedirs(out, exist_ok=True)
        self.seq = 0

    def agent_ips(self):
        return {p["spec"].get("nodeName"): p["status"].get("podIP")
                for p in self.api.pods(NS, selector="app=data-agent") if p.get("status", {}).get("podIP")}

    def http(self, url, timeout=20):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return r.read()
        except Exception as e:  # noqa: BLE001
            return f"ERR {e}".encode()

    def sut(self):
        out = {}
        for app in ("odag-controller", "wl-policy"):
            out[app] = sorted(p["metadata"]["name"] for p in self.api.pods(NS, selector=f"app={app}")
                              if not p["metadata"].get("deletionTimestamp"))
        return out

    def run(self, kind, cell, rep, spec, outputs=(), timeout=900):
        """Submit one run, wait for it, record status, per-task timings and the digests of `outputs`."""
        self.seq += 1
        name = f"e23-{kind}-{cell}-{self.seq}-{int(time.time()) % 100000}".replace("_", "-")[:62]
        obj = {"apiVersion": "wl.io/v1", "kind": "ODAG",
               "metadata": {"name": name, "namespace": NS, "labels": {"wl.io/experiment": "e23"}}, "spec": spec}
        before = self.sut()
        t0 = time.time()
        self.api.req("POST", self.api.ODAGS, obj)
        phase = None
        while time.time() - t0 < timeout:
            phase = (self.api.status(name) or {}).get("phase")
            if phase in ("Succeeded", "Failed"):
                break
            time.sleep(0.5)
        st = self.api.status(name) or {}
        ips = self.agent_ips()
        tasks = {t["name"]: t for t in st.get("tasks") or []}
        timings = {}
        for t, x in tasks.items():
            node = x.get("node")
            raw = self.http(f"http://{ips.get(node)}:8082/timings/{name}/{t}") if node in ips else b""
            try:
                timings[t] = json.loads(raw)
            except Exception:  # noqa: BLE001
                timings[t] = None
        digests = {}
        for obj_name in outputs:
            prod = obj_name.split(".")[0]
            node = (tasks.get(prod) or {}).get("node")
            if node in ips:
                digests[obj_name] = self.http(f"http://{ips[node]}:8082/digest/{name}/{obj_name}").decode().strip()
        starts = [ts(x.get("taskStartTime")) for x in tasks.values()]
        ends = [ts(x.get("taskCloseTime")) for x in tasks.values()]
        starts, ends = [s for s in starts if s], [e for e in ends if e]
        rec = {"kind": kind, "cell": cell, "rep": rep, "run": name, "submitted": t0, "phase": phase,
               "wall": time.time() - t0, "makespan": (max(ends) - min(starts)) if starts and ends else None,
               "status": st, "timings": timings, "digests": digests, "spec": spec,
               "sut": {"before": before, "after": self.sut()}}
        rec["valid"] = phase == "Succeeded" and rec["sut"]["before"] == rec["sut"]["after"]
        json.dump(rec, open(os.path.join(self.out, f"{kind}-{cell}-r{rep}.json"), "w"), indent=1, default=str)
        log(f"  {kind} {cell} r{rep}: {phase} makespan {rec['makespan'] and round(rec['makespan'], 2)} s"
            f"{'' if rec['valid'] else ' (INVALID)'}")
        for n, ip in ips.items():
            if n in (spec.get("nodes") or []):
                try:
                    urllib.request.urlopen(urllib.request.Request(f"http://{ip}:8082/data/{name}", method="DELETE"),
                                           timeout=30)
                except Exception:  # noqa: BLE001
                    pass
        self.api.delete_odag(name)
        return rec


def done(ctx, kind, cell, rep):
    return os.path.exists(os.path.join(ctx.out, f"{kind}-{cell}-r{rep}.json"))


# ------------------------------------------------------------------ overhead: 5-task chain pinned to one node
def chain_spec(node, warm, n=5, runtime=1.0):
    iters = int(runtime * RATE * SHA_SPEED[CLASS_OF[node]])
    tasks = []
    for i in range(1, n + 1):
        t = {"name": f"t{i}", "image": workloads.E11_IMAGE, "command": ["python", "task.py"],
             "dependencies": [f"t{i - 1}"] if i > 1 else [], "runtime": runtime,
             "resources": {"cpu": "1", "memory": "256Mi"}, "constraints": {"nodeNames": [node]},
             "env": [{"name": "E11_ITERS", "value": str(iters)},
                     {"name": "E11_INPUTS", "value": f"t{i - 1}.out" if i > 1 else ""},
                     {"name": "E11_OUTPUTS", "value": "out:100000"}],
             "outputs": [{"name": "out", "dataSize": "0.1MB"}]}
        if i > 1:
            t["inputs"] = [{"producer": f"t{i - 1}", "object": "out"}]
        if warm:
            t["runner"] = "e11"
        tasks.append(t)
    return {"scheduler": "heft", "nodes": [node], "tasks": tasks,
            "profiling": {"enabled": False, "runtimeSource": "manual", "bandwidthSource": "external"},
            "realizationPolicy": {"mode": "none", "repair": False, "revise": False}}


def exp_overhead(ctx, reps):
    for rep in range(1, reps + 1):
        for cls, nodes in OVERHEAD_NODES.items():
            for node in nodes:
                for mode in ("cold", "warm"):
                    cell = f"{cls}-{node}-{mode}"
                    if done(ctx, "overhead", cell, rep):
                        continue
                    ctx.run("overhead", cell, rep, chain_spec(node, mode == "warm"),
                            outputs=[f"t5.out"], timeout=600)


# ------------------------------------------------------------------ FOCAL, GDTM in their three shapes
class Pins:
    def __init__(self, m):
        self.m = m

    def resolve(self, role):
        return self.m[role]


def focal_spec(shape):
    spec = workloads.focal(Pins({"source#1": FOCAL_SENSORS[0], "source#2": FOCAL_SENSORS[1]}), shape)
    spec["nodes"] = FOCAL_UP + FOCAL_SENSORS
    for t in spec["tasks"]:   # preprocessing on the sensor that recorded the data; the model on the UP boards
        if t["name"].startswith("fft-"):
            src = "source-" + t["name"][4:]
            t["constraints"] = next(x for x in spec["tasks"] if x["name"] == src)["constraints"]
        elif not t["name"].startswith("source-"):
            t["constraints"] = {"nodeNames": FOCAL_UP}
        if t["name"].startswith(("source-", "fft-")):   # a Pi 3 allocates 926 MiB; a FOCAL stage uses ~310 MiB
            t.setdefault("resources", {})["memory"] = "512Mi"
    return spec


def gdtm_spec(shape):
    spec = workloads.gdtm(Pins({f"sensor#{k + 1}": s for k, s in enumerate(GDTM_SENSORS)}), shape)
    spec["nodes"] = GDTM_NODES
    return spec


PIPELINE_RUNNERS = {"focal": ("focal",), "gdtm": ("gdtm", "gdtm-model")}


def set_runners(kind, present, timeout=900):
    """The pipeline's warm runners on (mixed, warm) or off (pods: a pods-only deployment has no runners, and on a
    1 GB Pi 3 a resident runner would take memory a cold pod needs). Waits until every runner pod is ready, or gone."""
    items = [m for m in runner_manifests()["items"] if m["metadata"]["labels"]["wl.io/runner"] in PIPELINE_RUNNERS[kind]]
    names = [m["metadata"]["name"] for m in items]
    if present:
        for m in items:
            kube.apply(m)
    else:
        kube.kubectl(["delete", "ds", *names, "--ignore-not-found", "--wait=true"], timeout=timeout)
    t0 = time.time()
    while time.time() - t0 < timeout:
        pods = [p for p in kube.kubectl_json(["get", "pods", "-l", "e23.lab/runner"])["items"]
                if p["metadata"]["labels"].get("wl.io/runner") in PIPELINE_RUNNERS[kind]]
        if present:
            want = sum(len(m["spec"]["template"]["spec"]["affinity"]["nodeAffinity"]
                           ["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"][0]
                           ["matchExpressions"][0]["values"]) for m in items)
            ready = [p for p in pods if not p["metadata"].get("deletionTimestamp") and all(
                c.get("ready") for c in (p.get("status") or {}).get("containerStatuses") or [{}])]
            if len(ready) >= want:
                log(f"  runners {', '.join(names)}: {len(ready)} ready")
                return
        elif not pods:
            log(f"  runners {', '.join(names)}: removed")
            return
        time.sleep(3)
    raise SystemExit(f"runners {names} did not {'come up' if present else 'go away'} in {timeout} s")


def exp_pipeline(ctx, kind, reps):
    mk, outs = {"focal": (focal_spec, ["fuse-head.logits"]),
                "gdtm": (gdtm_spec, ["fuse.det", "track.track"])}[kind]
    for rep in range(1, reps + 1):
        for shape in SHAPES:          # pods first, with the runners removed; then mixed and warm with them
            if done(ctx, kind, shape, rep):
                continue
            set_runners(kind, shape != "pods")
            ctx.run(kind, shape, rep, mk(shape), outputs=outs)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("exp", choices=["runners", "overhead", "focal", "gdtm"])
    ap.add_argument("--reps", type=int, default=4)
    ap.add_argument("--out", default=os.path.expanduser("~/E23-results"))
    ap.add_argument("--smoke", action="store_true")
    a = ap.parse_args()
    if a.exp == "runners":
        print(json.dumps(runner_manifests(), indent=1))
        return
    lock = HostLock("e23")
    lock.acquire()
    try:
        ctx = Ctx(os.path.join(a.out, a.exp + ("-smoke" if a.smoke else "")))
        reps = 1 if a.smoke else a.reps
        if a.exp == "overhead":
            exp_overhead(ctx, reps)
        else:
            exp_pipeline(ctx, a.exp, reps)
        log("done")
    finally:
        lock.release()


if __name__ == "__main__":
    main()
