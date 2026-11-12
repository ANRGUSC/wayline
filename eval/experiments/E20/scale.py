#!/usr/bin/env python3
"""E20: control-plane cost and scale on the full testbed.

How much does Wayline itself cost as the cluster and the load grow? Tasks
sleep their planned runtime on warm runners (E17's Tier 1 runner, deployed on
all 65 workers), so everything measured beyond the plan is the runtime's own
overhead: dispatch, handoff, status, transfers.

A cell is (workers, concurrent runs N, DAG size T). Each run is a layered
random DAG of T tasks (fan-in up to 3), each task sleeping r seconds scaled by
its node class (UP 1x, Pi 4 98.2/29.2x, Pi 3 98.2/15.7x of the UP time, the
measured kernel speeds), one output per out-edge of `--edge-kb` KB. HEFT
places every run on the cell's workers (spec.nodes). All N runs are submitted
at once.

Per run it records the predicted makespan, the measured makespan, and per
task the predicted and actual start, so overhead = actual - predicted is
attributable to dispatch (start after the last input arrived) or handoff.
Every few seconds it samples the controller's and the policy process's CPU
and memory (metrics-server).

Run on eclipse:
  python3 scale.py --workers all --n 1 8 32 64 --tasks 20 --reps 3 --out ~/E20-results
"""
import argparse
import json
import random
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

NS = "wl-system"
SPEED = {"up": 98.2, "rpi4": 29.2, "rpi3": 15.7}   # kernel iterations/s (E17 calibration)


def kubectl(args, stdin=None, check=False):
    r = subprocess.run(["k3s", "kubectl", "-n", NS] + args, input=stdin, capture_output=True, text=True)
    if check and r.returncode:
        raise RuntimeError(r.stderr)
    return r


def workers(sel):
    out = json.loads(kubectl(["get", "nodes", "-o", "json"], check=True).stdout)
    nodes = []
    for n in out["items"]:
        name = n["metadata"]["name"]
        cls = n["metadata"].get("labels", {}).get("testbed.lab/class", "")
        if name == "eclipse" or cls not in SPEED:
            continue
        ready = any(c["type"] == "Ready" and c["status"] == "True" for c in n["status"]["conditions"])
        if ready:
            nodes.append((name, cls))
    nodes.sort()
    if sel == "all":
        return nodes
    if sel in SPEED:
        return [x for x in nodes if x[1] == sel]
    want = set(sel.split(","))
    return [x for x in nodes if x[0] in want]


def gen_dag(rng, T, layers):
    """Layered random DAG: T tasks over `layers` layers, each task with 1-3
    parents in the previous layer."""
    per = [T // layers + (1 if i < T % layers else 0) for i in range(layers)]
    names, parents = [], {}
    idx = 0
    prev = []
    for size in per:
        cur = []
        for _ in range(size):
            t = f"t{idx}"
            idx += 1
            parents[t] = rng.sample(prev, min(len(prev), rng.randint(1, 3))) if prev else []
            cur.append(t)
            names.append(t)
        prev = cur
    return names, parents


def odag(name, nodes, rng, T, layers, base_s, edge_kb, policy):
    names, parents = gen_dag(rng, T, layers)
    children = {t: [c for c in names if t in parents[c]] for t in names}
    tasks = []
    for t in names:
        r = round(base_s * rng.uniform(0.5, 1.5), 3)
        # One runtime per task, the same on every node: the suite measures
        # the control plane, and a per-node profile for 48 nodes made each
        # task's spec 1 KB (a 400-task run object reached 1.8 MB).
        spec = {"name": t, "image": "10.10.0.1:5000/wl-runner-e17:milcom1", "command": ["python", "/app/stage.py"],
                "runner": "e17", "runtime": r, "resources": {"cpu": "1", "memory": "64Mi"}}
        outs = [{"name": f"to-{c}", "dataSize": str(edge_kb * 1000)} for c in children[t]]
        if outs:
            spec["outputs"] = outs
            spec["env"] = [{"name": "WL_EDGE_SIZES", "value": ",".join(f"{o['name']}:{o['dataSize']}" for o in outs)}]
        if parents[t]:
            spec["dependencies"] = parents[t]
            spec["inputs"] = [{"producer": p, "object": f"to-{t}"} for p in parents[t]]
        tasks.append(spec)
    s = {"scheduler": "heft", "nodes": [n for n, _ in nodes], "tasks": tasks}
    if policy:
        s["realizationPolicy"] = policy
    return {"apiVersion": "wl.io/v1", "kind": "ODAG",
            "metadata": {"name": name, "namespace": NS, "labels": {"wl.io/experiment": "e20"}}, "spec": s}


def ts(s):
    """RFC 3339 with up to nanoseconds -> unix seconds."""
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    main, _, tz = s.partition("+")
    if "." in main:
        sec, frac = main.split(".", 1)
        main = f"{sec}.{(frac + '000000')[:6]}"
    return datetime.fromisoformat(main + "+" + (tz or "00:00")).timestamp()


class Sampler(threading.Thread):
    """Controller and policy CPU/memory every 5 s (metrics-server)."""

    def __init__(self):
        super().__init__(daemon=True)
        self.samples, self.stop = [], threading.Event()

    def run(self):
        while not self.stop.is_set():
            r = kubectl(["top", "pod", "--containers", "--no-headers"])
            now = time.time()
            for line in r.stdout.splitlines():
                f = line.split()
                if len(f) >= 4 and (f[0].startswith("odag-controller") or f[0].startswith("wl-policy")):
                    cpu = float(f[2].rstrip("m")) / 1000 if f[2].endswith("m") else float(f[2])
                    mem = f[3]
                    mib = float(mem[:-2]) * (1024 if mem.endswith("Gi") else 1) if mem[-2:] in ("Mi", "Gi") else 0
                    self.samples.append({"t": now, "pod": f[0], "container": f[1], "cpu": cpu, "memMiB": mib})
            self.stop.wait(5)


def summarize(obj):
    st = obj.get("status", {})
    pred = {p["name"]: p for p in st.get("predictedTasks", []) or []}
    tasks = []
    for t in st.get("tasks", []) or []:
        tasks.append({"name": t.get("name"), "node": t.get("node"), "phase": t.get("phase"),
                      "start": ts(t.get("taskStartTime") or t.get("startTime")),
                      "close": ts(t.get("taskCloseTime") or t.get("completionTime")),
                      "predStart": pred.get(t.get("name"), {}).get("estStart"),
                      "predEnd": pred.get(t.get("name"), {}).get("estEnd")})
    return {"phase": st.get("phase"), "makespan": st.get("makespan"),
            "predicted": max((p.get("estEnd", 0) for p in pred.values()), default=None),
            "startTime": st.get("startTime"), "completionTime": st.get("completionTime"), "tasks": tasks}


def run_cell(nodes, N, T, layers, base_s, edge_kb, rep, out, policy, timeout, partition=False):
    rng = random.Random(f"{len(nodes)}-{N}-{T}-{rep}")
    stamp = int(time.time()) % 1000000
    names = [f"e20-w{len(nodes)}-n{N}-t{T}-r{rep}-{i}-{stamp}" for i in range(N)]
    if partition:
        # Disjoint slices, each with a mix of classes (deal by class): runs do
        # not compete for nodes, so only the control plane's load grows.
        slices = [[] for _ in range(N)]
        for i, x in enumerate(sorted(nodes, key=lambda x: (x[1], x[0]))):
            slices[i % N].append(x)
        docs = [odag(n, slices[i] or nodes, rng, T, layers, base_s, edge_kb, policy) for i, n in enumerate(names)]
    else:
        docs = [odag(n, nodes, rng, T, layers, base_s, edge_kb, policy) for n in names]
    sampler = Sampler()
    sampler.start()
    t0 = time.time()
    kubectl(["create", "-f", "-"], stdin="\n---\n".join(json.dumps(d) for d in docs), check=True)
    done = {}
    while len(done) < N and time.time() - t0 < timeout:
        time.sleep(3)
        r = kubectl(["get", "odag", "-l", "wl.io/experiment=e20", "-o", "json"])
        for o in json.loads(r.stdout or '{"items": []}')["items"]:
            n = o["metadata"]["name"]
            if n in names and n not in done and o.get("status", {}).get("phase") in ("Succeeded", "Failed"):
                done[n] = summarize(o)
    sampler.stop.set()
    wall = time.time() - t0
    for n in names:
        if n not in done:
            r = kubectl(["get", "odag", n, "-o", "json"])
            done[n] = summarize(json.loads(r.stdout)) if r.returncode == 0 else {"phase": "missing"}
    rec = {"partition": partition, "workers": len(nodes), "classes": sorted({c for _, c in nodes}), "N": N, "T": T, "layers": layers,
           "baseS": base_s, "edgeKB": edge_kb, "rep": rep, "policy": policy, "wall": wall,
           "runs": done, "control": sampler.samples, "submittedUnix": t0}
    path = Path(out) / f"w{len(nodes)}-n{N}-t{T}-{'part' if partition else 'shared'}-r{rep}.json"
    path.write_text(json.dumps(rec))
    ok = sum(1 for r in done.values() if r.get("phase") == "Succeeded")
    ms = sorted(r["makespan"] for r in done.values() if r.get("makespan"))
    pr = sorted(r["predicted"] for r in done.values() if r.get("predicted"))
    print(f"  w{len(nodes)} N={N} T={T} rep {rep}: {ok}/{N} ok, wall {wall:.0f}s, "
          f"makespan median {ms[len(ms)//2] if ms else float('nan'):.1f}s vs predicted "
          f"{pr[len(pr)//2] if pr else float('nan'):.1f}s", flush=True)
    kubectl(["delete", "odag", "-l", "wl.io/experiment=e20", "--wait=false"])
    time.sleep(10)
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", default="all")
    ap.add_argument("--exclude", default="", help="comma-separated nodes to leave out")
    ap.add_argument("--n", type=int, nargs="+", default=[1, 8, 32])
    ap.add_argument("--tasks", type=int, default=20)
    ap.add_argument("--layers", type=int, default=5)
    ap.add_argument("--base-s", type=float, default=2.0)
    ap.add_argument("--edge-kb", type=int, default=100)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--policy", default="", help="json realizationPolicy, e.g. '{\"mode\": \"planAware\"}'")
    ap.add_argument("--timeout", type=float, default=900)
    ap.add_argument("--partition", action="store_true", help="give each run its own slice of the workers")
    ap.add_argument("--out", default=str(Path.home() / "E20-results"))
    a = ap.parse_args()
    nodes = [x for x in workers(a.workers) if x[0] not in set(a.exclude.split(","))]
    out = Path(a.out) / datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")
    out.mkdir(parents=True, exist_ok=True)
    img = kubectl(["get", "deploy", "odag-controller", "-o", "jsonpath={.spec.template.spec.containers[0].image}"]).stdout
    (out / "manifest.json").write_text(json.dumps({"controller": img, "workers": nodes, "args": vars(a)}))
    print(f"E20: {len(nodes)} workers, results in {out}", flush=True)
    policy = json.loads(a.policy) if a.policy else None
    for rep in range(1, a.reps + 1):
        for N in a.n:
            run_cell(nodes, N, a.tasks, a.layers, a.base_s, a.edge_kb, rep, out, policy, a.timeout, a.partition)


if __name__ == "__main__":
    main()
