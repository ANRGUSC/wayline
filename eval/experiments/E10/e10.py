#!/usr/bin/env python3
"""E10 orchestrator: the same DAG on Wayline and on Ray. Runs ON anrg-2.

Arms (one run each by default):
  wayline      E5's template, scheduler saga/heft, restricted to --nodes
  ray-plan     Ray, every task pinned to the node Wayline's HEFT chose
  ray-default  Ray, its own scheduling

The Wayline arm runs first; its realized placement and HEFT order become the
plan for ray-plan, so the two differ only in the runtime and data plane.

Env: RES (default ~/E10-results).
Usage: python3 e10.py [--nodes anrg-1 anrg-3 anrg-4 anrg-5 anrg-9] [--reps 1] [--keep-ray]
"""
import argparse
import csv
import importlib.util
import json
import os
import subprocess
import time
from datetime import datetime, timezone

NS = "wl-system"
HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.environ.get("RES", os.path.expanduser("~/E10-results"))
WAYLINE = os.path.expanduser("~/wayline/bin/wayline")
TEMPLATE = "e10-wayline"


def sh(cmd, timeout=300, stdin=None):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout, input=stdin)


def kubectl(a, timeout=120, stdin=None):
    return sh(f"kubectl -n {NS} {a}", timeout=timeout, stdin=stdin)


def load_gen_e5():
    spec = importlib.util.spec_from_file_location("gen_e5", os.path.join(HERE, "..", "E5", "gen_e5.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def template_yaml(nodes, cpu="5", enact="serial"):
    """E5's direct template, every task restricted to the given nodes."""
    g = load_gen_e5()
    y = g.direct(TEMPLATE, "saga/heft").replace('cpu: "5"', f'cpu: "{cpu}"').replace(
        "enactOrder: serial", f"enactOrder: {enact}")
    pin = f"    constraints:\n      nodeNames: [{', '.join(nodes)}]\n"
    out = []
    for block in y.split("\n  - name: "):
        out.append(block if block.startswith("apiVersion") else block.rstrip("\n") + "\n" + pin.rstrip("\n"))
    return "\n  - name: ".join(out) + "\n"


# ─── Wayline arm ─────────────────────────────────────────────────────────────

def agent_ips():
    raw = kubectl("get pods -l app=data-agent -o jsonpath="
                  "'{range .items[*]}{.spec.nodeName}={.status.podIP} {end}'").stdout
    return dict(t.split("=", 1) for t in raw.split() if "=" in t)


def task_timings(run, placement):
    """SDK phase boundaries per task, from the agent on the task's node."""
    import urllib.request
    ips, out = agent_ips(), {}
    for t, node in placement.items():
        try:
            with urllib.request.urlopen(f"http://{ips[node]}:8082/timings/{run}/{t}", timeout=5) as r:
                out[t] = json.loads(r.read().decode())
        except Exception:
            out[t] = {}
    return out


def wayline_run():
    t0 = time.time()
    r = sh(f"{WAYLINE} run {TEMPLATE} -n {NS}")
    run = r.stdout.split("Created run ")[1].split()[0]
    phase = ""
    while time.time() - t0 < 600:
        phase = kubectl(f"get odag {run} -o jsonpath='{{.status.phase}}'").stdout.strip()
        if phase in ("Succeeded", "Failed"):
            break
        time.sleep(0.25)
    wall = time.time() - t0
    o = json.loads(kubectl(f"get odag {run} -o json").stdout)
    st = o.get("status", {})
    placement = {t["name"]: t.get("node") for t in st.get("tasks", [])}
    pred = sorted(st.get("predictedTasks", []) or [], key=lambda p: p["estStart"])
    order = [p["name"] for p in pred] or list(placement)
    tim = task_timings(run, placement)
    return {"arm": "wayline", "run": run, "phase": phase, "wall": round(wall, 3),
            "makespan": st.get("makespan"), "placement": placement, "order": order,
            "timings": tim, "submitted": t0,
            "predicted_makespan": round(max((p["estEnd"] for p in pred), default=0), 3)}


# ─── Ray arms ────────────────────────────────────────────────────────────────

def ray_up(nodes):
    if "Running" in kubectl("get pod e10-ray-head -o jsonpath='{.status.phase}'").stdout:
        return
    worker = open(os.path.join(HERE, "ray", "worker.tmpl")).read()
    workers = "\n---\n".join(worker.replace("@@NODE@@", n) for n in nodes)
    y = open(os.path.join(HERE, "ray", "ray-cluster.yml.tmpl")).read().replace("@@WORKERS@@", workers)
    kubectl("apply -f -", stdin=y)
    for _ in range(120):
        out = kubectl("exec e10-ray-head -- ray status", timeout=60).stdout
        if out.count("node_") >= len(nodes) + 1:
            return
        time.sleep(5)
    raise RuntimeError("ray cluster did not come up")


def ray_down():
    kubectl("delete pod -l app=e10-ray --grace-period=5")


def ray_run(mode, plan_path=None):
    args = f"--mode {mode}"
    if plan_path:
        kubectl(f"cp {plan_path} e10-ray-head:/tmp/plan.json")
        args += " --plan /tmp/plan.json"
    t0 = time.time()
    r = kubectl(f"exec e10-ray-head -- python /e10/e10_ray.py {args}", timeout=900)
    wall = time.time() - t0
    line = [l for l in r.stdout.splitlines() if l.startswith("{")]
    if not line:
        return {"arm": f"ray-{mode}", "phase": "Failed", "error": (r.stderr or r.stdout)[-800:]}
    res = json.loads(line[-1])
    return {"arm": f"ray-{mode}", "phase": "Succeeded", "wall_incl_init": round(wall, 3),
            "makespan": res["makespan"], "placement": res["placement"], "tasks": res["tasks"]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", nargs="+", default=["anrg-1", "anrg-3", "anrg-4", "anrg-5", "anrg-9"])
    ap.add_argument("--reps", type=int, default=1)
    ap.add_argument("--keep-ray", action="store_true")
    ap.add_argument("--cpu", default="5", help="per-task CPU request for the Wayline arm")
    ap.add_argument("--enact", default="serial", help="Wayline enactOrder: serial|order|none")
    ap.add_argument("--arms", nargs="+", default=["wayline", "ray-plan", "ray-default"])
    args = ap.parse_args()

    out = os.path.join(RES, datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    os.makedirs(out)
    kubectl("apply -f -", stdin=template_yaml(args.nodes, args.cpu, args.enact))
    if any(a.startswith("ray") for a in args.arms):
        ray_up(args.nodes)

    results = []
    for rep in range(args.reps):
        w = wayline_run()
        results.append(w)
        print(f"[{rep}] wayline: {w['phase']} wall={w['wall']}s makespan={w['makespan']}s "
              f"(HEFT estimate {w['predicted_makespan']}s) placement={w['placement']}", flush=True)
        plan_path = os.path.join(out, f"plan-{rep}.json")
        json.dump({"placement": w["placement"], "order": w["order"]}, open(plan_path, "w"))
        for mode, p in (("plan", plan_path), ("default", None)):
            if f"ray-{mode}" not in args.arms:
                continue
            r = ray_run(mode, p)
            r["rep"] = rep
            results.append(r)
            print(f"[{rep}] ray-{mode}: {r['phase']} makespan={r.get('makespan')}s "
                  f"placement={r.get('placement')}", flush=True)
        json.dump(results, open(os.path.join(out, "results.json"), "w"), indent=1)
        with open(os.path.join(out, "summary.csv"), "w", newline="") as f:
            wr = csv.writer(f)
            wr.writerow(["rep", "arm", "phase", "makespan", "wall"])
            for x in results:
                wr.writerow([x.get("rep", rep), x["arm"], x["phase"], x.get("makespan"),
                             x.get("wall", x.get("wall_incl_init"))])
        sh(f"{WAYLINE} delete {w['run']} -n {NS}")
    if not args.keep_ray and any(a.startswith("ray") for a in args.arms):
        ray_down()
    print("results in", out)


if __name__ == "__main__":
    main()
