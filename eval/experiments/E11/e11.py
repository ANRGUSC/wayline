#!/usr/bin/env python3
"""E11 orchestrator: real CPU heterogeneity, 80% constraints. Runs ON anrg-2.

1. Cap each worker's CPU clock (scaling_max_freq over ssh, needs SUDO_PASS).
2. Calibrate every node's SHA-256 rate with a bench pod of the task image.
3. Generate the seeded DAG; runtimeProfile = iterations / calibrated rate.
4. Wayline arm per scheduler (uniform clean-network bandwidth profile).
5. Ray arms: each Wayline placement pinned onto Ray, then Ray's default
   restricted to the same allowed-node sets.
6. Restore clocks, bandwidth profile, and tear Ray down, whatever happens.

Env: RES (default ~/E11-results), SUDO_PASS (never written anywhere).
Usage: SUDO_PASS=... python3 e11.py [--reps 1] [--schedulers saga/heft ...]
"""
import argparse
import csv
import importlib.util
import json
import os
import re
import shlex
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("e10", os.path.join(HERE, "..", "E10", "e10.py"))
e10 = importlib.util.module_from_spec(spec); spec.loader.exec_module(e10)
spec = importlib.util.spec_from_file_location("gen", os.path.join(HERE, "gen_e11.py"))
gen = importlib.util.module_from_spec(spec); spec.loader.exec_module(gen)
sh, kubectl, NS, WAYLINE = e10.sh, e10.kubectl, e10.NS, e10.WAYLINE

RES = os.environ.get("RES", os.path.expanduser("~/E11-results"))
NODES = ["anrg-1", "anrg-3", "anrg-4", "anrg-5", "anrg-6", "anrg-7", "anrg-8", "anrg-9"]
FULL = 3800000
# Three clock classes, mixed across the old edge/compute groups.
CAPS = {"anrg-1": FULL, "anrg-6": FULL, "anrg-7": FULL,
        "anrg-3": 1900000, "anrg-5": 1900000, "anrg-8": 1900000,
        "anrg-4": 900000, "anrg-9": 900000}


# ─── clocks ──────────────────────────────────────────────────────────────────

def set_cap(node, khz):
    """Cap every core's scaling_max_freq on `node` (kHz). The sudo password is
    read from the environment on both ends and never appears in argv."""
    inner = (f"for f in /sys/devices/system/cpu/cpu*/cpufreq/scaling_max_freq; do echo {khz} > $f; done")
    remote = (f"read -r P; echo \"$P\" | sudo -S sh -c {shlex.quote(inner)} 2>/dev/null; "
              "cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_max_freq")
    r = subprocess.run(["sshpass", "-e", "ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10",
                        f"anrg@{node}.lan", remote], input=os.environ["SUDO_PASS"] + "\n",
                       env={**os.environ, "SSHPASS": os.environ["SUDO_PASS"]},
                       capture_output=True, text=True, timeout=60)
    got = r.stdout.strip()
    if got != str(khz):
        raise RuntimeError(f"{node}: cap {khz} not applied (got {got!r} {r.stderr[-200:]})")
    return int(got)


def set_caps(caps):
    with ThreadPoolExecutor(len(caps)) as ex:
        return dict(zip(caps, ex.map(lambda kv: set_cap(*kv), caps.items())))


# ─── calibration ─────────────────────────────────────────────────────────────

def calibrate(nodes, secs=4):
    pods = []
    for n in nodes:
        pods.append(f"""apiVersion: v1
kind: Pod
metadata: {{name: e11-bench-{n}, namespace: {NS}, labels: {{app: e11-bench}}}}
spec:
  nodeName: {n}
  restartPolicy: Never
  containers:
  - name: b
    image: {gen.REG}
    command: [python, task.py, --bench, "{secs}"]
    env: [{{name: NODE_NAME, value: {n}}}]""")
    kubectl("delete pod -l app=e11-bench --wait=true")
    kubectl("apply -f -", stdin="\n---\n".join(pods))
    rates = {}
    for _ in range(120):
        for n in nodes:
            if n not in rates:
                m = re.search(r"RATE \S+ ([\d.]+)", kubectl(f"logs e11-bench-{n}").stdout)
                if m:
                    rates[n] = float(m.group(1))
        if len(rates) == len(nodes):
            break
        time.sleep(2)
    kubectl("delete pod -l app=e11-bench --wait=false")
    if len(rates) != len(nodes):
        raise RuntimeError(f"calibration incomplete: {rates}")
    return rates


# ─── bandwidth profile ───────────────────────────────────────────────────────

def cm_data():
    return json.loads(kubectl("get cm wl-network-profile -o json").stdout).get("data", {})


def cm_restore(data):
    y = {"apiVersion": "v1", "kind": "ConfigMap",
         "metadata": {"name": "wl-network-profile", "namespace": NS}, "data": data}
    kubectl("delete cm wl-network-profile")
    kubectl("create -f -", stdin=json.dumps(y))


# ─── Wayline arm ─────────────────────────────────────────────────────────────

def wayline_run(template, sched):
    t0 = time.time()
    r = sh(f"{WAYLINE} run {template} -n {NS}")
    run = r.stdout.split("Created run ")[1].split()[0]
    phase = ""
    while time.time() - t0 < 1200:
        phase = kubectl(f"get odag {run} -o jsonpath='{{.status.phase}}'").stdout.strip()
        if phase in ("Succeeded", "Failed"):
            break
        time.sleep(0.25)
    wall = time.time() - t0
    st = json.loads(kubectl(f"get odag {run} -o json").stdout).get("status", {})
    placement = {t["name"]: t.get("node") for t in st.get("tasks", [])}
    pred = sorted(st.get("predictedTasks", []) or [], key=lambda p: p["estStart"])
    tim = e10.task_timings(run, placement)
    closes = [x["closeUnix"] for x in tim.values() if x]
    return {"arm": f"wayline:{sched}", "sched": sched, "run": run, "phase": phase,
            "wall": round(wall, 3), "makespan_status": st.get("makespan"),
            "makespan": round(max(closes) - t0, 3) if len(closes) == len(placement) else None,
            "placement": placement, "order": [p["name"] for p in pred],
            "predicted_makespan": round(max((p["estEnd"] for p in pred), default=0), 3) or None,
            "timings": tim, "submitted": t0, "scheduling": st.get("scheduling")}


# ─── Ray arms ────────────────────────────────────────────────────────────────

def ray_up(nodes):
    worker = open(os.path.join(HERE, "ray", "worker.tmpl")).read()
    workers = "\n---\n".join(worker.replace("@@NODE@@", n) for n in nodes)
    y = open(os.path.join(HERE, "ray", "ray-cluster.yml.tmpl")).read().replace("@@WORKERS@@", workers)
    kubectl("apply -f -", stdin=y)
    for _ in range(120):
        if kubectl("exec e11-ray-head -- ray status", timeout=60).stdout.count("node_") >= len(nodes) + 1:
            return
        time.sleep(5)
    raise RuntimeError("ray cluster did not come up")


def ray_down():
    kubectl("delete pod -l app=e11-ray --grace-period=5")


def ray_run(label, mode, cpus, plan_path=None):
    a = f"--dag /tmp/dag.json --rates /tmp/rates.json --mode {mode} --cpus {cpus}"
    if plan_path:
        kubectl(f"cp {plan_path} e11-ray-head:/tmp/plan.json")
        a += " --plan /tmp/plan.json"
    r = kubectl(f"exec e11-ray-head -- python /tmp/e11_ray.py {a}", timeout=1800)
    line = [l for l in r.stdout.splitlines() if l.startswith("{")]
    if not line:
        return {"arm": label, "phase": "Failed", "error": (r.stderr or r.stdout)[-800:]}
    res = json.loads(line[-1])
    return {"arm": label, "phase": "Succeeded", "makespan": res["makespan"],
            "placement": res["placement"], "tasks": res["tasks"], "submitted": res["submitted"]}


# ─── main ────────────────────────────────────────────────────────────────────

def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=1)
    ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--frac", type=float, default=0.8)
    ap.add_argument("--cpu", default="2")
    ap.add_argument("--ray-cpus", default=None, help="Ray num_cpus per task (default: --cpu); Ray needs whole numbers above 1")
    ap.add_argument("--enact", default="order")
    ap.add_argument("--schedulers", nargs="+",
                    default=["saga/heft", "saga/cpop", "saga/peft", "saga/minmin", "random"])
    ap.add_argument("--default-reps", type=int, default=1, help="ray-default samples per repetition")
    ap.add_argument("--modes", nargs="+", default=["cold"], help="cold (a pod per task) and/or warm (runner)")
    ap.add_argument("--runner", default="e12", help="runner name for warm mode")
    ap.add_argument("--image", default=gen.REG)
    ap.add_argument("--slots", default="", help="auto: SAGA models node CPU / task CPU processors per node")
    ap.add_argument("--no-caps", action="store_true", help="leave clocks uncapped (homogeneous control)")
    args = ap.parse_args(argv)
    if not args.no_caps and not os.environ.get("SUDO_PASS"):
        raise SystemExit("SUDO_PASS is required to cap clocks")

    out = os.path.join(RES, datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    os.makedirs(out)
    saved_cm = cm_data()
    results = []

    def dump():
        json.dump(results, open(os.path.join(out, "results.json"), "w"), indent=1)
        with open(os.path.join(out, "summary.csv"), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["rep", "arm", "phase", "makespan", "predicted", "wall"])
            for x in results:
                w.writerow([x.get("rep"), x["arm"], x["phase"], x.get("makespan"),
                            x.get("predicted_makespan"), x.get("wall")])

    try:
        caps = {n: FULL for n in NODES} if args.no_caps else CAPS
        print("caps", set_caps(caps) if not args.no_caps else "none", flush=True)
        rates = calibrate(NODES)
        print("rates (Mhash/s)", rates, flush=True)
        d = gen.dag(seed=args.seed, nodes=NODES, frac=args.frac)
        json.dump(d, open(os.path.join(out, "dag.json"), "w"), indent=1)
        json.dump({"caps": caps, "rates": rates, "args": vars(args)},
                  open(os.path.join(out, "setup.json"), "w"), indent=1)
        json.dump(rates, open(os.path.join(out, "rates.json"), "w"))
        kubectl("apply -f -", stdin=gen.bwconfig(NODES))     # clean network: uniform 942 Mbit/s

        ray_up(NODES)
        for f in ("dag.json", "rates.json"):
            kubectl(f"cp {os.path.join(out, f)} e11-ray-head:/tmp/{f}")
        kubectl(f"cp {os.path.join(HERE, 'e11_ray.py')} e11-ray-head:/tmp/e11_ray.py")

        for rep in range(args.reps):
            for sched in args.schedulers:
                short = sched.split("/")[-1].lower()
                for mode in args.modes:
                    warm = mode == "warm"
                    tmpl = f"e11-{short}" + ("-warm" if warm else "")
                    kubectl("apply -f -", stdin=gen.template(tmpl, sched, d, rates, args.cpu, args.enact,
                                                             runner=args.runner if warm else None,
                                                             image=args.image, slots=args.slots))
                    w = wayline_run(tmpl, sched); w["rep"] = rep; w["mode"] = mode
                    w["arm"] = ("wayline-warm:" if warm else "wayline:") + sched
                    results.append(w); dump()
                    sc = w.get("scheduling") or {}
                    if sc.get("used") != sc.get("requested"):
                        print(f"[{rep}] WARNING {w['arm']} fell back to {sc.get('used')}: {sc.get('fallbackError')}", flush=True)
                    print(f"[{rep}] {w['arm']}: {w['phase']} makespan={w['makespan']}s "
                          f"(estimate {w['predicted_makespan']}s, wall {w['wall']}s)", flush=True)
                    sh(f"{WAYLINE} delete {w['run']} -n {NS}")
                    if mode != args.modes[0]:
                        continue
                    plan = os.path.join(out, f"plan-{short}-{rep}.json")
                    json.dump({"placement": w["placement"], "order": w["order"]}, open(plan, "w"))
                    r = ray_run(f"ray-plan:{sched}", "plan", args.ray_cpus or args.cpu, plan); r["rep"] = rep
                    results.append(r); dump()
                    print(f"[{rep}] ray-plan:{sched}: {r['phase']} makespan={r.get('makespan')}s", flush=True)
            for k in range(args.default_reps):
                r = ray_run("ray-default", "default", args.ray_cpus or args.cpu); r["rep"] = rep; r["sample"] = k
                results.append(r); dump()
                print(f"[{rep}] ray-default#{k}: {r['phase']} makespan={r.get('makespan')}s", flush=True)
    finally:
        try:
            ray_down()
        finally:
            cm_restore(saved_cm)
            if not args.no_caps:
                print("restored", set_caps({n: FULL for n in NODES}), flush=True)
    print("results in", out)


if __name__ == "__main__":
    main()
