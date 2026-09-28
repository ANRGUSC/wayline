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
spec = importlib.util.spec_from_file_location("net", os.path.join(HERE, "..", "E13", "net.py"))
net = importlib.util.module_from_spec(spec); spec.loader.exec_module(net)
sh, kubectl, NS, WAYLINE = e10.sh, e10.kubectl, e10.NS, e10.WAYLINE

RES = os.environ.get("RES", os.path.expanduser("~/E11-results"))
NODES = ["anrg-1", "anrg-3", "anrg-4", "anrg-5", "anrg-6", "anrg-7", "anrg-8", "anrg-9"]
FULL = 3800000      # hardware ceiling (single-core turbo); restored afterwards
MIN_FREQ = 800000   # hardware floor; restored afterwards
# Three clock classes, locked (floor = ceiling), mixed across the old
# edge/compute groups. 2.4 GHz is the highest lock whose per-core rate stays
# within 4% from one busy core to all eight, so a task's runtime does not
# depend on how many others share its node; an unlocked 3.8 GHz ceiling let
# the power limit slow each task by up to 1.46x under load.
CAPS = {"anrg-1": 2400000, "anrg-6": 2400000, "anrg-7": 2400000,
        "anrg-3": 1200000, "anrg-5": 1200000, "anrg-8": 1200000,
        "anrg-4": 800000, "anrg-9": 800000}


# ─── clocks ──────────────────────────────────────────────────────────────────

def set_cap(node, khz, floor=None):
    """Lock every core of `node` to `khz` (floor and ceiling both), or with
    floor=MIN_FREQ restore the default range. A ceiling alone is not a lock:
    below it the chip's power limit still moves the real clock with the
    number of busy cores (see docs/limitations-and-future-work/cpu-clock-under-load.md).
    The sudo password is read from the environment and never appears in argv."""
    lo = khz if floor is None else floor
    # Drop the floor first, so the kernel never sees a ceiling below the floor.
    inner = ("for c in /sys/devices/system/cpu/cpu*/cpufreq; do "
             f"echo {MIN_FREQ} > $c/scaling_min_freq; echo {khz} > $c/scaling_max_freq; "
             f"echo {lo} > $c/scaling_min_freq; done")
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


def set_caps(caps, floor=None):
    with ThreadPoolExecutor(len(caps)) as ex:
        return dict(zip(caps, ex.map(lambda kv: set_cap(kv[0], kv[1], floor), caps.items())))


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


def ray_run(label, mode, cpus=None, plan_path=None):
    a = f"--dag /tmp/dag.json --rates /tmp/rates.json --mode {mode}" + (f" --cpus {cpus}" if cpus else "")
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
    ap.add_argument("--cpu", default="mixed",
                    help="CPU request per task: a number, or 'mixed' for a seeded 1, 2 or 3 per task")
    ap.add_argument("--ray-cpus", default=None,
                    help="override Ray num_cpus for every task (default: each task's own request)")
    ap.add_argument("--enact", default="order")
    ap.add_argument("--schedulers", nargs="*",
                    default=["saga/heft", "saga/cpop", "saga/peft", "saga/minmin", "random"])
    ap.add_argument("--default-reps", type=int, default=1, help="ray-default samples per repetition")
    ap.add_argument("--modes", nargs="+", default=["cold"],
                    help="cold (a pod per task), warm (runner) and/or mixed (every other task warm)")
    ap.add_argument("--runner", default="e12", help="runner name for warm mode")
    ap.add_argument("--image", default=gen.REG)
    ap.add_argument("--no-caps", action="store_true", help="leave clocks uncapped (homogeneous control)")
    ap.add_argument("--cpu-classes", choices=["hetero", "uniform"], default="hetero",
                    help="hetero: fast/medium/slow locks; uniform: every node locked at the fast clock")
    ap.add_argument("--net", choices=["none", "classes"], default="none",
                    help="classes: shape worker-to-worker links per E13/net.py (B, B/2, B/4, B/8)")
    ap.add_argument("--size-scale", type=float, default=1.0, help="multiply every edge's size")
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
        caps = ({n: FULL for n in NODES} if args.no_caps else
                CAPS if args.cpu_classes == "hetero" else {n: max(CAPS.values()) for n in NODES})
        print("caps", set_caps(caps) if not args.no_caps else "none", flush=True)
        rates = calibrate(NODES)
        print("rates (Mhash/s)", rates, flush=True)
        d = gen.dag(seed=args.seed, nodes=NODES, frac=args.frac,
                    cpus=(1, 2, 3) if args.cpu == "mixed" else (float(args.cpu),))
        gen.scale_sizes(d, args.size_scale)
        network = {"mode": args.net}
        if args.net == "classes":
            network["classes"] = net.apply()
            probe = net.verify()
            factor = sum(got / want for _, _, want, got in probe) / len(probe)
            network.update(measured=probe, goodput_factor=round(factor, 4))
            print("links", network["classes"], "goodput", round(factor, 3), probe, flush=True)
        json.dump(d, open(os.path.join(out, "dag.json"), "w"), indent=1)
        json.dump({"caps": caps, "rates": rates, "args": vars(args), "network": network},
                  open(os.path.join(out, "setup.json"), "w"), indent=1)
        json.dump(rates, open(os.path.join(out, "rates.json"), "w"))
        if args.net == "classes":
            # Schedulers get each pair's shaped rate at the measured TCP goodput.
            m = {k: b * network["goodput_factor"] for k, b in net.bw_matrix().items()}
            kubectl("apply -f -", stdin=gen.bwmatrix(m, min(m.values())))
        else:
            kubectl("apply -f -", stdin=gen.bwconfig(NODES))     # clean network: uniform 942 Mbit/s

        ray_up(NODES)
        for f in ("dag.json", "rates.json"):
            kubectl(f"cp {os.path.join(out, f)} e11-ray-head:/tmp/{f}")
        kubectl(f"cp {os.path.join(HERE, 'e11_ray.py')} e11-ray-head:/tmp/e11_ray.py")

        for rep in range(args.reps):
            for sched in args.schedulers:
                # Kubernetes names allow [a-z0-9-] only (saga/contention_heft -> contention-heft).
                short = sched.split("/")[-1].lower().replace("_", "-").replace(".", "-")
                for mode in args.modes:
                    warm = mode in ("warm", "mixed")
                    tmpl = f"e11-{short}" + ("" if mode == "cold" else f"-{mode}")
                    warm_set = set(d["order"][1::2]) if mode == "mixed" else None
                    applied = kubectl("apply -f -", stdin=gen.template(tmpl, sched, d, rates, None, args.enact,
                                                             runner=args.runner if warm else None,
                                                             image=args.image, warm_tasks=warm_set))
                    if applied.returncode != 0:
                        raise RuntimeError(f"template {tmpl} rejected: {applied.stderr.strip()}")
                    w = wayline_run(tmpl, sched); w["rep"] = rep; w["mode"] = mode
                    w["arm"] = {"cold": "wayline:", "warm": "wayline-warm:", "mixed": "wayline-mixed:"}[mode] + sched
                    if warm_set is not None:
                        w["warm_tasks"] = sorted(warm_set)
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
                    r = ray_run(f"ray-plan:{sched}", "plan", args.ray_cpus, plan); r["rep"] = rep
                    results.append(r); dump()
                    print(f"[{rep}] ray-plan:{sched}: {r['phase']} makespan={r.get('makespan')}s", flush=True)
            for k in range(args.default_reps):
                r = ray_run("ray-default", "default", args.ray_cpus); r["rep"] = rep; r["sample"] = k
                results.append(r); dump()
                print(f"[{rep}] ray-default#{k}: {r['phase']} makespan={r.get('makespan')}s", flush=True)
    finally:
        # Every step runs whatever the others do: no shaping, lock or
        # profile may outlive the experiment.
        steps = [("ray", ray_down), ("bandwidth profile", lambda: cm_restore(saved_cm))]
        if args.net == "classes":
            steps.append(("links", net.clear))
        if not args.no_caps:
            steps.append(("clocks", lambda: print("restored", set_caps({n: FULL for n in NODES}, floor=MIN_FREQ), flush=True)))
        for label, step in steps:
            try:
                step()
                print(f"cleanup {label}: ok", flush=True)
            except Exception as e:
                print(f"cleanup {label}: FAILED {e}", flush=True)
    print("results in", out)


if __name__ == "__main__":
    main()
