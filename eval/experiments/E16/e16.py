#!/usr/bin/env python3
"""E16 headline pass: plan-driven copies under loss and link degradation.

A deliberately small first pass (one repetition per cell) to reason about
before the full campaign in PLAN.md. Cells:

  cost        gdtm, iot  x  W, W-plan-only, W-static, W-frozen   no fault
  survival    gdtm, iot  x  same arms                            sensor node crashed mid-run
  recovery    chain      x  same arms                            node holding a co-located
                                                                 expensive chain crashed
  adaptation  chain-link x  W, W-frozen, W-plan-only, W-replan   link throttled after planning
  adaptation-copy  fan   x  same four arms                       only the producer-consumer link
                                                                 throttled; another consumer's node
                                                                 holds a copy over a healthy link

Network: E13 link classes (and the matching bandwidth ConfigMap), default
clocks. Each run's status, agent timings and fault timestamps go to
RES/<tag>/<cell>.json; analyze_e16.py summarizes them.

Run on anrg-2 with SUDO_PASS set:
  SUDO_PASS=... python3 e16.py [--only cost,survival] [--tag headline]
"""
import argparse
import importlib.util
import json
import os
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
EVAL = os.path.join(HERE, "..", "..")
sys.path.insert(0, os.path.join(EVAL, "tools"))
sys.path.insert(0, os.path.join(EVAL, "functional"))
sys.path.insert(0, os.path.expanduser("~/wayline-build-vertex/examples/gdtm"))
sys.path.insert(0, os.path.join(EVAL, "..", "examples", "focal-mod"))
import fault  # noqa: E402
import gen_focal  # noqa: E402
import ft_link  # noqa: E402
import ft_recovery as ft  # noqa: E402
import gen_gdtm  # noqa: E402


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


net = load("net", os.path.join(HERE, "..", "E13", "net.py"))
rayrun = load("rayrun", os.path.join(HERE, "ray", "rayrun.py"))
argorun = load("argorun", os.path.join(HERE, "argo", "argorun.py"))
gen_e11 = load("gen_e11", os.path.join(HERE, "..", "E11", "gen_e11.py"))

NS = ft.NS
RES = os.environ.get("RES", os.path.expanduser("~/E16-results"))
ARMS = {
    "W": {"mode": "planAware", "revise": True},
    "W-plan-only": {"mode": "none", "revise": False},
    "W-static": {"mode": "static", "replicas": 2, "revise": False},
    "W-frozen": {"mode": "planAware", "revise": False},
    "W-replan": {"mode": "none", "revise": True},
}
CORE = ["W", "W-plan-only", "W-static", "W-frozen"]


def policy_line(arm):
    return "  realizationPolicy: " + json.dumps(ARMS[arm]).replace('"', "") + "\n"


def with_policy(text, arm):
    """Insert the arm's realizationPolicy into a text template."""
    assert "  scheduler: heft\n" in text
    return text.replace("  scheduler: heft\n", "  scheduler: heft\n" + policy_line(arm), 1)


def strip_policy(text):
    return "\n".join(l for l in text.splitlines() if not l.startswith("  realizationPolicy:")) + "\n"


# ─── workloads ────────────────────────────────────────────────────────────────

def gdtm(name, arm, mode="mixed"):
    return with_policy(gen_gdtm.template(name, mode, 30, 0, "heft"), arm)


def iot(name, arm, warm=False):
    base = json.load(open(os.path.join(HERE, "iot-base.json")))
    if warm:  # every task a call on the iot warm runner (runner/Dockerfile.iot)
        base["tasks"] = [dict(t, runner="iot") for t in base["tasks"]]
    spec = {"description": "E16 iot: 4 pinned cameras, 80-150 MB captures", "scheduler": "heft",
            "realizationPolicy": ARMS[arm],
            "profiling": {"enabled": False, "runtimeSource": "manual", "bandwidthSource": "external"},
            "retention": {"maxRuns": 10, "data": {"policy": "keepLatest", "keepRuns": 1}},
            "defaults": base["defaults"], "tasks": base["tasks"]}
    return json.dumps({"apiVersion": "wl.io/v1", "kind": "ODAGTemplate",
                       "metadata": {"name": name, "namespace": NS}, "spec": spec})


def chain(name, arm, warm=False):
    y = with_policy(strip_policy(ft.template(name, "")), arm)
    if warm:  # both tasks calls on the E11 warm runner (runner/runners.yml)
        y = y.replace("    command: [python, task.py]\n", "    command: [python, task.py]\n    runner: e11\n")
    return y


def chain_link(name, arm):
    return with_policy(ft_link.template(name), arm)


def fan(name, arm):
    """ft_link's p -> c plus a short consumer d of the same object, placed on
    anrg-1: after p finishes, anrg-1 holds a copy of p.emb."""
    d = f"""  - name: d
    image: {ft.IMAGE}
    command: [python, task.py]
    dependencies: [p]
    inputs:
    - producer: p
      object: emb
    runtime: 1
    runtimeProfile: {ft_link.prof({"anrg-1": 1}, 5)}
    resources:
      cpu: "1"
      memory: 512Mi
    env:
    - name: E11_ITERS
      value: "{int(1 * ft.RATE)}"
    - name: E11_INPUTS
      value: "p.emb"
    - name: E11_OUTPUTS
      value: ""
"""
    return with_policy(ft_link.template(name), arm) + d


# ─── faults: (victim, trigger task, delay) resolved per workload ─────────────

SENSOR_MID = {"gdtm": ("anrg-5", "prep-zed-n2", 2.0), "iot": ("anrg-5", "preprocess-4", 1.0)}
CHAIN = ("anrg-7", "p", 3.0)


def cells(only, arms=None):
    out = []
    if "cost" in only:
        out += [("cost", w, a, None) for w in ("gdtm", "iot") for a in (arms or CORE)]
    if "survival" in only:
        out += [("survival", w, a, ("crash",) + SENSOR_MID[w]) for w in ("gdtm", "iot") for a in CORE]
    if "recovery" in only:
        out += [("recovery", "chain", a, ("crash",) + CHAIN) for a in CORE]
    if "adaptation" in only:
        out += [("adaptation", "chain-link", a, ("link", "anrg-7", 4.0))
                for a in ("W", "W-frozen", "W-plan-only", "W-replan")]
    if "focal" in only:
        out += [("cost", "focal", a, None) for a in (arms or ["W", "W-plan-only", "W-static"])]
        out += [("survival", "focal", a, ("crash",) + SENSOR_MID["focal"]) for a in ["W", "W-plan-only"]]
    if "sweep" in only:
        out += [("sweep", f"chain-r{r}-s{mb}", a, ("crash",) + CHAIN)
                for r, mb in SWEEP for a in ["W", "W-plan-only"]]
    if "adaptation-copy" in only:
        out += [("adaptation-copy", "fan", a, ("link-pair", "anrg-7", "anrg-6", 4.0))
                for a in ("W", "W-frozen", "W-plan-only", "W-replan")]
    return out


def focal(name, arm):
    return with_policy(gen_focal.template(name, "mixed", 64, 0, "heft"), arm)


def chain_param(name, arm, runtime, mb):
    """The warm chain with the producer's cost and output size varied: p
    runs `runtime` s and emits `mb` MB; c is unchanged."""
    y = chain(name, arm, warm=True)
    for a, b in ((f'value: "{int(12 * ft.RATE)}"', f'value: "{int(runtime * ft.RATE)}"'),
                 ("    runtime: 12\n", f"    runtime: {runtime}\n"),
                 ("runtimeProfile: " + ft.profile(12, 16), "runtimeProfile: " + ft.profile(runtime, round(runtime * 4 / 3, 1))),
                 ('dataSize: "8MB"', f'dataSize: "{mb}MB"'),
                 ("emb:8000000", f"emb:{mb * 1000000}")):
        assert a in y, a
        y = y.replace(a, b, 1)
    return y


SWEEP = [(r, mb) for r in (2, 12, 30) for mb in (8, 100)]

GEN = {"gdtm": gdtm, "iot": iot, "chain": chain, "chain-link": chain_link, "fan": fan, "focal": focal,
       "iot-warm": lambda n, a: iot(n, a, warm=True), "chain-warm": lambda n, a: chain(n, a, warm=True)}
SENSOR_MID["iot-warm"] = SENSOR_MID["iot"]
GEN["gdtm-warm"] = lambda n, a: gdtm(n, a, mode="warm")  # every stage on the gdtm / gdtm-model runners
SENSOR_MID["gdtm-warm"] = SENSOR_MID["gdtm"]
SENSOR_MID["focal"] = ("anrg-9", "fft-audio", 0.3)  # gen_focal pins source-audio to anrg-9
for _r, _mb in SWEEP:
    GEN[f"chain-r{_r}-s{_mb}"] = (lambda r, mb: lambda n, a: chain_param(n, a, r, mb))(_r, _mb)


def warm_cells(only, arms=None, gdtm_only=False):
    """iot and chain (or GDTM) on Wayline's warm runners, the dispatch Ray uses."""
    arms = arms or ["W", "W-plan-only"]
    if gdtm_only:
        out = [("cost", "gdtm-warm", a, None) for a in arms] if "cost" in only else []
        if "survival" in only:
            out += [("survival", "gdtm-warm", a, ("crash",) + SENSOR_MID["gdtm"]) for a in arms]
        return out
    out = []
    if "cost" in only:
        out += [("cost", "iot-warm", a, None) for a in arms]
    if "survival" in only:
        out += [("survival", "iot-warm", a, ("crash",) + SENSOR_MID["iot"]) for a in arms]
    if "recovery" in only:
        out += [("recovery", "chain-warm", a, ("crash",) + CHAIN) for a in arms]
    return out

# ─── Ray arms ────────────────────────────────────────────────────────────────

RAY_ARMS = ["Ray", "Ray+copies"]


ARGO_ARMS = ["Argo", "Argo+oos"]  # oos: the crashed node is tainted out-of-service once NotReady


GDTM_RAY_IMAGE = "192.168.1.163:5000/wl-gdtm-ray:latest"


def ray_cells(only, arms=RAY_ARMS, gdtm=False):
    if gdtm:
        out = []
        if "cost" in only:
            out += [("cost", "gdtm", a, None) for a in arms]
        if "survival" in only:
            out += [("survival", "gdtm", a, ("crash",) + SENSOR_MID["gdtm"]) for a in arms]
        return out
    out = []
    if "cost" in only:
        out += [("cost", "iot", a, None) for a in arms]
    if "survival" in only:
        out += [("survival", "iot", a, ("crash",) + SENSOR_MID["iot"]) for a in arms]
    if "recovery" in only:
        out += [("recovery", "chain", a, ("crash",) + CHAIN) for a in arms]
    return out


def run_argo_cell(claim, workload, arm, flt, tag):
    plan = ray_plan(workload)
    rec = {"claim": claim, "workload": workload, "arm": arm, "fault": flt, "tag": tag, "plan": plan}
    print(f"[e16] {claim} {workload} {arm}: Argo + S3 repository", flush=True)
    fn = trigger = None
    delay = 0.0
    if flt and flt[0] == "crash":
        _, victim, trigger, delay = flt
        fn = lambda: fault.crash(victim)
    try:
        res = argorun.run(workload, plan, os.path.join(HERE, "iot-base.json"), fn, trigger, delay,
                          out_of_service=(flt[1] if arm == "Argo+oos" and flt and flt[0] == "crash" else None))
    finally:
        if flt and flt[0] == "crash":
            argorun.clear_out_of_service(flt[1])
            fault.restore(flt[1])
    rec.update(res)
    rec["status"] = {"phase": res.get("phase") or "Failed", "message": res.get("message", "")}
    print(f"[e16]   {rec['status']['phase']} wall {res['wall']:.1f}s retries {res.get('retries')}", flush=True)
    return rec


def wayline_result(workload, arm):
    """The latest Wayline result of this workload and arm (for its plan and
    copy decisions)."""
    import glob
    import re
    exact = re.compile(rf"-{re.escape(workload)}-{re.escape(arm)}(-r\d+)?\.json$")
    paths = [p for tag in ("round2", "headline") for p in sorted(glob.glob(os.path.join(RES, tag, "*.json")))
             if exact.search(p)]
    if not paths:
        raise RuntimeError(f"no Wayline {arm} result for {workload}")
    return json.load(open(paths[0]))


def ray_plan(workload):
    st = wayline_result(workload, "W-plan-only")["status"]
    return {t["name"]: t["node"] for t in st["predictedTasks"]}


def ray_copies(workload):
    """The copies W's policy placed for this workload (status.objects entries
    of origin plan), keyed by producing task (by object key for GDTM, whose
    tasks have several named outputs)."""
    st = wayline_result(workload, "W")["status"]
    out = {}
    for o in st.get("objects") or []:
        if o.get("origin") != "plan":
            continue
        nodes = out.setdefault(o["object"] if workload == "gdtm" else o["object"].split(".")[0], [])
        for c in o.get("copies", []):
            if c["node"] not in nodes:
                nodes.append(c["node"])
    if not out:
        raise RuntimeError(f"no plan copies recorded for {workload}")
    return out


def run_ray_cell(claim, workload, arm, flt, tag):
    rayrun.up(fault.WORKERS)
    plan, copies = ray_plan(workload), (ray_copies(workload) if arm == "Ray+copies" else {})
    rec = {"claim": claim, "workload": workload, "arm": arm, "fault": flt, "tag": tag,
           "plan": plan, "copies": copies}
    print(f"[e16] {claim} {workload} {arm}: Ray, copies {copies}", flush=True)
    fn = trigger = None
    delay = 0.0
    if flt and flt[0] == "crash":
        _, victim, trigger, delay = flt
        fn = lambda: fault.crash(victim)
    try:
        res = rayrun.run(workload, plan, copies, os.path.join(HERE, "iot-base.json"), fn, trigger, delay)
    finally:
        if flt and flt[0] == "crash":
            fault.restore(flt[1])
    rec.update(res)
    ok = res.get("phase") == "Succeeded"
    rec["wall"] = res.get("completion") if ok else res.get("wall")
    rec["status"] = {"phase": res.get("phase", "Failed"), "message": json.dumps(res.get("errors", {}))[:300]}
    print(f"[e16]   {rec['status']['phase']} completion {res.get('completion', 0):.1f}s "
          f"errors {list(res.get('errors', {}))}", flush=True)
    return rec


# ─── one run ─────────────────────────────────────────────────────────────────

def agent_ips():
    pods = json.loads(ft.kubectl(["get", "pods", "-l", "app=data-agent", "-o", "json"]).stdout)["items"]
    return {p["spec"]["nodeName"]: p["status"].get("podIP") for p in pods}


def timings(run, placement):
    ips, out = agent_ips(), {}
    for t, n in placement.items():
        try:
            with urllib.request.urlopen(f"http://{ips[n]}:8082/timings/{run}/{t}", timeout=5) as r:
                out[t] = json.loads(r.read().decode())
        except Exception:
            out[t] = {}
    return out


def healthy():
    """Every worker Ready with a running data agent, and every warm runner
    DaemonSet fully ready (a crash kills the victim's runners too)."""
    for n in fault.WORKERS:
        ready = ft.kubectl(["get", "node", n, "-o",
                            "jsonpath={.status.conditions[?(@.type=='Ready')].status}"]).stdout.strip()
        if ready != "True":
            return False
    if len([p for p in agent_ips() if p in fault.WORKERS]) != len(fault.WORKERS):
        return False
    ds = json.loads(ft.kubectl(["get", "ds", "-l", "wl.io/runner", "-o", "json"]).stdout or "{}").get("items", [])
    if not all(d["status"].get("numberReady") == d["status"].get("desiredNumberScheduled") for d in ds):
        return False
    # Ready can be stale after a crash shorter than the node grace (the pod
    # keeps its old address until the kubelet reports): ask every runner.
    pods = json.loads(ft.kubectl(["get", "pods", "-l", "wl.io/runner", "-o", "json"]).stdout or "{}").get("items", [])
    for p in pods:
        ip = p["status"].get("podIP")
        try:
            urllib.request.urlopen(f"http://{ip}:8090/healthz", timeout=2).read()
        except Exception:
            return False
    return True


# Link evidence lives in the controller for 60 s (linkstate.go linkObsTTL).
# A link fault is removed after each run, so evidence left by one run would
# be stale in the next and let it react before any of its own transfers
# ran late: wait it out before every link-fault run.
LINK_COOLDOWN_S = 65
_last_unlink = [0.0]


def run_cell(claim, workload, arm, flt, tag):
    if flt and flt[0] in ("link", "link-pair"):
        wait = LINK_COOLDOWN_S - (time.time() - _last_unlink[0])
        if wait > 0:
            print(f"[e16]   waiting {wait:.0f}s for the previous run's link evidence to expire", flush=True)
            time.sleep(wait)
    tmpl = f"e16-{workload}-{arm.lower()}"
    y = GEN[workload](tmpl, arm)
    r = ft.kubectl(["apply", "-f", "-"], stdin=y)
    assert r.returncode == 0, r.stderr
    time.sleep(1)
    rec = {"claim": claim, "workload": workload, "arm": arm, "policy": ARMS[arm], "fault": flt, "tag": tag}
    t0 = time.time()
    run = ft.start(tmpl)
    rec.update(run=run, submitted=t0)
    print(f"[e16] {claim} {workload} {arm}: run {run}", flush=True)
    try:
        if flt and flt[0] == "link":
            fault.link(flt[1], flt[2])
            rec["faultAt"] = time.time()
        elif flt and flt[0] == "link-pair":
            fault.link_pair(flt[1], flt[2], flt[3])
            rec["faultAt"] = time.time()
        elif flt and flt[0] == "crash":
            _, victim, trigger, delay = flt
            while ft.task_phase(ft.status(run), trigger) != "Succeeded" and time.time() - t0 < 300:
                if ft.status(run).get("phase") in ("Succeeded", "Failed"):
                    break
                time.sleep(0.2)
            rec["triggerAt"] = time.time()
            time.sleep(delay)
            fault.crash(victim)
            rec["faultAt"] = time.time()
        st = ft.wait_done(run, timeout=900)
        rec["doneSeen"] = time.time()
    finally:
        if flt and flt[0] in ("link", "link-pair"):
            fault.unlink(flt[1])
            _last_unlink[0] = time.time()
        elif flt and flt[0] == "crash":
            fault.restore(flt[1])
    rec["wall"] = rec["doneSeen"] - t0
    rec["status"] = st
    placement = {t["name"]: t.get("node") for t in st.get("tasks", []) if t.get("node")}
    rec["timings"] = timings(run, placement)
    print(f"[e16]   {st.get('phase')} wall {rec['wall']:.1f}s makespan {st.get('makespan')} "
          f"copies {st.get('realizationPolicy', {}).get('copies')} recoveries {len(st.get('recovery', []))}",
          flush=True)
    subprocess.run([ft.WAYLINE, "delete", run, "-n", NS], capture_output=True)
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="cost,survival,recovery,adaptation")
    ap.add_argument("--tag", default="headline")
    ap.add_argument("--arms", default="", help="comma-separated arms for the cost cells")
    ap.add_argument("--reps", type=int, default=1)
    ap.add_argument("--ray", action="store_true", help="run the Ray arms instead of the Wayline ones")
    ap.add_argument("--argo", action="store_true", help="run the Argo + MinIO arm instead of the Wayline ones")
    ap.add_argument("--warm", action="store_true", help="iot and chain on Wayline's warm runners")
    ap.add_argument("--gdtm", action="store_true", help="with --ray: GDTM on its Ray image (head on anrg-8)")
    a = ap.parse_args()
    out = os.path.join(RES, a.tag)
    os.makedirs(out, exist_ok=True)

    # Network: E13 link classes and the matching bandwidth model.
    cm_before = json.loads(ft.kubectl(["get", "cm", "wl-network-profile", "-o", "json"]).stdout)["data"]
    json.dump(cm_before, open(os.path.join(out, "configmap-before.json"), "w"), indent=1)
    print("[e16] shaping:", net.apply(), flush=True)
    m = net.bw_matrix()
    ft.kubectl(["apply", "-f", "-"], stdin=gen_e11.bwmatrix(m, min(m.values())))
    try:
        arms = [x for x in a.arms.split(",") if x] or None
        # Repetitions interleave: every cell once, then every cell again.
        if a.argo:
            argorun.minio_up()
        if a.ray and a.gdtm:
            rayrun.configure(GDTM_RAY_IMAGE, head_node="anrg-8")
        pick = (ray_cells(a.only.split(","), gdtm=a.gdtm) if a.ray else ray_cells(a.only.split(","), arms or ["Argo"]) if a.argo
                else warm_cells(a.only.split(","), arms, gdtm_only=a.gdtm) if a.warm else cells(a.only.split(","), arms))
        todo = [(r, c) for r in range(1, a.reps + 1) for c in pick]
        for rep, (claim, workload, arm, flt) in todo:
            name = f"{claim}-{workload}-{arm}" + (f"-r{rep}" if a.reps > 1 else "")
            path = os.path.join(out, name + ".json")
            if os.path.exists(path):
                print(f"[e16] {name}: done already", flush=True)
                continue
            while not healthy():
                print("[e16] waiting for all workers Ready", flush=True)
                time.sleep(5)
            rec = (run_ray_cell if a.ray else run_argo_cell if a.argo else run_cell)(claim, workload, arm, flt, a.tag)
            rec["rep"] = rep
            json.dump(rec, open(path, "w"), indent=1)
    finally:
        if a.ray:
            rayrun.down()
        if a.argo:
            argorun.minio_down()
        net.clear()
        cm = {"apiVersion": "v1", "kind": "ConfigMap",
              "metadata": {"name": "wl-network-profile", "namespace": NS}, "data": cm_before}
        ft.kubectl(["apply", "-f", "-"], stdin=json.dumps(cm))
        print("[e16] shaping cleared, bandwidth ConfigMap restored", flush=True)


if __name__ == "__main__":
    main()
