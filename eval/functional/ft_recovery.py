#!/usr/bin/env python3
"""Functional checks for plan-aware copies and node-loss recovery.

Not an experiment: each scenario runs once and asserts behavior (what moved,
what ran again, which copies were made, that the run succeeds), not timing.

DAG: p (about 12 s, one 8 MB output "emb") -> c (about 20 s). The template's
runtime profiles make HEFT co-locate both on the victim node, so p's output
has a single planned copy there.

Scenarios
  plan-nofault  planAware policy, no fault: the policy copies p.emb to c's
                fallback node, the copy converges, the run succeeds.
  plan-crash    planAware, crash the victim 3 s after p succeeds (c running):
                c restarts on the node holding the copy; p does not run again.
  none-crash    no policy, same crash: p runs again elsewhere (lineage), then c.
  plan-partition  as plan-crash, but the victim is cut off the network while
                its processes keep running; after the partition heals, the
                run's result stands.

Run on anrg-2 with SUDO_PASS set (crash and restore need sudo on the victim):
  SUDO_PASS=... python3 ft_recovery.py plan-nofault plan-crash none-crash
"""
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tools"))
import fault  # noqa: E402

NS = "wl-system"
WAYLINE = os.path.expanduser("~/wayline/bin/wayline")
IMAGE = "192.168.1.163:5000/wl-e11:latest"
RATE = 2.568e6            # hashes/s on a full-clock UP board (E11 calibration)
VICTIM = os.environ.get("FT_VICTIM", "anrg-7")
WORKERS = ["anrg-1", "anrg-3", "anrg-4", "anrg-5", "anrg-6", "anrg-7", "anrg-8", "anrg-9"]


def kubectl(args, stdin=None):
    return subprocess.run(["kubectl", "-n", NS] + args, input=stdin, capture_output=True, text=True)


def profile(fast, slow):
    return "{" + ", ".join(f"{n}: {fast if n == VICTIM else slow}" for n in WORKERS) + "}"


def template(name, policy):
    pol = f"  realizationPolicy: {{mode: {policy}}}\n" if policy else ""
    return f"""apiVersion: wl.io/v1
kind: ODAGTemplate
metadata:
  name: {name}
  namespace: {NS}
spec:
  description: 'functional check: plan-aware copies and node-loss recovery'
  scheduler: heft
{pol}  profiling:
    enabled: false
    runtimeSource: manual
    bandwidthSource: external
  retention:
    maxRuns: 10
    data:
      policy: keepLatest
      keepRuns: 1
  defaults:
    runtime: 4
    dataSize: 1MB
  tasks:
  - name: p
    image: {IMAGE}
    command: [python, task.py]
    dependencies: []
    outputs:
    - name: emb
      dataSize: "8MB"
    runtime: 12
    runtimeProfile: {profile(12, 16)}
    resources:
      cpu: "2"
      memory: 512Mi
    env:
    - name: E11_ITERS
      value: "{int(12 * RATE)}"
    - name: E11_INPUTS
      value: ""
    - name: E11_OUTPUTS
      value: "emb:8000000"
  - name: c
    image: {IMAGE}
    command: [python, task.py]
    dependencies: [p]
    inputs:
    - producer: p
      object: emb
    runtime: 20
    runtimeProfile: {profile(20, 24)}
    resources:
      cpu: "2"
      memory: 512Mi
    env:
    - name: E11_ITERS
      value: "{int(20 * RATE)}"
    - name: E11_INPUTS
      value: "p.emb"
    - name: E11_OUTPUTS
      value: ""
"""


def status(run):
    r = kubectl(["get", "odag", run, "-o", "json"])
    return json.loads(r.stdout).get("status", {}) if r.returncode == 0 else {}


def task_phase(st, task):
    for t in st.get("tasks", []):
        if t.get("name") == task:
            return t.get("phase", "")
    return ""


def start(tmpl):
    r = subprocess.run([WAYLINE, "run", tmpl, "-n", NS], capture_output=True, text=True)
    return r.stdout.split("Created run ")[1].split()[0]


def wait_done(run, timeout=600):
    t0 = time.time()
    while time.time() - t0 < timeout:
        st = status(run)
        if st.get("phase") in ("Succeeded", "Failed"):
            return st
        time.sleep(0.5)
    return status(run)


def check(cond, msg, errors):
    print(("  ok    " if cond else "  FAIL  ") + msg, flush=True)
    if not cond:
        errors.append(msg)


def scenario(name):
    policy = "planAware" if name.startswith("plan") else ""
    tmpl = f"ft-{name}"
    kubectl(["apply", "-f", "-"], stdin=template(tmpl, policy))
    time.sleep(1)
    run = start(tmpl)
    print(f"[ft] {name}: run {run}", flush=True)
    errors = []
    t0 = time.time()
    if name.endswith("crash") or name.endswith("partition"):
        while task_phase(status(run), "p") != "Succeeded":
            if time.time() - t0 > 300:
                break
            time.sleep(0.2)
        time.sleep(3)
        (fault.partition if name.endswith("partition") else fault.crash)(VICTIM)
    st = wait_done(run)
    wall = time.time() - t0
    placed = {t["name"]: t.get("node") for t in st.get("predictedTasks", [])}
    print(f"[ft] {name}: phase {st.get('phase')} after {wall:.1f}s; planned {placed}", flush=True)
    check(st.get("phase") == "Succeeded", "run succeeded", errors)
    check(placed.get("p") == VICTIM and placed.get("c") == VICTIM,
          f"HEFT co-located p and c on {VICTIM}", errors)
    pol = st.get("realizationPolicy", {})
    copies = [d for d in pol.get("decisions", []) if d.get("copy")]
    objects = {o["object"]: o for o in st.get("objects", [])}
    if policy:
        check(len(copies) == 1 and copies[0]["object"] == "p.emb",
              f"policy copied p.emb once ({[(d['object'], d['fallback'], d['reason']) for d in pol.get('decisions', [])]})",
              errors)
    rec = st.get("recovery", [])
    if name == "plan-nofault":
        states = [c["state"] for c in objects.get("p.emb", {}).get("copies", [])]
        check(states == ["Installed"], f"plan copy installed ({states})", errors)
        check(not rec, "no recovery", errors)
    if name.endswith("crash") or name.endswith("partition"):
        check(len(rec) >= 1, f"recovery recorded ({len(rec)})", errors)
        plan = rec[0].get("plan", {}) if rec else {}
        moved = {m["task"]: m["to"] for m in plan.get("moved", [])}
        print(f"[ft] {name}: recovery {json.dumps(rec)}", flush=True)
        check("c" in plan.get("restarted", []), "c restarted", errors)
        if name.startswith("plan"):
            fb = copies[0]["fallback"] if copies else None
            check(moved.get("c") == fb, f"c moved to the copy's node {fb} ({moved})", errors)
            check(not plan.get("rerun"), "p did not run again", errors)
        else:
            check(plan.get("rerun") == ["p"], f"p ran again ({plan.get('rerun')})", errors)
            check(moved.get("p") and moved.get("c") == moved.get("p"),
                  f"p and c moved together ({moved})", errors)
        if name.endswith("partition"):
            fault.heal(VICTIM)
            time.sleep(20)   # the zombie's kubelet reconnects and reconciles
            after = status(run)
            check(after.get("phase") == "Succeeded", f"still Succeeded after heal ({after.get('phase')})", errors)
            check(len(after.get("recovery", [])) == len(rec), "no further recovery after heal", errors)
        else:
            fault.restore(VICTIM)
    return {"scenario": name, "run": run, "phase": st.get("phase"), "wall": round(wall, 1),
            "errors": errors}


if __name__ == "__main__":
    results = [scenario(n) for n in (sys.argv[1:] or ["plan-nofault"])]
    print(json.dumps(results, indent=1))
    sys.exit(1 if any(r["errors"] for r in results) else 0)
