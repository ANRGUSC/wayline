#!/usr/bin/env python3
"""Functional check: a link that degrades after planning.

DAG: p (about 12 s, 30 MB output) -> c (about 20 s). Runtime profiles make
HEFT put p on anrg-7 and c on anrg-6 (a planned 2 s transfer). Right after
the run is planned, anrg-7's links are throttled to 4 Mbit/s, so the
transfer would take about a minute.

  link-degrade  The controller sees the transfer running far past its
                prediction, judges the link degraded, and moves c (not yet
                started) to anrg-7, where its input already is. c does not
                wait for the transfer; the run succeeds.

Run on anrg-2 with SUDO_PASS set:
  SUDO_PASS=... python3 ft_link.py
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tools"))
import fault  # noqa: E402
from ft_recovery import IMAGE, NS, RATE, WORKERS, check, kubectl, start, wait_done  # noqa: E402

SRC, DST = "anrg-7", "anrg-6"


def prof(values, default):
    return "{" + ", ".join(f"{n}: {values.get(n, default)}" for n in WORKERS) + "}"


def template(name):
    return f"""apiVersion: wl.io/v1
kind: ODAGTemplate
metadata:
  name: {name}
  namespace: {NS}
spec:
  description: 'functional check: re-planning around a degraded link'
  scheduler: heft
  profiling:
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
      dataSize: "30MB"
    runtime: 12
    runtimeProfile: {prof({SRC: 12}, 16)}
    resources:
      cpu: "2"
      memory: 512Mi
    env:
    - name: E11_ITERS
      value: "{int(12 * RATE)}"
    - name: E11_INPUTS
      value: ""
    - name: E11_OUTPUTS
      value: "emb:30000000"
  - name: c
    image: {IMAGE}
    command: [python, task.py]
    dependencies: [p]
    inputs:
    - producer: p
      object: emb
    runtime: 20
    runtimeProfile: {prof({DST: 20, SRC: 24}, 30)}
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


def scenario():
    tmpl = "ft-link-degrade"
    kubectl(["apply", "-f", "-"], stdin=template(tmpl))
    time.sleep(1)
    run = start(tmpl)
    print(f"[ft] link-degrade: run {run}", flush=True)
    t0 = time.time()
    errors = []
    try:
        fault.link(SRC, 4)
        st = wait_done(run, timeout=600)
    finally:
        fault.unlink(SRC)
    wall = time.time() - t0
    placed = {t["name"]: t.get("node") for t in st.get("predictedTasks", [])}
    ran = {t["name"]: t.get("node") for t in st.get("tasks", [])}
    rec = st.get("recovery", [])
    print(f"[ft] link-degrade: phase {st.get('phase')} after {wall:.1f}s; planned {placed}; ran {ran}", flush=True)
    print(f"[ft] link-degrade: recovery {json.dumps(rec)}", flush=True)
    check(placed == {"p": SRC, "c": DST}, f"HEFT placed p on {SRC}, c on {DST}", errors)
    check(st.get("phase") == "Succeeded", "run succeeded", errors)
    moved = {m["task"]: m["to"] for r in rec for m in r.get("plan", {}).get("moved", [])}
    check(any(r.get("reason") == "degraded link" for r in rec), "re-planned for a degraded link", errors)
    check(moved.get("c") == SRC and ran.get("c") == SRC, f"c moved to its input on {SRC} ({moved}, ran {ran})",
          errors)
    check(wall < 75, f"did not wait out the ~60 s transfer (wall {wall:.1f}s)", errors)
    return {"scenario": "link-degrade", "run": run, "phase": st.get("phase"), "wall": round(wall, 1),
            "errors": errors}


if __name__ == "__main__":
    r = scenario()
    print(json.dumps(r, indent=1))
    sys.exit(1 if r["errors"] else 0)
