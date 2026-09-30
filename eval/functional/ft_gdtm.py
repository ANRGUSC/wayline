#!/usr/bin/env python3
"""Functional check: node loss during a GDTM run (mixed warm calls and pods).

HEFT keeps each sensor's chain (source -> prep -> embed) on its sensor node,
and each source is pinned there. Crashing a sensor node while its embeds run
loses the capture's only copy:

  plan-crash  planAware: the policy copied every source's capture off its
              node, so recovery re-runs the lost preps and embeds elsewhere
              from those copies and the run succeeds; no source runs again.
  none-crash  no policy: the capture is gone and its source cannot run
              anywhere else, so the run fails, naming the stuck source.
  none-nofault, plan-nofault
              no fault (regression): the run succeeds without recovery; with
              the policy, every plan copy is installed.

Run on anrg-2 with SUDO_PASS set:
  SUDO_PASS=... python3 ft_gdtm.py plan-crash none-crash
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tools"))
sys.path.insert(0, os.path.expanduser("~/wayline-build-vertex/examples/gdtm"))
import fault  # noqa: E402
import gen_gdtm as G  # noqa: E402
from ft_recovery import NS, check, kubectl, start, status, task_phase, wait_done  # noqa: E402

VICTIM, SENSOR = "anrg-5", "n2"     # gen_gdtm pins source-n2 to anrg-5
TRIGGER = f"prep-zed-{SENSOR}"


def scenario(name):
    tmpl = f"ft-gdtm-{name}"
    y = G.template(tmpl, "mixed", 30, 0, "heft")
    if name.startswith("plan"):
        y = y.replace("  scheduler: heft\n", "  scheduler: heft\n  realizationPolicy: {mode: planAware}\n", 1)
    kubectl(["apply", "-f", "-"], stdin=y)
    time.sleep(1)
    run = start(tmpl)
    print(f"[ft] {name}: run {run}", flush=True)
    t0 = time.time()
    if name.endswith("nofault"):
        st = wait_done(run, timeout=900)
        wall = time.time() - t0
        errors = []
        print(f"[ft] {name}: phase {st.get('phase')} after {wall:.1f}s", flush=True)
        check(st.get("phase") == "Succeeded", "run succeeded", errors)
        check(not st.get("recovery"), "no recovery", errors)
        objs = st.get("objects", [])
        if name.startswith("plan"):
            states = sorted({c["state"] for o in objs for c in o.get("copies", [])})
            check(bool(objs) and states == ["Installed"], f"{len(objs)} plan copies all installed ({states})",
                  errors)
        else:
            check(not objs, "no copies without a policy", errors)
        return {"scenario": name, "run": run, "phase": st.get("phase"), "wall": round(wall, 1), "errors": errors}
    while task_phase(status(run), TRIGGER) != "Succeeded" and time.time() - t0 < 300:
        time.sleep(0.2)
    time.sleep(2)
    fault.crash(VICTIM)
    st = wait_done(run, timeout=900)
    wall = time.time() - t0
    errors = []
    placed = {t["name"]: t.get("node") for t in st.get("predictedTasks", [])}
    rec = st.get("recovery", [])
    print(f"[ft] {name}: phase {st.get('phase')} ({st.get('message', '')}) after {wall:.1f}s", flush=True)
    print(f"[ft] {name}: recovery {json.dumps(rec)}", flush=True)
    check(placed.get(TRIGGER) == VICTIM, f"{TRIGGER} planned on {VICTIM}", errors)
    check(len(rec) >= 1, "recovery recorded", errors)
    reruns = sorted({t for r in rec for t in r.get("plan", {}).get("rerun", [])})
    stuck = sorted({t for r in rec for t in r.get("plan", {}).get("stuck", [])})
    if name == "plan-crash":
        copies = [d for d in st.get("realizationPolicy", {}).get("decisions", []) if d.get("copy")]
        srcs = sorted({d["object"] for d in copies if d["object"].startswith(f"source-{SENSOR}.")})
        check(len(srcs) == 4, f"policy copied the 4 captures of source-{SENSOR} ({srcs})", errors)
        check(st.get("phase") == "Succeeded", "run succeeded", errors)
        check(f"source-{SENSOR}" not in reruns, f"source-{SENSOR} did not run again (reruns {reruns})", errors)
        check(not stuck, "nothing stuck", errors)
        moved = {m["task"]: m["to"] for r in rec for m in r.get("plan", {}).get("moved", [])}
        check(all(n != VICTIM for n in moved.values()) and moved, f"tasks moved off {VICTIM} ({moved})", errors)
    else:
        check(st.get("phase") == "Failed", "run failed", errors)
        check(stuck == [f"source-{SENSOR}"], f"stuck on source-{SENSOR} ({stuck})", errors)
    fault.restore(VICTIM)
    return {"scenario": name, "run": run, "phase": st.get("phase"), "wall": round(wall, 1), "errors": errors}


if __name__ == "__main__":
    results = [scenario(n) for n in (sys.argv[1:] or ["plan-crash"])]
    print(json.dumps(results, indent=1))
    sys.exit(1 if any(r["errors"] for r in results) else 0)
