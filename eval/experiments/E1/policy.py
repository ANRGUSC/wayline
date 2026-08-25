#!/usr/bin/env python3
"""E1 adaptive policy: on the degradation signal, revise the live run's
realization (copy + serving point on the migration target). One patch,
run-scoped, terminates with the run. Logs every action with timestamps
so the adaptation trace is reconstructible."""
import json
import subprocess
import sys
import time

RUN, SIGNAL, TARGET = sys.argv[1], sys.argv[2], sys.argv[3]
NS = "wl-system"


def degraded():
    try:
        return open(SIGNAL).read().strip() == "on"
    except FileNotFoundError:
        return False


def phase():
    out = subprocess.run(
        ["kubectl", "-n", NS, "get", "odag", RUN, "-o",
         "jsonpath={.status.phase}"],
        capture_output=True, text=True).stdout.strip()
    return out or "Pending"


patched = False
patches = 0
while phase() not in ("Succeeded", "Failed"):
    if degraded() and not patched:
        t_obs = time.time()
        patch = {"spec": {"realization": [{
            "object": "produce", "copies": [TARGET],
            "servingCopy": TARGET}]}}
        subprocess.run(["kubectl", "-n", NS, "patch", "odag", RUN,
                        "--type", "merge", "-p", json.dumps(patch)],
                       check=True)
        t_patch = time.time()
        print(f"[policy] {RUN} observed={t_obs:.2f} patched={t_patch:.2f} "
              f"target={TARGET}", flush=True)
        patched = True
        patches += 1
    time.sleep(1)
print(f"[policy] {RUN} terminal; patches={patches}", flush=True)
