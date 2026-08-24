#!/usr/bin/env python3
"""E10 adaptive policy: migrate the serving copy when the producer's
egress degrades.

Observation provider: /tmp/wl-linkstate ("on" = anrg-3 egress degraded),
written by the same script that changes the shaper. Providers are
replaceable by contract; this one is deliberately trivial so the loop
under test is policy -> revision -> actuation.

Action: one kubectl patch on the LIVE run. No YAML files, no graph
change, no pod moves.
"""
import json
import subprocess
import sys
import time

RUN = sys.argv[1]
NS = "wl-system"
SAFE_NODE = "anrg-7"          # unaffected; F-tier to the other consumers

def degraded():
    try:
        return open("/tmp/wl-linkstate").read().strip() == "on"
    except FileNotFoundError:
        return False

def phase():
    out = subprocess.run(
        ["kubectl", "-n", NS, "get", "odag", RUN, "-o", "jsonpath={.status.phase}"],
        capture_output=True, text=True).stdout.strip()
    return out or "Pending"

patched = False
while phase() not in ("Succeeded", "Failed"):
    if degraded() and not patched:
        patch = {"spec": {"realization": [{
            "object": "produce",
            "copies": [SAFE_NODE],
            "servingCopy": SAFE_NODE,
        }]}}
        subprocess.run(["kubectl", "-n", NS, "patch", "odag", RUN,
                        "--type", "merge", "-p", json.dumps(patch)], check=True)
        print(f"[policy] {RUN}: degradation observed -> serving copy {SAFE_NODE}", flush=True)
        patched = True
    time.sleep(2)
print(f"[policy] {RUN}: run terminal, exiting", flush=True)
