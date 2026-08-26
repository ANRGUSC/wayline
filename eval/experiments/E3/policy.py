#!/usr/bin/env python3
"""E3 risk-aware replication policy (external).

Reads a risk signal written by an independent observer and revises the
object's realization through the same interface E1/E2 use. It never
touches the DAG, pods, or compute placements.

  risk  -> add a replica on the backup node (durability only)
  loss  -> rebind the serving copy to the backup (survive source loss)
  clear -> evict the replica (false positive: stop paying for it)

Usage: policy.py <run> <arm> <signal-file> <backup-node>
"""
import json
import subprocess
import sys
import time

RUN, ARM, SIGNAL, BACKUP = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
NS = "wl-system"


def signal():
    try:
        return open(SIGNAL).read().strip()
    except FileNotFoundError:
        return ""


def phase():
    return subprocess.run(
        ["kubectl", "-n", NS, "get", "odag", RUN, "-o",
         "jsonpath={.status.phase}"],
        capture_output=True, text=True).stdout.strip() or "Pending"


def patch(entry, label):
    t_obs = time.time()
    subprocess.run(["kubectl", "-n", NS, "patch", "odag", RUN, "--type",
                    "merge", "-p", json.dumps({"spec": {"realization": [entry]}})],
                   check=True)
    print(f"[policy] {RUN} {label} observed={t_obs:.2f} patched={time.time():.2f}",
          flush=True)


seen = set()
patches = 0
while phase() not in ("Succeeded", "Failed"):
    s = signal()
    if s and s not in seen:
        if s == "risk" and ARM in ("adaptive-loss", "adaptive-clear"):
            # Durability only: add a replica, keep serving from the source.
            patch({"object": "produce", "copies": [BACKUP],
                   "servingCopy": "", "evict": []}, "risk->replicate")
            patches += 1
        elif s == "loss" and ARM in ("always-loss", "adaptive-loss"):
            # Source copy is gone: serve from the surviving replica.
            patch({"object": "produce", "copies": [BACKUP],
                   "servingCopy": BACKUP, "evict": []}, "loss->rebind")
            patches += 1
        elif s == "clear" and ARM == "adaptive-clear":
            # False positive: stop paying for the replica.
            patch({"object": "produce", "copies": [],
                   "servingCopy": "", "evict": [BACKUP]}, "clear->evict")
            patches += 1
        seen.add(s)
    time.sleep(0.5)
print(f"[policy] {RUN} terminal; patches={patches}", flush=True)
