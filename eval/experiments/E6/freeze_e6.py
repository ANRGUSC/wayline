#!/usr/bin/env python3
"""Freeze the E6 MCMT reference schedule from the controller's OWN call.

Freezing from an independently constructed request is brittle: two
requests that differ only in irrelevant detail can break ties between
symmetric nodes differently, producing isomorphic schedules with
different hashes. The reference the direct arms must reproduce is the
one the deployed controller itself computes.

Usage: freeze_e6.py <template> <algo-short> <out-dir>
"""
import hashlib
import json
import os
import re
import subprocess
import sys
import time

NS = "wl-system"


def sh(cmd, timeout=300):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout)


def ctrl_pod():
    return sh(f"kubectl -n {NS} get pod -l app=odag-controller "
              f"--field-selector=status.phase=Running "
              f"-o jsonpath='{{.items[0].metadata.name}}'").stdout.strip()


def newest_dump(pod, kind):
    f = sh(f"kubectl -n {NS} exec {pod} -c odag-controller -- sh -c "
           f"'ls /tmp/saga-dumps/*.{kind}.json | sort | tail -1'").stdout.strip()
    body = sh(f"kubectl -n {NS} exec {pod} -c odag-controller -- cat {f}").stdout
    return f, json.loads(body)


def canonical_hash(placement, order):
    canon = json.dumps({"placement": dict(sorted(placement.items())),
                        "order": {k: v for k, v in sorted(order.items())}},
                       sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canon.encode()).hexdigest()


def one(template, short, out_dir):
    pod = ctrl_pod()
    r = sh(f"/home/anrg/wayline/bin/wayline run {template} -n {NS}")
    m = re.search(rf"({template}-run-[a-z0-9]+)", r.stdout + r.stderr)
    run = m.group(1)
    time.sleep(8)
    _, req = newest_dump(pod, "request")
    _, resp = newest_dump(pod, "response")
    placement = {a["task"]: a["node"] for a in resp["assignments"]}
    order = {}
    for a in sorted(resp["assignments"], key=lambda x: x["estimatedStart"]):
        order.setdefault(a["node"], []).append(a["task"])
    times = {a["task"]: [a["estimatedStart"], a["estimatedFinish"]]
             for a in resp["assignments"]}
    h = canonical_hash(placement, order)
    rec = {"algorithm": req.get("algorithm"), "algorithm_short": short,
           "placement": placement, "order": order, "times": times,
           "estimatedMakespan": resp.get("estimatedMakespan"),
           "costModelFitRMSE": resp.get("costModelFitRMSE"),
           "constraintOverrides": resp.get("constraintOverrides") or [],
           "schedule_hash": h, "raw_request": req, "raw_response": resp,
           "source": "controller live call"}
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, f"frozen-{short}.json"), "w") as f:
        json.dump(rec, f, indent=1)
    # let it finish so the cluster is clean
    for _ in range(60):
        ph = sh(f"kubectl -n {NS} get odag {run} -o jsonpath='{{.status.phase}}'"
                ).stdout.strip()
        if ph in ("Succeeded", "Failed"):
            break
        time.sleep(3)
    sh(f"kubectl -n {NS} delete odag {run} --ignore-not-found >/dev/null 2>&1")
    print(f"=== {short}: hash {h[:16]} est {resp.get('estimatedMakespan'):.2f}s "
          f"rmse {resp.get('costModelFitRMSE'):.1e} overrides "
          f"{len(resp.get('constraintOverrides') or [])}")
    for n, ts in sorted(order.items()):
        print(f"    {n:8} {' -> '.join(ts)}")
    # MCMT legitimately pins cross-camera-match and report to the
    # gateway: that is the workload's aggregation tier, not a scheduler
    # choice. What must never happen is a SCHEDULER-PLACED task landing
    # there, since those are constrained to the compute tier and the
    # store arm needs the gateway free of application compute.
    SCHEDULED = ("detect-embed-", "track-")
    stray = [t for t, n in placement.items()
             if n == "anrg-9" and t.startswith(SCHEDULED)]
    if stray:
        raise SystemExit(
            f"STOP: scheduler-placed tasks on the gateway: {stray}")
    pinned_gw = sorted(t for t, n in placement.items() if n == "anrg-9")
    print(f"    gateway-pinned by the workload (expected): {pinned_gw}")
    return rec


if __name__ == "__main__":
    one(sys.argv[1], sys.argv[2], sys.argv[3])
