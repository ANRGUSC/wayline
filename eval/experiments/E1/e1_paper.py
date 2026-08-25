#!/usr/bin/env python3
"""E1 paper-level campaign: 20 randomized blocks x 12 conditions = 240
runs, executed AFTER the duplicate-vertex fix. Reuses the pilot's
machinery (cap, triggers, policy, artifacts) unchanged; adds block
structure and richer per-run parsed fields. Failed runs are retained
and flagged needs_rerun=1, never silently replaced.
"""

import csv
import datetime
import os
import random
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import e1_pilot as base  # noqa: E402

BLOCKS = 20
SEED = 20260826
CONDITIONS = [
    ("fixed-clean", 0), ("adaptive-clean", 0),
    ("fixed-deg", 471), ("adaptive-late", 471),
    ("fixed-deg", 236), ("adaptive-late", 236),
    ("fixed-deg", 118), ("adaptive-early", 118),
    ("adaptive-late", 118), ("static-oracle", 118),
    ("fixed-deg", 59), ("adaptive-late", 59),
]

FIELDS = base.FIELDS + ["block", "patches", "cancels", "restarts",
                        "rebind_ts", "converged_ts", "needs_rerun", "cause"]


def logts(line):
    m = re.match(r"(\d{4}/\d\d/\d\d \d\d:\d\d:\d\d)", line)
    if not m:
        return ""
    return datetime.datetime.strptime(
        m.group(1) + " +0000", "%Y/%m/%d %H:%M:%S %z").timestamp()


class Writer:
    """Wraps csv.writer: intercepts the base row, appends paper fields."""

    def __init__(self, wcsv, res):
        self.w = wcsv
        self.res = res
        self.block = 0

    def writerow(self, row):
        rec = dict(zip(base.FIELDS, row))
        run = rec.get("run", "")
        patches = cancels = 0
        rebind = converged = ""
        try:
            patches = open(f"{self.res}/policy-{run}.log").read().count(
                "patched=")
        except FileNotFoundError:
            pass
        try:
            ag = open(f"{self.res}/agent3-{run}.log").read()
            cancels = ag.count("CANCELED")
        except FileNotFoundError:
            pass
        try:
            ctrl = open(f"{self.res}/ctrl-{run}.log").read()
            for line in ctrl.splitlines():
                if "executed on" in line and not rebind:
                    rebind = round(logts(line), 1)
                if "realization converged" in line and not converged:
                    converged = round(logts(line), 1)
        except FileNotFoundError:
            pass
        restarts = 0
        r = base.kubectl("get pods --no-headers 2>/dev/null")
        for line in r.stdout.splitlines():
            fl = line.split()
            if len(fl) >= 4 and run in fl[0]:
                try:
                    restarts += int(fl[3])
                except ValueError:
                    pass
        failed = rec.get("phase") != "Succeeded"
        self.w.writerow(row + [self.block, patches, cancels, restarts,
                               rebind, converged, 1 if failed else 0, ""])


def main():
    res = base.RES
    os.makedirs(res, exist_ok=True)
    for n in [base.PRODUCER] + base.CONSUMERS:
        base.IPS[n] = base.node_ip(n)
    base.shaper_up()
    iface = base.egress_iface()
    print(f"[e1p] egress iface: {iface}", flush=True)
    base.cap_off(iface)
    base.kubectl("set env ds/data-agent WL_PUSH_MIN_THROUGHPUT_KBS=600 "
                 ">/dev/null")
    base.kubectl("rollout status ds/data-agent --timeout=300s >/dev/null")
    base.sh(f"kubectl apply -f {base.REPO}/eval/experiments/E1/e1.yml "
            f"-f {base.REPO}/eval/experiments/E1/e1-static.yml >/dev/null")
    time.sleep(3)

    rng = random.Random(SEED)
    schedule = []
    for b in range(1, BLOCKS + 1):
        block = CONDITIONS[:]
        rng.shuffle(block)
        schedule += [(b, arm, cap) for arm, cap in block]
    with open(f"{res}/execution-order.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order", "block", "arm", "cap_mbit", "seed"])
        for i, (b, arm, cap) in enumerate(schedule, 1):
            w.writerow([i, b, arm, cap, SEED])

    try:
        with open(f"{res}/runs.csv", "w", newline="") as f:
            raw = csv.writer(f)
            raw.writerow(FIELDS)
            w = Writer(raw, res)
            for i, (b, arm, cap) in enumerate(schedule, 1):
                w.block = b
                base.run_one(i, arm, cap, b, iface, w, f)
    finally:
        base.cap_off(iface)
        base.kubectl("set env ds/data-agent WL_PUSH_MIN_THROUGHPUT_KBS- "
                     ">/dev/null")
        base.kubectl("rollout status ds/data-agent --timeout=300s >/dev/null")
        base.kubectl("delete pod e1-shaper --ignore-not-found >/dev/null 2>&1")
        base.clear_signal()
    print("E1 PAPER CAMPAIGN DONE", flush=True)


if __name__ == "__main__":
    main()
