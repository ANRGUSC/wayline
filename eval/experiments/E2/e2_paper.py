#!/usr/bin/env python3
"""E2 paper-level campaign: 20 randomized blocks x 4 arms = 80 runs.

Pilot semantics are preserved EXACTLY: same contact schedule (3->8
blocked throughout; 3->7 open [t0, +8); 7->8 open [+28, +36)), same
contact-scale transport deadline, fixed-direct censored at 120 s. The
transport deadline is deliberately NOT tuned to make static-relay fail:
static-relay is the foreknowledge oracle, and its agreement with
adaptive-relay is the result. The decisive comparison is fixed (cannot
complete: no contemporaneous path) versus either relay realization
(completes by retaining the object on a node absent from the task map).

Adds per run: block, seed, contact-to-installation latency for both
hops, and ok/failed flow counts.
"""

import csv
import json
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import e2_pilot as base  # noqa: E402

BLOCKS = 20
SEED = 20260827
ARMS = ["clean-direct", "fixed-direct", "static-relay", "adaptive-relay"]

BASE_FIELDS = ["order", "arm", "block", "run", "phase", "makespan_s",
               "t0_rel_s", "delivered_3_7", "delivered_3_8", "delivered_7_8",
               "attempted_3_7", "attempted_3_8", "attempted_7_8", "digest",
               "relay_pods", "placements", "n_events", "ok_flows",
               "failed_flows"]
FIELDS = BASE_FIELDS + ["hop1_install_s", "contact2_to_install_s", "seed"]


class Writer:
    """Appends paper fields computed from the run's saved event trace."""

    def __init__(self, wcsv, res):
        self.w = wcsv
        self.res = res
        self.block = 0

    def writerow(self, row):
        run = row[3] if len(row) > 3 else ""
        hop1 = c2i = ""
        path = os.path.join(self.res, f"events-{run}.json")
        if run and os.path.exists(path):
            d = json.load(open(path))
            ev = d.get("events", [])
            hist = d.get("object_history", [])
            t0 = next((t for t, n in ev if n.startswith("t0")), None)
            t_open2 = next((t for t, n in ev if n == "contact:open-7-8"), None)

            def first_installed(node):
                for t, snap in hist:
                    try:
                        objs = json.loads(snap)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    for o in objs:
                        for c in o.get("copies", []):
                            if c.get("node") == node and \
                                    c.get("state") == "Installed":
                                return t
                return None

            i7, i8 = first_installed("anrg-7"), first_installed("anrg-8")
            if t0 is not None and i7 is not None:
                hop1 = round(i7 - t0, 1)
            if t_open2 is not None and i8 is not None:
                c2i = round(i8 - t_open2, 1)
        self.w.writerow(list(row) + [hop1, c2i, SEED])


def main():
    res = base.RES
    os.makedirs(res, exist_ok=True)
    for n in ("anrg-3", "anrg-7", "anrg-8"):
        base.IPS[n] = base.node_ip(n)
    base.fw_pods()
    base.contacts("clear")
    base.kubectl("set env ds/data-agent WL_PUSH_TIMEOUT_BASE_S=5 "
                 "WL_PUSH_TIMEOUT_SAFETY_S=5 WL_PUSH_MIN_THROUGHPUT_KBS=20000 "
                 ">/dev/null")
    base.kubectl("rollout status ds/data-agent --timeout=300s >/dev/null")
    base.sh(f"kubectl apply -f {base.E2DIR}/e2.yml >/dev/null")

    rng = random.Random(SEED)
    schedule = []
    for b in range(1, BLOCKS + 1):
        block = ARMS[:]
        rng.shuffle(block)
        schedule += [(b, arm) for arm in block]
    with open(f"{res}/execution-order.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order", "block", "arm", "seed"])
        for i, (b, arm) in enumerate(schedule, 1):
            w.writerow([i, b, arm, SEED])

    try:
        with open(f"{res}/runs.csv", "w", newline="") as f:
            raw = csv.writer(f)
            raw.writerow(FIELDS)
            w = Writer(raw, res)
            for i, (b, arm) in enumerate(schedule, 1):
                w.block = b
                base.run_one(i, arm, b, w, f)
    finally:
        base.contacts("clear")
        base.kubectl("set env ds/data-agent WL_PUSH_TIMEOUT_BASE_S- "
                     "WL_PUSH_TIMEOUT_SAFETY_S- WL_PUSH_MIN_THROUGHPUT_KBS- "
                     ">/dev/null")
        base.kubectl("rollout status ds/data-agent --timeout=300s >/dev/null")
        for node in ("anrg-3", "anrg-7"):
            base.kubectl(f"delete pod e2-fw-{node} --ignore-not-found "
                         f">/dev/null 2>&1")
    print("E2 PAPER CAMPAIGN DONE", flush=True)


if __name__ == "__main__":
    main()
