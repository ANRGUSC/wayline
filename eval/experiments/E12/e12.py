#!/usr/bin/env python3
"""E12 orchestrator: warm runners vs pods vs Ray on E11's setting. Runs ON anrg-2.

Same clocks, calibration, DAG and bandwidth profile as E11 (it drives
E11's harness). Per scheduler: Wayline with a pod per task (cold), Wayline
with the same tasks as calls on warm runners (warm), and the cold
placement pinned onto Ray; then Ray's default once per repetition.

Env: RES (default ~/E12-results), SUDO_PASS.
Usage: SUDO_PASS=... python3 e12.py [--reps 1] [--schedulers saga/heft saga/minmin] [--keep-runners]
"""
import argparse
import importlib.util
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
os.environ.setdefault("RES", os.path.expanduser("~/E12-results"))
spec = importlib.util.spec_from_file_location("e11", os.path.join(HERE, "..", "E11", "e11.py"))
e11 = importlib.util.module_from_spec(spec); spec.loader.exec_module(e11)
IMAGE = "192.168.1.163:5000/wl-e11:v2"


def runners_up():
    e11.kubectl("apply -f -", stdin=open(os.path.join(HERE, "runner.yml")).read())
    e11.kubectl("rollout restart ds/wl-runner-e12")        # fresh runners, fresh image
    e11.kubectl("rollout status ds/wl-runner-e12 --timeout=300s", timeout=320)
    time.sleep(3)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", default="1")
    ap.add_argument("--schedulers", nargs="+", default=["saga/heft", "saga/minmin"])
    ap.add_argument("--keep-runners", action="store_true")
    a, rest = ap.parse_known_args()
    runners_up()
    try:
        e11.main(["--reps", a.reps, "--schedulers", *a.schedulers, "--modes", "cold", "warm",
                  "--runner", "e12", "--image", IMAGE, *rest])
    finally:
        if not a.keep_runners:
            e11.kubectl("delete ds wl-runner-e12")


if __name__ == "__main__":
    main()
