#!/usr/bin/env python3
"""E17 completeness check: which scenarios of a campaign still need a whole run.

  finalize.py --queue queue-X --results results-X --scenarios DIR --labels L ... [--requeue]

A scenario is complete when one lane holds every expected label Succeeded
(the oracle run only when the simulator oracle's placement differs from the
initial plan) and its m0 run is sane (overhead under --max-m0-overhead). With
--requeue, incomplete scenarios lose their partial results and go back into
the queue, to be run whole by one lane (sigma and M0 are lane-specific).
"""
import argparse
import glob
import json
import os
import shutil
from collections import defaultdict


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queue", required=True)
    ap.add_argument("--results", required=True)
    ap.add_argument("--scenarios", required=True)
    ap.add_argument("--labels", nargs="+", required=True)
    ap.add_argument("--repeat", type=int, default=0)
    ap.add_argument("--max-m0-overhead", type=float, default=0.35)
    ap.add_argument("--requeue", action="store_true")
    ap.add_argument("--invalidate", nargs="*", default=[],
                    help="scenario ids to re-run even if complete (their run was disturbed)")
    ap.add_argument("--window-file",
                    help="lines of 'START END' (ISO times): re-run every scenario with a run overlapping one")
    a = ap.parse_args()
    wins = []
    if a.window_file:
        from datetime import datetime
        for line in open(a.window_file):
            if line.strip():
                wins.append([datetime.fromisoformat(t).timestamp() for t in line.split()])
    files = {}
    for f in glob.glob(os.path.join(a.queue, "**", "*.json"), recursive=True):
        sid = json.load(open(f))["id"]
        files[sid] = f
    complete, todo = [], []
    for f in sorted(glob.glob(os.path.join(a.scenarios, "*.json"))):
        d = json.load(open(f))
        sid = d["id"]
        want = ["m0"] + [l for l in a.labels if l != "oracle"]
        if "oracle" in a.labels and d["oracle"]["placement"] is not None and d["oracle"]["placement"] != d["plan"]:
            want.append("oracle")
        lanes = defaultdict(dict)
        for r in glob.glob(os.path.join(a.results, sid, f"r{a.repeat}-*.json")):
            x = json.load(open(r))
            lanes[x["lane"][0]][x["label"]] = x
        ok = False
        for recs in lanes.values():
            if all(recs.get(l, {}).get("status") == "Succeeded" for l in want):
                m0 = recs["m0"]
                over = m0["final"]["makespan"] / m0["T"] - 1
                if over <= a.max_m0_overhead:
                    ok = True
        if sid in a.invalidate:
            ok = False
        if wins and ok:
            for recs in lanes.values():
                for x in recs.values():
                    t0 = x.get("t0")
                    for w in wins:
                        if t0 and t0 < w[1] and t0 + (x.get("wall_end") or 0) > w[0]:
                            ok = False
        (complete if ok else todo).append(sid)
    print(f"{a.queue}: complete {len(complete)}, incomplete {len(todo)}")
    for sid in todo:
        print("  ", sid)
    if a.requeue:
        for sid in todo:
            shutil.rmtree(os.path.join(a.results, sid), ignore_errors=True)
            src = files.get(sid)
            if src is None:
                src = os.path.join(a.scenarios, f"{sid}.json")
                shutil.copy(src, os.path.join(a.queue, f"redo-{sid}.json"))
            elif os.path.dirname(src) != a.queue.rstrip("/"):
                shutil.move(src, os.path.join(a.queue, os.path.basename(src)))
        print(f"requeued {len(todo)}")


if __name__ == "__main__":
    main()
