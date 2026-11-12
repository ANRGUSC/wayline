#!/usr/bin/env python3
"""The E16 headline matrix, built from the result files: one row per
scenario and dispatch, one column per arm. A cell is the run's completion
time (mean over repetitions, with *), "fails" (the run ended: a lost task
could run on no other node), "hangs" (no progress to the time limit), a
reference to the arm it would repeat ("= W-plan-only"), or "missing".

Usage: matrix_e16.py [RES]   (default ~/E16-results)
"""
import glob
import json
import os
import sys

RES = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser("~/E16-results")
ARMS = ["W", "W-plan-only", "W-static", "W-frozen", "W-replan", "Ray", "Ray+copies", "Argo", "Argo+oos"]

# Rows: (scenario, workload label, {arm: source}). A source is (claim, workload)
# looked up over every tag, "=Arm" for a cell that would repeat that arm, or
# None when the arm does not apply to this dispatch (Ray only runs warm
# workers, Argo only pods).
ROWS = [
    ("No fault", "GDTM (embeds as pods)", {
        "W": ("cost", "gdtm"), "W-plan-only": ("cost", "gdtm"), "W-static": ("cost", "gdtm"),
        "W-frozen": ("cost", "gdtm"), "W-replan": "=W-plan-only", "Argo": ("cost", "gdtm"), "Argo+oos": "=Argo"}),
    ("No fault", "GDTM (warm)", {
        "W": ("cost", "gdtm-warm"), "W-plan-only": ("cost", "gdtm-warm"), "W-static": ("cost", "gdtm-warm"),
        "W-frozen": "=W", "W-replan": "=W-plan-only", "Ray": ("cost", "gdtm"), "Ray+copies": ("cost", "gdtm")}),
    ("No fault", "iot (pods)", {
        "W": ("cost", "iot"), "W-plan-only": ("cost", "iot"), "W-static": ("cost", "iot"),
        "W-frozen": ("cost", "iot"), "W-replan": "=W-plan-only", "Argo": ("cost", "iot"), "Argo+oos": "=Argo"}),
    ("No fault", "iot (warm)", {
        "W": ("cost", "iot-warm"), "W-plan-only": ("cost", "iot-warm"), "W-static": ("cost", "iot-warm"),
        "W-frozen": "=W", "W-replan": "=W-plan-only", "Ray": ("cost", "iot"), "Ray+copies": ("cost", "iot")}),
    ("No fault", "FOCAL (encoders as pods)", {
        "W": ("cost", "focal"), "W-plan-only": ("cost", "focal"), "W-static": ("cost", "focal"),
        "W-frozen": "=W", "W-replan": "=W-plan-only", "Argo": ("cost", "focal"), "Argo+oos": "=Argo"}),
    ("No fault", "FOCAL (warm)", {
        "W": ("cost", "focal-warm"), "W-plan-only": ("cost", "focal-warm"), "W-static": ("cost", "focal-warm"),
        "W-frozen": "=W", "W-replan": "=W-plan-only", "Ray": ("cost", "focal"), "Ray+copies": ("cost", "focal")}),
    ("Sensor node crashed mid-run", "GDTM (embeds as pods)", {
        "W": ("survival", "gdtm"), "W-plan-only": ("survival", "gdtm"), "W-static": ("survival", "gdtm"),
        "W-frozen": ("survival", "gdtm"), "W-replan": "=W-plan-only", "Argo": ("survival", "gdtm"),
        "Argo+oos": ("survival", "gdtm")}),
    ("Sensor node crashed mid-run", "GDTM (warm)", {
        "W": ("survival", "gdtm-warm"), "W-plan-only": ("survival", "gdtm-warm"), "W-static": ("survival", "gdtm-warm"),
        "W-frozen": ("survival", "gdtm-warm"), "W-replan": "=W-plan-only", "Ray": ("survival", "gdtm"),
        "Ray+copies": ("survival", "gdtm")}),
    ("Sensor node crashed mid-run", "iot (pods)", {
        "W": ("survival", "iot"), "W-plan-only": ("survival", "iot"), "W-static": ("survival", "iot"),
        "W-frozen": ("survival", "iot"), "W-replan": "=W-plan-only", "Argo": ("survival", "iot"),
        "Argo+oos": ("survival", "iot")}),
    ("Sensor node crashed mid-run", "iot (warm)", {
        "W": ("survival", "iot-warm"), "W-plan-only": ("survival", "iot-warm"), "W-static": ("survival", "iot-warm"),
        "W-frozen": ("survival", "iot-warm"), "W-replan": "=W-plan-only", "Ray": ("survival", "iot"),
        "Ray+copies": ("survival", "iot")}),
    ("Source node crashed mid-run", "FOCAL (encoders as pods)", {
        "W": ("survival", "focal"), "W-plan-only": ("survival", "focal"), "W-static": ("survival", "focal"),
        "W-frozen": ("survival", "focal"), "W-replan": "=W-plan-only", "Argo": ("survival", "focal"),
        "Argo+oos": ("survival", "focal")}),
    ("Source node crashed mid-run", "FOCAL (warm)", {
        "W": ("survival", "focal-warm"), "W-plan-only": ("survival", "focal-warm"), "W-static": ("survival", "focal-warm"),
        "W-frozen": ("survival", "focal-warm"), "W-replan": "=W-plan-only", "Ray": ("survival", "focal"),
        "Ray+copies": ("survival", "focal")}),
    ("Co-located 12 s producer's node crashed", "chain (pods)", {
        "W": ("recovery", "chain"), "W-plan-only": ("recovery", "chain"), "W-static": ("recovery", "chain"),
        "W-frozen": ("recovery", "chain"), "W-replan": "=W-plan-only", "Argo": ("recovery", "chain"),
        "Argo+oos": ("recovery", "chain")}),
    ("Co-located 12 s producer's node crashed", "chain (warm)", {
        "W": ("recovery", "chain-warm"), "W-plan-only": ("recovery", "chain-warm"), "W-static": ("recovery", "chain-warm"),
        "W-frozen": ("recovery", "chain-warm"), "W-replan": "=W-plan-only", "Ray": ("recovery", "chain"),
        "Ray+copies": ("recovery", "chain")}),
    ("Second loss (the consumer's new node too)", "chain (warm; Argo pods)", {
        "W": ("revision", "chain-warm"), "W-plan-only": ("revision", "chain-warm"),
        "W-static": ("revision", "chain-warm"), "W-frozen": ("revision", "chain-warm"), "W-replan": "=W-plan-only",
        "Ray": ("revision", "chain"), "Ray+copies": ("revision", "chain"), "Argo": ("revision", "chain"),
        "Argo+oos": ("revision", "chain")}),
    ("Producer's links throttled after planning", "chain-link (pods)", {
        "W": ("adaptation", "chain-link"), "W-plan-only": ("adaptation", "chain-link"),
        "W-static": ("adaptation", "chain-link"), "W-frozen": ("adaptation", "chain-link"),
        "W-replan": ("adaptation", "chain-link"), "Ray": ("adaptation", "chain-link"), "Ray+copies": "=Ray",
        "Argo": ("adaptation", "chain-link"), "Argo+oos": "=Argo"}),
    ("Same, with a second copy on a healthy link", "fan (pods)", {
        "W": ("adaptation-copy", "fan"), "W-plan-only": ("adaptation-copy", "fan"),
        "W-static": ("adaptation-copy", "fan"), "W-frozen": ("adaptation-copy", "fan"),
        "W-replan": ("adaptation-copy", "fan"), "Ray": ("adaptation-copy", "fan"), "Ray+copies": "=Ray",
        "Argo": ("adaptation-copy", "fan"), "Argo+oos": "=Argo"}),
]
# Tags whose runs supersede earlier ones for the same cell (later wins).
ORDER = ["headline", "round2", "round3", "ray1", "argo1", "warm1", "ray-gdtm", "focal-sweep", "gaps", "fill"]


def runs(claim, workload, arm):
    """Every run of the cell in the latest tag that has it."""
    found = []
    for tag in ORDER:
        hits = []
        for p in glob.glob(os.path.join(RES, tag, f"{claim}-{workload}-{arm}*.json")):
            stem = os.path.basename(p)[:-5]
            rest = stem[len(f"{claim}-{workload}-{arm}"):]
            if rest == "" or (rest.startswith("-r") and rest[2:].isdigit()):
                hits.append(json.load(open(p)))
        if hits:
            found = hits
    return found


def cell(src, arm):
    if src is None:
        return ""
    if isinstance(src, str):
        return src.replace("=", "= ")
    rs = runs(src[0], src[1], arm)
    if not rs:
        return "missing"
    ok = [r for r in rs if (r.get("status") or {}).get("phase") == "Succeeded"]
    if not ok:
        wall = max(r.get("wall") or 0 for r in rs)
        msg = json.dumps(rs[0].get("status") or {})
        return "hangs" if (wall >= 290 and "no surviving node" not in msg) else "fails"
    t = sum(r["wall"] for r in ok) / len(ok)
    return f"{t:.1f}" + ("*" if len(ok) > 1 else "")


def main():
    print("| Scenario | Workload (dispatch) | " + " | ".join(ARMS) + " |")
    print("|" + "---|" * (len(ARMS) + 2))
    missing = 0
    for scenario, label, srcs in ROWS:
        cells = [cell(srcs.get(a), a) for a in ARMS]
        missing += cells.count("missing")
        print(f"| {scenario} | {label} | " + " | ".join(cells) + " |")
    print(f"\nmissing cells: {missing}")


if __name__ == "__main__":
    main()
