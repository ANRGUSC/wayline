#!/usr/bin/env python3
"""Summarize an E16 pass: one table per claim.

Usage: analyze_e16.py [RES/<tag>]   (default ~/E16-results/headline)
"""
import glob
import json
import os
import sys
from datetime import datetime


def ts(x):
    return datetime.fromisoformat(x.replace("Z", "+00:00")).timestamp() if x else None


def summarize(r):
    st = r["status"]
    flows = [f for f in st.get("actualNetworkFlows") or [] if f.get("ok")]
    push = sum(f["dataSize"] for f in flows if not f["toTask"].startswith("rev-"))
    copy = sum(f["dataSize"] for f in flows if f["toTask"].startswith("rev-"))
    rec = st.get("recovery") or []
    plan = {k: sorted({t for x in rec for t in (x.get("plan", {}).get(k) or [])})
            for k in ("rerun", "restarted", "stuck")}
    moved = sorted({m["task"] for x in rec for m in x.get("plan", {}).get("moved", [])})
    # Completion = submission to terminal phase. status.makespan counts only
    # the pods that exist at the end, and recovery deletes the lost ones.
    out = {"phase": st.get("phase"), "makespan": r.get("wall") if st.get("phase") == "Succeeded" else None,
           "wall": r.get("wall"),
           "policyCopies": (st.get("realizationPolicy") or {}).get("copies", 0),
           "pushMB": push / 1e6, "copyMB": copy / 1e6, "recoveries": len(rec),
           "moved": moved, **plan, "message": st.get("message", "")}
    fault_at = r.get("faultAt")
    if fault_at and rec:
        out["detectS"] = ts(rec[0]["at"]) - fault_at
        # Fault to the restart of the work the loss interrupted: tasks that
        # were running there, else (lineage) the first producer re-run.
        # Downstream tasks that were never started are not counted: they
        # start late in every arm.
        hit = plan["restarted"] or plan["rerun"]
        starts = [r["timings"].get(t, {}).get("taskStartUnix") for t in hit]
        starts = [s for s in starts if s and s > fault_at]
        if starts:
            out["resumeS"] = max(starts) - fault_at
    return out


def fmt(x, nd=1):
    if x is None:
        return "-"
    if isinstance(x, float):
        return f"{x:.{nd}f}"
    if isinstance(x, list):
        return ",".join(x) if x else "-"
    return str(x)


def main():
    d = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser("~/E16-results/headline")
    rows = [json.load(open(p)) for p in sorted(glob.glob(os.path.join(d, "*-*.json")))
            if not os.path.basename(p).startswith("configmap") and ".recover" not in p]
    repeated(rows)
    base = {(r["workload"], r["arm"]): summarize(r) for r in rows if r["claim"] == "cost"}
    for claim in ("cost", "survival", "recovery", "adaptation", "adaptation-copy", "sweep", "revision", "window"):
        sel = [r for r in rows if r["claim"] == claim]
        if not sel:
            continue
        print(f"\n## {claim}\n")
        print("| workload | arm | phase | completion s | vs no fault | copies | push MB | copy MB "
              "| recoveries | moved | rerun | restarted | stuck | detect s | restart s |")
        print("|" + "---|" * 15)
        for r in sel:
            s = summarize(r)
            b = base.get((r["workload"], r["arm"]))
            rel = (s["makespan"] / b["makespan"]) if (b and s["makespan"] and b["makespan"] and claim != "cost") else None
            print(f"| {r['workload']} | {r['arm']} | {s['phase']} | {fmt(s['makespan'])} | "
                  f"{fmt(rel, 2) if rel else '-'} | {s['policyCopies']} | {s['pushMB']:.0f} | {s['copyMB']:.0f} | "
                  f"{s['recoveries']} | {fmt(s['moved'])} | {fmt(s['rerun'])} | {fmt(s['restarted'])} | "
                  f"{fmt(s['stuck'])} | {fmt(s.get('detectS'))} | {fmt(s.get('resumeS'))} |")
            if s["phase"] != "Succeeded" and s["message"]:
                print(f"|  |  | {s['message']} |" + " |" * 12)


def repeated(rows):
    """Cells run more than once: completion time and copy bytes per arm."""
    groups = {}
    for r in rows:
        s = summarize(r)
        groups.setdefault((r["claim"], r["workload"], r["arm"]), []).append(s)
    multi = {k: v for k, v in groups.items() if len(v) > 1}
    if not multi:
        return
    print("\n## repeated cells\n")
    print("| claim | workload | arm | runs | succeeded | completion mean s | min | max | copy MB mean |")
    print("|" + "---|" * 9)
    for (claim, w, arm), v in sorted(multi.items()):
        t = [x["makespan"] for x in v if x["makespan"]]
        ok = sum(1 for x in v if x["phase"] == "Succeeded")
        mean = sum(t) / len(t) if t else None
        print(f"| {claim} | {w} | {arm} | {len(v)} | {ok} | {fmt(mean)} | {fmt(min(t) if t else None)} | "
              f"{fmt(max(t) if t else None)} | {sum(x['copyMB'] for x in v) / len(v):.0f} |")


if __name__ == "__main__":
    main()
