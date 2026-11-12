#!/usr/bin/env python3
"""The E18 matrix and per-run details, from a suite's results directory.

Matrix: one row per scenario and dispatch (suites.ROWS), one column per arm.
A cell is the mean completion time of its valid succeeded runs ("41.2 (3)"
for three), "fails" (every valid run ended Failed: a lost task could run on
no other node), "hangs" (the harness's time limit ended it, the expected
outcome of plain Argo on a crash), a mix ("38.5 (2) +1 fails"), "invalid"
(only runs that failed their validity check), "missing" (in the suite, not
run), "= Arm" (would repeat that arm), or blank (the arm does not run this
dispatch). Completion is wall time from submission to the terminal phase
(Wayline, Argo) or the driver's completion (Ray), as in E16.

Usage: matrix_e18.py DIR [--detail] [--invalid]
"""
import argparse
import glob
import json
import os
import statistics
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import arms  # noqa: E402
import suites  # noqa: E402


def load(d):
    """{cell id: {"valid": [records], "invalid": [records]}}."""
    out = {}
    for cdir in sorted(glob.glob(os.path.join(d, "*", ""))):
        cid = os.path.basename(os.path.dirname(cdir))
        if cid in ("plans",):
            continue
        valid = [json.load(open(p)) for p in sorted(glob.glob(os.path.join(cdir, "rep*.json")))
                 if ".attempt" not in p and not p.endswith(".tmp")]
        invalid = [json.load(open(p)) for p in sorted(glob.glob(os.path.join(cdir, "rep*.attempt*.invalid.json")))]
        if valid or invalid:
            out[cid] = {"valid": valid, "invalid": invalid}
    return out


def cell_text(runs, in_suite):
    if runs is None:
        return "missing" if in_suite else ""
    v = runs["valid"]
    if not v:
        return "invalid" if runs["invalid"] else "missing"
    ok = [r["wall"] for r in v if r.get("outcome") == "Succeeded" and r.get("wall") is not None]
    fails = sum(1 for r in v if r.get("outcome") == "Failed")
    hangs = sum(1 for r in v if r.get("outcome") == "Timeout")
    parts = []
    if ok:
        parts.append(f"{statistics.mean(ok):.1f}" + (f" ({len(ok)})" if len(ok) > 1 else ""))
    if fails:
        parts.append("fails" if not ok and not hangs else f"+{fails} fails")
    if hangs:
        parts.append("hangs" if not ok and not fails else f"+{hangs} hangs")
    return " ".join(parts)


def matrix(data, manifest):
    in_suite = {c["id"] for c in (manifest or {}).get("cells", [])}
    rows = [r for r in suites.ROWS if not in_suite or any(c in in_suite for c in _row_cells(r))]
    cols = [a for a in arms.ALL if any(a in r["arms"] for r in rows)]
    print("| Scenario | Workload (dispatch) | " + " | ".join(cols) + " |")
    print("|" + "---|" * (len(cols) + 2))
    missing = invalid = 0
    for r in rows:
        cells = []
        for a in cols:
            src = r["arms"].get(a)
            if src is None:
                cells.append("")
            elif src.startswith("="):
                cells.append(src.replace("=", "= "))
            else:
                cid = f"{r['claim']}-{src}-{a}"
                t = cell_text(data.get(cid), cid in in_suite or not in_suite)
                missing += t == "missing"
                invalid += t == "invalid"
                cells.append(t)
        print(f"| {r['scenario']} | {r['dispatch']} | " + " | ".join(cells) + " |")
    print(f"\nmissing cells: {missing}; cells with only invalid runs: {invalid}")


def _row_cells(r):
    return [f"{r['claim']}-{src}-{a}" for a, src in r["arms"].items() if not src.startswith("=")]


def ts(x):
    return datetime.fromisoformat(x.replace("Z", "+00:00")).timestamp() if x else None


def summarize(r):
    """E16's analyze_e16 summary of a Wayline run."""
    st = r.get("status") or {}
    flows = [f for f in st.get("actualNetworkFlows") or [] if f.get("ok")]
    push = sum(f["dataSize"] for f in flows if not str(f.get("toTask", "")).startswith("rev-"))
    copy = sum(f["dataSize"] for f in flows if str(f.get("toTask", "")).startswith("rev-"))
    rec = st.get("recovery") or []
    plan = {k: sorted({t for x in rec for t in ((x.get("plan") or {}).get(k) or [])}) for k in ("rerun", "restarted", "stuck")}
    moved = sorted({m["task"] for x in rec for m in (x.get("plan") or {}).get("moved", [])})
    out = {"outcome": r.get("outcome"), "wall": r.get("wall"),
           "copies": (st.get("realizationPolicy") or {}).get("copies", 0),
           "pushMB": push / 1e6, "copyMB": copy / 1e6, "recoveries": len(rec), "moved": moved, **plan,
           "message": st.get("message", "")}
    fault_at = r.get("faultAt")
    if fault_at and rec and rec[0].get("at"):
        out["detectS"] = ts(rec[0]["at"]) - fault_at
        hit = plan["restarted"] or plan["rerun"]
        starts = [(r.get("timings") or {}).get(t, {}).get("taskStartUnix") for t in hit]
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


def detail(data, show_invalid):
    by_claim = {}
    for cid, runs in data.items():
        for r in runs["valid"] + (runs["invalid"] if show_invalid else []):
            by_claim.setdefault(r["claim"], []).append(r)
    for claim in sorted(by_claim):
        rs = sorted(by_claim[claim], key=lambda r: (r["workload"], r["arm"], r["rep"], r.get("attempt", 1)))
        print(f"\n## {claim}\n")
        print("| workload | arm | rep | valid | outcome | wall s | copies | push MB | copy MB | recoveries | moved "
              "| rerun | restarted | stuck | detect s | restart s | note |")
        print("|" + "---|" * 17)
        for r in rs:
            ok = (r.get("validity") or {}).get("ok")
            if r["system"] == "wayline":
                s = summarize(r)
                note = s["message"][:80] if s["outcome"] != "Succeeded" else ""
                print(f"| {r['workload']} | {r['arm']} | {r['rep']} | {'yes' if ok else 'NO'} | {s['outcome']} | "
                      f"{fmt(s['wall'])} | {s['copies']} | {s['pushMB']:.0f} | {s['copyMB']:.0f} | {s['recoveries']} | "
                      f"{fmt(s['moved'])} | {fmt(s['rerun'])} | {fmt(s['restarted'])} | {fmt(s['stuck'])} | "
                      f"{fmt(s.get('detectS'))} | {fmt(s.get('resumeS'))} | {note} |")
            else:
                extra = (f"retries {r.get('retries')}" if r["system"] == "argo"
                         else f"errors {list((r.get('errors') or {}))}")
                print(f"| {r['workload']} | {r['arm']} | {r['rep']} | {'yes' if ok else 'NO'} | {r.get('outcome')} | "
                      f"{fmt(r.get('wall'))} | {len(r.get('copies') or {}) or '-'} | - | - | - | - | - | - | - | - | - | "
                      f"{extra} |")
            if not ok:
                v = r.get("validity") or {}
                why = {k: x for k, x in v.items() if x in (False, "violations", "mismatch", "unverified")
                       or (k == "lane_escape" and x)}
                print(f"|  |  |  | why: {json.dumps(why)[:200]} |" + " |" * 13)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dir")
    ap.add_argument("--detail", action="store_true", help="one line per run, per claim")
    ap.add_argument("--invalid", action="store_true", help="include invalid attempts in --detail")
    a = ap.parse_args()
    manifest = None
    try:
        manifest = json.load(open(os.path.join(a.dir, "manifest.json")))
    except (OSError, ValueError):
        pass
    if manifest:
        s0 = manifest["sessions"][0]
        print(f"# {manifest['suite']} on lane {manifest['lane']['name']} ({len(manifest['lane']['nodes'])} nodes), "
              f"controller {manifest['controllerTag']}, agent {manifest['agentTag']}, git {str(manifest['git'])[:12]}, "
              f"{len(manifest['sessions'])} session(s), first {s0['start']}\n")
    data = load(a.dir)
    matrix(data, manifest)
    if a.detail:
        detail(data, a.invalid)


if __name__ == "__main__":
    main()
