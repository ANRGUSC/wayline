#!/usr/bin/env python3
"""
Fidelity report: how closely did execution match what the scheduler predicted?

A schedule is a prediction. Wayline records both halves of that prediction
(status.predictedTasks / predictedNetworkFlows, written at deploy time) and
what actually happened (per-task timestamps, status.actualNetworkFlows), on
a common t0. This tool joins them and reports where the model was wrong and
by how much.

Three independent questions, reported separately because they fail
independently:

  1. Makespan fidelity  — did the run take as long as predicted?
  2. Task fidelity      — was each task where and when the model said?
                          Split into placement (did it land on the predicted
                          node), start-time error, and duration error.
  3. Transfer fidelity  — did each cross-node edge move when and as fast as
                          predicted?

Plus two diagnostics that explain *why* a prediction missed:

  Wall-clock decomposition. A cost model predicts compute time, but a task
  occupies its node for compute + interpreter boot + input reads + output
  handoff + teardown. The instrumented fields (computeSeconds and friends,
  see cmd/data-agent + sdk) let us report the share a model can see at all.
  Note that pod-boundary phases (boot, teardown) inherit Kubernetes'
  second-granularity timestamps; SDK-measured phases are sub-millisecond.

  Order inversions. Wayline dispatches on data-readiness rather than
  honoring predicted start times, which is the right call for makespan but
  means execution order can differ from the schedule's order. An inversion
  is a pair of tasks on one node whose execution order is reversed relative
  to the prediction. Rare inversions confirm ASAP dispatch is safe; frequent
  ones mean sim-vs-real comparisons are comparing different schedules.

Usage:
    fidelity.py RUN.json [RUN.json ...]      # one or many run dumps
    fidelity.py --dir results/iobt/heft/     # every *.json in a directory
    fidelity.py --json ...                   # machine-readable output

Input is `kubectl get odag <name> -o json` output, which is what
sweep-scheduler.sh already dumps per run.

CAVEAT — do not compare fidelity across schedulers naively. The predicted
schedule is not produced by the same model for every arm: the built-in HEFT
scheduler publishes its own contention-aware schedule (serialized egress,
TCP fair-share), while every other arm (random, saga/*) gets predictions
from computePredictedSchedule, which has no contention model. So a fidelity
difference between arms may be a difference between *predictors*, not
between placements. Comparing fidelity for one arm across workloads is
sound; comparing arms within a workload is not.
"""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import math
import os
import re
import statistics
import sys
from typing import Any, Optional


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def parse_ts(s: Optional[str]) -> Optional[dt.datetime]:
    """Parse an RFC3339 timestamp, including Kubernetes' nanosecond form.

    datetime.fromisoformat only accepts 9 fractional digits from Python
    3.11, and the controller emits RFC3339Nano, so truncate to microseconds
    rather than silently dropping every instrumented timestamp.
    """
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    s = re.sub(r"\.(\d{6})\d+", r".\1", s)
    try:
        return dt.datetime.fromisoformat(s)
    except ValueError:
        return None


def pct(x: float, total: float) -> float:
    return 100.0 * x / total if total else 0.0


def summarize(values: list[float]) -> dict[str, float]:
    """mean/median/p90/max of |values|, plus signed mean to expose bias."""
    if not values:
        return {}
    a = sorted(abs(v) for v in values)
    return {
        "n": len(a),
        "mean_abs": statistics.fmean(a),
        "median_abs": statistics.median(a),
        "p90_abs": a[min(len(a) - 1, int(0.9 * len(a)))],
        "max_abs": a[-1],
        "mean_signed": statistics.fmean(values),
    }


# --------------------------------------------------------------------------
# per-run analysis
# --------------------------------------------------------------------------

def analyze_run(obj: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Return a fidelity record for one ODAG run, or None if unusable."""
    status = obj.get("status") or {}
    if status.get("phase") != "Succeeded":
        return None
    tasks = status.get("tasks") or []
    predicted = status.get("predictedTasks") or []
    if not tasks or not predicted:
        return None

    # t0: earliest pod start, the same origin the controller uses when it
    # rebases actualNetworkFlows. Predicted times are seconds from t0.
    starts = [parse_ts(t.get("startTime")) for t in tasks]
    starts = [s for s in starts if s]
    ends = [parse_ts(t.get("completionTime")) for t in tasks]
    ends = [e for e in ends if e]
    if not starts or not ends:
        return None
    t0 = min(starts)

    pred_by_name = {p["name"]: p for p in predicted}
    rec: dict[str, Any] = {
        "run": (obj.get("metadata") or {}).get("name", "?"),
        "scheduler": ((obj.get("spec") or {}).get("scheduler")) or "?",
    }

    # ---- 1. makespan ----------------------------------------------------
    actual_makespan = (max(ends) - t0).total_seconds()
    predicted_makespan = max((p.get("estEnd", 0.0) for p in predicted), default=0.0)
    rec["makespan"] = {
        "predicted": predicted_makespan,
        "actual": actual_makespan,
        "error": actual_makespan - predicted_makespan,
        "ratio": (actual_makespan / predicted_makespan) if predicted_makespan else None,
    }

    # ---- 2. per-task ----------------------------------------------------
    start_errs: list[float] = []
    dur_errs: list[float] = []
    misplaced = 0
    compared = 0
    for t in tasks:
        p = pred_by_name.get(t.get("name"))
        if not p:
            continue
        st, ct = parse_ts(t.get("startTime")), parse_ts(t.get("completionTime"))
        if not st or not ct:
            continue
        compared += 1
        if p.get("node") and t.get("node") and p["node"] != t["node"]:
            # The controller may re-place on retry; a scheduler's prediction
            # is only meaningful for the node it chose.
            misplaced += 1
            continue
        start_errs.append((st - t0).total_seconds() - p.get("estStart", 0.0))
        dur_errs.append((ct - st).total_seconds()
                        - (p.get("estEnd", 0.0) - p.get("estStart", 0.0)))
    rec["tasks"] = {
        "compared": compared,
        "misplaced": misplaced,
        "start_error": summarize(start_errs),
        "duration_error": summarize(dur_errs),
    }

    # ---- 3. transfers ---------------------------------------------------
    pflows = {(f["fromTask"], f["toTask"]): f
              for f in (status.get("predictedNetworkFlows") or [])}
    aflows = {(f["fromTask"], f["toTask"]): f
              for f in (status.get("actualNetworkFlows") or [])}
    dur_flow_errs: list[float] = []
    start_flow_errs: list[float] = []
    for key, af in aflows.items():
        pf = pflows.get(key)
        if not pf:
            continue
        a_dur = af.get("end", 0.0) - af.get("start", 0.0)
        p_dur = pf.get("end", 0.0) - pf.get("start", 0.0)
        dur_flow_errs.append(a_dur - p_dur)
        start_flow_errs.append(af.get("start", 0.0) - pf.get("start", 0.0))
    rec["transfers"] = {
        "predicted": len(pflows),
        "actual": len(aflows),
        "matched": len(dur_flow_errs),
        # Predicted-but-absent means the scheduler expected a cross-node
        # transfer that never happened (e.g. it was served same-node).
        "predicted_not_observed": len(set(pflows) - set(aflows)),
        "observed_not_predicted": len(set(aflows) - set(pflows)),
        "duration_error": summarize(dur_flow_errs),
        "start_error": summarize(start_flow_errs),
    }

    # ---- wall-clock decomposition (instrumented runs only) --------------
    phases = {"boot": 0.0, "read": 0.0, "compute": 0.0,
              "handoff": 0.0, "teardown": 0.0, "wall": 0.0}
    instrumented = 0
    for t in tasks:
        st, ct = parse_ts(t.get("startTime")), parse_ts(t.get("completionTime"))
        ts_, tc = parse_ts(t.get("taskStartTime")), parse_ts(t.get("taskCloseTime"))
        if not all([st, ct, ts_, tc]):
            continue
        instrumented += 1
        phases["boot"] += (ts_ - st).total_seconds()
        phases["read"] += t.get("inputReadSeconds", 0.0)
        phases["compute"] += t.get("computeSeconds", 0.0)
        phases["handoff"] += t.get("handoffSeconds", 0.0)
        phases["teardown"] += (ct - tc).total_seconds()
        phases["wall"] += (ct - st).total_seconds()
    if instrumented:
        acct = sum(phases[k] for k in ("boot", "read", "compute", "handoff", "teardown"))
        rec["decomposition"] = {
            "tasks": instrumented,
            **{k: phases[k] for k in phases},
            "accounted_pct": pct(acct, phases["wall"]),
            "model_visible_pct": pct(phases["compute"], phases["wall"]),
            "occupancy_ratio": (phases["wall"] / phases["compute"]) if phases["compute"] else None,
        }

    # ---- order inversions ------------------------------------------------
    by_node: dict[str, list[tuple[float, float, str]]] = {}
    for t in tasks:
        p = pred_by_name.get(t.get("name"))
        st = parse_ts(t.get("startTime"))
        if not p or not st or p.get("node") != t.get("node"):
            continue
        by_node.setdefault(t["node"], []).append(
            (p.get("estStart", 0.0), (st - t0).total_seconds(), t["name"]))
    inversions, pairs = 0, 0
    for node, entries in by_node.items():
        for i in range(len(entries)):
            for j in range(i + 1, len(entries)):
                (pi, ai, _), (pj, aj, _) = entries[i], entries[j]
                if pi == pj or ai == aj:
                    continue
                pairs += 1
                if (pi < pj) != (ai < aj):
                    inversions += 1
    rec["order"] = {"pairs": pairs, "inversions": inversions,
                    "inversion_pct": pct(inversions, pairs)}
    return rec


# --------------------------------------------------------------------------
# aggregation + reporting
# --------------------------------------------------------------------------

def aggregate(records: list[dict[str, Any]]) -> dict[str, Any]:
    ratios = [r["makespan"]["ratio"] for r in records if r["makespan"].get("ratio")]
    errs = [r["makespan"]["error"] for r in records]
    agg: dict[str, Any] = {
        "runs": len(records),
        "makespan_ratio": summarize(ratios) if ratios else {},
        "makespan_error": summarize(errs),
        "task_start_error": summarize(
            [r["tasks"]["start_error"]["mean_signed"] for r in records
             if r["tasks"]["start_error"]]),
        "task_duration_error": summarize(
            [r["tasks"]["duration_error"]["mean_signed"] for r in records
             if r["tasks"]["duration_error"]]),
        "transfer_duration_error": summarize(
            [r["transfers"]["duration_error"]["mean_signed"] for r in records
             if r["transfers"]["duration_error"]]),
        "inversion_pct": summarize([r["order"]["inversion_pct"] for r in records]),
        "misplaced_total": sum(r["tasks"]["misplaced"] for r in records),
    }
    decomp = [r["decomposition"] for r in records if "decomposition" in r]
    if decomp:
        agg["decomposition"] = {
            "runs": len(decomp),
            "model_visible_pct": summarize([d["model_visible_pct"] for d in decomp]),
            "occupancy_ratio": summarize(
                [d["occupancy_ratio"] for d in decomp if d["occupancy_ratio"]]),
            "accounted_pct": summarize([d["accounted_pct"] for d in decomp]),
        }
    return agg


def fmt(s: dict[str, float], unit: str = "s") -> str:
    if not s:
        return "n/a"
    return ("mean %.2f%s  median %.2f%s  p90 %.2f%s  max %.2f%s  (signed mean %+.2f%s)"
            % (s["mean_abs"], unit, s["median_abs"], unit, s["p90_abs"], unit,
               s["max_abs"], unit, s["mean_signed"], unit))


def report(records: list[dict[str, Any]], agg: dict[str, Any]) -> None:
    print("=" * 78)
    print("FIDELITY REPORT — %d run(s)" % agg["runs"])
    print("=" * 78)

    print("\n1. MAKESPAN")
    mr = agg["makespan_ratio"]
    if mr:
        print("   actual/predicted ratio:  mean %.2fx  median %.2fx  max %.2fx"
              % (mr["mean_abs"], mr["median_abs"], mr["max_abs"]))
    print("   absolute error:          %s" % fmt(agg["makespan_error"]))

    print("\n2. PER-TASK (predicted vs actual, matched placements only)")
    print("   start-time error:        %s" % fmt(agg["task_start_error"]))
    print("   duration error:          %s" % fmt(agg["task_duration_error"]))
    if agg["misplaced_total"]:
        print("   NOTE: %d task(s) ran on a node other than predicted (excluded)"
              % agg["misplaced_total"])

    print("\n3. TRANSFERS (cross-node edges)")
    print("   duration error:          %s" % fmt(agg["transfer_duration_error"]))
    tot_pred_missing = sum(r["transfers"]["predicted_not_observed"] for r in records)
    tot_unpred = sum(r["transfers"]["observed_not_predicted"] for r in records)
    print("   predicted but not observed: %d     observed but not predicted: %d"
          % (tot_pred_missing, tot_unpred))

    if "decomposition" in agg:
        d = agg["decomposition"]
        print("\n4. WALL-CLOCK DECOMPOSITION (%d instrumented run(s))" % d["runs"])
        print("   model-visible (compute): mean %.1f%% of pod wall-clock"
              % d["model_visible_pct"]["mean_abs"])
        print("   occupancy ratio:         mean %.2fx  (a model predicting pure"
              % d["occupancy_ratio"]["mean_abs"])
        print("                            compute under-predicts node occupancy by this)")
        print("   accounting closes to:    %.1f%%" % d["accounted_pct"]["mean_abs"])
        tot = {k: sum(r["decomposition"][k] for r in records if "decomposition" in r)
               for k in ("boot", "read", "compute", "handoff", "teardown", "wall")}
        print("   phase shares:  boot %.1f%%  read %.1f%%  compute %.1f%%  "
              "handoff %.1f%%  teardown %.1f%%"
              % tuple(pct(tot[k], tot["wall"]) for k in
                      ("boot", "read", "compute", "handoff", "teardown")))
        print("   (boot/teardown inherit Kubernetes' 1s pod-timestamp granularity;")
        print("    read/compute/handoff are SDK-measured and sub-millisecond)")

    inv = agg["inversion_pct"]
    total_pairs = sum(r["order"]["pairs"] for r in records)
    total_inv = sum(r["order"]["inversions"] for r in records)
    print("\n5. EXECUTION ORDER vs SCHEDULED ORDER")
    if total_pairs:
        print("   same-node pair inversions: %d/%d pairs (%.1f%%); per-run mean %.1f%%  max %.1f%%"
              % (total_inv, total_pairs, pct(total_inv, total_pairs),
                 inv["mean_abs"], inv["max_abs"]))
    else:
        print("   no comparable same-node pairs (predictions lack distinct start times)")
    print("   (Wayline dispatches on data-readiness, not predicted start times;")
    print("    low inversion rates mean the executed order matched the schedule)")
    print()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runs", nargs="*", help="ODAG run JSON dumps")
    ap.add_argument("--dir", help="directory of *.json run dumps")
    ap.add_argument("--json", action="store_true", help="emit JSON instead of a report")
    args = ap.parse_args()

    paths = list(args.runs)
    if args.dir:
        paths += sorted(glob.glob(os.path.join(args.dir, "*.json")))
    if not paths:
        ap.error("no run files given")

    records, skipped = [], 0
    for p in paths:
        try:
            with open(p) as f:
                obj = json.load(f)
        except (OSError, json.JSONDecodeError):
            skipped += 1
            continue
        # sweep dumps are sometimes lists of runs
        objs = obj.get("items", [obj]) if isinstance(obj, dict) else obj
        for o in objs:
            r = analyze_run(o)
            if r:
                records.append(r)
            else:
                skipped += 1

    if not records:
        print("no usable runs (need status.phase=Succeeded with predictedTasks); "
              "skipped %d" % skipped, file=sys.stderr)
        return 1

    agg = aggregate(records)
    if args.json:
        json.dump({"aggregate": agg, "runs": records}, sys.stdout, indent=2)
        print()
    else:
        report(records, agg)
        if skipped:
            print("(%d input(s) skipped: unparseable or not Succeeded)" % skipped)
    return 0


if __name__ == "__main__":
    sys.exit(main())
