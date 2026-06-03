#!/usr/bin/env python3
"""
Record one rep's results for the heterogeneity campaign.

Reads the completed ODAG's status JSON on stdin, plus a (readable) snapshot of
the profiler DB, and appends:
  results/<cell>/makespan.csv   cell,rep,mode,net,phase,actual_ms,predicted_ms,wall_s,n_tasks_ok
  results/<cell>/profiles.csv   cell,rep,task,node,runtime_learned,samples
  results/<cell>/runs/run-<rep>.json   full status (for later deep analysis)

Convergence is then: profile error / makespan as a function of rep within a cell.

Usage: record.py <cell> <rep> <mode> <net> <wall_s> <template> <snapdb> <results_dir>  < odag.json
"""
import sys
import os
import json
import csv
import sqlite3
import datetime as dt


def parse_ts(s):
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def main():
    cell, rep, mode, net, wall_s, template, snapdb, rdir = sys.argv[1:9]
    rep = int(rep)
    status = json.load(sys.stdin).get("status", {})
    tasks = status.get("tasks", [])
    pred = status.get("predictedTasks", [])

    phase = status.get("phase", "?")
    actual_ms = status.get("makespan", "")
    predicted_ms = ""
    if pred:
        ends = [p.get("estEnd", 0) for p in pred]
        if ends:
            predicted_ms = round(max(ends), 1)
    n_ok = sum(1 for t in tasks if t.get("phase") == "Succeeded")

    celldir = os.path.join(rdir, cell)
    os.makedirs(os.path.join(celldir, "runs"), exist_ok=True)

    # full status dump
    json.dump(status, open(os.path.join(celldir, f"runs/run-{rep:02d}.json"), "w"), indent=1)

    # makespan.csv
    msf = os.path.join(celldir, "makespan.csv")
    new = not os.path.exists(msf)
    with open(msf, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["cell", "rep", "mode", "net", "phase",
                        "actual_ms", "predicted_ms", "wall_s", "n_tasks_ok"])
        w.writerow([cell, rep, mode, net, phase, actual_ms, predicted_ms, wall_s, n_ok])

    # per-task actual runtime (completion - start), for predicted-vs-actual later
    pred_rt = {}
    for p in pred:
        st, en = p.get("estStart"), p.get("estEnd")
        if st is not None and en is not None:
            pred_rt[p.get("name")] = round(en - st, 1)
    taskf = os.path.join(celldir, "tasks.csv")
    new = not os.path.exists(taskf)
    with open(taskf, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["cell", "rep", "task", "node", "actual_rt_s", "pred_rt_s"])
        for t in tasks:
            s, c = parse_ts(t.get("startTime")), parse_ts(t.get("completionTime"))
            art = round((c - s).total_seconds(), 1) if (s and c) else ""
            w.writerow([cell, rep, t.get("name"), t.get("node", ""), art,
                        pred_rt.get(t.get("name"), "")])

    # profiles.csv (learned per-(task,node) runtime + sample count)
    prof = os.path.join(celldir, "profiles.csv")
    new = not os.path.exists(prof)
    try:
        c = sqlite3.connect(snapdb)
        rows = c.execute(
            "select task,node,runtime,samples from task_profiles where template=?",
            (template,)).fetchall()
        c.close()
    except Exception as e:
        rows = []
        sys.stderr.write(f"profile read error: {e}\n")
    with open(prof, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["cell", "rep", "task", "node", "runtime_learned", "samples"])
        for task, node, rt, samp in rows:
            w.writerow([cell, rep, task, node, round(rt, 2), samp])

    print(f"  recorded {cell} rep{rep}: phase={phase} actual={actual_ms}s "
          f"predicted={predicted_ms}s n_ok={n_ok} profiles={len(rows)}")


if __name__ == "__main__":
    main()
