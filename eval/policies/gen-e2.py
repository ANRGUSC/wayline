#!/usr/bin/env python3
"""Render the E2 expressiveness templates: freed constraints x schedulers.

The stock benchmarks pin most tasks (iot pins 9/14), which collapses every
scheduler to the same placement and makes ranking undefined (measured in
E8). The freed variants keep only the pins with a physical meaning:

  iot     capture-* stay pinned (sensors are attached to nodes);
          preprocess/infer/fuse/report are free.
  hetero  everything free (runtimeProfile hints remain).
  wpf     source (ingest at anrg-1) and output (gateway anrg-9) stay
          pinned; the pipeline body is free.

One template per (dag, scheduler). Schedulers span distinct objectives:
makespan (heft builtin, saga/cpop), greedy (saga/minmin), load balance
(saga/olb), communication minimization (the user-defined data-gravity
class loaded by dotted path), and random as the control.
"""
import copy
import os
import sys

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
SCHED_DIR = os.path.join(HERE, "..", "synthetic-dags", "scheduler")
OUT = os.path.join(HERE, "e2")

KEEP = {
    "iot": lambda t: t["name"].startswith("capture-"),
    "hetero-compute": lambda t: False,
    "wide-pipeline-flex": lambda t: t["name"] in ("source", "output"),
}
SCHEDULERS = {
    "heft": "heft",
    "cpop": "saga/cpop",
    "minmin": "saga/minmin",
    "olb": "saga/olb",
    "gravity": "saga/datagravity.DataGravityScheduler",
    "random": "random",
}

os.makedirs(OUT, exist_ok=True)
rendered = []
for dag, keep in KEEP.items():
    base = yaml.safe_load(open(f"{SCHED_DIR}/{dag}/template-heft.yml"))
    short = {"iot": "iot", "hetero-compute": "het",
             "wide-pipeline-flex": "wpf"}[dag]
    for slug, sched in SCHEDULERS.items():
        t = copy.deepcopy(base)
        t["metadata"]["name"] = f"e2-{short}-{slug}"
        t["spec"]["scheduler"] = sched
        t["spec"].pop("schedulerConfig", None)
        for task in t["spec"]["tasks"]:
            if not keep(task):
                task.pop("constraints", None)
        path = os.path.join(OUT, f"e2-{short}-{slug}.yml")
        yaml.safe_dump(t, open(path, "w"), sort_keys=False)
        rendered.append(os.path.basename(path))
print("rendered:", len(rendered), "templates")
