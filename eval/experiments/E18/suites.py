"""E18 suites: which cells run, on which lane.

ROWS is E16's headline matrix (matrix_e16.py), plus its sweep and window
cells and the partition fault E16 planned but never ran. A row is one
scenario and dispatch; each arm column names the workload the arm runs, or
"=Arm" for a cell that would repeat another arm (W-frozen equals W without a
fault, W-replan equals W-plan-only without a degraded link). The report
(matrix_e18.py) reads the same table.

Faults name roles, never hosts (lanes.py resolves them per lane):
  ("crash", victim, trigger task, delay s)      k3s-killall on the victim, delay s after the trigger finished
  ("crash2", victim, trigger, delay, consumer, delay2)
                                                then the node the consumer was moved to, delay2 s after it restarts there
  ("partition", victim, trigger, delay)         the victim's data VLAN cut both ways (eclipse too); healed after the run
  ("link", node, mbit)                          every link of node at mbit right after planning
  ("link-pair", u, v, mbit)                     only u <-> v
"""
import arms
import images

CRASH_SENSOR = {"iot": ("crash", "camera#4", "preprocess-4", 1.0),
                "iot-pi": ("crash", "camera#4", "preprocess-4", 1.0),
                "gdtm": ("crash", "sensor#2", "prep-zed-n2", 2.0),
                "focal": ("crash", "source#2", "fft-audio", 0.3)}
CRASH_CHAIN = ("crash", "victim", "p", 3.0)
CRASH2 = ("crash2", "victim", "p", 3.0, "c", 3.0)
LINK = ("link", "victim", 4.0)
LINK_PAIR = ("link-pair", "victim", "dst", 4.0)
PARTITION = ("partition", "victim", "p", 3.0)
WINDOW_DELAY = {"chain-r12-s100": 3.0, "chain-r30-s100": 10.0}

# repo:tag in kube.REG, by images.py's set (mx1: wl-ray-baseline:2.58.0-mx1, amd64 + arm64)
RAY_IMAGES = {"plain": images.repo_tag("wl-ray-baseline"), "gdtm": images.repo_tag("wl-gdtm-ray"),
              "focal": images.repo_tag("wl-focal-ray")}


def base(workload):
    return workload[:-5] if workload.endswith("-warm") else workload


def fault_for(claim, workload):
    if claim == "cost":
        return None
    if claim == "survival":
        return CRASH_SENSOR[base(workload)]
    if claim in ("recovery", "sweep"):
        return CRASH_CHAIN
    if claim == "window":
        return ("crash", "victim", "p", WINDOW_DELAY[workload])
    if claim == "revision":
        return CRASH2
    if claim == "adaptation":
        return LINK
    if claim == "adaptation-copy":
        return LINK_PAIR
    if claim == "partition":
        return PARTITION
    if claim.startswith("linkrate"):          # the degradation sweep: the producer's links at <mbit> after planning
        return ("link", "victim", float(claim[len("linkrate"):]))
    raise KeyError(claim)


def ray_image_kind(workload):
    return {"gdtm": "gdtm", "focal": "focal"}.get(base(workload), "plain")


_W4 = ["W", "W-plan-only", "W-static", "W-frozen"]


def _row(rid, scenario, dispatch, claim, cols):
    return {"id": rid, "scenario": scenario, "dispatch": dispatch, "claim": claim, "arms": cols}


def _wl(workload, frozen_same=False, replan_same=True, static=True):
    cols = {"W": workload, "W-plan-only": workload}
    if static:
        cols["W-static"] = workload
    cols["W-frozen"] = "=W" if frozen_same else workload
    cols["W-replan"] = "=W-plan-only" if replan_same else workload
    return cols


ROWS = [
    _row("cost/gdtm", "No fault", "GDTM (embeds as pods)", "cost",
         {**_wl("gdtm"), "Argo": "gdtm", "Argo+oos": "=Argo"}),
    _row("cost/gdtm-warm", "No fault", "GDTM (warm)", "cost",
         {**_wl("gdtm-warm", frozen_same=True), "Ray": "gdtm", "Ray+copies": "gdtm"}),
    _row("cost/iot", "No fault", "iot (pods)", "cost",
         {**_wl("iot"), "Argo": "iot", "Argo+oos": "=Argo"}),
    _row("cost/iot-warm", "No fault", "iot (warm)", "cost",
         {**_wl("iot-warm", frozen_same=True), "Ray": "iot", "Ray+copies": "iot", "Dask": "iot", "Dask+copies": "iot"}),
    _row("cost/focal", "No fault", "FOCAL (encoders as pods)", "cost",
         {**_wl("focal", frozen_same=True), "Argo": "focal", "Argo+oos": "=Argo"}),
    _row("cost/focal-warm", "No fault", "FOCAL (warm)", "cost",
         {**_wl("focal-warm", frozen_same=True), "Ray": "focal", "Ray+copies": "focal"}),
    _row("survival/gdtm", "Sensor node crashed mid-run", "GDTM (embeds as pods)", "survival",
         {**_wl("gdtm"), "Argo": "gdtm", "Argo+oos": "gdtm"}),
    _row("survival/gdtm-warm", "Sensor node crashed mid-run", "GDTM (warm)", "survival",
         {**_wl("gdtm-warm"), "Ray": "gdtm", "Ray+copies": "gdtm"}),
    _row("survival/iot", "Sensor node crashed mid-run", "iot (pods)", "survival",
         {**_wl("iot"), "Argo": "iot", "Argo+oos": "iot"}),
    _row("cost/iot-pi-warm", "No fault", "iot-pi (warm)", "cost",
         {**_wl("iot-pi-warm", frozen_same=True), "Ray": "iot-pi", "Ray+copies": "iot-pi",
          "Dask": "iot-pi", "Dask+copies": "iot-pi"}),
    _row("survival/iot-pi-warm", "Sensor node crashed mid-run", "iot-pi (warm)", "survival",
         {**_wl("iot-pi-warm"), "Ray": "iot-pi", "Ray+copies": "iot-pi", "Dask": "iot-pi", "Dask+copies": "iot-pi"}),
    _row("cost/iot-pi", "No fault", "iot-pi (pods)", "cost", {**_wl("iot-pi"), "Argo": "iot-pi", "Argo+oos": "=Argo"}),
    _row("survival/iot-pi", "Sensor node crashed mid-run", "iot-pi (pods)", "survival",
         {**_wl("iot-pi"), "Argo": "iot-pi", "Argo+oos": "iot-pi"}),
    _row("survival/iot-warm", "Sensor node crashed mid-run", "iot (warm)", "survival",
         {**_wl("iot-warm"), "Ray": "iot", "Ray+copies": "iot", "Dask": "iot", "Dask+copies": "iot"}),
    _row("survival/focal", "Source node crashed mid-run", "FOCAL (encoders as pods)", "survival",
         {**_wl("focal"), "Argo": "focal", "Argo+oos": "focal"}),
    _row("survival/focal-warm", "Source node crashed mid-run", "FOCAL (warm)", "survival",
         {**_wl("focal-warm"), "Ray": "focal", "Ray+copies": "focal"}),
    _row("recovery/chain", "Co-located 12 s producer's node crashed", "chain (pods)", "recovery",
         {**_wl("chain"), "Argo": "chain", "Argo+oos": "chain"}),
    _row("recovery/chain-warm", "Co-located 12 s producer's node crashed", "chain (warm)", "recovery",
         {**_wl("chain-warm"), "Ray": "chain", "Ray+copies": "chain", "Dask": "chain", "Dask+copies": "chain",
          "Ray+policy": "chain", "Dask+policy": "chain"}),
    _row("revision/chain-warm", "Second loss (the consumer's new node too)", "chain (warm; Argo pods)", "revision",
         {**_wl("chain-warm"), "Ray": "chain", "Ray+copies": "chain", "Argo": "chain", "Argo+oos": "chain",
          "Dask": "chain", "Dask+copies": "chain",
          "Ray+policy": "chain", "Dask+policy": "chain"}),
    _row("adaptation/chain-link", "Producer's links throttled after planning", "chain-link (pods)", "adaptation",
         {**_wl("chain-link", replan_same=False), "Ray": "chain-link", "Ray+copies": "=Ray",
          "Argo": "chain-link", "Argo+oos": "=Argo", "Dask": "chain-link", "Dask+copies": "=Dask"}),
    _row("adaptation-copy/fan", "Same, with a second copy on a healthy link", "fan (pods)", "adaptation-copy",
         {**_wl("fan", replan_same=False), "Ray": "fan", "Ray+copies": "=Ray", "Argo": "fan", "Argo+oos": "=Argo",
          "Dask": "fan", "Dask+copies": "=Dask"}),
    _row("partition/chain-warm", "Producer's node cut off the data plane", "chain (warm)", "partition",
         {**_wl("chain-warm")}),
]
for _r, _mb in [(r, mb) for r in (2, 12, 30) for mb in (8, 100)]:
    ROWS.append(_row(f"sweep/chain-r{_r}-s{_mb}", f"Sweep: producer {_r} s, {_mb} MB, its node crashed",
                     "chain (warm)", "sweep", {"W": f"chain-r{_r}-s{_mb}", "W-plan-only": f"chain-r{_r}-s{_mb}"}))
ROWS += [
    _row("window/chain-r12-s100", "Copy landing window: 12 s producer, 100 MB, crash 3 s after", "chain (warm)",
         "window", {"W": "chain-r12-s100"}),
    _row("window/chain-r30-s100", "Copy landing window: 30 s producer, 100 MB, crash 10 s after", "chain (warm)",
         "window", {"W": "chain-r30-s100", "W-plan-only": "chain-r30-s100"}),
]
# The degradation sweep (the 4 Mbit/s point is adaptation/chain-link itself).
LINK_SWEEP_MBIT = (64, 32, 16, 8)
for _mbit in LINK_SWEEP_MBIT:
    ROWS.append(_row(f"linksweep/chain-link-{_mbit}", f"Producer's links at {_mbit} Mbit/s after planning",
                     "chain-link (pods)", f"linkrate{_mbit}",
                     {"W": "chain-link", "W-plan-only": "chain-link", "W-static": "chain-link"}))
LINK_SWEEP_ROWS = [f"linksweep/chain-link-{m}" for m in LINK_SWEEP_MBIT]
ROW_BY_ID = {r["id"]: r for r in ROWS}
MIXED_ROWS = [r["id"] for r in ROWS if not any(w in r["id"] for w in ("gdtm", "focal"))]
IOT_ROWS = [r for r in MIXED_ROWS if "/iot" in r]
RD_ROWS = [r for r in MIXED_ROWS if not (r.startswith(("cost/iot", "survival/iot")) and "iot-pi" not in r)]

SUITES = {
    # Harness check on three UP boards with pod tasks only (no runner
    # DaemonSet needed) and no node crash (that needs the controller's
    # node-loss detection on): iot without a fault, one degraded link.
    "smoke": {"lane": "smoke", "rows": ["cost/iot", "adaptation/chain-link"],
              "arms": ["W", "W-plan-only"], "systems": ["wayline"]},
    # E16's whole matrix on E16's eight boards and link classes. The lane is
    # a subset of the 65 workers and W-static places copies cluster-wide, so
    # these suites cordon the other workers while they run.
    "e16": {"lane": "e16", "rows": None, "fence": "cordon"},
    "e16-wayline": {"lane": "e16", "rows": None, "systems": ["wayline"], "fence": "cordon"},
    "e16-baselines": {"lane": "e16", "rows": None, "systems": ["ray", "argo", "dask"]},
    # Heterogeneous lanes: what runs once the images exist for arm64 (see README).
    "mixed16": {"lane": "mixed16",
                "rows": ["cost/iot-warm", "survival/iot-warm", "recovery/chain-warm", "revision/chain-warm",
                         "adaptation/chain-link", "adaptation-copy/fan", "partition/chain-warm"],
                "systems": ["wayline"]},
    # Mixed lanes run the rows whose images exist for arm64 (iot and the
    # chain family); GDTM's model and FOCAL's runner stay on UP-only lanes.
    "mixA": {"lane": "mixA", "rows": MIXED_ROWS, "systems": ["wayline"], "fence": "nodes"},
    "mixB": {"lane": "mixB", "rows": MIXED_ROWS, "systems": ["wayline"], "fence": "nodes"},
    "mixC": {"lane": "mixC", "rows": MIXED_ROWS, "systems": ["wayline"], "fence": "nodes"},
    # Baselines on the mixed lanes. Ray and Dask leave eclipse's links alone
    # (their data moves between workers); Argo's repository is on eclipse, so
    # its suite shapes eclipse's links and must not run beside another such suite.
    # iot rows again after the compute stages got per-class runtimes (2026-11-08 evening), on every mixed lane.
    "mixA-iot": {"lane": "mixA", "rows": IOT_ROWS, "systems": ["wayline"], "fence": "nodes"},
    "mixB-iot": {"lane": "mixB", "rows": IOT_ROWS, "systems": ["wayline"], "fence": "nodes"},
    "mixC-iot": {"lane": "mixC", "rows": IOT_ROWS, "systems": ["wayline"], "fence": "nodes"},
    # Ray and Dask without iot's 80-150 MB captures: Ray's workers on a 1 GB Pi 3 were killed for lack of memory
    # (kernel OOM on rpi2, rpi5 rebooted, 2026-11-08 20:01); they run iot-pi and the chains.
    "mixA-rd": {"lane": "mixA", "rows": RD_ROWS, "systems": ["ray", "dask"], "fence": "nodes"},
    "mixB-rd": {"lane": "mixB", "rows": RD_ROWS, "systems": ["ray", "dask"], "fence": "nodes"},
    "mixC-argo": {"lane": "mixC", "rows": MIXED_ROWS, "systems": ["argo"], "fence": "nodes"},
    # Ray and Dask given Wayline's repair decisions (where to re-run the consumer, where to re-protect its input),
    # read from the lane's W runs of the same row: the runtime held apart from the policy (2026-11-11).
    "mixA-pol": {"lane": "mixA", "rows": ["recovery/chain-warm", "revision/chain-warm"],
                 "arms": ["Ray+policy", "Dask+policy"], "fence": "nodes"},
    "mixB-pol": {"lane": "mixB", "rows": ["recovery/chain-warm", "revision/chain-warm"],
                 "arms": ["Ray+policy", "Dask+policy"], "fence": "nodes"},
    # The degradation sweep of the data-control result, on each mixed lane.
    "mixA-sweep": {"lane": "mixA", "rows": LINK_SWEEP_ROWS, "systems": ["wayline"], "fence": "nodes"},
    "mixB-sweep": {"lane": "mixB", "rows": LINK_SWEEP_ROWS, "systems": ["wayline"], "fence": "nodes"},
    "mixC-sweep": {"lane": "mixC", "rows": LINK_SWEEP_ROWS, "systems": ["wayline"], "fence": "nodes"},
    "all65": {"lane": "all",
              "rows": ["cost/iot-warm", "survival/iot-warm", "recovery/chain-warm", "adaptation/chain-link"],
              "systems": ["wayline"]},
}


def cells(rows=None, systems=None, arms_=None, claims=None, workloads=None):
    """[(cell dict)] of the selected rows, without repeats."""
    out, seen = [], set()
    for row in ROWS:
        if rows is not None and row["id"] not in rows:
            continue
        if claims and row["claim"] not in claims:
            continue
        for arm, src in row["arms"].items():
            if src.startswith("="):
                continue
            if arms_ and arm not in arms_:
                continue
            sysname = arms.system(arm)
            if systems and sysname not in systems:
                continue
            if workloads and src not in workloads:
                continue
            key = (row["claim"], src, arm)
            if key in seen:
                continue
            seen.add(key)
            flt = fault_for(row["claim"], src)
            out.append({
                "id": f"{row['claim']}-{src}-{arm}", "row": row["id"], "claim": row["claim"], "workload": src,
                "arm": arm, "system": sysname, "fault": flt,
                # Plain Argo does not recover from a crashed node (E16): its
                # run is cut at the time limit, and that is its result.
                "expect_hang": arm == "Argo" and bool(flt) and flt[0] in ("crash", "crash2"),
                "group": sysname if sysname != "ray" else f"ray:{ray_image_kind(src)}",
            })
    return out


def suite_cells(name, **filters):
    s = SUITES[name]
    f = {"rows": s.get("rows"), "systems": s.get("systems"), "arms_": s.get("arms")}
    f.update({k: v for k, v in filters.items() if v})
    return cells(**f)
