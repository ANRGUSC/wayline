#!/usr/bin/env python3
"""Is OLB's schedule a property of the algorithm, or of tie ordering?

Pinning PYTHONHASHSEED makes scheduling reproducible, but it also locks
in ONE arbitrary tie-breaking order. If that order happens to be
unfavourable to a baseline, every block reproduces it exactly and the
baseline looks bad with zero measured variance to reveal it. That would
understate a baseline we are comparing ourselves against, so it has to
be checked rather than assumed.

This asks the scheduler directly, without running any DAG: for each of
N hash seeds, restart the sidecar and record the placement OLB returns
for the same template. Then report how many distinct placements appear
and how often each node is used.

  all seeds agree      -> the schedule is the algorithm's, and reporting
                          a single OLB number is fair
  seeds disagree       -> OLB is tie-break sensitive; report it with the
                          spread across seeds, not as a point estimate

Usage: olb_seed_sensitivity.py [template] [n_seeds]
Run on anrg-2. Restarts the controller deployment N times, so do NOT run
this while a campaign is in flight.
"""
import collections
import json
import subprocess
import sys

NS = "wl-system"
TEMPLATE = sys.argv[1] if len(sys.argv) > 1 else "e5-olb"
NSEEDS = int(sys.argv[2]) if len(sys.argv) > 2 else 5
GATEWAY = "anrg-9"


def sh(cmd, timeout=420):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout)


def set_seed(seed):
    sh(f"kubectl -n {NS} set env deploy/odag-controller -c saga-sidecar "
       f"PYTHONHASHSEED={seed}")
    sh(f"kubectl -n {NS} rollout status deploy/odag-controller --timeout=300s")


def placement_for():
    """Submit one run, read the placement the scheduler chose, delete it."""
    r = sh(f"/home/anrg/wayline/bin/wayline run {TEMPLATE} -n {NS}")
    run = ""
    for tok in (r.stdout + r.stderr).split():
        if tok.startswith(f"{TEMPLATE}-run-"):
            run = tok.strip().strip('",')
            break
    if not run:
        return None, None
    for _ in range(150):
        p = sh(f"kubectl -n {NS} get odag {run} "
               f"-o jsonpath='{{.status.phase}}'").stdout.strip()
        if p in ("Succeeded", "Failed"):
            break
        sh("sleep 2")
    out = sh(f"kubectl -n {NS} get odag {run} -o json").stdout
    sh(f"kubectl -n {NS} delete odag {run}")
    try:
        d = json.loads(out)
    except json.JSONDecodeError:
        return None, run
    st = d.get("status", {})
    return ({t["name"]: t.get("node")
             for t in st.get("tasks", []) if t.get("node")},
            st.get("makespan"))


def main():
    try:
        sweep()
    finally:
        # Always hand the cluster back at the campaign seed. Printing a
        # reminder is not enough: this script leaves the sidecar on the
        # LAST seed it tried, and a campaign started on that seed would
        # silently fail every frozen-schedule check.
        print("\nrestoring PYTHONHASHSEED=0 ...")
        set_seed(0)
        print("restored.")


def sweep():
    seen = collections.Counter()
    makespans = []
    node_use = collections.Counter()
    gw_runs = 0
    print(f"template={TEMPLATE} seeds=0..{NSEEDS - 1}\n")
    for seed in range(NSEEDS):
        set_seed(seed)
        place, mk = placement_for()
        if not place:
            print(f"  seed {seed}: FAILED to schedule")
            continue
        makespans.append((seed, mk))
        key = json.dumps(place, sort_keys=True)
        seen[key] += 1
        for n in place.values():
            node_use[n] += 1
        on_gw = [t for t, n in place.items() if n == GATEWAY]
        gw_runs += bool(on_gw)
        print(f"  seed {seed}: {len(set(place.values()))} nodes, "
              f"{len(on_gw)} task(s) on {GATEWAY} {sorted(on_gw)}, "
              f"makespan={mk}")

    print(f"\ndistinct placements across {sum(seen.values())} seed(s): "
          f"{len(seen)}")
    for key, n in seen.most_common():
        print(f"  x{n}  {key}")
    print(f"\nnode usage (task-placements summed over seeds): "
          f"{dict(node_use.most_common())}")
    print(f"runs placing work on {GATEWAY}: {gw_runs}/{sum(seen.values())}")
    if len(seen) == 1:
        print("\nVERDICT: schedule is seed-independent. Reporting a single "
              "number for this policy is fair.")
    else:
        print("\nVERDICT: schedule is TIE-BREAK SENSITIVE. Report this "
              "policy with its across-seed spread, not a point estimate.")
    mk = [m for _, m in makespans if isinstance(m, (int, float))]
    if mk:
        print(f"\nmakespan across seeds: min={min(mk)} max={max(mk)} "
              f"median={sorted(mk)[len(mk)//2]}  (n={len(mk)})")
        print("  per seed:", makespans)



if __name__ == "__main__":
    main()
