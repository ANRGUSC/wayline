"""E18 arms: every system configuration a cell can run, in one place.

The Wayline arms differ only in the run's realizationPolicy, exactly as in
E16 (PLAN.md there). A later refactor moves these policies into an external
process; swapping this module (policy() and WAYLINE) is meant to be the only
change the harness needs for that.

Baselines are the real systems E16 compared against; they differ from
Wayline in more than the policy, which the analysis must say.
"""

# realizationPolicy per Wayline arm, as in E16's e16.py ARMS.
WAYLINE = {
    "W": {"mode": "planAware", "revise": True},              # plan copies, link re-planning, re-protection
    "W-plan-only": {"mode": "none", "revise": False},        # Jupiter-style placement, lineage recovery
    "W-static": {"mode": "static", "replicas": 2, "revise": False},  # Arvados/Rucio-style replication rule
    "W-frozen": {"mode": "planAware", "revise": False},      # Iridium-style plan-time copies
    "W-replan": {"mode": "none", "revise": True},            # Tactical-Jupiter-style re-planning, no copies
}
CORE = ["W", "W-plan-only", "W-static", "W-frozen"]

# Baseline arms -> the system that runs them.
RAY = ["Ray", "Ray+copies", "Ray+policy"]      # plan-pinned Ray (lineage); + app-level copies of W's copies;
#                                                 + W's repair and re-protection decisions, enacted by the app
ARGO = ["Argo", "Argo+oos"]      # central S3 repository; + out-of-service taint once the node is NotReady
DASK = ["Dask", "Dask+copies", "Dask+policy"]   # plan-pinned Dask (lineage); + client.replicate() of W's copies;
#                                                  + W's repair and re-protection decisions, enacted by the driver

ALL = list(WAYLINE) + RAY + ARGO + DASK


def system(arm):
    """'wayline', 'ray', 'argo' or 'dask'."""
    if arm in WAYLINE:
        return "wayline"
    if arm in RAY:
        return "ray"
    if arm in ARGO:
        return "argo"
    if arm in DASK:
        return "dask"
    raise KeyError(f"unknown arm {arm!r}")


def policy(arm):
    """The run's spec.realizationPolicy for a Wayline arm (a fresh dict)."""
    return dict(WAYLINE[arm])


def ray_copies(arm):
    """Whether the Ray arm emulates W's copies in application code."""
    return arm in ("Ray+copies", "Ray+policy")


def dask_copies(arm):
    """Whether the Dask arm replicates W's copied objects (client.replicate)."""
    return arm in ("Dask+copies", "Dask+policy")


def baseline_policy(arm):
    """Whether a Ray / Dask arm is given W's repair decisions (where to re-run, where to re-protect)."""
    return arm in ("Ray+policy", "Dask+policy")


def argo_out_of_service(arm):
    """Whether the Argo arm taints a crashed node out-of-service once NotReady."""
    return arm == "Argo+oos"
