"""Run the GDTM template once (gen_gdtm.py), report per-stage timing and
placement, and check the pipeline's detections and track against
reference.py's unsplit run on the same window.

Usage: gdtmrun.py <mode> <frames> <reference.npz> [--start 0] [--scheduler heft] [--tag x]
"""
import argparse
import io
import json
import os
import subprocess
import sys
import time
import urllib.request

import numpy as np

sys.path.insert(0, os.path.expanduser("~/wayline-build-vertex/examples/gdtm"))
import gen_gdtm as G  # noqa: E402

K = "kubectl -n wl-system "
WL = os.path.expanduser("~/wayline/bin/wayline")
OUT = os.path.expanduser("~/E15-results")
sh = lambda c, i=None: subprocess.run(c, shell=True, capture_output=True, text=True, input=i)


def ts(x):
    from datetime import datetime
    return datetime.fromisoformat(x.replace("Z", "+00:00")).timestamp() if x else None


def pod_lifecycle(run):
    """Per task pod: created, container started/finished (unix s) and the
    kubelet's image-pull message. Warm calls have no pod."""
    out = {}
    items = json.loads(sh(K + f"get pods -l wl-odag={run} -o json").stdout or "{}").get("items", [])
    for p in items:
        task = p["metadata"]["labels"].get("wl-task", "")
        cs = (p["status"].get("containerStatuses") or [{}])[0].get("state", {})
        st = cs.get("terminated") or cs.get("running") or {}
        ev = sh(K + f"get events --field-selector involvedObject.name={p['metadata']['name']},reason=Pulled "
                "-o jsonpath='{.items[0].message}'").stdout
        pull = ev.split(" in ")[1].split(" (")[0] if " in " in ev else ""
        cond = {c["type"]: c for c in p["status"].get("conditions", [])}
        ready = cond.get("Ready", {})
        out[task] = {"created": ts(p["metadata"]["creationTimestamp"]), "started": ts(st.get("startedAt")),
                     "finished": ts(st.get("finishedAt")), "phase": p["status"].get("phase"), "pull": pull,
                     # when the kubelet posted the exit (Ready -> False, reason PodCompleted)
                     "reported": ts(ready.get("lastTransitionTime")) if ready.get("status") == "False" else None}
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode")
    ap.add_argument("frames", type=int)
    ap.add_argument("reference")
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--scheduler", default="heft")
    ap.add_argument("--tag", default="")
    a = ap.parse_args()
    name = f"gdtm-{a.mode}"
    r = sh(K + "apply -f -", G.template(name, a.mode, a.frames, a.start, a.scheduler))
    assert r.returncode == 0, r.stderr
    t0 = time.time()
    run = sh(f"{WL} run {name} -n wl-system").stdout.split("Created run ")[1].split()[0]
    ph = ""
    while time.time() - t0 < 900:
        ph = sh(K + f"get odag {run} -o jsonpath='{{.status.phase}}'").stdout
        if ph in ("Succeeded", "Failed"):
            break
        time.sleep(0.25)
    wall = time.time() - t0
    done_seen = time.time()
    st = json.loads(sh(K + f"get odag {run} -o json").stdout)["status"]
    pl = {t["name"]: t.get("node", "") for t in st["tasks"]}
    ips = json.loads(sh(K + "get pods -l app=data-agent -o json").stdout)
    ips = {p["spec"]["nodeName"]: p["status"]["podIP"] for p in ips["items"]}
    get = lambda u: urllib.request.urlopen(u, timeout=30).read()
    T = {}
    for t, n in pl.items():
        try:
            T[t] = json.loads(get(f"http://{ips[n]}:8082/timings/{run}/{t}"))
        except Exception as e:  # a failed task has no timing
            T[t] = {"error": str(e)}
    pods = pod_lifecycle(run)
    ok = {t: x for t, x in T.items() if "closeUnix" in x}
    last = max((x["closeUnix"] for x in ok.values()), default=t0)
    est = (st.get("scheduling") or {}).get("externalEstimatedMakespan")
    print(f"{name} {run}: {ph}  makespan {last - t0:.2f}s  wall {wall:.2f}s  estimate {est}")
    for t in sorted(pl, key=lambda t: ok.get(t, {}).get("taskStartUnix", 1e20)):
        x = T[t]
        if "closeUnix" not in x:
            print(f"  {t:16} {pl[t]:7} {x}")
            continue
        p = pods.get(t)
        pod = (f"  pod created +{p['created'] - t0:5.1f} container +{p['started'] - t0:5.1f}"
               f" pull {p['pull']}" if p and p.get("started") else "  function" if not p else f"  pod {p}")
        print(f"  {t:16} {pl[t]:7} start +{x['taskStartUnix'] - t0:6.2f}  compute {x['computeSeconds']:6.2f}s"
              f"  close +{x['closeUnix'] - t0:6.2f}{pod}")
    check = {}
    if ph == "Succeeded":
        ref = np.load(a.reference)
        det = dict(np.load(io.BytesIO(get(f"http://{ips[pl['fuse']]}:8082/{run}/fuse.det/output"))))
        trk = dict(np.load(io.BytesIO(get(f"http://{ips[pl['track']]}:8082/{run}/track.track/output"))))
        d = lambda x, y: float(np.max(np.abs(np.asarray(x, np.float64) - np.asarray(y, np.float64))))
        err = np.linalg.norm(trk["pos"] - ref["gt_pos"], axis=1)
        check = {"det_means_maxdiff_cm": d(det["means"], ref["det_means"]),
                 "det_covs_maxdiff": d(det["covs"], ref["det_covs"]),
                 "track_maxdiff_cm": d(trk["pos"], ref["track_means"]),
                 "track_error_cm_mean": float(err.mean()),
                 "reference_track_error_cm_mean": float(np.linalg.norm(ref["track_means"] - ref["gt_pos"], axis=1).mean())}
        print("check vs unsplit reference:", json.dumps(check))
    os.makedirs(OUT, exist_ok=True)
    json.dump({"mode": a.mode, "frames": a.frames, "scheduler": a.scheduler, "tag": a.tag, "run": run, "phase": ph,
               "makespan": last - t0, "wall": wall, "estimate": est, "placement": pl, "timings": T, "pods": pods,
               "check": check, "submitted": t0},
              open(f"{OUT}/{name}-{a.tag + '-' if a.tag else ''}{int(t0)}.json", "w"), indent=1)
    sh(f"{WL} delete {run} -n wl-system")


if __name__ == "__main__":
    main()
