#!/usr/bin/env python3
"""Freeze the HEFT and MaxTP reference schedules for E5.

Builds the SAGA request from the exact DAG and symmetric network, posts
it to the live sidecar, and archives placement, per-node order, times,
raw request/response, and a canonical schedule hash. Refuses to freeze a
schedule that places an application task on the gateway.

Runs ON anrg-2. Usage: freeze_e5.py <out-dir>
"""
import hashlib
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gen_e5 as G  # noqa: E402

NS = "wl-system"
ALGOS = {"heft": "heft",
         "maxtp": "saga.schedulers.throughput.maxtp.MaxTPScheduler"}


def build_request(algorithm):
    tasks = []
    for t in G.ORDER:
        entry = {
            "name": t,
            "dependencies": sorted({p for p, _ in G.INPUTS.get(t, [])}),
            "runtime": G.WORK[t],
            "runtimeProfile": G.runtime_profile(t),
        }
        inputs = []
        for prod, obj in G.INPUTS.get(t, []):
            size = next(s for o, s in G.OUTPUTS[prod] if o == obj)
            inputs.append({"producer": prod, "object": obj, "bytes": size})
        if inputs:
            entry["inputs"] = inputs
        tasks.append(entry)
    bandwidth = [{"from": u, "to": v, "bytesPerSec": G.bps(G.rate(u, v))}
                 for u in G.NODES for v in G.NODES if u != v]
    nodes = [{"name": n, "ready": True, "cpuMillis": 8000,
              "memBytes": 17179869184} for n in G.NODES]
    return {"algorithm": algorithm, "dag": {"tasks": tasks},
            "clusterState": {"nodes": nodes, "bandwidth": bandwidth}}


def post(req):
    pod = subprocess.run(
        ["kubectl", "-n", NS, "get", "pod", "-l", "app=odag-controller",
         "-o", "jsonpath={.items[0].metadata.name}"],
        capture_output=True, text=True).stdout.strip()
    script = ("import json,sys,urllib.request;"
              "req=json.load(sys.stdin);"
              "r=urllib.request.urlopen(urllib.request.Request("
              "'http://127.0.0.1:8090/schedule',data=json.dumps(req).encode(),"
              "headers={'Content-Type':'application/json'}),timeout=60);"
              "print(json.dumps(json.load(r)))")
    p = subprocess.run(
        ["kubectl", "-n", NS, "exec", "-i", pod, "-c", "saga-sidecar",
         "--", "python3", "-c", script],
        input=json.dumps(req), capture_output=True, text=True, timeout=180)
    if p.returncode != 0:
        raise SystemExit(f"sidecar call failed: {p.stderr[:400]}")
    return json.loads(p.stdout)


def canonical_hash(placement, order):
    canon = json.dumps({"placement": dict(sorted(placement.items())),
                        "order": {k: v for k, v in sorted(order.items())}},
                       sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canon.encode()).hexdigest()


def main():
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    os.makedirs(out_dir, exist_ok=True)
    summary = {}
    for short, algo in ALGOS.items():
        req = build_request(algo)
        resp = post(req)
        placement = {a["task"]: a["node"] for a in resp["assignments"]}
        times = {a["task"]: [a["estimatedStart"], a["estimatedFinish"]]
                 for a in resp["assignments"]}
        order = {}
        for a in sorted(resp["assignments"], key=lambda x: x["estimatedStart"]):
            order.setdefault(a["node"], []).append(a["task"])
        on_gateway = [t for t, n in placement.items() if n == G.GATEWAY]
        h = canonical_hash(placement, order)
        rec = {"algorithm": algo, "algorithm_short": short,
               "placement": placement, "order": order, "times": times,
               "estimatedMakespan": resp.get("estimatedMakespan"),
               "costModelFitRMSE": resp.get("costModelFitRMSE"),
               "constraintOverrides": resp.get("constraintOverrides", []),
               "schedule_hash": h, "raw_request": req, "raw_response": resp}
        with open(os.path.join(out_dir, f"frozen-{short}.json"), "w") as f:
            json.dump(rec, f, indent=1)
        summary[short] = {"hash": h, "makespan": resp.get("estimatedMakespan"),
                          "rmse": resp.get("costModelFitRMSE"),
                          "on_gateway": on_gateway, "placement": placement,
                          "order": order}
        print(f"=== {short} ({algo})")
        print(f"  hash      {h[:16]}  est. makespan "
              f"{resp.get('estimatedMakespan'):.2f}s  rmse "
              f"{resp.get('costModelFitRMSE'):.2e}")
        for n, ts in sorted(order.items()):
            print(f"  {n:8} {' -> '.join(ts)}")
        if on_gateway:
            print(f"  !! application tasks on the gateway: {on_gateway}")
    bad = {k: v["on_gateway"] for k, v in summary.items() if v["on_gateway"]}
    if bad:
        raise SystemExit(f"STOP: frozen schedules place tasks on "
                         f"{G.GATEWAY}: {bad}")
    same = (summary["heft"]["hash"] == summary["maxtp"]["hash"])
    print(f"\nHEFT and MaxTP schedules "
          f"{'are IDENTICAL (workload may not discriminate)' if same else 'DIFFER'}")
    with open(os.path.join(out_dir, "frozen-summary.json"), "w") as f:
        json.dump(summary, f, indent=1)


if __name__ == "__main__":
    main()
