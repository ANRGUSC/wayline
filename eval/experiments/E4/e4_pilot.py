#!/usr/bin/env python3
"""E4: per-object control with named outputs. Runs ON anrg-2.

One producer emits three named objects (alert 1 MB, features 200 MB,
snapshot 200 MB); each has its own pod-less serving vertex and its own
consumers. The DAG, containers, and every compute placement are
identical across arms — only which object's serving point is rebound
to anrg-7 differs:

  fixed          no realization patch
  alert-only     produce.alert    -> anrg-7
  features-only  produce.features -> anrg-7
  snapshot-only  produce.snapshot -> anrg-7
  all-outputs    all three, in ONE patch

A single 59 Mbit/s class on anrg-3 covers destinations {7,8,9} on
tcp/8082, so direct feature fan-out and any migration to anrg-7 contend
for the same measured bottleneck. anrg-4 and anrg-6 stay clean, as does
anrg-7's own egress.
"""

import csv
import json
import os
import random
import re
import subprocess
import time

NS = "wl-system"
E4DIR = "/home/anrg/wayline-build-vertex/eval/experiments/E4"
RES = os.environ.get("RES", os.path.expanduser("~/E4-results"))
BLOCKS = int(os.environ.get("BLOCKS", "3"))
SEED = int(os.environ.get("SEED", "20260830"))
CENSOR = int(os.environ.get("CENSOR", "180"))
TARGET = "anrg-7"
ARMS = ["fixed", "alert-only", "features-only", "snapshot-only",
        "all-outputs"]
OBJECTS = {"alert-only": ["alert"], "features-only": ["features"],
           "snapshot-only": ["snapshot"],
           "all-outputs": ["alert", "features", "snapshot"], "fixed": []}
VERTICES = ["serve-alert", "serve-features", "serve-snapshot"]
CONSUMERS = ["actuator", "analyze-a", "analyze-b", "archive"]

FIELDS = ["order", "block", "arm", "run", "phase", "makespan_s",
          "cap_bytes", "cap_MB", "patches", "patch_objects",
          "copies_on_target", "serving_state",
          "vertex_exec_counts", "vertex_exec_total",
          "delivered_by_path", "consumer_verify", "digests_ok",
          "branch_times", "placements", "restarts", "target_pods",
          "cap_verified", "qdisc_clean_after", "seed"]


def sh(cmd, timeout=180):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout)


def kubectl(a, timeout=120):
    return sh(f"kubectl -n {NS} {a}", timeout=timeout)


def agent_ip(node):
    for line in kubectl("get pods -o wide --no-headers -l app=data-agent"
                        ).stdout.splitlines():
        f = line.split()
        if len(f) >= 7 and f[6] == node:
            return f[5]
    return ""


AGENTS = {}


def net(cmd):
    r = sh(f"{E4DIR}/e4_net.sh {cmd}", timeout=120)
    return (r.stdout + r.stderr).strip()


def fw_pod():
    spec = {"spec": {"nodeName": "anrg-3", "hostNetwork": True,
            "restartPolicy": "Never",
            "containers": [{"name": "c", "image": "alpine",
                            "command": ["sh", "-c",
                                        "apk add -q iproute2 >/dev/null && sleep 86400"],
                            "securityContext": {"privileged": True}}]}}
    kubectl("delete pod e4-fw-anrg-3 --ignore-not-found >/dev/null 2>&1")
    sh(f"kubectl run e4-fw-anrg-3 -n {NS} --restart=Never --image=alpine "
       f"--overrides='{json.dumps(spec)}' >/dev/null 2>&1")
    for _ in range(40):
        if "tc utility" in kubectl("exec e4-fw-anrg-3 -- sh -c "
                                   "'tc -V 2>/dev/null'").stdout:
            return
        time.sleep(3)


def flows(node, run):
    ip = AGENTS.get(node)
    if not ip:
        return []
    try:
        return json.loads(sh(f"curl -s -m 5 http://{ip}:8082/flows/{run}",
                             timeout=10).stdout)
    except (json.JSONDecodeError, ValueError):
        return []


def purge(run):
    for line in kubectl("get pods -o wide --no-headers -l app=data-agent"
                        ).stdout.splitlines():
        f = line.split()
        if len(f) >= 7 and f[6].startswith("anrg-"):
            sh(f"curl -s -m 30 -X DELETE http://{f[5]}:8082/data/{run} "
               f">/dev/null", timeout=40)


def installed_objects(node, run):
    """Which named objects of this run are installed on a node."""
    ip = AGENTS.get(node)
    if not ip:
        return []
    out = []
    for obj in ("produce.alert", "produce.features", "produce.snapshot"):
        r = sh(f"curl -s -m 3 http://{ip}:8082/ready/{run}/{obj}", timeout=8)
        if "true" in r.stdout.lower():
            out.append(obj.split(".", 1)[1])
    return out


def run_one(idx, block, arm, wcsv, f):
    if "verified" not in net("apply"):
        print(f"[e4] #{idx} {arm}: CAP NOT VERIFIED -> InfraFail", flush=True)
        wcsv.writerow([idx, block, arm, "", "InfraFail-cap"] +
                      [""] * (len(FIELDS) - 6) + [SEED])
        f.flush()
        return
    r = sh(f"/home/anrg/wayline/bin/wayline run e4objects -n {NS}")
    m = re.search(r"(e4objects-run-[a-z0-9]+)", r.stdout + r.stderr)
    if not m:
        print(f"[e4] #{idx} {arm}: SUBMIT FAILED", flush=True)
        wcsv.writerow([idx, block, arm, "", "SubmitFail"] +
                      [""] * (len(FIELDS) - 6) + [SEED])
        f.flush()
        return
    run = m.group(1)
    print(f"[e4] #{idx} block={block} {arm} -> {run}", flush=True)

    # One patch carrying every selected object, applied before the
    # producer emits anything.
    patches = 0
    objs = OBJECTS[arm]
    if objs:
        entries = [{"object": f"produce.{o}", "copies": [TARGET],
                    "servingCopy": TARGET, "evict": []} for o in objs]
        kubectl(f"patch odag {run} --type merge -p "
                f"'{json.dumps({'spec': {'realization': entries}})}'")
        patches = 1

    start = time.time()
    obj_hist, last = [], ""
    while time.time() - start < CENSOR:
        ph = kubectl(f"get odag {run} -o jsonpath='{{.status.phase}}'"
                     ).stdout.strip()
        if ph in ("Succeeded", "Failed"):
            break
        snap = kubectl(f"get odag {run} -o jsonpath='{{.status.objects}}'"
                       ).stdout.strip()
        if snap and snap != last:
            obj_hist.append((round(time.time(), 2), snap))
            last = snap
        time.sleep(1.5)

    ph = kubectl(f"get odag {run} -o jsonpath='{{.status.phase}}'"
                 ).stdout.strip()
    mk = kubectl(f"get odag {run} -o jsonpath='{{.status.makespan}}'"
                 ).stdout.strip()
    cap_bytes = net("stats") or "0"
    cap_ver = "verified" in net("verify")

    # Per-branch completion times and placements from the run object.
    odag = kubectl(f"get odag {run} -o json").stdout
    open(f"{RES}/odag-{run}.json", "w").write(odag)
    branch, placements = {}, []
    try:
        st_tasks = json.loads(odag)["status"].get("tasks", [])
        for t in st_tasks:
            if t.get("node"):
                placements.append(f"{t['name']}@{t['node']}")
            if t["name"] in CONSUMERS and t.get("completionTime"):
                branch[t["name"]] = t["completionTime"]
    except (json.JSONDecodeError, KeyError):
        st_tasks = []

    # Vertex execution counts + delivered bytes per (object, path).
    ctrl = kubectl("logs deploy/odag-controller --tail=4000").stdout
    ctrl = "\n".join(l for l in ctrl.splitlines() if run in l)
    open(f"{RES}/ctrl-{run}.log", "w").write(ctrl)
    vex = {v: len(re.findall(rf"data vertex {run}/{v} executed", ctrl))
           for v in VERTICES}

    paths = {}
    for node in ("anrg-3", TARGET):
        for fl in flows(node, run):
            if fl.get("dataSize", 0) < 1e5:
                continue
            if not fl.get("ok", fl.get("Ok", False)):
                continue
            key = f"{fl.get('fromTask', '?')}:{fl.get('srcNode')}->{fl.get('dstNode')}"
            paths[key] = paths.get(key, 0) + fl["dataSize"]

    pods = kubectl(f"get pods -o wide --no-headers | grep {run}").stdout
    target_pods = sum(1 for ln in pods.splitlines() if f" {TARGET} " in ln)
    restarts = sum(int(ln.split()[3]) for ln in pods.splitlines()
                   if len(ln.split()) >= 4 and ln.split()[3].isdigit())

    # Consumer verification lines (size + digest checked in-task).
    verify = {}
    for c in CONSUMERS:
        pname = next((ln.split()[0] for ln in pods.splitlines()
                      if f"{run}-{c}" in ln), "")
        if pname:
            lg = kubectl(f"logs {pname} 2>/dev/null").stdout
            mm = re.search(r"verify=(\w+)", lg)
            verify[c] = mm.group(1) if mm else "?"
    digests_ok = all(v == "OK" for v in verify.values()) and \
        len(verify) == len(CONSUMERS)

    copies_target = installed_objects(TARGET, run)
    with open(f"{RES}/objects-{run}.json", "w") as of:
        json.dump({"history": obj_hist, "copies_on_target": copies_target},
                  of, indent=1)

    purge(run)
    kubectl(f"delete odag {run} --ignore-not-found >/dev/null 2>&1")
    clean = "verified" in net("clear")

    wcsv.writerow([idx, block, arm, run, ph, mk, cap_bytes,
                   round(int(cap_bytes) / 1e6, 1) if cap_bytes.isdigit() else "",
                   patches, ",".join(objs), ",".join(copies_target),
                   last[:300], json.dumps(vex), sum(vex.values()),
                   json.dumps(paths), json.dumps(verify), digests_ok,
                   json.dumps(branch), ";".join(placements), restarts,
                   target_pods, cap_ver, clean, SEED])
    f.flush()
    print(f"[e4] #{idx} {arm}: {ph} makespan={mk or 'censored'} "
          f"cap={round(int(cap_bytes)/1e6) if cap_bytes.isdigit() else '?'}MB "
          f"target-copies={copies_target or 'none'} vertices={sum(vex.values())} "
          f"digests={'ok' if digests_ok else verify} pods@7={target_pods}",
          flush=True)
    time.sleep(5)


def main():
    os.makedirs(RES, exist_ok=True)
    for n in ("anrg-3", "anrg-7", "anrg-8", "anrg-9"):
        AGENTS[n] = agent_ip(n)
    fw_pod()
    net("clear")
    kubectl("set env ds/data-agent WL_PUSH_TIMEOUT_BASE_S=240 "
            "WL_PUSH_TIMEOUT_SAFETY_S=0 WL_PUSH_MIN_THROUGHPUT_KBS=20000 "
            ">/dev/null")
    kubectl("rollout status ds/data-agent --timeout=300s >/dev/null")
    for n in ("anrg-3", "anrg-7", "anrg-8", "anrg-9"):
        AGENTS[n] = agent_ip(n)
    sh(f"kubectl apply -f {E4DIR}/e4.yml >/dev/null")

    rng = random.Random(SEED)
    schedule = []
    for b in range(1, BLOCKS + 1):
        blk = ARMS[:]
        rng.shuffle(blk)
        schedule += [(b, a) for a in blk]
    with open(f"{RES}/execution-order.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order", "block", "arm", "seed"])
        for i, (b, a) in enumerate(schedule, 1):
            w.writerow([i, b, a, SEED])

    try:
        with open(f"{RES}/runs.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(FIELDS)
            for i, (b, a) in enumerate(schedule, 1):
                run_one(i, b, a, w, f)
    finally:
        net("clear")
        kubectl("set env ds/data-agent WL_PUSH_TIMEOUT_BASE_S- "
                "WL_PUSH_TIMEOUT_SAFETY_S- WL_PUSH_MIN_THROUGHPUT_KBS- "
                ">/dev/null")
        kubectl("rollout status ds/data-agent --timeout=300s >/dev/null")
        kubectl("delete pod e4-fw-anrg-3 --ignore-not-found >/dev/null 2>&1")
    print("E4 CAMPAIGN DONE", flush=True)


if __name__ == "__main__":
    main()
