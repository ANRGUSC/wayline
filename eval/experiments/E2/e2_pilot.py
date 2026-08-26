#!/usr/bin/env python3
"""E2 temporal-relay pilot. Runs ON anrg-2.

The logical DAG has NO relay task (produce@anrg-3 -> consume@anrg-8 ->
report@anrg-9); anrg-7 relays purely through realization revisions and
runs no container. Contact plan, relative to t0 = object installed on
anrg-3 (TCP 8082 only, REJECT so failures are fast):

  3->8: blocked the whole run
  3->7: open [t0, t0+8), closed at +8
  7->8: blocked until +28, open [+28, +36), closed at +36

No contemporaneous producer->consumer path ever exists; the object
must survive on the relay across the 20 s gap.

Arms x REPS: clean-direct (no blocking), fixed-direct (blocked, no
policy, censored at 120 s), static-relay (copies {7,8} serving 7,
patched immediately after submission), adaptive-relay (policy patches
copy->7 during contact 1, copy->8 sourced from 7 at contact 2).
"""

import csv
import json
import os
import re
import subprocess
import time

NS = "wl-system"
REPS = 3
E2DIR = "/home/anrg/wayline-build-vertex/eval/experiments/E2"
RES = os.environ.get("RES", os.path.expanduser("~/E2-results"))
IPS = {}
CENSOR_FIXED = 120
RUN_TIMEOUT = 300


def sh(cmd, timeout=180):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout)


def kubectl(args, timeout=120):
    return sh(f"kubectl -n {NS} {args}", timeout=timeout)


def node_ip(name):
    return sh(f"kubectl get node {name} -o jsonpath="
              f"'{{.status.addresses[?(@.type==\"InternalIP\")].address}}'"
              ).stdout.strip()


def agent_ip(node):
    for line in kubectl("get pods -o wide --no-headers -l app=data-agent"
                        ).stdout.splitlines():
        f = line.split()
        if len(f) >= 7 and f[6] == node:
            return f[5]
    return ""


def agent_ready(aip, run, task):
    r = sh(f"curl -s -m 3 http://{aip}:8082/ready/{run}/{task}", timeout=10)
    return "true" in r.stdout.lower()


def contacts(cmd):
    r = sh(f"{E2DIR}/contacts.sh {cmd}", timeout=60)
    return r.stdout.strip()


def fw_pods():
    for node in ("anrg-3", "anrg-7", "anrg-8"):
        spec = {"spec": {"nodeName": node, "hostNetwork": True,
                "restartPolicy": "Never",
                "containers": [{"name": "c", "image": "alpine",
                                "command": ["sh", "-c",
                                            "apk add -q iptables >/dev/null && sleep 86400"],
                                "securityContext": {"privileged": True}}]}}
        kubectl(f"delete pod e2-fw-{node} --ignore-not-found >/dev/null 2>&1")
        sh(f"kubectl run e2-fw-{node} -n {NS} --restart=Never --image=alpine "
           f"--overrides='{json.dumps(spec)}' >/dev/null 2>&1")
    for node in ("anrg-3", "anrg-7", "anrg-8"):
        for _ in range(30):
            r = kubectl(f"exec e2-fw-{node} -- iptables -L -n 2>/dev/null "
                        f"| head -1")
            if "Chain" in r.stdout:
                break
            time.sleep(3)


def patch_realization(run, entries):
    p = json.dumps({"spec": {"realization": entries}})
    kubectl(f"patch odag {run} --type merge -p '{p}'")


def flows(node, run):
    aip = agent_ip(node)
    if not aip:
        return []
    r = sh(f"curl -s -m 5 http://{aip}:8082/flows/{run}", timeout=10)
    try:
        return json.loads(r.stdout)
    except (json.JSONDecodeError, ValueError):
        return []


def digest_map(run):
    vals = {}
    for line in kubectl("get pods -o wide --no-headers -l app=data-agent"
                        ).stdout.splitlines():
        f = line.split()
        if len(f) >= 7 and f[6] in ("anrg-3", "anrg-7", "anrg-8"):
            r = kubectl(f"exec {f[0]} -- cat "
                        f"/data/wl-outputs/{run}/produce/.wl-sha256 2>/dev/null")
            if r.stdout.strip():
                vals[f[6]] = r.stdout.strip()
    return vals


def purge(run):
    for line in kubectl("get pods -o wide --no-headers -l app=data-agent"
                        ).stdout.splitlines():
        f = line.split()
        if len(f) >= 7 and f[6].startswith("anrg-"):
            sh(f"curl -s -m 30 -X DELETE http://{f[5]}:8082/data/{run} "
               f">/dev/null", timeout=40)


def snapshot_objects(run):
    return kubectl(f"get odag {run} -o jsonpath='{{.status.objects}}'"
                   ).stdout.strip()


def run_one(idx, arm, rep, wcsv, f):
    blocked = arm != "clean-direct"
    censor = CENSOR_FIXED if arm == "fixed-direct" else RUN_TIMEOUT
    if blocked:
        contacts("init")
    else:
        contacts("clear")
    r = sh(f"/home/anrg/wayline/bin/wayline run e2relay -n {NS}")
    m = re.search(r"(e2relay-run-[a-z0-9]+)", r.stdout + r.stderr)
    if not m:
        print(f"[e2] #{idx} {arm}/{rep}: SUBMIT FAILED", flush=True)
        return
    run = m.group(1)
    print(f"[e2] #{idx} {arm} rep={rep} -> {run}", flush=True)
    events = []

    def ev(name):
        events.append((round(time.time(), 2), name))
        print(f"[e2] #{idx}   {name}", flush=True)

    if arm == "static-relay":
        patch_realization(run, [{"object": "produce",
                                 "copies": ["anrg-7", "anrg-8"],
                                 "servingCopy": "anrg-7"}])
        ev("patch:static-copies-7-8")

    aip3 = agent_ip("anrg-3")
    t0 = None
    t_start = time.time()
    obj_hist = []
    last_snap = ""
    while time.time() - t_start < censor:
        ph = kubectl(f"get odag {run} -o jsonpath='{{.status.phase}}'"
                     ).stdout.strip()
        if ph in ("Succeeded", "Failed"):
            break
        now = time.time()
        if blocked and t0 is None and aip3 and agent_ready(aip3, run, "produce"):
            t0 = now
            ev("t0:object-installed-anrg-3")
            if arm == "adaptive-relay":
                patch_realization(run, [{"object": "produce",
                                         "copies": ["anrg-7"],
                                         "servingCopy": "anrg-7"}])
                ev("patch:copy-to-relay(contact-1)")
        if blocked and t0 is not None:
            rel = now - t0
            done = {n for _, n in events}
            if rel >= 8 and "contact:close-3-7" not in done:
                contacts("close-3-7")
                ev("contact:close-3-7")
            if rel >= 28 and "contact:open-7-8" not in done:
                contacts("open-7-8")
                ev("contact:open-7-8")
                if arm == "adaptive-relay":
                    patch_realization(run, [{"object": "produce",
                                             "copies": ["anrg-7", "anrg-8"],
                                             "servingCopy": "anrg-7"}])
                    ev("patch:copy-to-consumer(contact-2)")
            if rel >= 36 and "contact:close-7-8" not in done:
                contacts("close-7-8")
                ev("contact:close-7-8")
        snap = snapshot_objects(run)
        if snap != last_snap and snap:
            obj_hist.append((round(now, 2), snap))
            last_snap = snap
        time.sleep(0.5 if (blocked and t0 is None) else 1.5)

    ph = kubectl(f"get odag {run} -o jsonpath='{{.status.phase}}'"
                 ).stdout.strip()
    mk = kubectl(f"get odag {run} -o jsonpath='{{.status.makespan}}'"
                 ).stdout.strip()
    # Proof the relay ran no pod + placements.
    pods = kubectl(f"get pods -o wide --no-headers | grep {run}").stdout
    relay_pods = sum(1 for ln in pods.splitlines() if " anrg-7 " in ln)
    placements = ";".join(f"{ln.split()[0].split(run+'-')[-1]}@{ln.split()[6]}"
                          for ln in pods.splitlines() if len(ln.split()) >= 7)
    fl3 = [x for x in flows("anrg-3", run) if x.get("dataSize", 0) > 1e6]
    fl7 = [x for x in flows("anrg-7", run) if x.get("dataSize", 0) > 1e6]
    b37 = sum(x["dataSize"] for x in fl3 if x.get("dstNode") == "anrg-7")
    b38 = sum(x["dataSize"] for x in fl3 if x.get("dstNode") == "anrg-8")
    b78 = sum(x["dataSize"] for x in fl7 if x.get("dstNode") == "anrg-8")
    dg = digest_map(run)
    dg_ok = "ok" if len(set(dg.values())) == 1 and dg else f"vals={len(set(dg.values()))}"
    open(f"{RES}/odag-{run}.json", "w").write(
        kubectl(f"get odag {run} -o json").stdout)
    sh(f"kubectl -n {NS} logs deploy/odag-controller --tail=500 2>/dev/null "
       f"| grep {run} > {RES}/ctrl-{run}.log; true")
    with open(f"{RES}/events-{run}.json", "w") as ef:
        json.dump({"events": events, "object_history": obj_hist}, ef, indent=1)
    contacts("clear")
    purge(run)
    kubectl(f"delete odag {run} --ignore-not-found >/dev/null 2>&1")
    wcsv.writerow([idx, arm, rep, run, ph, mk,
                   round(t0 - t_start, 1) if t0 else "",
                   b37, b38, b78, dg_ok, relay_pods, placements,
                   len(events)])
    f.flush()
    print(f"[e2] #{idx} {arm} rep={rep}: {ph} makespan={mk}s "
          f"bytes 3->7={b37} 3->8={b38} 7->8={b78} digest={dg_ok} "
          f"relay-pods={relay_pods}", flush=True)
    time.sleep(5)


def main():
    os.makedirs(RES, exist_ok=True)
    for n in ("anrg-3", "anrg-7", "anrg-8"):
        IPS[n] = node_ip(n)
    fw_pods()
    contacts("clear")
    sh(f"kubectl apply -f {E2DIR}/e2.yml >/dev/null")
    time.sleep(3)
    try:
        with open(f"{RES}/runs.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["order", "arm", "rep", "run", "phase", "makespan_s",
                        "t0_rel_s", "bytes_3_7", "bytes_3_8", "bytes_7_8",
                        "digest", "relay_pods", "placements", "n_events"])
            idx = 1
            for rep in range(1, REPS + 1):
                for arm in ("clean-direct", "fixed-direct", "static-relay",
                            "adaptive-relay"):
                    run_one(idx, arm, rep, w, f)
                    idx += 1
    finally:
        contacts("clear")
        for node in ("anrg-3", "anrg-7", "anrg-8"):
            kubectl(f"delete pod e2-fw-{node} --ignore-not-found "
                    f">/dev/null 2>&1")
    print("E2 PILOT DONE", flush=True)


if __name__ == "__main__":
    main()
