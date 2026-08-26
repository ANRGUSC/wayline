#!/usr/bin/env python3
"""E1 pilot: controlled source-uplink degradation. Runs ON anrg-2.

Question: when a producer's uplink degrades during a live workflow, can
Wayline revise the object's realization (no DAG change, no compute
placement change) and improve completion time?

Shaping: ONE aggregate htb class on anrg-3's egress interface (found
via `ip route get`, never hard-coded), filtered to exactly the three
consumer IPs. Default traffic passes through an unlimited class.
`off` deletes the experiment qdisc entirely. Applied via a persistent
privileged pod so cap latency is ~1s.

Arms (reconstruction of the garbled spec, stated explicitly):
  fixed-clean      no cap, no policy                       x3
  adaptive-clean   no cap, policy watching (0 patches)     x3
  per cap in {471, 236, 118, 59} Mbit/s, x3 each:
    fixed-deg      cap once fan-out active, no policy
    adaptive-early cap at t=15 (during produce), policy revises
    adaptive-late  cap from runtime state (object installed AND
                   fan-out active), policy revises
    static-oracle  cap at t=15, serve pre-pinned anrg-7
Degraded (cap, arm, rep) order randomized (seed saved).

Transport deadline: 600s for EVERY arm (WL_PUSH_MIN_THROUGHPUT_KBS=600
on the agent DS for the whole campaign) so timeouts never decide the
comparison.
"""

import csv
import json
import os
import random
import re
import subprocess
import time

NS = "wl-system"
PRODUCER = "anrg-3"
CONSUMERS = ["anrg-6", "anrg-7", "anrg-8"]
TARGET = "anrg-7"
CAPS = [471, 236, 118, 59]
REPS = 3
SEED = 20260825
WAYLINE = "/home/anrg/wayline/bin/wayline"
REPO = "/home/anrg/wayline-build-vertex"
RES = os.environ.get("RES", os.path.expanduser("~/E1-results"))
SIGNAL = os.path.join(RES, "wl-linkstate")
RUN_TIMEOUT = 900


def sh(cmd, timeout=180):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout)


def kubectl(args, timeout=120):
    return sh(f"kubectl -n {NS} {args}", timeout=timeout)


def node_ip(name):
    r = sh(f"kubectl get node {name} -o jsonpath="
           f"'{{.status.addresses[?(@.type==\"InternalIP\")].address}}'")
    return r.stdout.strip()


IPS = {}


def shaper_up():
    spec = {"spec": {"nodeName": PRODUCER, "hostNetwork": True,
            "restartPolicy": "Never",
            "containers": [{"name": "c", "image": "alpine",
                            "command": ["sh", "-c",
                                        "apk add -q iproute2 >/dev/null && sleep 86400"],
                            "securityContext": {"privileged": True}}]}}
    kubectl("delete pod e1-shaper --ignore-not-found >/dev/null 2>&1")
    sh(f"kubectl run e1-shaper -n {NS} --restart=Never --image=alpine "
       f"--overrides='{json.dumps(spec)}' >/dev/null 2>&1")
    for _ in range(30):
        if kubectl("get pod e1-shaper -o jsonpath='{.status.phase}'"
                   ).stdout.strip() == "Running":
            time.sleep(2)
            return
        time.sleep(3)
    raise SystemExit("shaper pod did not start")


def shaper(cmd, timeout=60):
    return kubectl(f"exec e1-shaper -- sh -c '{cmd}'", timeout=timeout)


def egress_iface():
    r = shaper(f"ip route get {IPS[CONSUMERS[0]]}")
    toks = r.stdout.split()
    return toks[toks.index("dev") + 1] if "dev" in toks else ""


def cap_on(iface, mbit):
    filters = "; ".join(
        f"tc filter add dev {iface} parent 1: protocol ip prio 1 u32 "
        f"match ip dst {IPS[c]}/32 flowid 1:10" for c in CONSUMERS)
    shaper(f"tc qdisc del dev {iface} root 2>/dev/null; "
           f"tc qdisc add dev {iface} root handle 1: htb default 30 && "
           f"tc class add dev {iface} parent 1: classid 1:30 htb "
           f"rate 10gbit ceil 10gbit && "
           f"tc class add dev {iface} parent 1: classid 1:10 htb "
           f"rate {mbit}mbit ceil {mbit}mbit && {filters}")
    out = shaper(f"tc class show dev {iface}").stdout
    m = re.search(r"htb 1:10.*?rate (\S+)", out)
    return m.group(1) if m else "MISSING"


def cap_stats(iface):
    out = shaper(f"tc -s class show dev {iface} classid 1:10 2>/dev/null").stdout
    m = re.search(r"Sent (\d+) bytes", out)
    return int(m.group(1)) if m else 0


def cap_off(iface):
    shaper(f"tc qdisc del dev {iface} root 2>/dev/null; true")
    left = shaper(f"tc qdisc show dev {iface}").stdout
    return not any(k in left for k in ("htb", "tbf", "netem"))


def agent_ip(node):
    r = kubectl(f"get pods -o wide --no-headers -l app=data-agent")
    for line in r.stdout.splitlines():
        f = line.split()
        if len(f) >= 7 and f[6] == node:
            return f[5]
    return ""


def agent_ready(aip, run, task):
    r = sh(f"curl -s -m 3 http://{aip}:8082/ready/{run}/{task}", timeout=10)
    return "true" in r.stdout.lower() or r.stdout.strip() == "1"


def submit(template):
    r = sh(f"sudo -n true 2>/dev/null; {WAYLINE} run {template} -n {NS}")
    m = re.search(rf"({template}-run-[a-z0-9]+)", r.stdout + r.stderr)
    return m.group(1) if m else ""


def phase_of(run):
    return kubectl(f"get odag {run} -o jsonpath='{{.status.phase}}'"
                   ).stdout.strip()


def start_policy(run, log):
    return subprocess.Popen(
        ["python3", os.path.join(REPO, "eval/experiments/E1/policy.py"),
         run, SIGNAL, TARGET],
        stdout=open(log, "a"), stderr=subprocess.STDOUT)


def clear_signal():
    try:
        os.remove(SIGNAL)
    except FileNotFoundError:
        pass


def purge_run_data(run):
    """Delete the run's data on every agent. Per-run ODAG deletion does
    not purge agent bytes, and at ~1 GB/run a campaign fills 57 GB eMMC
    nodes into DiskPressure (which is how the first paper campaign
    died). Called after digests are read, before the ODAG is deleted."""
    r = kubectl("get pods -o wide --no-headers -l app=data-agent")
    for line in r.stdout.splitlines():
        fl = line.split()
        if len(fl) >= 7 and fl[6].startswith("anrg-"):
            sh(f"curl -s -m 30 -X DELETE http://{fl[5]}:8082/data/{run} "
               f">/dev/null", timeout=40)


def digests(run):
    vals = {}
    for node in [PRODUCER, TARGET] + CONSUMERS:
        pod = kubectl(f"get pods -o wide --no-headers -l app=data-agent"
                      ).stdout
        pname = ""
        for line in pod.splitlines():
            f = line.split()
            if len(f) >= 7 and f[6] == node:
                pname = f[0]
        if not pname:
            continue
        r = kubectl(f"exec {pname} -- cat "
                    f"/data/wl-outputs/{run}/serve/.wl-sha256 2>/dev/null")
        d = r.stdout.strip()
        if d:
            vals[node] = d
    uniq = set(vals.values())
    return ("ok" if len(uniq) == 1 else f"MISMATCH:{len(uniq)}") if vals else "none"


def run_one(idx, arm, cap, rep, iface, wcsv, f):
    tpl = "e1-static" if arm == "static-oracle" else "e1"
    t0 = time.time()
    run = submit(tpl)
    if not run:
        print(f"[e1] {arm}/{cap}/{rep}: SUBMIT FAILED", flush=True)
        return
    rec = {"order": idx, "arm": arm, "cap_mbit": cap or "", "rep": rep,
           "run": run, "tc_rate": "", "object_ready_ts": "",
           "degrade_ts": "", "cap_bytes": "", "digest": ""}
    print(f"[e1] #{idx} {arm} cap={cap} rep={rep} -> {run}", flush=True)

    pol = None
    if arm in ("adaptive-clean", "adaptive-early", "adaptive-late"):
        pol = start_policy(run, f"{RES}/policy-{run}.log")

    aip3 = agent_ip(PRODUCER)
    deg_done = False

    def degrade():
        nonlocal deg_done
        rec["tc_rate"] = cap_on(iface, cap)
        rec["degrade_ts"] = round(time.time(), 2)
        with open(SIGNAL, "w") as sf:
            sf.write("on")
        deg_done = True
        print(f"[e1] #{idx} cap {cap}mbit applied (tc={rec['tc_rate']})",
              flush=True)

    deadline = time.time() + RUN_TIMEOUT
    early_at = t0 + 15 if arm in ("adaptive-early", "static-oracle") else None
    while time.time() < deadline:
        ph = phase_of(run)
        if ph in ("Succeeded", "Failed"):
            break
        if cap and not deg_done:
            if early_at and time.time() >= early_at:
                degrade()
            elif arm in ("fixed-deg", "adaptive-late"):
                if not rec["object_ready_ts"] and aip3 and \
                        agent_ready(aip3, run, "produce"):
                    rec["object_ready_ts"] = round(time.time(), 2)
                if rec["object_ready_ts"] and aip3 and \
                        agent_ready(aip3, run, "serve"):
                    # serve aliased => fan-out enqueued; cap now.
                    degrade()
        time.sleep(0.4 if (cap and not deg_done) else 3)

    mk = kubectl(f"get odag {run} -o jsonpath='{{.status.makespan}}'"
                 ).stdout.strip()
    rec["phase"], rec["makespan_s"] = phase_of(run), mk
    if deg_done:
        rec["cap_bytes"] = cap_stats(iface)
    kubectl(f"get odag {run} -o json").stdout and open(
        f"{RES}/odag-{run}.json", "w").write(
        kubectl(f"get odag {run} -o json").stdout)
    sh(f"kubectl -n {NS} logs deploy/odag-controller --tail=400 2>/dev/null "
       f"| grep {run} > {RES}/ctrl-{run}.log; true")
    if aip3:
        pods = kubectl("get pods -o wide --no-headers -l app=data-agent"
                       ).stdout
        for line in pods.splitlines():
            fl = line.split()
            if len(fl) >= 7 and fl[6] == PRODUCER:
                sh(f"kubectl -n {NS} logs {fl[0]} --tail=300 2>/dev/null "
                   f"| grep {run} > {RES}/agent3-{run}.log; true")
    rec["digest"] = digests(run)
    purge_run_data(run)
    if pol:
        pol.terminate()
    ok = cap_off(iface)
    clear_signal()
    kubectl(f"delete odag {run} --ignore-not-found >/dev/null 2>&1")
    rec["qdisc_clean_after"] = ok
    row = [rec.get(k, "") for k in FIELDS]
    wcsv.writerow(row)
    f.flush()
    print(f"[e1] #{idx} {arm} cap={cap} rep={rep}: {rec['phase']} "
          f"makespan={mk}s capbytes={rec['cap_bytes']} digest={rec['digest']}",
          flush=True)
    time.sleep(5)


FIELDS = ["order", "arm", "cap_mbit", "rep", "run", "phase", "makespan_s",
          "tc_rate", "object_ready_ts", "degrade_ts", "cap_bytes",
          "digest", "qdisc_clean_after"]


def main():
    os.makedirs(RES, exist_ok=True)
    for n in [PRODUCER] + CONSUMERS:
        IPS[n] = node_ip(n)
    shaper_up()
    iface = egress_iface()
    print(f"[e1] producer egress iface: {iface} (via ip route get)", flush=True)
    if not iface:
        raise SystemExit("no egress iface found")
    cap_off(iface)

    # Uniform 600s transfer deadline for the whole campaign.
    kubectl("set env ds/data-agent WL_PUSH_MIN_THROUGHPUT_KBS=600 >/dev/null")
    kubectl("rollout status ds/data-agent --timeout=300s >/dev/null")
    sh(f"kubectl apply -f {REPO}/eval/experiments/E1/e1.yml "
       f"-f {REPO}/eval/experiments/E1/e1-static.yml >/dev/null")
    time.sleep(3)

    combos = [(cap, arm, rep) for cap in CAPS
              for arm in ("fixed-deg", "adaptive-early", "adaptive-late",
                          "static-oracle")
              for rep in range(1, REPS + 1)]
    random.Random(SEED).shuffle(combos)
    with open(f"{RES}/execution-order.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order", "cap_mbit", "arm", "rep", "seed"])
        for i, (cap, arm, rep) in enumerate(combos, start=7):
            w.writerow([i, cap, arm, rep, SEED])

    try:
        with open(f"{RES}/runs.csv", "w", newline="") as f:
            wcsv = csv.writer(f)
            wcsv.writerow(FIELDS)
            idx = 1
            for rep in range(1, REPS + 1):
                run_one(idx, "fixed-clean", 0, rep, iface, wcsv, f)
                idx += 1
            for rep in range(1, REPS + 1):
                run_one(idx, "adaptive-clean", 0, rep, iface, wcsv, f)
                idx += 1
            for cap, arm, rep in combos:
                run_one(idx, arm, cap, rep, iface, wcsv, f)
                idx += 1
    finally:
        cap_off(iface)
        kubectl("set env ds/data-agent WL_PUSH_MIN_THROUGHPUT_KBS- >/dev/null")
        kubectl("rollout status ds/data-agent --timeout=300s >/dev/null")
        kubectl("delete pod e1-shaper --ignore-not-found >/dev/null 2>&1")
        clear_signal()
    print("E1 PILOT DONE", flush=True)


if __name__ == "__main__":
    main()
