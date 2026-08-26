#!/usr/bin/env python3
"""E3 pilot: risk-aware replication under source-copy loss. Runs ON anrg-2.

Six arms; the DAG, the containers, and every compute placement are
identical in all of them. Only the object's realization differs.

  fixed-clean     no replica,            no loss
  always-clean    replica at submission, no loss
  fixed-loss      no replica,            source-copy loss at t0+30
  always-loss     replica at submission, loss -> policy rebinds serving
  adaptive-loss   replica at t0+15 risk, loss -> policy rebinds serving
  adaptive-clear  replica at t0+15 risk, risk clears -> policy evicts

t0 = produce installed on anrg-3. Caps: 3->8 and 7->8 at 59 Mbit/s
(tcp/8082 only), 3->7 unshaped, so a 300 MB delivery needs >=40.7 s and
is provably incomplete when loss is injected at t0+30.

Source-copy loss is deterministic state deletion, NOT a node crash:
cancel the fan-out, delete produce and serve on anrg-3, verify neither
is ready. No task restarts, no placement changes.
"""

import csv
import datetime
import json
import os
import random
import re
import subprocess
import sys
import time

NS = "wl-system"
E3DIR = "/home/anrg/wayline-build-vertex/eval/experiments/E3"
RES = os.environ.get("RES", os.path.expanduser("~/E3-results"))
BLOCKS = int(os.environ.get("BLOCKS", "3"))
SEED = int(os.environ.get("SEED", "20260828"))
BACKUP = "anrg-7"
OBJ_BYTES = 300_000_000
CENSOR = 140
ARMS = ["fixed-clean", "always-clean", "fixed-loss",
        "always-loss", "adaptive-loss", "adaptive-clear"]
LOSS_ARMS = {"fixed-loss", "always-loss", "adaptive-loss"}
REPLICA_AT_SUBMIT = {"always-clean", "always-loss"}
ADAPTIVE = {"adaptive-loss", "adaptive-clear"}

FIELDS = ["order", "block", "arm", "run", "phase", "makespan_s", "digest",
          "relay_pods", "placements", "restarts",
          "risk_to_patch_s", "patch_to_backup_install_s",
          "loss_to_rebind_s", "rebind_to_consumer_install_s",
          "delivered_3_7", "delivered_3_8", "delivered_7_8",
          "attempted_3_7", "attempted_3_8", "attempted_7_8",
          "backup_install_rel_event_s", "backup_evict_rel_event_s",
          "replica_residence_s", "replica_storage_MB_s",
          "patches", "cancels", "failed_flows", "ok_flows",
          "cap_bytes_3", "cap_bytes_7", "cap_verified", "qdisc_clean_after",
          "seed"]


def sh(cmd, timeout=180):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout)


def kubectl(a, timeout=120):
    return sh(f"kubectl -n {NS} {a}", timeout=timeout)


def node_ip(n):
    return sh(f"kubectl get node {n} -o jsonpath="
              f"'{{.status.addresses[?(@.type==\"InternalIP\")].address}}'"
              ).stdout.strip()


def agent_ip(node):
    for line in kubectl("get pods -o wide --no-headers -l app=data-agent"
                        ).stdout.splitlines():
        f = line.split()
        if len(f) >= 7 and f[6] == node:
            return f[5]
    return ""


AGENTS = {}


def ready(node, run, obj):
    ip = AGENTS.get(node)
    if not ip:
        return False
    return "true" in sh(f"curl -s -m 3 http://{ip}:8082/ready/{run}/{obj}",
                        timeout=8).stdout.lower()


def net(cmd):
    r = sh(f"{E3DIR}/e3_net.sh {cmd}", timeout=120)
    return (r.stdout + r.stderr).strip()


def fw_pods():
    for node in ("anrg-3", "anrg-7"):
        spec = {"spec": {"nodeName": node, "hostNetwork": True,
                "restartPolicy": "Never",
                "containers": [{"name": "c", "image": "alpine",
                                "command": ["sh", "-c",
                                            "apk add -q iproute2 >/dev/null && sleep 86400"],
                                "securityContext": {"privileged": True}}]}}
        kubectl(f"delete pod e3-fw-{node} --ignore-not-found >/dev/null 2>&1")
        sh(f"kubectl run e3-fw-{node} -n {NS} --restart=Never --image=alpine "
           f"--overrides='{json.dumps(spec)}' >/dev/null 2>&1")
    for node in ("anrg-3", "anrg-7"):
        for _ in range(40):
            if "tc utility" in kubectl(f"exec e3-fw-{node} -- sh -c "
                                       f"'tc -V 2>/dev/null'").stdout:
                break
            time.sleep(3)


def patch(run, entry):
    kubectl(f"patch odag {run} --type merge -p "
            f"'{json.dumps({'spec': {'realization': [entry]}})}'")


def inject_loss(run):
    """Deterministic source-copy loss: cancel the fan-out, delete the
    local objects, verify neither remains ready. Returns (ok, ts)."""
    ip3 = AGENTS["anrg-3"]
    sh(f"curl -s -m 10 -X POST http://{ip3}:8082/cancel/{run}/serve "
       f">/dev/null", timeout=15)
    ts = time.time()
    for obj in ("serve", "produce"):
        sh(f"curl -s -m 20 -X DELETE http://{ip3}:8082/data/{run}/{obj} "
           f">/dev/null", timeout=30)
    gone = not ready("anrg-3", run, "produce") and not ready("anrg-3", run, "serve")
    return gone, ts


def flows(node, run):
    ip = AGENTS.get(node)
    if not ip:
        return []
    try:
        return json.loads(sh(f"curl -s -m 5 http://{ip}:8082/flows/{run}",
                             timeout=10).stdout)
    except (json.JSONDecodeError, ValueError):
        return []


def digest_ok(run):
    vals = {}
    for line in kubectl("get pods -o wide --no-headers -l app=data-agent"
                        ).stdout.splitlines():
        f = line.split()
        if len(f) >= 7 and f[6] in ("anrg-7", "anrg-8"):
            r = kubectl(f"exec {f[0]} -- cat "
                        f"/data/wl-outputs/{run}/serve/.wl-sha256 2>/dev/null")
            if r.stdout.strip():
                vals[f[6]] = r.stdout.strip()
    if not vals:
        return "none"
    return "ok" if len(set(vals.values())) == 1 else "MISMATCH"


def purge(run):
    for line in kubectl("get pods -o wide --no-headers -l app=data-agent"
                        ).stdout.splitlines():
        f = line.split()
        if len(f) >= 7 and f[6].startswith("anrg-"):
            sh(f"curl -s -m 30 -X DELETE http://{f[5]}:8082/data/{run} "
               f">/dev/null", timeout=40)


def logts(line):
    m = re.match(r"(\d{4}/\d\d/\d\d \d\d:\d\d:\d\d)", line)
    return datetime.datetime.strptime(m.group(1) + " +0000",
                                      "%Y/%m/%d %H:%M:%S %z").timestamp() \
        if m else None


def run_one(idx, block, arm, wcsv, f):
    out = net("apply")
    if "verified" not in out:
        print(f"[e3] #{idx} {arm}: CAP NOT VERIFIED -> InfraFail ({out})",
              flush=True)
        wcsv.writerow([idx, block, arm, "", "InfraFail-cap"] +
                      [""] * (len(FIELDS) - 6) + [SEED])
        f.flush()
        return
    r = sh("/home/anrg/wayline/bin/wayline run e3risk -n " + NS)
    m = re.search(r"(e3risk-run-[a-z0-9]+)", r.stdout + r.stderr)
    if not m:
        print(f"[e3] #{idx} {arm}: SUBMIT FAILED", flush=True)
        wcsv.writerow([idx, block, arm, "", "SubmitFail"] +
                      [""] * (len(FIELDS) - 6) + [SEED])
        f.flush()
        return
    run = m.group(1)
    events = []

    def ev(name):
        events.append((round(time.time(), 2), name))
        print(f"[e3] #{idx}   {name}", flush=True)

    print(f"[e3] #{idx} block={block} {arm} -> {run}", flush=True)
    sig = os.path.join(RES, f"signal-{run}")
    pol = subprocess.Popen(
        ["python3", os.path.join(E3DIR, "policy.py"), run, arm, sig, BACKUP],
        stdout=open(f"{RES}/policy-{run}.log", "a"),
        stderr=subprocess.STDOUT)

    if arm in REPLICA_AT_SUBMIT:
        patch(run, {"object": "produce", "copies": [BACKUP],
                    "servingCopy": "", "evict": []})
        ev("submit:replica-requested")

    t0 = t_risk = t_inject = t_backup_install = t_backup_evict = None
    t_clear = None
    t_consumer_install = None
    obj_hist, last = [], ""
    start = time.time()
    while time.time() - start < CENSOR:
        ph = kubectl(f"get odag {run} -o jsonpath='{{.status.phase}}'"
                     ).stdout.strip()
        if ph in ("Succeeded", "Failed"):
            break
        now = time.time()
        if t0 is None and ready("anrg-3", run, "produce"):
            t0 = now
            ev("t0:produce-installed-anrg-3")
        if t0 is not None:
            rel = now - t0
            names = {n for _, n in events}
            if rel >= 15 and "risk:high" not in names:
                if arm in ADAPTIVE:
                    open(sig, "w").write("risk")
                t_risk = time.time()
                ev("risk:high")
            if rel >= 30 and "t30" not in names:
                t_event_marker = time.time()
                if arm in LOSS_ARMS:
                    gone, t_inject = inject_loss(run)
                    ev("loss:source-copy-deleted" if gone
                       else "INFRA:loss-injection-unverified")
                    if arm != "fixed-loss":
                        open(sig, "w").write("loss")
                elif arm == "adaptive-clear":
                    open(sig, "w").write("clear")
                    t_clear = t_event_marker
                    ev("risk:cleared")
                ev("t30")
        # object copy history
        snap = kubectl(f"get odag {run} -o jsonpath='{{.status.objects}}'"
                       ).stdout.strip()
        if snap and snap != last:
            obj_hist.append((round(now, 2), snap))
            last = snap
            try:
                for o in json.loads(snap):
                    for c in o.get("copies", []):
                        if c.get("node") == BACKUP:
                            if c.get("state") == "Installed" and \
                                    t_backup_install is None:
                                t_backup_install = now
                                ev("backup:installed")
                            if c.get("state") == "Evicted" and \
                                    t_backup_evict is None:
                                t_backup_evict = now
                                ev("backup:evicted")
            except (json.JSONDecodeError, TypeError):
                pass
        if t_consumer_install is None and ready("anrg-8", run, "serve"):
            t_consumer_install = now
            ev("consumer:input-installed-anrg-8")
        time.sleep(0.5 if t0 is None else 1.0)

    ph = kubectl(f"get odag {run} -o jsonpath='{{.status.phase}}'"
                 ).stdout.strip()
    if any(n.startswith("INFRA:") for _, n in events):
        ph = "InfraFail-loss-injection"
    mk = kubectl(f"get odag {run} -o jsonpath='{{.status.makespan}}'"
                 ).stdout.strip()
    cap_stats = net("stats")
    cap_b3 = cap_b7 = ""
    for line in cap_stats.splitlines():
        p = line.split()
        if len(p) == 2 and p[0] == "anrg-3":
            cap_b3 = p[1]
        if len(p) == 2 and p[0] == "anrg-7":
            cap_b7 = p[1]
    cap_ver = "verified" in net("verify")

    pods = kubectl(f"get pods -o wide --no-headers | grep {run}").stdout
    backup_pods = sum(1 for ln in pods.splitlines() if f" {BACKUP} " in ln)
    placements = ";".join(
        f"{ln.split()[0].split(run + '-')[-1]}@{ln.split()[6]}"
        for ln in pods.splitlines() if len(ln.split()) >= 7)
    restarts = sum(int(ln.split()[3]) for ln in pods.splitlines()
                   if len(ln.split()) >= 4 and ln.split()[3].isdigit())

    fl3, fl7 = flows("anrg-3", run), flows("anrg-7", run)

    def tot(fl, dst, ok_only):
        return sum(x["dataSize"] for x in fl
                   if x.get("dstNode") == dst and x.get("dataSize", 0) > 1e6
                   and (x.get("ok", x.get("Ok", False)) if ok_only else True))

    allfl = [x for x in fl3 + fl7 if x.get("dataSize", 0) > 1e6]
    ok_flows = sum(1 for x in allfl if x.get("ok", x.get("Ok", False)))
    dg = digest_ok(run)

    ctrl = kubectl(f"logs deploy/odag-controller --tail=600").stdout
    ctrl = "\n".join(l for l in ctrl.splitlines() if run in l)
    open(f"{RES}/ctrl-{run}.log", "w").write(ctrl)
    t_rebind = None
    for line in ctrl.splitlines():
        if f"serve executed on {BACKUP}" in line or \
                (f"executed on {BACKUP}" in line and "serve" in line):
            t_rebind = logts(line)
    polog = open(f"{RES}/policy-{run}.log").read() if \
        os.path.exists(f"{RES}/policy-{run}.log") else ""
    patches = polog.count("patched=")
    cancels = ctrl.count("CANCELED")
    m_first = re.search(r"observed=([\d.]+) patched=([\d.]+)", polog)

    def d(a, b):
        return round(b - a, 2) if (a and b) else ""

    risk_to_patch = ""
    if m_first:
        risk_to_patch = round(float(m_first.group(2)) - float(m_first.group(1)), 2)
    patch_to_install = ""
    if m_first and t_backup_install:
        patch_to_install = round(t_backup_install - float(m_first.group(2)), 2)
    elif arm in REPLICA_AT_SUBMIT and t_backup_install:
        patch_to_install = ""
    residence = ""
    if t_backup_install:
        end = t_backup_evict or time.time()
        residence = round(end - t_backup_install, 1)
    storage = round(residence * OBJ_BYTES / 1e6, 1) if residence != "" else ""

    with open(f"{RES}/events-{run}.json", "w") as ef:
        json.dump({"events": events, "object_history": obj_hist,
                   "t0": t0, "t_inject": t_inject}, ef, indent=1)

    if pol:
        pol.terminate()
    try:
        os.remove(sig)
    except FileNotFoundError:
        pass
    purge(run)
    kubectl(f"delete odag {run} --ignore-not-found >/dev/null 2>&1")
    clean = "verified" in net("clear")

    wcsv.writerow([idx, block, arm, run, ph, mk, dg, backup_pods, placements,
                   restarts, risk_to_patch, patch_to_install,
                   d(t_inject, t_rebind), d(t_rebind, t_consumer_install),
                   tot(fl3, "anrg-7", True), tot(fl3, "anrg-8", True),
                   tot(fl7, "anrg-8", True),
                   tot(fl3, "anrg-7", False), tot(fl3, "anrg-8", False),
                   tot(fl7, "anrg-8", False),
                   d(t_inject or t_clear, t_backup_install),
                   d(t_inject or t_clear, t_backup_evict),
                   residence, storage, patches, cancels,
                   len(allfl) - ok_flows, ok_flows, cap_b3, cap_b7,
                   cap_ver, clean, SEED])
    f.flush()
    print(f"[e3] #{idx} {arm}: {ph} makespan={mk or 'censored'} digest={dg} "
          f"backup-pods={backup_pods} residence={residence}s "
          f"patches={patches}", flush=True)
    time.sleep(5)


def main():
    os.makedirs(RES, exist_ok=True)
    for n in ("anrg-3", "anrg-7", "anrg-8"):
        AGENTS[n] = agent_ip(n)
    fw_pods()
    net("clear")
    kubectl("set env ds/data-agent WL_PUSH_TIMEOUT_BASE_S=180 "
            "WL_PUSH_TIMEOUT_SAFETY_S=0 WL_PUSH_MIN_THROUGHPUT_KBS=20000 "
            ">/dev/null")
    kubectl("rollout status ds/data-agent --timeout=300s >/dev/null")
    for n in ("anrg-3", "anrg-7", "anrg-8"):
        AGENTS[n] = agent_ip(n)
    sh(f"kubectl apply -f {E3DIR}/e3.yml >/dev/null")

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
        for node in ("anrg-3", "anrg-7"):
            kubectl(f"delete pod e3-fw-{node} --ignore-not-found "
                    f">/dev/null 2>&1")
    print("E3 PILOT DONE", flush=True)


if __name__ == "__main__":
    main()
