#!/usr/bin/env python3
"""E6 Part A harness: 7 WfChef workflows x {direct-frozen, store-frozen}.

Pilot: BLOCKS=3 -> 42 runs. Paper: BLOCKS=20 -> 280 runs.

Each block runs all 14 (workflow, arm) pairs in shuffled order. Both
arms replay the workflow's frozen HEFT placement and per-node order;
only the data path differs.

Validity per run (all blocking):
  * Succeeded within the deadline. Tasks self-verify every input against
    the deterministic content function and exit nonzero on mismatch, so
    a digest failure fails the run rather than passing silently.
  * placement and per-node order match the frozen reference
  * serial order enactment confirmed from the controller log
  * every named object has an install record on its serving node
    (producer's node for direct, the gateway for store)
  * byte paths: direct moves zero bytes through the gateway (no task
    and no vertex lives there); store moves zero bytes on any pair that
    does not touch the gateway
  * zero restarts, zero scheduler fallbacks
  * shaping verified live before each run, disk headroom above 15%

Usage (on anrg-2):
  BLOCKS=3 SEED=... RES=... FROZEN=... TPL=... python3 e6a_pilot.py
"""
import csv
import hashlib
import json
import os
import random
import re
import subprocess
import time

NS = "wl-system"
E6DIR = "/home/anrg/wayline-build-vertex/eval/experiments/E6"
RES = os.environ.get("RES", os.path.expanduser("~/E6A-results"))
FROZEN = os.environ.get("FROZEN", os.path.expanduser("~/E6A-frozen"))
TPL = os.environ.get("TPL", os.path.expanduser("~/E6A-templates"))
BLOCKS = int(os.environ.get("BLOCKS", "3"))
SEED = int(os.environ.get("SEED", "20260829"))
DEADLINE = 900
GW = "anrg-9"
NODES = ["anrg-1", "anrg-3", "anrg-4", "anrg-5",
         "anrg-6", "anrg-7", "anrg-8", GW]
WORKFLOWS = ["blast", "bwa", "cycles", "1000genome", "montage",
             "seismology", "soykb"]
ARMS = ["frozen", "store"]  # template suffixes; 'frozen' is the direct arm

FIELDS = ["order", "block", "workflow", "arm", "run", "phase", "makespan_s",
          "wall_s", "placement_matches_frozen", "order_matches_frozen",
          "enact_confirmed", "objects_installed", "gw_bytes_in",
          "gw_bytes_out", "nongw_bytes", "total_flow_bytes", "path_ok",
          "restarts", "fallbacks", "net_verified", "valid",
          "invalid_reasons", "seed"]


def sh(cmd, timeout=300):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout)


def kubectl(a, timeout=180):
    return sh(f"kubectl -n {NS} {a}", timeout=timeout)


def net(cmd):
    r = sh(f"{E6DIR}/e6_net.sh {cmd}", timeout=600)
    return (r.stdout + r.stderr).strip()


AGENTS = {}


def agent_ips():
    out = {}
    for line in kubectl("get pods -o wide --no-headers -l app=data-agent"
                        ).stdout.splitlines():
        f = line.split()
        if len(f) >= 7:
            out[f[6]] = f[5]
    return out


def free_pct():
    r = sh("df --output=pcent / | tail -1", timeout=20)
    try:
        return 100 - int(r.stdout.strip().rstrip("%"))
    except (ValueError, AttributeError):
        return None


def flows(node, run):
    ip = AGENTS.get(node)
    if not ip:
        return []
    try:
        return json.loads(sh(f"curl -s -m 8 http://{ip}:8082/flows/{run}",
                             timeout=15).stdout)
    except (json.JSONDecodeError, ValueError):
        return []


def installed(node, run, key):
    ip = AGENTS.get(node)
    if not ip:
        return False
    r = sh(f"curl -s -o /dev/null -w '%{{http_code}}' -m 6 "
           f"http://{ip}:8082/installed/{run}/{key}", timeout=12)
    return r.stdout.strip() == "200"


def purge(run):
    for ip in AGENTS.values():
        sh(f"curl -s -m 45 -X DELETE http://{ip}:8082/data/{run} >/dev/null",
           timeout=60)


def ctrl_slice(run):
    out = kubectl("logs deploy/odag-controller --tail=8000").stdout
    return "\n".join(l for l in out.splitlines() if run in l)


def run_one(wf, arm, meta, frozen):
    template = f"e6a-{wf}-{arm}"
    store = (arm == "store")
    r = sh(f"/home/anrg/wayline/bin/wayline run {template} -n {NS}")
    m = re.search(rf"({template}-run-[a-z0-9]+)", r.stdout + r.stderr)
    if not m:
        return None, ["submit failed"], {}
    run, t0 = m.group(1), time.time()
    while time.time() - t0 < DEADLINE:
        ph = kubectl(f"get odag {run} -o jsonpath='{{.status.phase}}'"
                     ).stdout.strip()
        if ph in ("Succeeded", "Failed"):
            break
        time.sleep(4)
    wall = time.time() - t0

    obj = kubectl(f"get odag {run} -o json").stdout
    bad, rec = [], {"run": run, "wall": wall}
    try:
        d = json.loads(obj)
    except json.JSONDecodeError:
        return None, ["collect failed"], {}
    st = d.get("status", {})
    rec["phase"] = st.get("phase")
    rec["makespan"] = st.get("makespan")
    app = set(meta["tasks"])
    placement, start = {}, {}
    for t in st.get("tasks", []):
        if t["name"] in app and t.get("node"):
            placement[t["name"]] = t["node"]
            if t.get("taskStartTime"):
                start[t["name"]] = t["taskStartTime"]
    order = {}
    for t in sorted(start, key=lambda x: start[x]):
        order.setdefault(placement[t], []).append(t)

    if rec["phase"] != "Succeeded":
        bad.append(f"phase={rec['phase']}")
    if wall >= DEADLINE:
        bad.append(f"exceeded {DEADLINE}s deadline")

    place_ok = (placement == frozen["placement"])
    if not place_ok:
        diff = {k: (frozen["placement"].get(k), placement.get(k))
                for k in set(placement) | set(frozen["placement"])
                if placement.get(k) != frozen["placement"].get(k)}
        bad.append(f"placement != frozen ({len(diff)} tasks)")
    order_ok = True
    for node, seq in frozen["order"].items():
        obs = [t for t in order.get(node, []) if t in seq]
        if obs != [t for t in seq if t in obs]:
            order_ok = False
    if not order_ok:
        bad.append("per-node order != frozen")

    ctrl = ctrl_slice(run)
    open(f"{RES}/ctrl-{run}.log", "w").write(ctrl)
    enact = "mode=serial" in ctrl
    if not enact:
        bad.append("enactOrder=serial not confirmed")
    fallb = int("falling back" in ctrl)
    if fallb:
        bad.append("scheduler fallback")

    pods = kubectl(f"get pods --no-headers | grep {run}").stdout
    restarts = sum(int(x.split()[3]) for x in pods.splitlines()
                   if len(x.split()) >= 4 and x.split()[3].isdigit())
    if restarts:
        bad.append(f"restarts={restarts}")

    # named-object installs on the serving node
    inst = 0
    missing = []
    for key in meta["objects"]:
        prod = meta["producers"][key]
        node = GW if store else placement.get(prod)
        if node and installed(node, run, key):
            inst += 1
        else:
            missing.append(key)
    if inst < len(meta["objects"]):
        bad.append(f"objects installed {inst}/{len(meta['objects'])} "
                   f"missing={missing[:3]}")

    gw_in = gw_out = nongw = tot = 0
    for node in NODES:
        for fl in flows(node, run):
            if not fl.get("ok", False) or fl.get("dataSize", 0) <= 0:
                continue
            szb = fl["dataSize"]
            tot += szb
            if fl.get("dstNode") == GW:
                gw_in += szb
            if fl.get("srcNode") == GW:
                gw_out += szb
            if fl.get("srcNode") != GW and fl.get("dstNode") != GW:
                nongw += szb
    path_ok = True
    if store:
        if nongw > 0:
            path_ok = False
            bad.append(f"store arm moved {nongw}B off-gateway")
        if tot > 0 and (gw_in == 0 or gw_out == 0):
            path_ok = False
            bad.append("store arm shows no gateway round-trip")
    else:
        if gw_in > 0 or gw_out > 0:
            path_ok = False
            bad.append(f"direct arm touched the gateway "
                       f"({gw_in}B in/{gw_out}B out)")

    purge(run)
    kubectl(f"delete odag {run} --ignore-not-found >/dev/null 2>&1")
    extra = dict(place_ok=place_ok, order_ok=order_ok, enact=enact,
                 inst=f"{inst}/{len(meta['objects'])}", gw_in=gw_in,
                 gw_out=gw_out, nongw=nongw, tot=tot, path_ok=path_ok,
                 restarts=restarts, fallbacks=fallb)
    return rec, bad, extra


def main():
    os.makedirs(RES, exist_ok=True)
    AGENTS.update(agent_ips())
    metas, frozens = {}, {}
    for wf in WORKFLOWS:
        metas[wf] = json.load(open(f"{TPL}/meta-{wf}.json"))
        frozens[wf] = json.load(open(f"{FROZEN}/frozen-{wf}.json"))

    print(net("apply"), flush=True)
    img = sh("kubectl -n wl-system get deploy odag-controller -o "
             "jsonpath='{.spec.template.spec.containers[*].image}'").stdout
    dig = sh("kubectl -n wl-system get pods -l app=odag-controller -o "
             "jsonpath='{.items[0].status.containerStatuses[*].imageID}'"
             ).stdout
    with open(f"{RES}/PROVENANCE.txt", "w") as pf:
        pf.write(f"seed {SEED}\nblocks {BLOCKS}\n"
                 f"controller_image {img.strip()}\n"
                 f"controller_imageID {dig.strip()}\n"
                 f"frozen_dir {FROZEN}\ntemplates {TPL}\n"
                 f"workloads WfChef-derived instances, synthetic executors; "
                 f"see partA/manifest.json for scale factors\n")

    rng = random.Random(SEED)
    schedule = []
    for b in range(1, BLOCKS + 1):
        blk = [(wf, arm) for wf in WORKFLOWS for arm in ARMS]
        rng.shuffle(blk)
        schedule += [(b, wf, arm) for wf, arm in blk]

    try:
        with open(f"{RES}/runs.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(FIELDS)
            f.flush()
            for idx, (block, wf, arm) in enumerate(schedule, 1):
                fp = free_pct()
                if fp is not None and fp < 15:
                    raise SystemExit(f"disk headroom {fp}% below 15%")
                nv = "verified" in net("verify")
                print(f"[e6a] #{idx}/{len(schedule)} block={block} "
                      f"{wf}-{arm} disk={fp}% "
                      f"net={'ok' if nv else 'NOT VERIFIED'}", flush=True)
                rec, bad, extra = run_one(wf, arm, metas[wf], frozens[wf])
                if rec is None:
                    rec = {"run": "", "phase": "SubmitFail",
                           "makespan": "", "wall": 0}
                    extra = {}
                if not nv:
                    bad.append("net-not-verified")
                valid = not bad
                w.writerow([idx, block, wf, arm, rec["run"], rec["phase"],
                            rec.get("makespan", ""),
                            round(rec.get("wall", 0), 1),
                            extra.get("place_ok", ""),
                            extra.get("order_ok", ""),
                            extra.get("enact", ""), extra.get("inst", ""),
                            extra.get("gw_in", ""), extra.get("gw_out", ""),
                            extra.get("nongw", ""), extra.get("tot", ""),
                            extra.get("path_ok", ""),
                            extra.get("restarts", ""),
                            extra.get("fallbacks", ""), nv, valid,
                            ";".join(bad), SEED])
                f.flush()
                print(f"[e6a] #{idx} {wf}-{arm}: valid={valid} "
                      f"{'reasons=' + ';'.join(bad) if bad else ''} "
                      f"makespan={rec.get('makespan', '')} "
                      f"wall={round(rec.get('wall', 0), 1)}s "
                      f"gw={extra.get('gw_in', '')}", flush=True)
                time.sleep(4)
    finally:
        print(net("clear"), flush=True)
    print("E6A DONE", flush=True)


if __name__ == "__main__":
    main()
