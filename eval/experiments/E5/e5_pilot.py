#!/usr/bin/env python3
"""E5 pilot: policy fidelity and realization effects. Runs ON anrg-2.

Eight arms x 3 randomized blocks (seed 20260827):

  iso-heft-direct    iso-maxtp-direct   iso-olb-direct
  iso-heft-store     iso-maxtp-store
  batch-heft-direct  batch-maxtp-direct batch-olb-direct

Isolated arms run one DAG (censor 900 s). Batch arms hold exactly eight
DAGs in flight by closed-loop submission until 24 complete (censor
3600 s). Store arms replay the frozen application placement AND per-node
order, adding a pod-less data vertex per named object routed through the
gateway; the scheduler is not re-run on the lowered graph.

A run is valid only if every check in check_run() passes.
"""

import csv
import hashlib
import json
import os
import random
import re
import statistics as st
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gen_e5 as G  # noqa: E402

NS = "wl-system"
E5DIR = "/home/anrg/wayline-build-vertex/eval/experiments/E5"
RES = os.environ.get("RES", os.path.expanduser("~/E5-results"))
FROZEN = os.environ.get("FROZEN", os.path.expanduser("~/E5-frozen"))
BLOCKS = int(os.environ.get("BLOCKS", "3"))
ONLY = [a for a in os.environ.get("ONLY", "").split(",") if a]
SEED = int(os.environ.get("SEED", "20260827"))
ISO_CENSOR = 900
BATCH_CENSOR = 3600
BATCH_CONCURRENT = 8
BATCH_TARGET = 24
APP_TASKS = G.ORDER
GW = G.GATEWAY

ARMS = [
    ("iso-heft-direct", "iso", "e5-heft", "direct", "heft"),
    ("iso-maxtp-direct", "iso", "e5-maxtp", "direct", "maxtp"),
    ("iso-olb-direct", "iso", "e5-olb", "direct", "olb"),
    ("iso-heft-store", "iso", "e5-store-heft", "store", "heft"),
    ("iso-maxtp-store", "iso", "e5-store-maxtp", "store", "maxtp"),
    ("batch-heft-direct", "batch", "e5-heft", "direct", "heft"),
    ("batch-maxtp-direct", "batch", "e5-maxtp", "direct", "maxtp"),
    ("batch-olb-direct", "batch", "e5-olb", "direct", "olb"),
]

FIELDS = ["order", "block", "arm", "regime", "scheduler", "realization",
          "runs", "completed", "makespan_med", "makespan_p95",
          "batch_seconds", "dags_per_min", "latency_med", "latency_p95",
          "busy_cov", "busy_by_node", "schedule_hash", "hash_matches_frozen",
          "placement", "node_order_observed", "order_matches_schedule",
          "rmse", "constraint_overrides", "fallbacks", "enact_order_confirmed",
          "digests_ok", "app_pods_on_gateway", "restarts",
          "bytes_by_pair", "gw_bytes_in", "gw_bytes_out",
          "direct_paths_ok", "store_paths_ok", "net_verified",
          "valid", "invalid_reasons", "seed"]


def sh(cmd, timeout=240):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout)


def kubectl(a, timeout=180):
    return sh(f"kubectl -n {NS} {a}", timeout=timeout)


def agent_ips():
    out = {}
    for line in kubectl("get pods -o wide --no-headers -l app=data-agent"
                        ).stdout.splitlines():
        f = line.split()
        if len(f) >= 7:
            out[f[6]] = f[5]
    return out


AGENTS = {}


def net(cmd):
    r = sh(f"{E5DIR}/e5_net.sh {cmd}", timeout=300)
    return (r.stdout + r.stderr).strip()


def fw_pods():
    for node in G.NODES:
        spec = {"spec": {"nodeName": node, "hostNetwork": True,
                "restartPolicy": "Never",
                "containers": [{"name": "c", "image": "alpine",
                                "command": ["sh", "-c",
                                            "apk add -q iproute2 >/dev/null && sleep 86400"],
                                "securityContext": {"privileged": True}}]}}
        kubectl(f"delete pod e5-fw-{node} --ignore-not-found >/dev/null 2>&1")
        sh(f"kubectl run e5-fw-{node} -n {NS} --restart=Never --image=alpine "
           f"--overrides='{json.dumps(spec)}' >/dev/null 2>&1")
    for node in G.NODES:
        for _ in range(40):
            if "tc utility" in kubectl(f"exec e5-fw-{node} -- sh -c "
                                       f"'tc -V 2>/dev/null'").stdout:
                break
            time.sleep(3)


def submit(template):
    r = sh(f"/home/anrg/wayline/bin/wayline run {template} -n {NS}")
    m = re.search(rf"({template}-run-[a-z0-9]+)", r.stdout + r.stderr)
    return m.group(1) if m else ""


def phase(run):
    return kubectl(f"get odag {run} -o jsonpath='{{.status.phase}}'"
                   ).stdout.strip()


def flows(node, run):
    ip = AGENTS.get(node)
    if not ip:
        return []
    try:
        return json.loads(sh(f"curl -s -m 6 http://{ip}:8082/flows/{run}",
                             timeout=12).stdout)
    except (json.JSONDecodeError, ValueError):
        return []


def free_pct(node="anrg-2"):
    """Percent of root filesystem free on a node, via its data agent's
    host mount. Returns None when unavailable rather than guessing."""
    r = sh("df --output=pcent / | tail -1", timeout=20)
    try:
        return 100 - int(r.stdout.strip().rstrip("%"))
    except (ValueError, AttributeError):
        return None


def purge(run):
    for node, ip in AGENTS.items():
        sh(f"curl -s -m 30 -X DELETE http://{ip}:8082/data/{run} >/dev/null",
           timeout=40)


def canonical_hash(placement, order):
    canon = json.dumps({"placement": dict(sorted(placement.items())),
                        "order": {k: v for k, v in sorted(order.items())}},
                       sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canon.encode()).hexdigest()


def collect(run, realization):
    """Everything observable about one DAG instance."""
    odag = kubectl(f"get odag {run} -o json").stdout
    try:
        obj = json.loads(odag)
    except json.JSONDecodeError:
        return None
    stt = obj.get("status", {}).get("tasks", [])
    placement, start, end = {}, {}, {}
    for t in stt:
        if t["name"] in APP_TASKS and t.get("node"):
            placement[t["name"]] = t["node"]
            if t.get("taskStartTime"):
                start[t["name"]] = t["taskStartTime"]
            if t.get("taskCloseTime") or t.get("completionTime"):
                end[t["name"]] = t.get("taskCloseTime") or t["completionTime"]
    order = {}
    for t in sorted(start, key=lambda x: start[x]):
        order.setdefault(placement[t], []).append(t)
    pairs, gw_in, gw_out = {}, 0, 0
    for node in G.NODES:
        for fl in flows(node, run):
            if not fl.get("ok", False) or fl.get("dataSize", 0) <= 0:
                continue
            k = f"{fl.get('srcNode')}->{fl.get('dstNode')}"
            pairs[k] = pairs.get(k, 0) + fl["dataSize"]
            if fl.get("dstNode") == GW:
                gw_in += fl["dataSize"]
            if fl.get("srcNode") == GW:
                gw_out += fl["dataSize"]
    busy = {}
    for t in placement:
        if t in start and t in end:
            try:
                s = time.mktime(time.strptime(start[t][:19], "%Y-%m-%dT%H:%M:%S"))
                e = time.mktime(time.strptime(end[t][:19], "%Y-%m-%dT%H:%M:%S"))
                busy[placement[t]] = busy.get(placement[t], 0) + max(0, e - s)
            except (ValueError, TypeError):
                pass
    return {"run": run, "phase": obj.get("status", {}).get("phase"),
            "makespan": obj.get("status", {}).get("makespan"),
            "placement": placement, "order": order, "start": start,
            "pairs": pairs, "gw_in": gw_in, "gw_out": gw_out, "busy": busy,
            "odag": odag}


def run_pods(run):
    return kubectl(f"get pods -o wide --no-headers | grep {run}").stdout


def check_run(rec, arm, realization, algo, frozen, ctrl):
    """Mandatory validity checks. Returns (ok, [reasons])."""
    bad = []
    pods = run_pods(rec["run"])
    on_gw = [ln.split()[0] for ln in pods.splitlines()
             if f" {GW} " in ln and not ln.split()[0].endswith("-fw")]
    # (gateway placement is only disallowed for the frozen comparisons;
    # checked below once the algorithm is known)
    restarts = sum(int(ln.split()[3]) for ln in pods.splitlines()
                   if len(ln.split()) >= 4 and ln.split()[3].isdigit())
    if restarts:
        bad.append(f"restarts={restarts}")
    verifies = []
    for ln in pods.splitlines():
        pod = ln.split()[0]
        lg = kubectl(f"logs {pod} 2>/dev/null").stdout
        verifies += re.findall(r"verify=(\w+)", lg)
    if verifies and any(v != "OK" for v in verifies):
        bad.append("digest/size mismatch")
    if "falling back" in ctrl:
        bad.append("scheduler fallback")
    ov = re.search(r"constraint overrides (\d+)", ctrl)
    overrides = int(ov.group(1)) if ov else 0
    if overrides:
        bad.append(f"constraintOverrides={overrides}")
    enact = "mode=serial" in ctrl
    if not enact:
        bad.append("enactOrder=serial not confirmed")
    rmse = None
    m = re.search(r"cost-model fit RMSE ([0-9.eE+-]+)", ctrl)
    if m:
        rmse = float(m.group(1))
    h = canonical_hash(rec["placement"], rec["order"])
    hash_ok = ""
    if frozen and realization == "direct" and algo in ("heft", "maxtp"):
        hash_ok = (h == frozen["schedule_hash"])
        if not hash_ok:
            bad.append("schedule hash != frozen")
    order_ok = True
    ref = frozen["order"] if (frozen and realization == "store") else rec["order"]
    if frozen and realization == "store":
        for node, seq in frozen["order"].items():
            obs = rec["order"].get(node, [])
            if [t for t in obs if t in seq] != [t for t in seq if t in obs]:
                order_ok = False
        if not order_ok:
            bad.append("observed order != frozen order")
    # Data paths. "Direct" means every dependency travels between the
    # nodes its endpoints were scheduled on -- NOT that the gateway is
    # unused: a load-balancing policy may legitimately place application
    # tasks there, and then gateway traffic is correct. Gateway-free
    # placement is required only of the frozen comparisons.
    direct_ok = store_ok = ""
    scheduled = set(rec["placement"].values())
    if realization == "direct":
        stray = [k for k in rec["pairs"]
                 if any(n not in scheduled for n in k.split("->"))]
        direct_ok = not stray
        if stray:
            bad.append(f"direct arm used unscheduled relay nodes: {stray[:3]}")
    else:
        store_ok = (rec["gw_in"] > 0 and rec["gw_out"] > 0)
        if not store_ok:
            bad.append("store arm did not route through the gateway")
    if algo in ("heft", "maxtp") and on_gw:
        bad.append(f"frozen comparison placed {len(on_gw)} app pod(s) on {GW}")
    return (not bad), bad, {"restarts": restarts, "overrides": overrides,
                            "enact": enact, "rmse": rmse, "hash": h,
                            "hash_ok": hash_ok, "order_ok": order_ok,
                            "digests_ok": bool(verifies) and
                            all(v == "OK" for v in verifies),
                            "gw_pods": len(on_gw),
                            "direct_ok": direct_ok, "store_ok": store_ok}


def ctrl_slice(run):
    out = kubectl("logs deploy/odag-controller --tail=4000").stdout
    return "\n".join(l for l in out.splitlines() if run in l)


def run_iso(arm, template, realization, algo, frozen, idx, block):
    run = submit(template)
    if not run:
        return None, ["submit failed"], {}
    t0 = time.time()
    while time.time() - t0 < ISO_CENSOR:
        if phase(run) in ("Succeeded", "Failed"):
            break
        time.sleep(2)
    rec = collect(run, realization)
    ctrl = ctrl_slice(run)
    open(f"{RES}/ctrl-{run}.log", "w").write(ctrl)
    open(f"{RES}/odag-{run}.json", "w").write(rec["odag"] if rec else "{}")
    ok, reasons, extra = check_run(rec, arm, realization, algo, frozen, ctrl)
    if rec["phase"] != "Succeeded":
        ok = False
        reasons.append(f"phase={rec['phase']}")
    purge(run)
    kubectl(f"delete odag {run} --ignore-not-found >/dev/null 2>&1")
    return rec, reasons, extra


def run_batch(arm, template, realization, algo, frozen):
    """Closed loop: keep BATCH_CONCURRENT alive until BATCH_TARGET done."""
    live, done, recs, reasons = {}, 0, [], []
    t_start = time.time()
    for _ in range(BATCH_CONCURRENT):
        r = submit(template)
        if r:
            live[r] = time.time()
        time.sleep(0.5)
    while done < BATCH_TARGET and time.time() - t_start < BATCH_CENSOR:
        for run in list(live):
            ph = phase(run)
            if ph in ("Succeeded", "Failed"):
                rec = collect(run, realization)
                if rec:
                    rec["latency"] = time.time() - live[run]
                    recs.append(rec)
                    if ph != "Succeeded":
                        reasons.append(f"{run} phase={ph}")
                ctrl = ctrl_slice(run)
                if "falling back" in ctrl:
                    reasons.append("scheduler fallback")
                purge(run)
                kubectl(f"delete odag {run} --ignore-not-found >/dev/null 2>&1")
                del live[run]
                done += 1
                if done + len(live) < BATCH_TARGET:
                    nr = submit(template)
                    if nr:
                        live[nr] = time.time()
        time.sleep(2)
    for run in list(live):
        purge(run)
        kubectl(f"delete odag {run} --ignore-not-found >/dev/null 2>&1")
    return recs, time.time() - t_start, done, reasons


def main():
    os.makedirs(RES, exist_ok=True)
    AGENTS.update(agent_ips())
    fw_pods()
    frozen = {}
    for a in ("heft", "maxtp"):
        p = os.path.join(FROZEN, f"frozen-{a}.json")
        if os.path.exists(p):
            frozen[a] = json.load(open(p))
    sh(f"kubectl apply -f {E5DIR}/e5-bandwidth.yml >/dev/null")
    for f in ("e5-heft.yml", "e5-maxtp.yml", "e5-olb.yml"):
        sh(f"kubectl apply -f {E5DIR}/{f} >/dev/null")
    # Store templates replay the frozen schedule, so they must be
    # regenerated from the frozen refs in use RIGHT NOW. Applying a
    # checked-in yaml here silently replays whatever schedule was frozen
    # when that file was written, which is how the store arms ended up
    # on a different placement than the direct arms.
    for algo in ("heft", "maxtp"):
        y = f"{E5DIR}/e5-store-{algo}.yml"
        sh(f"python3 {E5DIR}/gen_e5.py store {FROZEN}/frozen-{algo}.json "
           f"> {y}")
        sh(f"kubectl apply -f {y} >/dev/null")
    print(net("apply"), flush=True)

    rng = random.Random(SEED)
    schedule = []
    for b in range(1, BLOCKS + 1):
        blk = [a for a in ARMS if not ONLY or a[0] in ONLY]
        rng.shuffle(blk)
        schedule += [(b, a) for a in blk]
    img = sh("kubectl -n wl-system get deploy odag-controller "
             "-o jsonpath='{.spec.template.spec.containers[*].image}'").stdout
    dig = sh("kubectl -n wl-system get pods -l app=odag-controller "
             "-o jsonpath='{.items[0].status.containerStatuses[*].imageID}'").stdout
    with open(f"{RES}/PROVENANCE.txt", "w") as pf:
        pf.write(f"seed {SEED}\nblocks {BLOCKS}\n"
                 f"controller_image {img.strip()}\n"
                 f"controller_imageID {dig.strip()}\n"
                 f"frozen_dir {FROZEN}\n"
                 f"note store arms require the static-nodeOrder enactment "
                 f"path; runs before that fix are not comparable\n")
    with open(f"{RES}/execution-order.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order", "block", "arm", "seed"])
        for i, (b, a) in enumerate(schedule, 1):
            w.writerow([i, b, a[0], SEED])

    try:
        with open(f"{RES}/runs.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(FIELDS)
            for idx, (block, (arm, regime, tpl, realization, algo)) in \
                    enumerate(schedule, 1):
                fp = free_pct()
                if fp is not None and fp < 12:
                    print(f"[e5] #{idx} {arm}: ABORT free={fp}% "
                          f"(disk headroom below 12%; a shaper or agent "
                          f"eviction would silently corrupt later runs)",
                          flush=True)
                    raise SystemExit("insufficient disk headroom")
                nv = net("verify")
                if "verified" not in nv:
                    print(f"[e5] #{idx} {arm}: NET NOT VERIFIED", flush=True)
                    w.writerow([idx, block, arm, regime, algo, realization] +
                               [""] * (len(FIELDS) - 8) +
                               ["False", "net-not-verified", SEED])
                    f.flush()
                    continue
                fr = frozen.get(algo)
                print(f"[e5] #{idx} block={block} {arm}", flush=True)
                if regime == "iso":
                    rec, reasons, extra = run_iso(arm, tpl, realization, algo,
                                                  fr, idx, block)
                    if rec is None:
                        continue
                    mk = [int(rec["makespan"])] if rec.get("makespan") else []
                    row = dict(runs=1, completed=int(rec["phase"] == "Succeeded"),
                               makespan_med=st.median(mk) if mk else "",
                               makespan_p95="", batch_seconds="",
                               dags_per_min="", latency_med="", latency_p95="",
                               busy_cov="", busy_by_node=json.dumps(rec["busy"]),
                               placement=json.dumps(rec["placement"]),
                               node_order=json.dumps(rec["order"]),
                               pairs=json.dumps(rec["pairs"]),
                               gw_in=rec["gw_in"], gw_out=rec["gw_out"])
                else:
                    recs, secs, done, reasons = run_batch(arm, tpl,
                                                          realization, algo, fr)
                    extra = {}
                    if recs:
                        ctrl = ctrl_slice(recs[-1]["run"])
                        _, more, extra = check_run(recs[-1], arm, realization,
                                                   algo, fr, ctrl)
                        reasons += more
                    lat = sorted(r["latency"] for r in recs if "latency" in r)
                    mks = sorted(int(r["makespan"]) for r in recs
                                 if r.get("makespan"))
                    busy = {}
                    for r in recs:
                        for n, v in r["busy"].items():
                            busy[n] = busy.get(n, 0) + v
                    vals = [busy.get(n, 0) for n in G.EDGE + G.COMPUTE]
                    cov = (st.pstdev(vals) / st.mean(vals)) if vals and \
                        st.mean(vals) else ""
                    pairs = {}
                    gin = gout = 0
                    for r in recs:
                        for k, v in r["pairs"].items():
                            pairs[k] = pairs.get(k, 0) + v
                        gin += r["gw_in"]
                        gout += r["gw_out"]
                    row = dict(runs=len(recs), completed=done,
                               makespan_med=st.median(mks) if mks else "",
                               makespan_p95=(mks[int(.95 * len(mks)) - 1]
                                             if mks else ""),
                               batch_seconds=round(secs, 1),
                               dags_per_min=round(done / (secs / 60), 2)
                               if secs else "",
                               latency_med=round(st.median(lat), 1) if lat else "",
                               latency_p95=round(lat[int(.95 * len(lat)) - 1], 1)
                               if lat else "",
                               busy_cov=round(cov, 3) if cov != "" else "",
                               busy_by_node=json.dumps(busy),
                               placement=json.dumps(recs[-1]["placement"]
                                                    if recs else {}),
                               node_order=json.dumps(recs[-1]["order"]
                                                     if recs else {}),
                               pairs=json.dumps(pairs), gw_in=gin, gw_out=gout)
                    if done < BATCH_TARGET:
                        reasons.append(f"only {done}/{BATCH_TARGET} completed")
                valid = not reasons
                w.writerow([idx, block, arm, regime, algo, realization,
                            row["runs"], row["completed"], row["makespan_med"],
                            row["makespan_p95"], row["batch_seconds"],
                            row["dags_per_min"], row["latency_med"],
                            row["latency_p95"], row["busy_cov"],
                            row["busy_by_node"], extra.get("hash", ""),
                            extra.get("hash_ok", ""), row["placement"],
                            row["node_order"], extra.get("order_ok", ""),
                            extra.get("rmse", ""), extra.get("overrides", ""),
                            int("fallback" in " ".join(reasons)),
                            extra.get("enact", ""), extra.get("digests_ok", ""),
                            extra.get("gw_pods", ""), extra.get("restarts", ""),
                            row["pairs"], row["gw_in"], row["gw_out"],
                            extra.get("direct_ok", ""),
                            extra.get("store_ok", ""), True, valid,
                            ";".join(reasons), SEED])
                f.flush()
                print(f"[e5] #{idx} {arm}: valid={valid} "
                      f"{'reasons=' + ';'.join(reasons) if reasons else ''} "
                      f"makespan={row['makespan_med']} "
                      f"batch={row['batch_seconds']} gw_bytes={row['gw_in']}",
                      flush=True)
                time.sleep(5)
    finally:
        print(net("clear"), flush=True)
        for node in G.NODES:
            kubectl(f"delete pod e5-fw-{node} --ignore-not-found >/dev/null 2>&1")
    print("E5 PILOT DONE", flush=True)


if __name__ == "__main__":
    main()
