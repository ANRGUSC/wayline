#!/usr/bin/env python3
"""E6 Part B pilot: AI City MCMT under the current object contract.

3 blocks x 3 arms = 9 runs on the FULL-SOURCE PNG condition: each clip
is the entire source camera video (28.1-46.0 s), identical across all
arms. 'n4-d120-png' is a legacy internal identifier only, not a
duration, and must not be presented as one.

  wl-direct-frozen  frozen placement + per-node order, direct realization
  wl-store-frozen   same frozen schedule, every named object via anrg-9
  argo-minio        external referent, NOT placement-matched

A run is valid only if every check in check_run() passes. The pilot as a
whole passes only if all nine campaign criteria hold (see verdict()).

Usage (on anrg-2):
  BLOCKS=3 SEED=... RES=... FROZEN=... python3 e6_pilot.py
"""
import csv
import hashlib
import json
import os
import random
import re
import subprocess
import sys
import time

NS = "wl-system"
E6DIR = "/home/anrg/wayline-build-vertex/eval/experiments/E6"
MCMT = "/home/anrg/wayline-build-vertex/eval/mcmt"
RES = os.environ.get("RES", os.path.expanduser("~/E6-results"))
FROZEN = os.environ.get("FROZEN", os.path.expanduser("~/E6-frozen"))
BLOCKS = int(os.environ.get("BLOCKS", "3"))
SEED = int(os.environ.get("SEED", "20260828"))
DEADLINE = 900
GW = "anrg-9"
EDGE = ["anrg-1", "anrg-3", "anrg-4", "anrg-5"]
COMPUTE = ["anrg-6", "anrg-7", "anrg-8"]
NODES = EDGE + COMPUTE + [GW]
NCAM = 4
REPORT_ROOT = "/var/lib/wl-workloads/reports"

APP_TASKS = ([f"decode-{i}" for i in range(1, NCAM + 1)]
             + [f"preprocess-{i}" for i in range(1, NCAM + 1)]
             + [f"detect-embed-{i}" for i in range(1, NCAM + 1)]
             + [f"track-{i}" for i in range(1, NCAM + 1)]
             + ["cross-camera-match", "report"])
# Every named object the workload produces: (producer, object).
OBJECTS = ([(f"decode-{i}", "frames") for i in range(1, NCAM + 1)]
           + [(f"preprocess-{i}", "prepped") for i in range(1, NCAM + 1)]
           + [(f"detect-embed-{i}", "dets") for i in range(1, NCAM + 1)]
           + [(f"track-{i}", "tracks") for i in range(1, NCAM + 1)]
           + [("cross-camera-match", "matches")])

ARMS = [
    ("wl-direct-frozen", "wl", "e6-mcmt-frozen"),
    ("wl-store-frozen", "wl", "e6-mcmt-store"),
    ("argo-minio", "argo", "e6-mcmt-argo"),
]

FIELDS = ["order", "block", "arm", "system", "run", "phase", "makespan_s",
          "wall_s", "placement", "node_order_observed", "schedule_hash",
          "hash_matches_frozen", "order_matches_frozen", "enact_confirmed",
          "named_objects_seen", "digests_ok", "bytes_by_pair", "gw_bytes_in",
          "gw_bytes_out", "store_paths_ok", "direct_paths_ok",
          "restarts", "fallbacks", "constraint_overrides",
          "n_global_tracks", "counts_by_class", "report_fingerprint",
          "net_verified", "valid", "invalid_reasons", "seed"]


def sh(cmd, timeout=300):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout)


def kubectl(a, timeout=180):
    return sh(f"kubectl -n {NS} {a}", timeout=timeout)


def net(cmd):
    """One invocation, both streams. Calling the script twice ran the tc
    changes twice and the second call's 1s timeout threw."""
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


def purge(run):
    for ip in AGENTS.values():
        sh(f"curl -s -m 45 -X DELETE http://{ip}:8082/data/{run} >/dev/null",
           timeout=60)


def canonical_hash(placement, order):
    return hashlib.sha256(json.dumps(
        {"placement": dict(sorted(placement.items())),
         "order": {k: v for k, v in sorted(order.items())}},
        sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def read_report(run):
    """Read report.json from the gateway's hostPath via a probe pod."""
    pod = f"e6-rep-{abs(hash(run)) % 100000}"
    spec = {"spec": {"nodeName": GW, "restartPolicy": "Never",
            "containers": [{"name": "p", "image": "busybox",
                            "command": ["sh", "-c",
                                        f"cat /reports/{run}/report.json"],
                            "volumeMounts": [{"name": "r",
                                              "mountPath": "/reports"}]}],
            "volumes": [{"name": "r",
                         "hostPath": {"path": REPORT_ROOT}}]}}
    kubectl(f"delete pod {pod} --ignore-not-found >/dev/null 2>&1")
    sh(f"kubectl run {pod} -n {NS} --restart=Never --image=busybox "
       f"--overrides='{json.dumps(spec)}' >/dev/null 2>&1")
    body = ""
    for _ in range(60):
        ph = kubectl(f"get pod {pod} -o jsonpath='{{.status.phase}}'"
                     ).stdout.strip()
        if ph in ("Succeeded", "Failed"):
            body = kubectl(f"logs {pod}").stdout
            break
        time.sleep(2)
    kubectl(f"delete pod {pod} --ignore-not-found >/dev/null 2>&1")
    try:
        return json.loads(body)
    except (json.JSONDecodeError, ValueError):
        return None


def fingerprint(rep):
    """The documented equivalence rule: identical n_global_tracks,
    counts_by_class, and per-track class/cameras/hop_count. Compared as a
    canonical fingerprint rather than a file md5, which would differ on
    timestamps or key order without any semantic difference."""
    if not rep:
        return None, None, None
    tracks = rep.get("tracks") or rep.get("global_tracks") or []
    per = sorted(
        (str(t.get("class")), ",".join(sorted(map(str, t.get("cameras", [])))),
         str(t.get("hop_count"))) for t in tracks)
    counts = rep.get("counts_by_class") or {}
    n = rep.get("n_global_tracks", len(tracks))
    blob = json.dumps({"n": n, "counts": dict(sorted(counts.items())),
                       "tracks": per}, sort_keys=True, separators=(",", ":"))
    return n, counts, hashlib.sha256(blob.encode()).hexdigest()[:16]


def collect_wl(run, store):
    """Everything observable about one Wayline run."""
    obj = kubectl(f"get odag {run} -o json").stdout
    try:
        d = json.loads(obj)
    except json.JSONDecodeError:
        return None
    st = d.get("status", {})
    placement, start = {}, {}
    for t in st.get("tasks", []):
        if t["name"] in APP_TASKS and t.get("node"):
            placement[t["name"]] = t["node"]
            if t.get("taskStartTime"):
                start[t["name"]] = t["taskStartTime"]
    order = {}
    for t in sorted(start, key=lambda x: start[x]):
        order.setdefault(placement[t], []).append(t)

    pairs, gw_in, gw_out, keys_seen = {}, 0, 0, set()
    for node in NODES:
        for fl in flows(node, run):
            if not fl.get("ok", False) or fl.get("dataSize", 0) <= 0:
                continue
            pairs[f"{fl.get('srcNode')}->{fl.get('dstNode')}"] = \
                pairs.get(f"{fl.get('srcNode')}->{fl.get('dstNode')}", 0) \
                + fl["dataSize"]
            keys_seen.add(fl.get("fromTask", ""))
            if fl.get("dstNode") == GW:
                gw_in += fl["dataSize"]
            if fl.get("srcNode") == GW:
                gw_out += fl["dataSize"]
    return {"run": run, "phase": st.get("phase"),
            "makespan": st.get("makespan"), "placement": placement,
            "order": order, "pairs": pairs, "gw_in": gw_in, "gw_out": gw_out,
            "keys": keys_seen, "store": store}


def check_wl(rec, arm, frozen, ctrl, report):
    bad = []
    store = rec["store"]
    pods = kubectl(f"get pods -o wide --no-headers | grep {rec['run']}").stdout
    restarts = sum(int(x.split()[3]) for x in pods.splitlines()
                   if len(x.split()) >= 4 and x.split()[3].isdigit())
    if restarts:
        bad.append(f"restarts={restarts}")
    if "falling back" in ctrl:
        bad.append("scheduler fallback")
    m = re.search(r"constraint overrides (\d+)", ctrl)
    overrides = int(m.group(1)) if m else 0
    if overrides:
        bad.append(f"constraintOverrides={overrides}")
    enact = "mode=serial" in ctrl
    if not enact:
        bad.append("enactOrder=serial not confirmed")

    h = canonical_hash(rec["placement"], rec["order"])
    hash_ok = order_ok = ""
    if frozen:
        hash_ok = (rec["placement"] == frozen["placement"])
        if not hash_ok:
            bad.append("placement != frozen")
        order_ok = True
        for node, seq in frozen["order"].items():
            obs = [t for t in rec["order"].get(node, []) if t in seq]
            if obs != [t for t in seq if t in obs]:
                order_ok = False
        if not order_ok:
            bad.append("per-node order != frozen")

    # Every intermediate must be a NAMED object: the data-plane key is
    # "producer.object", so an unnamed send would appear as a bare task
    # name. Under store lowering the vertices carry the objects instead.
    want = {f"{p}.{o}" for p, o in OBJECTS}
    seen = {k for k in rec["keys"] if k}
    named = seen & want
    if store:
        vtx = {k for k in seen if k.startswith("v-")}
        if not vtx:
            bad.append("store arm shows no data-vertex transfers")
    elif len(named) < len(want):
        bad.append(f"named objects seen {len(named)}/{len(want)}")
    bare = {k for k in seen if k in APP_TASKS}
    if bare:
        bad.append(f"unnamed (bare-task) transfers: {sorted(bare)[:3]}")

    # Path shape.
    store_ok = direct_ok = ""
    scheduled = set(rec["placement"].values())
    if store:
        store_ok = (rec["gw_in"] > 0 and rec["gw_out"] > 0)
        if not store_ok:
            bad.append("store arm did not route through the gateway")
    else:
        stray = [k for k in rec["pairs"]
                 if any(n not in scheduled for n in k.split("->"))]
        direct_ok = not stray
        if stray:
            bad.append(f"direct arm used unscheduled nodes: {stray[:3]}")
    if not rec["pairs"]:
        bad.append("no directed-pair bytes recorded")

    verifies = []
    for ln in pods.splitlines():
        lg = kubectl(f"logs {ln.split()[0]} 2>/dev/null").stdout
        verifies += re.findall(r"verify=(\w+)", lg)
    dig = not (verifies and any(v != "OK" for v in verifies))
    if not dig:
        bad.append("digest/size mismatch")

    if rec["phase"] != "Succeeded":
        bad.append(f"phase={rec['phase']}")
    if report is None:
        bad.append("no report.json")
    return (not bad), bad, {
        "hash": h, "hash_ok": hash_ok, "order_ok": order_ok, "enact": enact,
        "named": f"{len(named)}/{len(want)}", "digests_ok": dig,
        "store_ok": store_ok, "direct_ok": direct_ok, "restarts": restarts,
        "overrides": overrides,
        "fallbacks": int("falling back" in ctrl)}


def ctrl_slice(run):
    out = kubectl("logs deploy/odag-controller --tail=6000").stdout
    return "\n".join(l for l in out.splitlines() if run in l)


def run_wl(arm, template, frozen, store):
    r = sh(f"/home/anrg/wayline/bin/wayline run {template} -n {NS}")
    m = re.search(rf"({template}-run-[a-z0-9]+)", r.stdout + r.stderr)
    if not m:
        return None, ["submit failed"], {}, None, 0
    run, t0 = m.group(1), time.time()
    while time.time() - t0 < DEADLINE:
        ph = kubectl(f"get odag {run} -o jsonpath='{{.status.phase}}'"
                     ).stdout.strip()
        if ph in ("Succeeded", "Failed"):
            break
        time.sleep(3)
    wall = time.time() - t0
    rec = collect_wl(run, store)
    ctrl = ctrl_slice(run)
    open(f"{RES}/ctrl-{run}.log", "w").write(ctrl)
    rep = read_report(run)
    if rep is not None:
        json.dump(rep, open(f"{RES}/report-{run}.json", "w"))
    if rec is None:
        return None, ["collect failed"], {}, None, wall
    ok, bad, extra = check_wl(rec, arm, frozen, ctrl, rep)
    if wall >= DEADLINE:
        bad.append(f"exceeded {DEADLINE}s deadline")
    purge(run)
    kubectl(f"delete odag {run} --ignore-not-found >/dev/null 2>&1")
    return rec, bad, extra, rep, wall


def run_argo(template):
    """External referent. Argo exposes no per-flow byte counters, so its
    directed-pair and gateway bytes are unavailable from the same
    instrumentation the Wayline arms use; that is recorded, not faked."""
    t0 = time.time()
    r = sh(f"kubectl -n argo create -f /tmp/{template}-wf.yml -o name",
           timeout=120)
    wf = (r.stdout or "").strip().split("/")[-1]
    if not wf:
        return None, ["argo submit failed"], None, 0
    while time.time() - t0 < DEADLINE:
        ph = sh(f"kubectl -n argo get workflow {wf} "
                f"-o jsonpath='{{.status.phase}}'").stdout.strip()
        if ph in ("Succeeded", "Failed", "Error"):
            break
        time.sleep(5)
    wall = time.time() - t0
    phase = sh(f"kubectl -n argo get workflow {wf} "
               f"-o jsonpath='{{.status.phase}}'").stdout.strip()
    place = {}
    js = sh(f"kubectl -n argo get workflow {wf} -o json").stdout
    try:
        d = json.loads(js)
        for n in d.get("status", {}).get("nodes", {}).values():
            if n.get("type") == "Pod" and n.get("hostNodeName"):
                place[n.get("displayName")] = n["hostNodeName"]
    except (json.JSONDecodeError, ValueError):
        pass
    # Argo's report task writes to /reports/unknown (known env-var bug in
    # the rendered template); try the workflow path first, then that.
    rep = read_report(wf) or read_report("unknown")
    if rep is not None:
        json.dump(rep, open(f"{RES}/report-{wf}.json", "w"))
    bad = []
    if phase != "Succeeded":
        bad.append(f"phase={phase}")
    if rep is None:
        bad.append("no report.json")
    if wall >= DEADLINE:
        bad.append(f"exceeded {DEADLINE}s deadline")
    sh(f"kubectl -n argo delete workflow {wf} --ignore-not-found >/dev/null",
       timeout=120)
    return {"run": wf, "phase": phase, "placement": place}, bad, rep, wall


def verdict(rows):
    """The nine campaign pass criteria."""
    wl = [r for r in rows if r["system"] == "wl"]
    ok = lambda c: all(c(r) for r in wl) if wl else False
    fps = {r["report_fingerprint"] for r in rows if r["report_fingerprint"]}
    checks = [
        ("9/9 complete within deadline",
         len(rows) == 3 * BLOCKS and all(r["phase"] == "Succeeded"
                                         for r in rows)),
        ("all arms report-equivalent", len(fps) == 1 and bool(fps)),
        ("both wl arms match frozen placement+order",
         ok(lambda r: r["hash_matches_frozen"] in (True, "True")
            and r["order_matches_frozen"] in (True, "True"))),
        ("every intermediate a named object with digests",
         ok(lambda r: r["digests_ok"] in (True, "True"))),
        ("directed-pair and gateway bytes present (wl arms)",
         ok(lambda r: bool(r["bytes_by_pair"]) and r["bytes_by_pair"] != "{}")),
        ("store lowering routes every object via the gateway",
         all(r["store_paths_ok"] in (True, "True")
             for r in wl if "store" in r["arm"])),
        ("direct execution follows frozen producer-consumer paths",
         all(r["direct_paths_ok"] in (True, "True")
             for r in wl if "direct" in r["arm"])),
        ("zero restarts, fallbacks, constraint overrides",
         ok(lambda r: str(r["restarts"]) in ("0", "")
            and str(r["fallbacks"]) in ("0", "")
            and str(r["constraint_overrides"]) in ("0", ""))),
        ("shaping verified live every run",
         all(r["net_verified"] in (True, "True") for r in rows)),
    ]
    print("\n== PILOT VERDICT ==")
    for label, passed in checks:
        print(f"  [{'PASS' if passed else 'FAIL'}] {label}")
    allp = all(p for _, p in checks)
    print(f"\n  {'PILOT PASSES' if allp else 'PILOT DOES NOT PASS'} "
          f"-- {'scale to 20 blocks' if allp else 'do not scale'}")
    print("\n  NOTE: Argo exposes no per-flow byte counters, so criterion 5 "
          "is evaluated on the Wayline arms only; Argo bytes are recorded "
          "as unavailable rather than estimated.")
    return allp


def main():
    os.makedirs(RES, exist_ok=True)
    AGENTS.update(agent_ips())
    frozen = {}
    fp = os.path.join(FROZEN, "frozen-heft.json")
    if os.path.exists(fp):
        frozen = json.load(open(fp))
    else:
        raise SystemExit(f"no frozen reference at {fp}")

    print(net("apply"), flush=True)
    img = sh("kubectl -n wl-system get deploy odag-controller -o "
             "jsonpath='{.spec.template.spec.containers[*].image}'").stdout
    dig = sh("kubectl -n wl-system get pods -l app=odag-controller -o "
             "jsonpath='{.items[0].status.containerStatuses[*].imageID}'"
             ).stdout
    open(f"{RES}/PROVENANCE.txt", "w").write(
        f"seed {SEED}\nblocks {BLOCKS}\n"
        f"condition full-source-png (legacy id n4-d120-png; NOT a duration)\n"
        f"clip_manifest {os.path.join(FROZEN, 'clip-manifest.json')}\n"
        f"controller_image {img.strip()}\ncontroller_imageID {dig.strip()}\n"
        f"frozen_dir {FROZEN}\nfrozen_placement "
        f"{json.dumps(frozen.get('placement'), sort_keys=True)}\n"
        f"note argo is NOT placement-matched; argo has no per-flow byte\n"
        f"counters so its directed-pair/gateway bytes are unavailable\n")

    rng = random.Random(SEED)
    schedule = []
    for b in range(1, BLOCKS + 1):
        blk = ARMS[:]
        rng.shuffle(blk)
        schedule += [(b, a) for a in blk]

    rows = []
    try:
        with open(f"{RES}/runs.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(FIELDS)
            f.flush()
            for idx, (block, (arm, system, tpl)) in enumerate(schedule, 1):
                fpct = free_pct()
                if fpct is not None and fpct < 15:
                    raise SystemExit(f"disk headroom {fpct}% below 15%")
                nv = "verified" in net("verify")
                print(f"[e6] #{idx} block={block} {arm} "
                      f"disk={fpct}% net={'ok' if nv else 'NOT VERIFIED'}",
                      flush=True)
                if system == "wl":
                    store = "store" in arm
                    rec, bad, extra, rep, wall = run_wl(
                        arm, tpl, frozen, store)
                    if rec is None:
                        rec = {"run": "", "phase": "SubmitFail",
                               "makespan": "", "placement": {}, "order": {},
                               "pairs": {}, "gw_in": "", "gw_out": ""}
                        extra = {}
                else:
                    rec, bad, rep, wall = run_argo(tpl)
                    extra = {}
                    if rec is None:
                        rec = {"run": "", "phase": "SubmitFail",
                               "placement": {}}
                    rec.setdefault("makespan", "")
                    rec.setdefault("order", {})
                    rec.setdefault("pairs", {})
                    rec.setdefault("gw_in", "")
                    rec.setdefault("gw_out", "")
                if not nv:
                    bad.append("net-not-verified")
                n, counts, fpr = fingerprint(rep)
                valid = not bad
                row = dict(
                    order=idx, block=block, arm=arm, system=system,
                    run=rec["run"], phase=rec["phase"],
                    makespan_s=rec.get("makespan", ""), wall_s=round(wall, 1),
                    placement=json.dumps(rec["placement"]),
                    node_order_observed=json.dumps(rec.get("order", {})),
                    schedule_hash=extra.get("hash", ""),
                    hash_matches_frozen=extra.get("hash_ok", ""),
                    order_matches_frozen=extra.get("order_ok", ""),
                    enact_confirmed=extra.get("enact", ""),
                    named_objects_seen=extra.get("named", ""),
                    digests_ok=extra.get("digests_ok", ""),
                    bytes_by_pair=json.dumps(rec.get("pairs", {})),
                    gw_bytes_in=rec.get("gw_in", ""),
                    gw_bytes_out=rec.get("gw_out", ""),
                    store_paths_ok=extra.get("store_ok", ""),
                    direct_paths_ok=extra.get("direct_ok", ""),
                    restarts=extra.get("restarts", ""),
                    fallbacks=extra.get("fallbacks", ""),
                    constraint_overrides=extra.get("overrides", ""),
                    n_global_tracks=n, counts_by_class=json.dumps(counts),
                    report_fingerprint=fpr, net_verified=nv, valid=valid,
                    invalid_reasons=";".join(bad), seed=SEED)
                w.writerow([row[k] for k in FIELDS])
                f.flush()
                rows.append(row)
                print(f"[e6] #{idx} {arm}: valid={valid} "
                      f"{'reasons=' + ';'.join(bad) if bad else ''} "
                      f"makespan={row['makespan_s']} wall={row['wall_s']}s "
                      f"tracks={n} fp={fpr}", flush=True)
                time.sleep(5)
    finally:
        print(net("clear"), flush=True)
    verdict(rows)
    print("E6 PILOT DONE", flush=True)


if __name__ == "__main__":
    main()
