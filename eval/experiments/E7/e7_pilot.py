#!/usr/bin/env python3
"""E7 reconciliation-safety pilot. Runs ON anrg-2.

3 randomized blocks x 8 arms = 24 runs (pilot); 20 blocks (paper).

Each run: apply per-destination 59 Mbit caps, submit e7recon, wait for
the injection precondition from OBSERVED state (payload installed on
anrg-3 and an original fan-out active), apply the arm's disturbance,
wait for terminal, then evaluate the per-run acceptance criteria. Raw
evidence (controller slice, status.objects timeline, pod UIDs, patch
bodies) is written per run so anything not auto-checked stays auditable.

A run is valid only if check_run() finds no violation. This is a
correctness campaign: do not scale a failing semantic case.
"""
import csv
import json
import os
import random
import re
import subprocess
import sys
import time

NS = "wl-system"
E7DIR = "/home/anrg/wayline-build-vertex/eval/experiments/E7"
RES = os.environ.get("RES", os.path.expanduser("~/E7-results"))
BLOCKS = int(os.environ.get("BLOCKS", "3"))
SEED = int(os.environ.get("SEED", "20260907"))
DEADLINE = 300
TEMPLATE = "e7recon"
PRODUCER = "anrg-3"
TARGET = "anrg-7"       # first revision target
ALT = "anrg-5"          # alternate target
OBJ = "produce.payload"
CONSUMERS = {"consume-6": "anrg-6", "consume-8": "anrg-8", "consume-9": "anrg-9"}
APP_TASKS = ["produce", "consume-6", "consume-8", "consume-9", "report"]
NO_APP_NODES = [ALT, TARGET]

ARMS = ["revision-control", "repeat-identical", "controller-restart",
        "source-agent-restart", "target-agent-restart", "superseding-revision",
        "conflicting-request", "last-copy-eviction"]

FIELDS = ["order", "block", "arm", "run", "phase", "makespan_s", "wall_s",
          "precondition_met", "inject_ts", "generations", "consumer_verifies",
          "report_verify", "final_serving", "final_copies", "app_restarts",
          "placement_ok", "no_app_on_targets", "refusal_logged",
          "source_copy_valid", "superseded_inactive", "partial_ready_seen",
          "ctrl_uid_changed", "agent_uid_changed", "fault_to_recovery_s",
          "net_verified", "net_clean_after", "attempted_bytes_anrg7",
          "transfers_to_anrg7", "valid", "invalid_reasons", "seed"]


def sh(cmd, timeout=120):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout)


def kubectl(a, timeout=90):
    return sh(f"kubectl -n {NS} {a}", timeout=timeout)


def net(cmd):
    r = sh(f"{E7DIR}/e7_net.sh {cmd}", timeout=300)
    return (r.stdout + r.stderr).strip()


AGENTS = {}


def agent_ip(node):
    return AGENTS.get(node, "")


def refresh_agents():
    AGENTS.clear()
    out = kubectl("get pods -l app=data-agent -o wide --no-headers").stdout
    for ln in out.splitlines():
        f = ln.split()
        if len(f) >= 7:
            AGENTS[f[6]] = f[5]


def phase(run):
    return kubectl(f"get odag {run} -o jsonpath='{{.status.phase}}'").stdout.strip()


def status_objects(run):
    out = kubectl(f"get odag {run} -o json").stdout
    try:
        d = json.loads(out)
    except json.JSONDecodeError:
        return None, []
    st = d.get("status", {})
    return st, st.get("objects", []) or []


def obj_state(objs):
    """Return (servingCopy, {node: state}) for OBJ."""
    for o in objs:
        if o.get("object") == OBJ:
            copies = {c["node"]: c.get("state") for c in o.get("copies", [])}
            return o.get("servingCopy", ""), copies
    return "", {}


def generation(run):
    return kubectl(f"get odag {run} -o jsonpath='{{.metadata.generation}}'").stdout.strip()


def installed(node, run):
    ip = agent_ip(node)
    if not ip:
        return None
    r = sh(f"curl -s -o /dev/null -w '%{{http_code}}' -m 6 "
           f"http://{ip}:8082/installed/{run}/{OBJ}", timeout=10)
    return r.stdout.strip() == "200"


def ready(node, run):
    ip = agent_ip(node)
    if not ip:
        return None
    r = sh(f"curl -s -m6 http://{ip}:8082/ready/{run}/serve", timeout=10)
    return "true" in r.stdout.lower()


def patch(run, entry):
    body = json.dumps({"spec": {"realization": [entry]}})
    kubectl(f"patch odag {run} --type merge -p '{body}'")
    return {"ts": time.time(), "body": entry}


def pod_uid(selector):
    return kubectl(f"get pods -l {selector} -o jsonpath='{{.items[0].metadata.uid}}'"
                   ).stdout.strip()


def agent_pod_on(node):
    return kubectl(f"get pods -l app=data-agent --field-selector spec.nodeName={node} "
                   f"-o jsonpath='{{.items[0].metadata.name}}'").stdout.strip()


def agent_uid_on(node):
    return kubectl(f"get pods -l app=data-agent --field-selector spec.nodeName={node} "
                   f"-o jsonpath='{{.items[0].metadata.uid}}'").stdout.strip()


def ctrl_slice(run):
    out = kubectl("logs deploy/odag-controller --tail=8000").stdout
    return "\n".join(ln for ln in out.splitlines() if run in ln)


def run_pods(run):
    return kubectl(f"get pods -o wide --no-headers | grep {run}").stdout


def wait_precondition(run, t0):
    """Payload installed on anrg-3 AND at least one original fan-out active
    (a consumer copy transferring, observed as the payload not yet ready
    on all consumer nodes while anrg-3 has it)."""
    while time.time() - t0 < 120:
        if installed(PRODUCER, run):
            # original fan-out active: at least one consumer node not yet
            # holding the full object
            got = sum(1 for n in CONSUMERS.values() if installed(n, run))
            if got < len(CONSUMERS):
                return True, time.time()
        if phase(run) in ("Succeeded", "Failed"):
            return False, time.time()
        time.sleep(1)
    return False, time.time()


def wait_rev_transferring(run, target, t0, timeout=60):
    """After a revision, wait until the target copy is present (Transferring
    or Installed) in status.objects."""
    while time.time() - t0 < timeout:
        _, copies = obj_state(status_objects(run)[1])
        if target in copies:
            return copies[target]
        if phase(run) in ("Succeeded", "Failed"):
            return None
        time.sleep(0.5)
    return None


def inject(run, arm, t0, ev):
    """Apply the arm's disturbance from observed state. Records into ev
    (evidence dict). Returns fault_to_recovery seconds where meaningful."""
    if arm == "revision-control":
        ev["patches"].append(patch(run, {"object": OBJ, "copies": [TARGET],
                                          "servingCopy": TARGET, "evict": []}))
        return None

    if arm == "repeat-identical":
        e = {"object": OBJ, "copies": [TARGET], "servingCopy": TARGET, "evict": []}
        ev["patches"].append(patch(run, e))
        wait_rev_transferring(run, TARGET, time.time())
        for _ in range(5):
            time.sleep(0.2)
            ev["patches"].append(patch(run, e))
        return None

    if arm in ("controller-restart", "source-agent-restart",
               "target-agent-restart"):
        ev["patches"].append(patch(run, {"object": OBJ, "copies": [TARGET],
                                         "servingCopy": TARGET, "evict": []}))
        st = wait_rev_transferring(run, TARGET, time.time())
        ev["rev_state_at_inject"] = st
        tf = time.time()
        if arm == "controller-restart":
            ev["ctrl_uid_before"] = pod_uid("app=odag-controller")
            kubectl("delete pod -l app=odag-controller --grace-period=0 --force "
                    ">/dev/null 2>&1")
            kubectl("rollout status deploy/odag-controller --timeout=180s", timeout=200)
            refresh_agents()
            ev["ctrl_uid_after"] = pod_uid("app=odag-controller")
        elif arm == "source-agent-restart":
            ev["agent_uid_before"] = agent_uid_on(PRODUCER)
            p = agent_pod_on(PRODUCER)
            kubectl(f"delete pod {p} --grace-period=0 --force >/dev/null 2>&1")
            _wait_agent(PRODUCER, ev["agent_uid_before"])
            ev["agent_uid_after"] = agent_uid_on(PRODUCER)
        else:  # target-agent-restart
            ev["agent_uid_before"] = agent_uid_on(TARGET)
            p = agent_pod_on(TARGET)
            kubectl(f"delete pod {p} --grace-period=0 --force >/dev/null 2>&1")
            # poll target during recovery: a ready marker must never sit
            # over a partial/mismatched payload.
            _poll_partial_ready(run, TARGET, ev, seconds=40)
            _wait_agent(TARGET, ev["agent_uid_before"])
            ev["agent_uid_after"] = agent_uid_on(TARGET)
        # recovery = target copy Installed and serving == TARGET
        while time.time() - t0 < DEADLINE:
            serving, copies = obj_state(status_objects(run)[1])
            if copies.get(TARGET) == "Installed" and serving == TARGET:
                return time.time() - tf
            if phase(run) in ("Succeeded", "Failed"):
                break
            time.sleep(1)
        return time.time() - tf

    if arm == "superseding-revision":
        ev["patches"].append(patch(run, {"object": OBJ, "copies": [TARGET],
                                         "servingCopy": TARGET, "evict": []}))
        wait_rev_transferring(run, TARGET, time.time())
        # newer generation: serve from ALT, evict TARGET
        ev["patches"].append(patch(run, {"object": OBJ, "copies": [ALT],
                                         "servingCopy": ALT, "evict": [TARGET]}))
        return None

    if arm == "conflicting-request":
        # one entry listing TARGET in both copies and evict, no serving override
        ev["patches"].append(patch(run, {"object": OBJ, "copies": [TARGET],
                                         "servingCopy": "", "evict": [TARGET]}))
        return None

    if arm == "last-copy-eviction":
        # evict the only copy (anrg-3) before any other exists
        ev["patches"].append(patch(run, {"object": OBJ, "copies": [],
                                         "servingCopy": "", "evict": [PRODUCER]}))
        return None
    return None


def _wait_agent(node, old_uid, timeout=120):
    t = time.time()
    while time.time() - t < timeout:
        u = agent_uid_on(node)
        if u and u != old_uid:
            # wait until Running+Ready
            ph = kubectl(f"get pods -l app=data-agent --field-selector "
                         f"spec.nodeName={node} -o jsonpath='{{.items[0].status.phase}}'"
                         ).stdout.strip()
            if ph == "Running":
                refresh_agents()
                return True
        time.sleep(2)
    refresh_agents()
    return False


def _poll_partial_ready(run, node, ev, seconds):
    """Sample the target's install record during recovery. The agent writes
    /installed only AFTER the atomic install of the complete payload, so a
    200 whose recorded bytes < 300 MiB would be a ready-over-partial safety
    failure (the precise check the spec asks for). Early serving-binding is
    NOT a false positive here because we read the object install record, not
    the vertex readiness."""
    t = time.time()
    full = 314572800
    partial = False
    min_ready_bytes = None
    ip = agent_ip(node)
    while time.time() - t < seconds:
        if ip:
            r = sh(f"curl -s -m6 http://{ip}:8082/installed/{run}/{OBJ}", timeout=10)
            body = r.stdout.strip()
            if body.startswith("{"):
                try:
                    b = json.loads(body).get("bytes", 0)
                    min_ready_bytes = b if min_ready_bytes is None else min(min_ready_bytes, b)
                    if 0 < b < full:
                        partial = True
                except ValueError:
                    pass
        time.sleep(1)
    ev["partial_ready_seen"] = partial
    ev["min_ready_bytes"] = min_ready_bytes


def check_run(run, arm, ev, rec):
    """Per-run acceptance criteria (spec s.Per-run). Returns (ok, reasons,
    fields)."""
    bad = []
    pods = run_pods(run)
    ctrl = ev["ctrl"]

    # 1. Succeeded within deadline
    if rec["phase"] != "Succeeded":
        bad.append(f"phase={rec['phase']}")
    if rec["wall"] >= DEADLINE:
        bad.append(f"exceeded {DEADLINE}s")

    # 2. three consumers + report verify. Primary signal is durable task
    # state: a consumer/report task exits nonzero on any digest or length
    # mismatch, so reaching Succeeded IS the verification (pod logs are
    # ephemeral -- GC'd on longer arms, which false-failed controller-
    # restart in an earlier build). Pod-log grep kept as best-effort
    # corroboration only.
    st0, _objs0 = status_objects(run)
    tstate = {t.get("name"): (t.get("phase"), bool(t.get("taskCloseTime")))
              for t in st0.get("tasks", [])}
    def task_ok(name):
        ph, closed = tstate.get(name, (None, False))
        return ph == "Succeeded" or closed
    cv = {}
    for c in CONSUMERS:
        lg = kubectl(f"logs {run}-{c} 2>/dev/null").stdout
        if "MISMATCH" in lg:
            cv[c] = "MISMATCH"
        elif "verify=OK" in lg or task_ok(c):
            cv[c] = "OK"
        else:
            cv[c] = "-"
    rep = kubectl(f"logs {run}-report 2>/dev/null").stdout
    report_ok = ("report verify=OK" in rep) or task_ok("report")
    if any(v != "OK" for v in cv.values()):
        bad.append(f"consumer verify {cv}")
    if not report_ok:
        bad.append("report did not verify 3 consumers")

    # 4. no partial object ever marked ready (target-agent-restart)
    if ev.get("partial_ready_seen"):
        bad.append("ready marker seen over partial copy")

    # 5. each app task once, zero restarts, no placement/order change
    restarts = 0
    placement = {}
    for ln in pods.splitlines():
        f = ln.split()
        if len(f) < 8:
            continue
        name = f[0].rsplit("-", 1)
        if len(f) >= 8 and f[3].isdigit():
            restarts += int(f[3])
        # map pod -> node
    # placement from odag status
    st, objs = status_objects(run)
    for t in st.get("tasks", []):
        if t.get("name") in APP_TASKS and t.get("node"):
            placement[t["name"]] = t["node"]
    want = {"produce": "anrg-3", "consume-6": "anrg-6", "consume-8": "anrg-8",
            "consume-9": "anrg-9", "report": "anrg-1"}
    placement_ok = all(placement.get(k) == v for k, v in want.items()
                       if k in placement)
    if restarts:
        bad.append(f"app restarts={restarts}")
    if not placement_ok:
        bad.append(f"placement changed: {placement}")

    # 6. no app container on anrg-5/anrg-7
    app_on_targets = [ln.split()[0] for ln in pods.splitlines()
                      if ln.split() and (f" {ALT} " in ln or f" {TARGET} " in ln)
                      and not ln.split()[0].endswith("-fw")]
    if app_on_targets:
        bad.append(f"app pods on target nodes: {app_on_targets[:3]}")

    # 7/8. final serving + copy state matches newest request
    serving, copies = obj_state(objs)
    superseded_inactive = "n/a"
    if arm == "revision-control" or arm in (
            "controller-restart", "source-agent-restart",
            "target-agent-restart", "repeat-identical"):
        if serving != TARGET:
            bad.append(f"final serving={serving} != {TARGET}")
    if arm == "superseding-revision":
        superseded_inactive = (serving == ALT and
                               copies.get(TARGET) in (None, "Evicted"))
        if serving != ALT:
            bad.append(f"final serving={serving} != {ALT} (supersede)")
        if copies.get(TARGET) not in (None, "Evicted"):
            bad.append(f"superseded {TARGET} still {copies.get(TARGET)}")

    # 9. conflicting-request: refusal logged + source copy valid
    refusal = "n/a"
    source_valid = "n/a"
    if arm == "conflicting-request":
        refusal = "in both copies and evict" in ctrl
        source_valid = bool(installed(PRODUCER, run))
        if not refusal:
            bad.append("no conflict refusal logged")
        if not source_valid:
            bad.append("source copy on anrg-3 not valid")

    # 10. last-copy-eviction: refusal logged + last copy preserved
    if arm == "last-copy-eviction":
        refusal = "refusing to evict last copy" in ctrl
        source_valid = bool(installed(PRODUCER, run))
        if not refusal:
            bad.append("no last-copy refusal logged")
        if not source_valid:
            bad.append("last copy on anrg-3 not preserved")

    # repeat-identical: the 3->7 copy must install exactly once. Redundant
    # retransfers (a reset copy) show up as >1 completed transfer to anrg-7
    # or inflated attempted bytes -- reported separately so eventual
    # idempotence cannot hide them (spec).
    attempted7 = 0
    ntx7 = 0
    ip3 = agent_ip(PRODUCER)
    if ip3:
        rr = sh(f"curl -s -m8 http://{ip3}:8082/flows/{run}", timeout=12)
        try:
            for fl in json.loads(rr.stdout or "[]"):
                if fl.get("dstNode") == TARGET and fl.get("ok") and fl.get("dataSize", 0) > 0:
                    attempted7 += fl["dataSize"]
                    ntx7 += 1
        except ValueError:
            pass
    if arm == "repeat-identical" and ntx7 > 1:
        bad.append(f"repeat-identical reset the copy: {ntx7} transfers to {TARGET}")

    # 11. net verified + clean after
    if not ev.get("net_verified"):
        bad.append("net not verified at start")
    if not ev.get("net_clean_after"):
        bad.append("qdiscs not clean after")

    # restart arms: prove the injected pod was replaced
    ctrl_changed = ("ctrl_uid_before" in ev and
                    ev.get("ctrl_uid_after") and
                    ev["ctrl_uid_before"] != ev["ctrl_uid_after"])
    agent_changed = ("agent_uid_before" in ev and
                     ev.get("agent_uid_after") and
                     ev["agent_uid_before"] != ev["agent_uid_after"])
    if arm == "controller-restart" and not ctrl_changed:
        bad.append("controller pod not proven replaced")
    if arm in ("source-agent-restart", "target-agent-restart") and not agent_changed:
        bad.append("agent pod not proven replaced")

    return (not bad), bad, dict(
        consumer_verifies=json.dumps(cv), report_verify=report_ok,
        final_serving=serving, final_copies=json.dumps(copies),
        app_restarts=restarts, placement_ok=placement_ok,
        no_app_on_targets=(not app_on_targets), refusal_logged=refusal,
        source_copy_valid=source_valid, superseded_inactive=superseded_inactive,
        partial_ready_seen=ev.get("partial_ready_seen", "n/a"),
        ctrl_uid_changed=(ctrl_changed if arm == "controller-restart" else "n/a"),
        agent_uid_changed=(agent_changed if "agent" in arm else "n/a"),
        attempted_bytes_anrg7=attempted7, transfers_to_anrg7=ntx7)


def one_run(idx, block, arm):
    refresh_agents()
    ev = {"patches": []}
    nv = "cap live" in net("apply")
    ev["net_verified"] = nv
    r = sh(f"/home/anrg/wayline/bin/wayline run {TEMPLATE} -n {NS}")
    m = re.search(rf"({TEMPLATE}-run-[a-z0-9]+)", r.stdout + r.stderr)
    if not m:
        net("clear")
        return dict(order=idx, block=block, arm=arm, run="", phase="SubmitFail",
                    valid=False, invalid_reasons="submit failed", seed=SEED)
    run = m.group(1)
    t0 = time.time()
    pre, inj_ts = wait_precondition(run, t0)
    ftr = None
    if pre:
        ftr = inject(run, arm, t0, ev)
    while time.time() - t0 < DEADLINE:
        if phase(run) in ("Succeeded", "Failed"):
            break
        time.sleep(2)
    wall = time.time() - t0
    ev["ctrl"] = ctrl_slice(run)
    open(f"{RES}/ctrl-{run}.log", "w").write(ev["ctrl"])
    st, objs = status_objects(run)
    json.dump({"objects": objs, "patches": ev["patches"],
               "rev_state_at_inject": ev.get("rev_state_at_inject")},
              open(f"{RES}/ev-{run}.json", "w"), indent=1, default=str)
    ev["net_clean_after"] = "cleared" in net("clear")
    rec = {"phase": phase(run), "wall": wall,
           "makespan": st.get("makespan", "")}
    ok, reasons, extra = check_run(run, arm, ev, rec)
    gens = generation(run)
    row = dict(order=idx, block=block, arm=arm, run=run, phase=rec["phase"],
               makespan_s=rec["makespan"], wall_s=round(wall, 1),
               precondition_met=pre, inject_ts=round(inj_ts - t0, 1),
               generations=gens, fault_to_recovery_s=(round(ftr, 1) if ftr else ""),
               net_verified=nv, net_clean_after=ev["net_clean_after"],
               valid=ok, invalid_reasons=";".join(reasons), seed=SEED, **extra)
    # purge run data
    for ip in AGENTS.values():
        sh(f"curl -s -m30 -X DELETE http://{ip}:8082/data/{run} >/dev/null", timeout=40)
    kubectl(f"delete odag {run} --ignore-not-found >/dev/null 2>&1")
    return row


def main():
    os.makedirs(RES, exist_ok=True)
    kubectl(f"apply -f {E7DIR}/e7.yml >/dev/null")
    img = kubectl("get deploy odag-controller -o "
                  "jsonpath='{.spec.template.spec.containers[*].image}'").stdout
    open(f"{RES}/PROVENANCE.txt", "w").write(
        f"seed {SEED}\nblocks {BLOCKS}\ncontroller_image {img.strip()}\n"
        f"deadline {DEADLINE}s\ntemplate {TEMPLATE}\n")
    rng = random.Random(SEED)
    schedule = []
    for b in range(1, BLOCKS + 1):
        blk = ARMS[:]
        rng.shuffle(blk)
        schedule += [(b, a) for a in blk]
    with open(f"{RES}/runs.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        f.flush()
        for idx, (block, arm) in enumerate(schedule, 1):
            print(f"[e7] #{idx}/{len(schedule)} block={block} {arm}", flush=True)
            row = one_run(idx, block, arm)
            w.writerow({k: row.get(k, "") for k in FIELDS})
            f.flush()
            print(f"[e7] #{idx} {arm}: valid={row.get('valid')} "
                  f"{'reasons=' + row.get('invalid_reasons','') if row.get('invalid_reasons') else ''} "
                  f"phase={row.get('phase')} serving={row.get('final_serving')} "
                  f"ftr={row.get('fault_to_recovery_s')}", flush=True)
            time.sleep(3)
    print("E7 PILOT DONE", flush=True)


if __name__ == "__main__":
    main()
