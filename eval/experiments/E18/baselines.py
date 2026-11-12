#!/usr/bin/env python3
"""E18 baselines outside the campaign harness: bring up Argo's repository,
Ray or Dask on a lane, run a workload pinned to a plan, check where every task
ran and which network carried the data, and tear down. No link shaping, no
faults, no sudo: everything goes through `k3s kubectl` on eclipse.

On eclipse, in ~/testbed/wl-e18/eval/experiments/E18 (P=~/testbed/venv/bin/python):

  $P baselines.py up   argo|ray|dask --lane smoke [--procs]
  $P baselines.py down argo|ray|dask --lane smoke
  $P baselines.py probe --lane smoke --workload chain,iot --out DIR
  $P baselines.py plan --from <a Wayline run record (rep<k>.json)> --out DIR
  $P baselines.py run  argo|ray|dask --lane smoke --workload chain --plans DIR
                       [--copies] [--reps N] [--out DIR] [--pin soft|hard] [--procs]
  $P baselines.py footprint --nodes anrg-3 [--window 60] [--out FILE]
  $P baselines.py argo-overhead --node anrg-3 [--lane smoke] [--image IMG] [--window 60] [--out FILE]

`probe` runs e18.py's plan probe (creates the workload's W run on the lane,
reads its plan and W's plan-time copy decisions once the policy has decided,
deletes it) and writes DIR/plans/<workload>.json; `plan` writes the same file
from a Wayline run record (its predictedTasks and decisions). `run` writes one JSON
per repetition to DIR/<system>-<workload>[-copies]/rep<k>.json with the
driver's or workflow's result, the plan, whether every task ran on its
planned node, and the bytes each lane node and eclipse moved on their
data-VLAN interfaces during the run (kubelet's node network stats; they
refresh every 10 to 15 s, so the window is wider than the run and includes
background traffic such as heartbeats). `footprint` reads kubelet's per
container memory and CPU for the systems' pods on the nodes, twice, and the
per-process resident memory inside the Ray and Dask pods.
"""
import argparse
import datetime
import importlib.util
import json
import os
import sys
import time
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import images  # noqa: E402
import kube  # noqa: E402
import lanes  # noqa: E402
import workloads  # noqa: E402


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


rayrun = _load("rayrun", os.path.join(HERE, "ray", "rayrun.py"))
argorun = _load("argorun", os.path.join(HERE, "argo", "argorun.py"))
daskrun = _load("daskrun", os.path.join(HERE, "dask", "daskrun.py"))
RAY_IMAGE = os.environ.get("E18_RAY_IMAGE", images.ref("wl-ray-baseline"))   # images.py: mx1 is amd64 + arm64
DASK_IMAGE = os.environ.get("E18_DASK_IMAGE", daskrun.IMAGE)


def log(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def get_lane(name):
    return lanes.resolve(name, lanes.discover())


def workers_for(lane, image):
    return {n: lane.by_name[n] for n in lane.allowed(kube.image_archs(image))}


def ray_cluster(lane, image=None):
    image = image or RAY_IMAGE
    return rayrun.Cluster(image, workers_for(lane, image), lane.master)


def dask_cluster(lane, procs=False, image=None):
    image = image or DASK_IMAGE
    return daskrun.Cluster(image, workers_for(lane, image), lane.master, procs=procs)


def set_argo_image(image):
    """Argo's generic task image (argorun.images_of and workflow() read the module's TASK_IMAGE)."""
    if image:
        argorun.TASK_IMAGE = image


# ------------------------------------------------------------- kubelet stats
def summary(node):
    r = kube.kubectl(["get", "--raw", f"/api/v1/nodes/{node}/proxy/stats/summary"], ns=None, timeout=30)
    return json.loads(r.stdout) if r.returncode == 0 else {}


def _ts(s):
    return datetime.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def data_counters(names, after=None, settle=45):
    """{node: {"rx", "tx", "at"}} of each node's data-VLAN interface from
    kubelet's node stats; with `after`, waits until every node's stats were
    taken after that time (up to `settle` s)."""
    out = {}
    t0 = time.time()
    for name in names:
        dif = lanes.DATA_IF["master"] if name == lanes.MASTER else None
        while True:
            s = summary(name)
            net = (s.get("node") or {}).get("network") or {}
            at = _ts(net["time"]) if net.get("time") else 0
            if after is None or at > after or time.time() - t0 > settle:
                break
            time.sleep(2)
        ifs = {i["name"]: i for i in net.get("interfaces") or []}
        want = dif or next((n for n in ifs if n.endswith(".20")), None)
        i = ifs.get(want) or {}
        out[name] = {"if": want, "rx": i.get("rxBytes", 0), "tx": i.get("txBytes", 0), "at": at}
    return out


def counter_delta(before, after):
    return {n: {"if": after[n]["if"], "rxMB": round((after[n]["rx"] - before[n]["rx"]) / 1e6, 2),
                "txMB": round((after[n]["tx"] - before[n]["tx"]) / 1e6, 2),
                "windowS": round(after[n]["at"] - before[n]["at"], 1)} for n in after if n in before}


# ------------------------------------------------------------------ up/down
def cmd_up(a):
    lane = get_lane(a.lane)
    if a.system == "argo":
        if not argorun.installed():
            raise SystemExit("Argo is not installed: argo/install.sh")
        argorun.repo_up(lane.master)
        log(f"S3 repository up on {lane.master['data_ip']}:{argorun.S3_PORT}")
    elif a.system == "ray":
        c = ray_cluster(lane, a.image)
        c.up()
        log(f"Ray up: head on {lane.master['data_ip']}, workers {sorted(c.alive_ips())}")
    else:
        c = dask_cluster(lane, a.procs, a.image)
        c.up()
        log(f"Dask up: scheduler {c.address}, workers {c.worker_hosts()}")


def cmd_down(a):
    lane = get_lane(a.lane)
    if a.system == "argo":
        argorun.cleanup_stale()
        argorun.repo_down()
    elif a.system == "ray":
        ray_cluster(lane).down()
    else:
        dask_cluster(lane).down()
    log(f"{a.system} down")


# --------------------------------------------------------------------- plan
def cmd_plan(a):
    """A plan file from a Wayline run record (as e18.py's plan_for)."""
    rec = json.load(open(a.source))
    st = rec.get("status") or {}
    workload = a.workload or rec.get("workload")
    plan = {t["name"]: t["node"] for t in st.get("predictedTasks") or []}
    if not plan:
        raise SystemExit(f"{a.source}: no predictedTasks")
    copies = {}
    for d in (st.get("realizationPolicy") or {}).get("decisions") or []:
        if not d.get("copy"):
            continue
        key = d["object"] if workload in ("gdtm", "focal") else d["object"].split(".")[0]
        for n in str(d.get("fallback", "")).split(","):
            if n and n not in copies.setdefault(key, []):
                copies[key].append(n)
    doc = {"workload": workload, "probeRun": rec.get("run"), "fromRecord": os.path.abspath(a.source),
           "at": datetime.datetime.now().isoformat(timespec="seconds"), "plan": plan, "copies": copies,
           "decisions": (st.get("realizationPolicy") or {}).get("decisions")}
    path = os.path.join(a.out, "plans", f"{workload}.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    json.dump(doc, open(path, "w"), indent=1)
    log(f"{path}: {len(plan)} tasks, copies {copies}")


def cmd_probe(a):
    """e18.py's plan probe on the lane, one plan file per workload."""
    e18 = _load("e18", os.path.join(HERE, "e18.py"))
    api = kube.Api()
    try:
        lane = lanes.resolve(a.lane, lanes.discover(api))
        ctx = types.SimpleNamespace(plans={}, out=a.out, api=api, lane=lane,
                                    a=types.SimpleNamespace(arch="strict", fence="constraints"))
        for w in a.workload.split(","):
            doc = e18.plan_for(ctx, w)
            log(f"{w}: plan {doc['plan']} copies {doc['copies']}")
    finally:
        api.close()


# ---------------------------------------------------------------------- run
def placement(system, res, plan, pins, ip2node):
    """[(task, planned, ran)] for every task that did not run where planned."""
    if system == "argo":
        ran = {t["task"].split("(")[0]: t["node"] for t in res.get("tasks") or [] if t.get("phase") == "Succeeded"}
    else:
        ran = {n: ip2node.get(m.get("node"), m.get("node")) for n, m in (res.get("tasks") or {}).items()}
    want = dict(plan, **pins)
    return [(t, want.get(t), ran.get(t)) for t in sorted(want) if ran.get(t) != want.get(t)]


def argo_artifacts(st):
    """{task: [s3 keys]} of every pod's output artifacts."""
    out = {}
    for n in (st.get("nodes") or {}).values():
        if n.get("type") == "Pod":
            keys = [x.get("s3", {}).get("key") for x in (n.get("outputs") or {}).get("artifacts") or []]
            out[n.get("displayName")] = [k for k in keys if k]
    return out


def cmd_run(a):
    lane = get_lane(a.lane)
    pf = os.path.join(a.plans, "plans", f"{a.workload}.json")
    if not os.path.exists(pf):
        raise SystemExit(f"no plan {pf} (baselines.py plan --from <Wayline run record> --out {a.plans})")
    probe = json.load(open(pf))
    plan, copies = probe["plan"], (probe["copies"] if a.copies else {})
    iot_base = workloads.iot_base(lane)
    pins = {}
    names = sorted(set(plan.values()))
    cell = f"{a.system}-{a.workload}{'-copies' if a.copies else ''}{'-hard' if a.pin == 'hard' else ''}" \
           f"{'-procs' if a.procs else ''}"
    out_dir = os.path.join(a.out, cell)
    os.makedirs(out_dir, exist_ok=True)
    if a.system == "ray":
        cluster = ray_cluster(lane, a.image)
        cluster.up()
    elif a.system == "dask":
        cluster = dask_cluster(lane, a.procs, a.image)
        cluster.up()
    else:
        set_argo_image(a.image)
        cluster = None
        if a.copies:
            raise SystemExit("Argo has no copies arm (every artifact is in the repository)")
        argorun.repo_up(lane.master)
    for rep in range(1, a.reps + 1):
        path = os.path.join(out_dir, f"rep{rep}.json")
        if os.path.exists(path) and not a.force:
            log(f"{cell} rep{rep}: exists, skipped")
            continue
        before = data_counters(names + [lane.master["name"]])
        t_start = time.time()
        if a.system == "argo":
            lane_nodes = {img: lane.allowed(kube.image_archs(img)) for img in argorun.images_of(a.workload)}
            wf = argorun.workflow(a.workload, plan, iot_base, pins, lane_nodes)
            res = argorun.run(wf, timeout=a.timeout)
            ip2node = {}
        elif a.system == "ray":
            res = cluster.run(a.workload, plan, copies, pins, iot_base, timeout=a.timeout)
            ip2node = cluster.ip2node
        else:
            res = cluster.run(a.workload, plan, copies, pins, iot_base, timeout=a.timeout, pin=a.pin)
            ip2node = cluster.ip2node
        t_end = time.time()
        after = data_counters(names + [lane.master["name"]], after=t_end + 1)
        off = placement(a.system, res, plan, pins, ip2node)
        rec = {"system": a.system, "workload": a.workload, "copies": copies, "rep": rep, "lane": lane.describe(),
               "plan": plan, "planFile": os.path.abspath(pf), "start": t_start, "end": t_end,
               "offPlan": off, "dataVlanMB": counter_delta(before, after), "result": res}
        if a.system == "argo":
            rec["taskImage"] = argorun.TASK_IMAGE
            rec["artifacts"] = argo_artifacts(res.get("argoStatus") or {})
            rec["repository"] = f"{lane.master['data_ip']}:{argorun.S3_PORT}"
        elif a.system == "ray":
            rec["rayImage"] = cluster.image
            rec["rayNodes"] = sorted(cluster.alive_ips())
        else:
            rec["daskImage"] = cluster.image
            rec["daskWorkers"] = cluster.worker_hosts()
        json.dump(rec, open(path, "w"), indent=1)
        phase = res.get("phase")
        t = res.get("completion") if a.system != "argo" else res.get("wall")
        moved = {n: (d["rxMB"], d["txMB"]) for n, d in rec["dataVlanMB"].items()}
        log(f"{cell} rep{rep}: {phase} {t or 0:.1f}s (driver/workflow wall {res.get('wall', 0):.1f}s) "
            f"off-plan {off or 'none'}; data VLAN rx/tx MB {moved}"
            + (f"; errors {res.get('errors')}" if res.get("errors") else "")
            + (f"; replicas {res.get('replicas')}" if a.copies and res.get("replicas") else ""))
        if phase != "Succeeded":
            log("  tail: " + " | ".join((res.get("driverTail") or [])[-8:]) + (res.get("message") or ""))
        time.sleep(a.gap)


# ---------------------------------------------------------------- footprint
PROCS = r'''
import os, json
out = []
for p in os.listdir("/proc"):
    if not p.isdigit():
        continue
    try:
        st = dict(l.split(":", 1) for l in open(f"/proc/{p}/status").read().splitlines() if ":" in l)
        cmd = open(f"/proc/{p}/cmdline", "rb").read().replace(b"\0", b" ").decode(errors="replace").strip()
        f = open(f"/proc/{p}/stat").read().rsplit(")", 1)[1].split()
        out.append({"pid": int(p), "cmd": cmd[:160], "rssKB": int(st.get("VmRSS", "0 kB").split()[0]),
                    "anonKB": int(st.get("RssAnon", "0 kB").split()[0]),
                    "shmemKB": int(st.get("RssShmem", "0 kB").split()[0]),
                    "cpuTicks": int(f[11]) + int(f[12])})
    except (OSError, ValueError, IndexError):
        pass
print(json.dumps(out))
'''


def pod_procs(ns, pod):
    r = kube.kubectl(["exec", pod, "--", "python3", "-c", PROCS], ns=ns, timeout=60)
    try:
        return json.loads(r.stdout.strip().splitlines()[-1])
    except (IndexError, ValueError):
        return None


SYSTEM_NS = {"wl-system": "wayline", rayrun.NS: "ray", daskrun.NS: "dask", argorun.NS: "argo"}


def containers(s):
    """{(ns, pod, container): stats} of the systems' pods in a summary."""
    out = {}
    for p in s.get("pods") or []:
        ref = p["podRef"]
        if ref["namespace"] not in SYSTEM_NS:
            continue
        for c in p.get("containers") or []:
            out[(ref["namespace"], ref["name"], c["name"])] = {
                "ws": (c.get("memory") or {}).get("workingSetBytes", 0),
                "rss": (c.get("memory") or {}).get("rssBytes", 0),
                "cpuNs": (c.get("cpu") or {}).get("usageCoreNanoSeconds", 0),
                "at": _ts(c["cpu"]["time"]) if (c.get("cpu") or {}).get("time") else 0}
    return out


def cmd_footprint(a):
    nodes = [n for n in a.nodes.split(",") if n]
    first = {n: containers(summary(n)) for n in nodes}
    procs1 = {}
    for n in nodes:
        for (ns, pod, _c) in first[n]:
            if ns in (rayrun.NS, daskrun.NS) and (ns, pod) not in procs1:
                procs1[(ns, pod)] = pod_procs(ns, pod)
    log(f"sampling {a.window} s")
    time.sleep(a.window)
    report = {"at": datetime.datetime.now().isoformat(timespec="seconds"), "window": a.window, "nodes": {}}
    for n in nodes:
        second = containers(summary(n))
        rows = []
        for key, s2 in sorted(second.items()):
            s1 = first[n].get(key)
            dt = (s2["at"] - s1["at"]) if s1 else 0
            cpu = (s2["cpuNs"] - s1["cpuNs"]) / 1e9 / dt if s1 and dt > 0 else None
            row = {"system": SYSTEM_NS[key[0]], "ns": key[0], "pod": key[1], "container": key[2],
                   "workingSetMB": round(s2["ws"] / 2**20, 1), "rssMB": round(s2["rss"] / 2**20, 1),
                   "cpuCores": round(cpu, 4) if cpu is not None else None}
            if key[0] in (rayrun.NS, daskrun.NS):
                p1 = {p["pid"]: p for p in procs1.get((key[0], key[1])) or []}
                p2 = pod_procs(key[0], key[1]) or []
                hz = os.sysconf("SC_CLK_TCK")
                row["processes"] = [{"cmd": p["cmd"], "rssMB": round(p["rssKB"] / 1024, 1),
                                     "anonMB": round(p["anonKB"] / 1024, 1), "shmemMB": round(p["shmemKB"] / 1024, 1),
                                     "cpuCores": round((p["cpuTicks"] - p1[p["pid"]]["cpuTicks"]) / hz / a.window, 4)
                                     if p["pid"] in p1 else None} for p in p2 if "python3 -c" not in p["cmd"]]
            rows.append(row)
        report["nodes"][n] = rows
        for r in rows:
            log(f"{n} {r['system']:8s} {r['pod'][:40]:40s} {r['container']:12s} ws {r['workingSetMB']:8.1f} MB "
                f"rss {r['rssMB']:8.1f} MB cpu {r['cpuCores']}")
            for p in r.get("processes") or []:
                log(f"      {p['rssMB']:8.1f} MB (shmem {p['shmemMB']:.1f}) cpu {p['cpuCores']}  {p['cmd'][:110]}")
    if a.out:
        json.dump(report, open(a.out, "w"), indent=1)
        log(f"wrote {a.out}")


ARGO_PROBE = "import time, os; os.makedirs('/out', exist_ok=True); open('/out/data', 'wb').write(b'x' * 1024); time.sleep({secs})"


def cmd_argo_overhead(a):
    """Argo's per-pod cost on a node: one workflow pod (init, wait and the
    emissary-wrapped main container, one output artifact to the repository)
    and a plain pod running the same image and command, side by side; their
    containers' memory and CPU from kubelet, twice, `window` s apart."""
    lane = get_lane(a.lane) if a.lane else None
    set_argo_image(a.image)
    image = argorun.TASK_IMAGE
    if lane:
        argorun.repo_up(lane.master)
    secs = int(a.window + 90)
    cmd = ["python", "-c", ARGO_PROBE.format(secs=secs)]
    wf = {"apiVersion": "argoproj.io/v1alpha1", "kind": "Workflow",
          "metadata": {"generateName": "e18-overhead-", "namespace": argorun.NS,
                       "labels": {kube.EXPERIMENT_LABEL: kube.EXPERIMENT}},
          "spec": {"entrypoint": "probe", "serviceAccountName": "argo",
                   "artifactRepositoryRef": {"configMap": f"{argorun.NAME}-artifacts", "key": "s3"},
                   "templates": [{"name": "probe", "nodeSelector": {"kubernetes.io/hostname": a.node},
                                  "container": {"image": image, "command": cmd},
                                  "outputs": {"artifacts": [{"name": "out", "path": "/out/data",
                                                             "archive": {"none": {}}}]}}]}}
    plain = {"apiVersion": "v1", "kind": "Pod",
             "metadata": {"name": f"e18-overhead-plain-{a.node}", "namespace": argorun.NS,
                          "labels": {kube.EXPERIMENT_LABEL: kube.EXPERIMENT}},
             "spec": {"nodeName": a.node, "restartPolicy": "Never",
                      "containers": [{"name": "main", "image": image, "command": cmd}]}}
    r = kube.kubectl(["create", "-f", "-", "-o", "jsonpath={.metadata.name}"], stdin=json.dumps(wf), ns=argorun.NS)
    if r.returncode != 0:
        raise SystemExit(f"argo create: {r.stderr}")
    wf_name = r.stdout.strip()
    kube.kubectl(["delete", "pod", plain["metadata"]["name"], "--ignore-not-found", "--wait=true"], ns=argorun.NS)
    kube.kubectl(["apply", "-f", "-"], stdin=json.dumps(plain), ns=argorun.NS)
    try:
        pod = ""
        for _ in range(120):
            pods = kube.kubectl_json(["get", "pods", "-l", f"workflows.argoproj.io/workflow={wf_name}"],
                                     ns=argorun.NS)["items"]
            running = [p for p in pods if p["status"].get("phase") == "Running"]
            plain_ok = kube.kubectl(["get", "pod", plain["metadata"]["name"], "-o", "jsonpath={.status.phase}"],
                                    ns=argorun.NS).stdout.strip() == "Running"
            if running and plain_ok:
                pod = running[0]["metadata"]["name"]
                break
            time.sleep(2)
        if not pod:
            raise SystemExit("the probe pods did not start")
        st = kube.kubectl_json(["get", "pod", pod], ns=argorun.NS)["status"]
        init = next((c for c in st.get("initContainerStatuses") or [] if c["name"] == "init"), {})
        term = (init.get("state") or {}).get("terminated") or {}
        log(f"Argo pod {pod} on {a.node}; init container ran {term.get('startedAt')} -> {term.get('finishedAt')}")
        time.sleep(20)   # past start-up, into the steady state kubelet reports
        want = {pod, plain["metadata"]["name"]}

        def sample():
            return {k: v for k, v in containers(summary(a.node)).items() if k[1] in want}
        first = sample()
        procs = pod_procs(argorun.NS, pod)    # python3 is in the task image; the wait container has no shell
        time.sleep(a.window)
        second = sample()
        rows = []
        for key, s2 in sorted(second.items()):
            s1 = first.get(key)
            dt = (s2["at"] - s1["at"]) if s1 else 0
            cpu = (s2["cpuNs"] - s1["cpuNs"]) / 1e9 / dt if s1 and dt > 0 else None
            rows.append({"pod": key[1], "container": key[2], "workingSetMB": round(s2["ws"] / 2**20, 1),
                         "rssMB": round(s2["rss"] / 2**20, 1), "cpuCores": round(cpu, 4) if cpu is not None else None})
            log(f"{a.node} {key[1][:40]:40s} {key[2]:6s} ws {rows[-1]['workingSetMB']:7.1f} MB rss "
                f"{rows[-1]['rssMB']:7.1f} MB cpu {rows[-1]['cpuCores']}")
        for p_ in procs or []:
            log(f"   main container process: {p_['rssKB'] / 1024:6.1f} MB  {p_['cmd'][:100]}")
        report = {"node": a.node, "image": image, "window": a.window, "workflow": wf_name, "pod": pod,
                  "initContainer": {"startedAt": term.get("startedAt"), "finishedAt": term.get("finishedAt")},
                  "containers": rows, "mainProcesses": procs}
        if a.out:
            json.dump(report, open(a.out, "w"), indent=1)
            log(f"wrote {a.out}")
    finally:
        kube.kubectl(["delete", "wf", wf_name, "--wait=false"], ns=argorun.NS)
        kube.kubectl(["delete", "pod", plain["metadata"]["name"], "--wait=false"], ns=argorun.NS)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("up", "down"):
        p = sub.add_parser(name)
        p.add_argument("system", choices=["argo", "ray", "dask"])
        p.add_argument("--lane", default="smoke")
        p.add_argument("--procs", action="store_true", help="Dask: one single-threaded process per CPU")
        p.add_argument("--image", help="Ray or Dask image (default: the harness's)")
    p = sub.add_parser("probe")
    p.add_argument("--lane", default="smoke")
    p.add_argument("--workload", required=True, help="comma-separated")
    p.add_argument("--out", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--from", dest="source", required=True)
    p.add_argument("--workload")
    p.add_argument("--out", required=True)
    p = sub.add_parser("run")
    p.add_argument("system", choices=["argo", "ray", "dask"])
    p.add_argument("--lane", default="smoke")
    p.add_argument("--workload", required=True, choices=["chain", "chain-link", "fan", "iot"])
    p.add_argument("--plans", required=True, help="directory with plans/<workload>.json")
    p.add_argument("--out", default=os.path.expanduser(f"~/E18-results/{datetime.date.today()}_baselines"))
    p.add_argument("--copies", action="store_true", help="Ray / Dask: copies of W's copied objects")
    p.add_argument("--pin", choices=["soft", "hard"], default="soft", help="Dask: unpinned tasks soft or hard")
    p.add_argument("--procs", action="store_true", help="Dask: one single-threaded process per CPU")
    p.add_argument("--image", help="Argo task image, Ray image or Dask image (default: the harness's)")
    p.add_argument("--reps", type=int, default=1)
    p.add_argument("--gap", type=float, default=5.0, help="seconds between repetitions")
    p.add_argument("--timeout", type=float, default=900)
    p.add_argument("--force", action="store_true", help="rerun repetitions that exist")
    p = sub.add_parser("argo-overhead")
    p.add_argument("--node", required=True)
    p.add_argument("--lane", help="bring the S3 repository up for this lane first")
    p.add_argument("--image", help="the main container's image (default: Argo's task image)")
    p.add_argument("--window", type=float, default=60)
    p.add_argument("--out")
    p = sub.add_parser("footprint")
    p.add_argument("--nodes", required=True)
    p.add_argument("--window", type=float, default=60)
    p.add_argument("--out")
    a = ap.parse_args()
    {"up": cmd_up, "down": cmd_down, "probe": cmd_probe, "plan": cmd_plan, "run": cmd_run,
     "footprint": cmd_footprint, "argo-overhead": cmd_argo_overhead}[a.cmd](a)


if __name__ == "__main__":
    main()
