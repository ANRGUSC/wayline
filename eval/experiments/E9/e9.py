#!/usr/bin/env python3
"""E9 per-task pod overhead harness. Runs ON anrg-2. See PLAN.md.

Env: RES (results dir, default ~/E9-results).
"""
import argparse
import csv
import json
import os
import random
import subprocess
import threading
import time
import urllib.request
from datetime import datetime, timezone

NS = "wl-system"
IMAGE = "192.168.1.163:5000/wl-multi-odag-task:latest"
AGENT_PORT = 8082
PROBE_PORT = 18099
CHAIN = 5
RES = os.environ.get("RES", os.path.expanduser("~/E9-results"))


def sh(cmd, timeout=120, stdin=None):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout, input=stdin)


def kubectl(a, timeout=90, stdin=None):
    return sh(f"kubectl -n {NS} {a}", timeout=timeout, stdin=stdin)


def node_ips():
    raw = sh("kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}="
             "{.status.addresses[?(@.type==\"InternalIP\")].address} {end}'").stdout
    return dict(t.split("=", 1) for t in raw.split() if "=" in t)


def agent_ips():
    raw = kubectl("get pods -l app=data-agent -o jsonpath="
                  "'{range .items[*]}{.spec.nodeName}={.status.podIP} {end}'").stdout
    return dict(t.split("=", 1) for t in raw.split() if "=" in t)


def http_json(url, timeout=5):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read().decode())


# ─── clock offset (worker clock - control clock) ─────────────────────────────

def clock_offset(node, ip, samples=15):
    name = f"e9-clockprobe-{node}"
    server = ("import http.server,time\n"
              "class H(http.server.BaseHTTPRequestHandler):\n"
              "  def do_GET(s):\n"
              "    b=repr(time.time()).encode();s.send_response(200);s.end_headers();s.wfile.write(b)\n"
              "  def log_message(s,*a):pass\n"
              f"http.server.HTTPServer(('0.0.0.0',{PROBE_PORT}),H).serve_forever()")
    pod = {"apiVersion": "v1", "kind": "Pod",
           "metadata": {"name": name, "namespace": NS, "labels": {"app": "e9-clockprobe"}},
           "spec": {"nodeName": node, "hostNetwork": True, "restartPolicy": "Never",
                    "containers": [{"name": "p", "image": IMAGE,
                                    "command": ["python3", "-c", server]}]}}
    kubectl(f"delete pod {name} --ignore-not-found --grace-period=0 --force")
    kubectl("apply -f -", stdin=json.dumps(pod))
    got, deadline = [], time.time() + 90
    while time.time() < deadline and len(got) < samples:
        try:
            t0 = time.time()
            with urllib.request.urlopen(f"http://{ip}:{PROBE_PORT}/", timeout=2) as r:
                remote = float(r.read().decode())
            t1 = time.time()
            got.append((t1 - t0, remote - (t0 + t1) / 2))
        except Exception:
            time.sleep(0.5)
    kubectl(f"delete pod {name} --grace-period=0 --force")
    if not got:
        raise RuntimeError(f"clock probe on {node} failed")
    rtt, off = min(got)
    return {"offset": off, "rtt": rtt, "samples": len(got)}


# ─── pod watch ───────────────────────────────────────────────────────────────

def watch_pods(odag, events, stop):
    """Record control-clock arrival of each pod lifecycle step."""
    p = subprocess.Popen(
        ["kubectl", "-n", NS, "get", "pods", "-w", "--output-watch-events", "-o", "json",
         "-l", f"wl-odag={odag}"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    dec, buf = json.JSONDecoder(), ""
    while not stop.is_set():
        chunk = os.read(p.stdout.fileno(), 65536).decode()
        if not chunk:
            break
        now = time.time()
        buf += chunk
        while True:
            buf = buf.lstrip()
            try:
                ev, end = dec.raw_decode(buf)
            except ValueError:
                break
            buf = buf[end:]
            pod = ev.get("object", {})
            task = pod.get("metadata", {}).get("labels", {}).get("wl-task")
            if not task:
                continue
            rec = events.setdefault(task, {})
            rec.setdefault("added", now)
            if pod.get("spec", {}).get("nodeName"):
                rec.setdefault("bound", now)
                rec["node"] = pod["spec"]["nodeName"]
            for cs in pod.get("status", {}).get("containerStatuses", []) or []:
                st = cs.get("state", {})
                if "running" in st or "terminated" in st:
                    rec.setdefault("running", now)
                if "terminated" in st:
                    rec.setdefault("terminated", now)
    p.kill()


# ─── one run ─────────────────────────────────────────────────────────────────

def chain_odag(name, d, nodes, layout):
    tasks = [{"name": f"t{i}", "image": IMAGE, "command": ["python", "task.py"],
              "dependencies": [f"t{i-1}"] if i else [], "runtime": d, "dataSize": "1KB",
              "constraints": {"nodeNames": [nodes[0] if layout == "same" else nodes[i % 2]]}}
             for i in range(CHAIN)]
    return {"apiVersion": "wl.io/v1", "kind": "ODAG",
            "metadata": {"name": name, "namespace": NS},
            "spec": {"scheduler": "heft", "tasks": tasks}}


def run_one(name, d, nodes, layout, agents, offsets):
    events, stop = {}, threading.Event()
    th = threading.Thread(target=watch_pods, args=(name, events, stop), daemon=True)
    th.start()
    time.sleep(1.0)
    t_created = time.time()
    kubectl("apply -f -", stdin=json.dumps(chain_odag(name, d, nodes, layout)))
    phase, deadline = "", time.time() + 120 + CHAIN * (d + 30)
    while time.time() < deadline:
        phase = kubectl(f"get odag {name} -o jsonpath='{{.status.phase}}'").stdout.strip()
        if phase in ("Succeeded", "Failed"):
            break
        time.sleep(0.25)
    makespan = kubectl(f"get odag {name} -o jsonpath='{{.status.makespan}}'").stdout.strip()
    time.sleep(2)
    stop.set()

    def span(a, b):
        return round(b - a, 4) if a is not None and b is not None else ""

    rows = []
    for i in range(CHAIN):
        t = f"t{i}"
        ev = events.get(t, {})
        node = ev.get("node")
        off = offsets.get(node, {}).get("offset", 0.0)
        ip = agents.get(node)
        tim, inst_up = {}, None
        try:
            tim = http_json(f"http://{ip}:{AGENT_PORT}/timings/{name}/{t}")
        except Exception:
            pass
        if i:
            try:
                inst_up = http_json(f"http://{ip}:{AGENT_PORT}/installed/{name}/t{i-1}")["unixTime"] - off
            except Exception:
                pass
        w = lambda k: tim[k] - off if k in tim else None      # worker clock -> control clock
        sdk0, c0, c1, close = w("taskStartUnix"), w("computeStartUnix"), w("computeEndUnix"), w("closeUnix")
        added, bound, running, term = (ev.get(k) for k in ("added", "bound", "running", "terminated"))
        ref = inst_up if i else t_created
        # The kubelet reports "running" to the API server late, so the watch's
        # running event trails the SDK's own start; container start is
        # therefore measured bound -> SDK start (cross-clock, offset-corrected)
        # and the report lag is kept as a diagnostic. exit is off the critical
        # path: the controller dispatches the next task on install, not on
        # pod termination.
        r = {"run": name, "layout": layout, "d": d, "task": t, "node": node,
             "dispatch": span(ref, added), "schedule": span(added, bound),
             "start": span(bound, sdk0), "input": span(sdk0, c0),
             "compute": span(c0, c1), "handoff": span(c1, close),
             "exit": span(close, term), "running_report_lag": span(sdk0, running),
             "total": span(ref, term)}
        crit = [r[k] for k in ("dispatch", "schedule", "start", "input", "handoff")]
        r["critical_overhead"] = round(sum(crit), 4) if "" not in crit else ""
        rows.append(r)
    kubectl(f"delete odag {name} --ignore-not-found")
    for n, ip in agents.items():
        try:
            urllib.request.urlopen(urllib.request.Request(
                f"http://{ip}:{AGENT_PORT}/data/{name}", method="DELETE"), timeout=5)
        except Exception:
            pass
    terms = [e["terminated"] for e in events.values() if "terminated" in e]
    run = {"run": name, "layout": layout, "d": d, "phase": phase, "makespan": makespan,
           "sum_compute": CHAIN * d, "wall": round(max(terms) - t_created, 3) if terms else ""}
    return run, rows, events


def write_csv(path, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--durations", type=float, nargs="+", default=[0.5, 1, 2, 5, 10])
    ap.add_argument("--reps", type=int, default=1)
    ap.add_argument("--nodes", nargs=2, default=["anrg-4", "anrg-5"])
    ap.add_argument("--layouts", nargs="+", default=["same", "cross"])
    ap.add_argument("--seed", type=int, default=20260925)
    args = ap.parse_args()

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = os.path.join(RES, stamp)
    os.makedirs(out)
    nips, agents = node_ips(), agent_ips()
    offsets = {n: clock_offset(n, nips[n]) for n in args.nodes}
    json.dump(offsets, open(os.path.join(out, "offsets.json"), "w"), indent=1)
    print("clock offsets (worker - control):",
          {n: f"{o['offset'] * 1000:+.1f} ms (rtt {o['rtt'] * 1000:.1f} ms)" for n, o in offsets.items()},
          flush=True)

    rng = random.Random(args.seed)
    plan = [(d, lay, r) for d in args.durations for lay in args.layouts for r in range(args.reps)]
    rng.shuffle(plan)
    runs, tasks, raw = [], [], {}
    for k, (d, lay, r) in enumerate(plan, 1):
        name = f"e9-{lay}-{str(d).replace('.', 'p')}-{r}-{rng.randint(0, 9999):04d}"
        run, rows, ev = run_one(name, d, args.nodes, lay, agents, offsets)
        runs.append(run)
        tasks.extend(rows)
        raw[name] = ev
        ovh = sorted(x["critical_overhead"] for x in rows if x["critical_overhead"] != "")
        print(f"[{k}/{len(plan)}] {name}: {run['phase']} wall={run['wall']}s "
              f"compute={run['sum_compute']}s median critical-path overhead per task="
              f"{ovh[len(ovh) // 2] if ovh else 'n/a'}s", flush=True)
        write_csv(os.path.join(out, "runs.csv"), runs)
        write_csv(os.path.join(out, "tasks.csv"), tasks)
        json.dump(raw, open(os.path.join(out, "raw.json"), "w"))
        time.sleep(3)
    print("results in", out)


if __name__ == "__main__":
    main()
