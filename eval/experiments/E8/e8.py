#!/usr/bin/env python3
"""E8 control-plane overhead and scaling harness. Runs ON anrg-2 (root).

Parts, dispatched by argv[1]:
  A-before / A-after : idle window (5 min settle + 10 min sample)
  B                  : named-object / desired-copy scaling
  C                  : simultaneously-active-transfer scaling
  D                  : concurrent-run scaling

Env: BLOCKS (default 3), SEED, RES (results dir), E8DIR.

The 1 Hz resource sampler (e8_sample.py) is started around each measured
window/trial and stopped with SIGTERM. Convergence, latency decomposition,
digests, and the acceptance criteria are recorded per trial.
"""
import json
import os
import random
import signal
import statistics as st
import subprocess
import sys
import time

NS = "wl-system"
E8DIR = os.environ.get("E8DIR",
                       "/home/anrg/wayline-build-vertex/eval/experiments/E8")
RES = os.environ.get("RES", os.path.expanduser("~/E8-results"))
BLOCKS = int(os.environ.get("BLOCKS", "3"))
SEED = int(os.environ.get("SEED", "20260914"))
WORKERS = ["anrg-1", "anrg-3", "anrg-4", "anrg-5",
           "anrg-6", "anrg-7", "anrg-8", "anrg-9"]
DEADLINE = 300


def sh(cmd, timeout=120):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout)


def kubectl(a, timeout=90):
    return sh(f"kubectl -n {NS} {a}", timeout=timeout)


def agent_ips():
    raw = kubectl("get pods -l app=data-agent -o jsonpath="
                  "'{range .items[*]}{.spec.nodeName}={.status.podIP} {end}'").stdout
    out = {}
    for tok in raw.split():
        if "=" in tok:
            n, ip = tok.split("=", 1)
            if n in WORKERS and ip:
                out[n] = ip
    return out


def metrics(ip):
    try:
        return json.loads(sh(f"curl -s -m4 http://{ip}:8082/metrics",
                             timeout=6).stdout)
    except (ValueError, TypeError):
        return None


def preconditions(need_unshaped=True):
    """Common pre-trial gate. Returns (ok, reasons)."""
    bad = []
    if kubectl("get odags --no-headers 2>/dev/null").stdout.strip():
        bad.append("non-E8 odags present")
    nodes = kubectl("get nodes --no-headers").stdout
    for n in WORKERS:
        if not any(n in ln and " Ready" in ln for ln in nodes.splitlines()):
            bad.append(f"{n} not Ready")
    ips = agent_ips()
    if len(ips) < len(WORKERS):
        bad.append(f"only {len(ips)}/{len(WORKERS)} agents")
    for n, ip in ips.items():
        m = metrics(ip)
        if not m:
            bad.append(f"{n} /metrics unreachable")
            continue
        if m.get("push", {}).get("inflight", 0) != 0:
            bad.append(f"{n} push_inflight={m['push']['inflight']}")
    return (not bad), bad


class Sampler:
    """Wraps e8_sample.py as a subprocess writing JSONL, stopped on exit."""
    def __init__(self, path):
        self.path = path
        self.p = None

    def __enter__(self):
        self.p = subprocess.Popen(
            ["python3", f"{E8DIR}/e8_sample.py", self.path, "1"])
        return self

    def __exit__(self, *exc):
        if self.p:
            self.p.send_signal(signal.SIGTERM)
            try:
                self.p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.p.kill()


def load_samples(path):
    rows = []
    with open(path) as f:
        for ln in f:
            ln = ln.strip()
            if ln:
                try:
                    rows.append(json.loads(ln))
                except ValueError:
                    pass
    return rows


def cpu_millicores(samples, who, node=None):
    """usec-counter deltas -> millicores series. who in {controller, agent}."""
    series = []
    prev_t = prev_c = None
    for s in samples:
        if who == "controller":
            c = s["controller"]["cpu_usage_usec"]
        else:
            a = s["agents"].get(node) or {}
            c = a.get("cpu_usage_usec", -1)
        t = s["t"]
        if prev_c is not None and c >= 0 and prev_c >= 0 and t > prev_t:
            dt = t - prev_t
            mc = (c - prev_c) / (dt * 1e6) * 1000.0
            series.append(max(0.0, mc))
        prev_t, prev_c = t, c
    return series


def rss_series(samples, who, node=None):
    out = []
    for s in samples:
        if who == "controller":
            v = s["controller"]["memory_current"]
        else:
            v = (s["agents"].get(node) or {}).get("memory_current", -1)
        if v and v >= 0:
            out.append(v)
    return out


def pctl(v, p):
    if not v:
        return None
    v = sorted(v)
    return v[min(len(v) - 1, int(p * len(v)))]


def summarize_window(path):
    s = load_samples(path)
    exp = len(s)
    cc = cpu_millicores(s, "controller")
    cr = rss_series(s, "controller")
    rep = {
        "samples": exp,
        "controller_cpu_mc_median": round(st.median(cc), 1) if cc else None,
        "controller_cpu_mc_p95": round(pctl(cc, 0.95), 1) if cc else None,
        "controller_rss_median_mb": round(st.median(cr) / 1e6, 1) if cr else None,
        "controller_rss_max_mb": round(max(cr) / 1e6, 1) if cr else None,
        "agents": {},
    }
    agg_cpu, agg_rss, max_cpu, max_rss = [], [], 0, 0
    for n in WORKERS:
        ac = cpu_millicores(s, "agent", n)
        ar = rss_series(s, "agent", n)
        if not ac or not ar:
            continue
        med_cpu = st.median(ac)
        med_rss = st.median(ar) / 1e6
        rep["agents"][n] = {
            "cpu_mc_median": round(med_cpu, 1),
            "cpu_mc_p95": round(pctl(ac, 0.95), 1),
            "rss_median_mb": round(med_rss, 1),
            "rss_max_mb": round(max(ar) / 1e6, 1),
        }
        agg_cpu.append(med_cpu)
        agg_rss.append(med_rss)
        max_cpu = max(max_cpu, med_cpu)
        max_rss = max(max_rss, med_rss)
    rep["agent_cpu_mc_max"] = round(max_cpu, 1)
    rep["agent_rss_mb_max"] = round(max_rss, 1)
    rep["agent_cpu_mc_aggregate"] = round(sum(agg_cpu), 1)
    rep["agent_rss_mb_aggregate"] = round(sum(agg_rss), 1)
    # push_inflight stays zero, counters flat?
    infl = [ (a or {}).get("push_inflight", 0)
             for row in s for a in row["agents"].values() ]
    rep["push_inflight_max"] = max(infl) if infl else None
    bi = [ (row["agents"].get(n) or {}).get("bytes_in", 0)
           for row in s for n in WORKERS ]
    rep["idle_counters_flat"] = (max(infl) == 0) if infl else None
    return rep


def part_idle(tag):
    os.makedirs(RES, exist_ok=True)
    ok, reasons = preconditions()
    print(f"[e8] {tag}: preconditions ok={ok} {reasons}", flush=True)
    print(f"[e8] {tag}: settling 5 min ...", flush=True)
    time.sleep(300)
    path = f"{RES}/idle-{tag}.jsonl"
    print(f"[e8] {tag}: sampling 10 min ...", flush=True)
    with Sampler(path):
        time.sleep(600)
    rep = summarize_window(path)
    json.dump(rep, open(f"{RES}/idle-{tag}.json", "w"), indent=1)
    print(f"[e8] {tag}: controller CPU med={rep['controller_cpu_mc_median']}mc "
          f"p95={rep['controller_cpu_mc_p95']}mc RSS med={rep['controller_rss_median_mb']}MB "
          f"max={rep['controller_rss_max_mb']}MB", flush=True)
    print(f"[e8] {tag}: agent CPU max={rep['agent_cpu_mc_max']}mc "
          f"aggregate={rep['agent_cpu_mc_aggregate']}mc RSS agg={rep['agent_rss_mb_aggregate']}MB "
          f"push_inflight_max={rep['push_inflight_max']}", flush=True)
    print(f"[e8] {tag} DONE", flush=True)



IMG = "192.168.1.163:5000/wl-e8:latest"


def render_d_templates():
    """One template per worker; a run pins its producer+consumer there."""
    for w in WORKERS:
        y = f"""apiVersion: wl.io/v1
kind: ODAGTemplate
metadata: {{name: e8d-{w}, namespace: wl-system}}
spec:
  scheduler: random
  profiling: {{enabled: false, runtimeSource: manual, bandwidthSource: external}}
  defaults: {{runtime: 1, dataSize: 1MB}}
  retention: {{maxRuns: 200, data: {{policy: keepLatest, keepRuns: 1}}}}
  tasks:
    - name: producer
      image: {IMG}
      command: [python, task.py]
      dependencies: []
      dataSize: "1048576"
      runtime: 1
      resources: {{cpu: "1", memory: "256Mi"}}
      constraints: {{nodeNames: [{w}]}}
      env:
        - {{name: WL_ROLE, value: produce}}
        - {{name: WL_BYTES, value: "1048576"}}
        - {{name: WL_NOBJ, value: "1"}}
      outputs:
        - {{name: obj0, dataSize: "1048576"}}
    - name: consumer
      image: {IMG}
      command: [python, task.py]
      dependencies: [producer]
      dataSize: "16"
      runtime: 1
      resources: {{cpu: "1", memory: "256Mi"}}
      constraints: {{nodeNames: [{w}]}}
      env:
        - {{name: WL_ROLE, value: consume}}
        - {{name: WL_BYTES, value: "1048576"}}
      inputs:
        - {{producer: producer, object: obj0}}
"""
        open(f"/tmp/e8d-{w}.yml", "w").write(y)
        kubectl(f"apply -f /tmp/e8d-{w}.yml >/dev/null")


def submit_run(template):
    r = sh(f"/home/anrg/wayline/bin/wayline run {template} -n {NS}")
    import re
    m = re.search(rf"({template}-run-[a-z0-9]+)", r.stdout + r.stderr)
    return m.group(1) if m else None


def part_d(block):
    os.makedirs(RES, exist_ok=True)
    render_d_templates()
    rng = random.Random(SEED + block)
    concurrencies = [1] if SMOKE else [1, 4, 8, 16, 32, 64]
    rng.shuffle(concurrencies)
    rows = []
    for R in concurrencies:
        ok, reasons = preconditions()
        if not ok:
            print(f"[e8] D R={R} b{block}: PRECOND FAIL {reasons}", flush=True)
            rows.append({"part": "D", "block": block, "R": R, "valid": False,
                         "reasons": ";".join(reasons)})
            continue
        path = f"{RES}/D-R{R}-b{block}.jsonl"
        t_submit0 = time.time()
        with Sampler(path):
            # burst-submit R runs within 2s using background processes
            procs = []
            for i in range(R):
                w = WORKERS[i % len(WORKERS)]
                procs.append(subprocess.Popen(
                    f"/home/anrg/wayline/bin/wayline run e8d-{w} -n {NS} "
                    f">/tmp/e8d-sub-{block}-{R}-{i}.out 2>&1", shell=True))
            for pr in procs:
                pr.wait()
            submit_span = time.time() - t_submit0
            runs = []
            import re
            for i in range(R):
                try:
                    txt = open(f"/tmp/e8d-sub-{block}-{R}-{i}.out").read()
                    m = re.search(r"(e8d-[a-z0-9-]+-run-[a-z0-9]+)", txt)
                    if m:
                        runs.append(m.group(1))
                except OSError:
                    pass
            # poll to convergence, tracking live counts + first-pod
            first_pod = {}
            max_pods = max_odags = max_goro = 0
            t0 = time.time()
            done = set()
            while time.time() - t0 < DEADLINE and len(done) < len(runs):
                pods = kubectl("get pods --no-headers 2>/dev/null | grep e8d- || true").stdout
                np = len([l for l in pods.splitlines() if l.strip()])
                max_pods = max(max_pods, np)
                for ln in pods.splitlines():
                    nm = ln.split()[0] if ln.split() else ""
                    for run in runs:
                        if nm.startswith(run) and run not in first_pod:
                            first_pod[run] = time.time()
                od = kubectl("get odags --no-headers 2>/dev/null | grep e8d- || true").stdout
                max_odags = max(max_odags, len([l for l in od.splitlines() if l.strip()]))
                for ln in od.splitlines():
                    f = ln.split()
                    if len(f) >= 2 and f[1] in ("Succeeded", "Failed"):
                        done.add(f[0])
                time.sleep(1)
            drain_span = time.time() - t0
        # collect makespans + placement + restarts
        mks, restarts_total, placement_ok = [], 0, True
        for run in runs:
            d = kubectl(f"get odag {run} -o json").stdout
            try:
                obj = json.loads(d); stt = obj.get("status", {})
            except ValueError:
                continue
            if stt.get("makespan"):
                mks.append(float(stt["makespan"]))
            w_expect = run.split("-run-")[0].replace("e8d-", "")
            for t in stt.get("tasks", []):
                if t.get("node") and t["node"] != w_expect:
                    placement_ok = False
        # agent goroutine peak from samples
        samp = load_samples(path)
        for srow in samp:
            g = sum((a or {}).get("goroutines", 0) for a in srow["agents"].values())
            max_goro = max(max_goro, g)
        cc = cpu_millicores(samp, "controller"); cr = rss_series(samp, "controller")
        succeeded = sum(1 for run in runs if "Succeeded" in
                        kubectl(f"get odag {run} -o jsonpath='{{.status.phase}}'").stdout)
        fp = [first_pod[r] - t_submit0 for r in runs if r in first_pod]
        row = {"part": "D", "block": block, "R": R, "submitted": len(runs),
               "succeeded": succeeded, "submit_span_s": round(submit_span, 2),
               "drain_span_s": round(drain_span, 1),
               "runs_per_min": round(succeeded / (drain_span / 60), 1) if drain_span else 0,
               "sub_to_firstpod_med_s": round(st.median(fp), 2) if fp else None,
               "makespan_med_s": round(st.median(mks), 1) if mks else None,
               "ctrl_cpu_mc_median": round(st.median(cc), 1) if cc else None,
               "ctrl_cpu_mc_p95": round(pctl(cc, 0.95), 1) if cc else None,
               "ctrl_rss_max_mb": round(max(cr) / 1e6, 1) if cr else None,
               "max_live_pods": max_pods, "max_live_odags": max_odags,
               "max_agent_goroutines": max_goro,
               "placement_ok": placement_ok,
               "valid": (succeeded == len(runs) == R and placement_ok and drain_span < DEADLINE)}
        rows.append(row)
        print(f"[e8] D R={R} b{block}: valid={row['valid']} succeeded={succeeded}/{R} "
              f"drain={row['drain_span_s']}s runs/min={row['runs_per_min']} "
              f"ctrl_cpu_med={row['ctrl_cpu_mc_median']}mc firstpod_med={row['sub_to_firstpod_med_s']}s "
              f"max_pods={max_pods}", flush=True)
        # cleanup this burst's runs
        for run in runs:
            kubectl(f"delete odag {run} --ignore-not-found >/dev/null 2>&1")
        for n, ip in agent_ips().items():
            for run in runs:
                sh(f"curl -s -m20 -X DELETE http://{ip}:8082/data/{run} >/dev/null", timeout=25)
        time.sleep(5)
    open(f"{RES}/D-b{block}.json", "w").write(json.dumps(rows, indent=1))
    return rows



import hashlib

# ---------------------------------------------------------------------------
# Shared realization / object helpers (Parts B and C)
# ---------------------------------------------------------------------------

def agent_ip_map():
    return agent_ips()


def gen_digest(key, size):
    """SHA-256 of the deterministic content task.py would emit for (key,size).
    Streamed so a 256 MiB object needs no 256 MiB allocation."""
    block = hashlib.sha256(key.encode()).digest() * 32768  # 1 MiB
    h = hashlib.sha256()
    rem = size
    while rem > 0:
        chunk = block if rem >= len(block) else block[:rem]
        h.update(chunk)
        rem -= len(chunk)
    return h.hexdigest()


def render_source_template(name, producers, mib, gate_s=280):
    """producers: list of (task, worker, nobj). One 256/1 MiB producer per
    source worker plus a gate that keeps the run live for realization."""
    nbytes = mib * 1024 * 1024
    tasks = []
    for task, worker, nobj in producers:
        outs = "".join(
            f"        - {{name: obj{j}, dataSize: \"{nbytes}\"}}\n"
            for j in range(nobj))
        tasks.append(
f"""    - name: {task}
      image: {IMG}
      command: [python, task.py]
      dependencies: []
      dataSize: "{nbytes}"
      runtime: 1
      constraints: {{nodeNames: [{worker}]}}
      env:
        - {{name: WL_ROLE, value: produce}}
        - {{name: WL_BYTES, value: "{nbytes}"}}
        - {{name: WL_NOBJ, value: "{nobj}"}}
      outputs:
{outs}""")
    deps = ", ".join(t for t, _, _ in producers)
    tasks.append(
f"""    - name: gate
      image: {IMG}
      command: [python, task.py]
      dependencies: [{deps}]
      dataSize: "16"
      runtime: 1
      constraints: {{nodeNames: [{producers[0][1]}]}}
      env:
        - {{name: WL_ROLE, value: gate}}
        - {{name: WL_GATE_S, value: "{gate_s}"}}
""")
    y = (f"apiVersion: wl.io/v1\nkind: ODAGTemplate\n"
         f"metadata: {{name: {name}, namespace: {NS}}}\nspec:\n"
         f"  scheduler: random\n"
         f"  profiling: {{enabled: false, runtimeSource: manual, bandwidthSource: external}}\n"
         f"  defaults: {{runtime: 1, dataSize: 1MB}}\n"
         f"  retention: {{maxRuns: 50, data: {{policy: keepLatest, keepRuns: 1}}}}\n"
         f"  tasks:\n" + "\n".join(tasks))
    open(f"/tmp/{name}.yml", "w").write(y)
    kubectl(f"apply -f /tmp/{name}.yml >/dev/null")


def wl_run(template, timeout=60):
    import re
    r = sh(f"/home/anrg/wayline/bin/wayline run {template} -n {NS}", timeout=timeout)
    m = re.search(rf"({template}-run-[a-z0-9]+)", r.stdout + r.stderr)
    return m.group(1) if m else None


def installed_record(ip, run, objkey):
    r = sh(f"curl -s -m6 http://{ip}:8082/installed/{run}/{objkey}", timeout=8)
    try:
        return json.loads(r.stdout)
    except (ValueError, TypeError):
        return None


def copy_digest(ip, run, objkey):
    return sh(f"curl -s -m6 http://{ip}:8082/digest/{run}/{objkey}",
              timeout=8).stdout.strip()


def wait_source_installed(run, src_objs, ips, timeout=180):
    """src_objs: list of (objkey, src_worker). Wait until every source copy is
    installed (its producer ran and the local output is readable)."""
    t0 = time.time()
    pending = list(src_objs)
    while pending and time.time() - t0 < timeout:
        still = []
        for objkey, src in pending:
            rec = installed_record(ips.get(src, ""), run, objkey)
            if not rec:
                still.append((objkey, src))
        pending = still
        if pending:
            time.sleep(2)
    return not pending


def patch_realization(run, entries):
    """Merge-patch spec.realization with a full entry list. Returns
    (ack_time, patch_bytes, resource_version, generation)."""
    body = json.dumps({"spec": {"realization": entries}})
    open("/tmp/e8-realization.json", "w").write(body)
    t = time.time()
    kubectl(f"patch odag {run} --type merge --patch-file /tmp/e8-realization.json "
            ">/dev/null", timeout=60)
    ack = time.time()
    rv = kubectl(f"get odag {run} -o jsonpath='{{.metadata.resourceVersion}}'").stdout.strip()
    gen = kubectl(f"get odag {run} -o jsonpath='{{.metadata.generation}}'").stdout.strip()
    return ack, len(body.encode()), rv, gen


def status_objects(run):
    d = kubectl(f"get odag {run} -o json").stdout
    try:
        objs = json.loads(d).get("status", {}).get("objects", []) or []
    except ValueError:
        objs = []
    return objs, len(json.dumps(objs).encode())


def targets_for(k, src_idx, C):
    """C distinct target workers for object k, none equal to the source,
    rotated so targets spread across workers."""
    ts = []
    for t in range(C):
        off = 1 + ((k + t) % (len(WORKERS) - 1))
        ts.append(WORKERS[(src_idx + off) % len(WORKERS)])
    # de-dup defensively while keeping order (offsets already distinct for C<=7)
    seen, out = set(), []
    for w in ts:
        if w not in seen and w != WORKERS[src_idx]:
            seen.add(w); out.append(w)
    return out


# ---------------------------------------------------------------------------
# Part B: scaling with named objects and desired copies
# ---------------------------------------------------------------------------

SMOKE = os.environ.get("E8_SMOKE", "") == "1"
B_CONFIGS = [(1, 1)] if SMOKE else \
    [(1, 1), (8, 1), (32, 1), (128, 1), (64, 2), (32, 4), (16, 7)]


def part_b(block):
    os.makedirs(RES, exist_ok=True)
    rng = random.Random(SEED + 100 + block)
    cfgs = B_CONFIGS[:]
    rng.shuffle(cfgs)
    rows = []
    for (O, C) in cfgs:
        row = run_b_trial(O, C, block)
        rows.append(row)
        print(f"[e8] B O={O} C={C} b{block}: valid={row.get('valid')} "
              f"conv={row.get('patch_to_conv_s')}s copies={row.get('copies_installed')}/"
              f"{row.get('copies_requested')} inst_p95={row.get('install_p95_s')}s "
              f"ops/s={row.get('copy_ops_per_s')} ctrl_cpu_cs={row.get('ctrl_cpu_core_s')} "
              f"digests_ok={row.get('digests_ok')}", flush=True)
    json.dump(rows, open(f"{RES}/B-b{block}.json", "w"), indent=1)
    return rows


def run_b_trial(O, C, block):
    ips = agent_ip_map()
    ok, reasons = preconditions()
    if not ok:
        return {"part": "B", "block": block, "O": O, "C": C, "valid": False,
                "reasons": ";".join(reasons)}
    # 8 producers, object k -> worker k%8, local index k//8
    counts = [0] * len(WORKERS)
    objmap = []  # (objkey, src_worker, src_idx)
    for k in range(O):
        wi = k % len(WORKERS)
        j = counts[wi]; counts[wi] += 1
        objmap.append((f"bsrc{wi}.obj{j}", WORKERS[wi], wi))
    producers = [(f"bsrc{i}", WORKERS[i], counts[i])
                 for i in range(len(WORKERS)) if counts[i] > 0]
    tmpl = f"e8b{O}x{C}"
    render_source_template(tmpl, producers, mib=1, gate_s=290)
    run = wl_run(tmpl)
    if not run:
        return {"part": "B", "block": block, "O": O, "C": C, "valid": False,
                "reasons": "submit failed"}
    if not wait_source_installed(run, [(o, s) for o, s, _ in objmap], ips, timeout=180):
        cleanup_run(run, ips)
        return {"part": "B", "block": block, "O": O, "C": C, "valid": False,
                "reasons": "source objects not installed"}
    # build realization: one entry per object, C rotating targets, serve first
    entries, want = [], {}
    for k, (objkey, src, si) in enumerate(objmap):
        ts = targets_for(k, si, C)
        entries.append({"object": objkey, "copies": ts,
                        "servingCopy": ts[0], "evict": []})
        want[objkey] = (src, ts)
    total_copies = sum(len(v[1]) for v in want.values())
    path = f"{RES}/B-O{O}C{C}-b{block}.jsonl"
    with Sampler(path):
        time.sleep(2)  # baseline samples
        ack, patch_bytes, rv, gen = patch_realization(run, entries)
        conv, max_status, install_ts, bind_ts = poll_convergence(
            run, want, ips, ack, want_state="Installed")
    # metrics from samples
    samp = load_samples(path)
    max_infl = max_push_inflight_from(samp)
    cc = cpu_millicores(samp, "controller")
    cr = rss_series(samp, "controller")
    ctrl_cs = round(sum(cc) / 1000.0, 2) if cc else None
    agent_cs, agent_rss_peak = agent_cpu_coreseconds(samp)
    # latency decomposition
    inst_lat = sorted(v - ack for v in install_ts.values())
    bind_lat = sorted(v - ack for v in bind_ts.values())
    installed = len(install_ts)
    conv_ok = installed == total_copies and len(bind_ts) == len(want)
    conv_s = round(conv - ack, 2) if conv else None
    # verification: digest + length + provenance on every installed copy
    digests_ok, length_ok, flow_ok = verify_copies(run, want, ips, expect_mib=1)
    # cleanup: evict all copies, verify absence, delete run
    evict_ok = evict_and_verify(run, want, ips)
    cleanup_run(run, ips)
    valid = (conv_ok and conv_s is not None and conv_s < DEADLINE and
             digests_ok and length_ok and flow_ok and evict_ok)
    return {
        "part": "B", "block": block, "O": O, "C": C,
        "copies_requested": total_copies, "copies_installed": installed,
        "patch_bytes": patch_bytes, "max_status_bytes": max_status,
        "resource_version": rv, "generation": gen,
        "patch_to_conv_s": conv_s,
        "install_med_s": rnd(pctl_l(inst_lat, 0.5)),
        "install_p95_s": rnd(pctl_l(inst_lat, 0.95)),
        "install_p99_s": rnd(pctl_l(inst_lat, 0.99)),
        "bind_med_s": rnd(pctl_l(bind_lat, 0.5)),
        "bind_p95_s": rnd(pctl_l(bind_lat, 0.95)),
        "bind_p99_s": rnd(pctl_l(bind_lat, 0.99)),
        "copy_ops_per_s": round(installed / conv_s, 2) if conv_s else None,
        "ctrl_cpu_core_s": ctrl_cs,
        "ctrl_rss_peak_mb": round(max(cr) / 1e6, 1) if cr else None,
        "agent_cpu_core_s": agent_cs, "agent_rss_peak_mb": agent_rss_peak,
        "max_push_inflight": max_infl,
        "digests_ok": digests_ok, "length_ok": length_ok, "flow_ok": flow_ok,
        "evict_verified": evict_ok, "valid": valid,
    }


# ---------------------------------------------------------------------------
# Part C: scaling with simultaneously-active transfers
# ---------------------------------------------------------------------------

C_CONCURRENCIES = [1] if SMOKE else [1, 4, 8, 16, 32]


def balanced_c_pairs(A):
    """A objects on distinct directed src->dst pairs; <=4 flows per node in
    either direction. Object m at a source keeps a per-source local index so
    its key is csrc<srcidx>.obj<local>."""
    from collections import Counter
    pairs, local = [], Counter()
    for k in range(A):
        si = k % len(WORKERS)
        off = 1 + (k // len(WORKERS))
        di = (si + off) % len(WORKERS)
        j = local[si]; local[si] += 1
        pairs.append((f"csrc{si}.obj{j}", WORKERS[si], si, WORKERS[di]))
    sc = Counter(p[1] for p in pairs)
    dc = Counter(p[3] for p in pairs)
    assert max(sc.values()) <= 4 and max(dc.values()) <= 4, (dict(sc), dict(dc))
    return pairs


def part_c(block):
    os.makedirs(RES, exist_ok=True)
    rng = random.Random(SEED + 200 + block)
    cfgs = C_CONCURRENCIES[:]
    rng.shuffle(cfgs)
    rows = []
    for A in cfgs:
        row = run_c_trial(A, block, mib=256)
        rows.append(row)
        print(f"[e8] C A={A} b{block}: valid={row.get('valid')} "
              f"peak_concurrent={row.get('peak_concurrent')} "
              f"overlap_ok={row.get('overlap_ok')} conv={row.get('patch_to_conv_s')}s "
              f"goodput={row.get('agg_goodput_mbps')}Mbps digests_ok={row.get('digests_ok')}",
              flush=True)
    json.dump(rows, open(f"{RES}/C-b{block}.json", "w"), indent=1)
    return rows


def run_c_trial(A, block, mib=256):
    ips = agent_ip_map()
    ok, reasons = preconditions()
    if not ok:
        return {"part": "C", "block": block, "A": A, "valid": False,
                "reasons": ";".join(reasons)}
    pairs = balanced_c_pairs(A)
    from collections import Counter
    cnt = Counter(p[2] for p in pairs)
    producers = [(f"csrc{si}", WORKERS[si], cnt[si]) for si in sorted(cnt)]
    tmpl = f"e8c{A}"
    render_source_template(tmpl, producers, mib=mib, gate_s=290)
    run = wl_run(tmpl)
    if not run:
        return {"part": "C", "block": block, "A": A, "valid": False,
                "reasons": "submit failed"}
    src_objs = [(objkey, src) for objkey, src, si, dst in pairs]
    if not wait_source_installed(run, src_objs, ips, timeout=300):
        cleanup_run(run, ips)
        return {"part": "C", "block": block, "A": A, "valid": False,
                "reasons": "source objects not installed"}
    # shape selected pairs at 59 mbit
    cap_pairs = [(objkey, src, dst) for objkey, src, si, dst in pairs]
    shaping_applied = c_cap(cap_pairs, "apply")
    want = {objkey: (src, [dst]) for objkey, src, si, dst in pairs}
    entries = [{"object": objkey, "copies": [dst], "servingCopy": dst, "evict": []}
               for objkey, src, si, dst in pairs]
    path = f"{RES}/C-A{A}-b{block}.jsonl"
    with Sampler(path):
        time.sleep(2)
        ack, patch_bytes, rv, gen = patch_realization(run, entries)
        conv, max_status, install_ts, bind_ts, concur = \
            poll_convergence(run, want, ips, ack, want_state="Installed",
                             track_transferring=True)
    samp = load_samples(path)
    src_dst = sorted({src for _, src, _, _ in pairs} |
                     {dst for _, _, _, dst in pairs})
    max_infl = max_push_inflight_from(samp, src_dst)
    cc = cpu_millicores(samp, "controller"); cr = rss_series(samp, "controller")
    agent_cs, agent_rss_peak = agent_cpu_coreseconds(samp)
    # concurrency: peak simultaneous Transferring + longest run of >=A (>=min(A,..))
    peak = max(concur) if concur else 0
    need = A if A >= 4 else 1
    overlap_ok = longest_run_ge(concur, need) >= (5 if A >= 4 else 1)
    installed = len(install_ts)
    conv_s = round(conv - ack, 2) if conv else None
    inst_lat = sorted(v - ack for v in install_ts.values())
    nbytes = mib * 1024 * 1024
    goodput = None
    if conv_s and conv_s > 0:
        goodput = round(installed * nbytes * 8 / conv_s / 1e6, 1)  # Mbit/s
    per_xfer = [round(x, 2) for x in inst_lat]
    digests_ok, length_ok, flow_ok = verify_copies(run, want, ips, expect_mib=mib)
    # src+dst push_inflight peak already in max_infl
    evict_ok = evict_and_verify(run, want, ips)
    c_cap(cap_pairs, "clear")
    shaping_gone = verify_no_qdisc(cap_pairs)
    cleanup_run(run, ips)
    valid = (installed == len(want) and conv_s is not None and conv_s < DEADLINE
             and overlap_ok and digests_ok and length_ok and flow_ok
             and evict_ok and shaping_gone and shaping_applied)
    return {
        "part": "C", "block": block, "A": A, "object_mib": mib,
        "requested": len(want), "installed": installed,
        "peak_concurrent": peak, "overlap_ok": overlap_ok,
        "concurrency_series": concur,
        "patch_bytes": patch_bytes, "max_status_bytes": max_status,
        "patch_to_conv_s": conv_s,
        "per_transfer_latency_s": per_xfer,
        "xfer_med_s": rnd(pctl_l(inst_lat, 0.5)),
        "xfer_p95_s": rnd(pctl_l(inst_lat, 0.95)),
        "agg_goodput_mbps": goodput,
        "ctrl_cpu_core_s": round(sum(cc) / 1000.0, 2) if cc else None,
        "ctrl_rss_peak_mb": round(max(cr) / 1e6, 1) if cr else None,
        "agent_cpu_core_s": agent_cs, "agent_rss_peak_mb": agent_rss_peak,
        "max_push_inflight": max_infl,
        "digests_ok": digests_ok, "length_ok": length_ok, "flow_ok": flow_ok,
        "evict_verified": evict_ok, "shaping_removed": shaping_gone,
        "shaping_applied_ok": shaping_applied,
        "valid": valid,
    }


# ---------------------------------------------------------------------------
# Convergence polling + verification + cleanup (shared)
# ---------------------------------------------------------------------------

def poll_convergence(run, want, ips, ack, want_state="Installed",
                     track_transferring=False):
    """Poll status.objects until every requested copy reaches want_state and
    every object's servingCopy is bound. Returns
    (conv_time|None, max_status_bytes, install_ts{objkey#node->t},
     bind_ts{objkey->t}, max_push_inflight[, concurrency_series])."""
    install_ts, bind_ts = {}, {}
    max_status = 0
    concur = []
    total = sum(len(v[1]) for v in want.values())
    t0 = time.time()
    conv = None
    # push_inflight is captured by the 1 Hz sampler; do not add per-tick
    # /metrics curls here -- that would perturb the CPU we are measuring.
    while time.time() - t0 < DEADLINE:
        objs, sz = status_objects(run)
        max_status = max(max_status, sz)
        by = {o.get("object"): o for o in objs}
        transferring = 0
        for objkey, (src, targets) in want.items():
            o = by.get(objkey, {})
            states = {c.get("node"): c.get("state") for c in o.get("copies", [])}
            for tgt in targets:
                stt = states.get(tgt)
                if stt == "Transferring":
                    transferring += 1
                key = f"{objkey}#{tgt}"
                if stt == want_state and key not in install_ts:
                    rec = installed_record(ips.get(tgt, ""), run, objkey)
                    install_ts[key] = rec["unixTime"] if rec else time.time()
            serving = o.get("servingCopy", "")
            if (objkey not in bind_ts and serving == targets[0]
                    and states.get(targets[0]) == want_state):
                bind_ts[objkey] = time.time()
        if track_transferring:
            concur.append(transferring)
        if len(install_ts) >= total and len(bind_ts) >= len(want):
            conv = time.time()
            break
        time.sleep(1)
    if track_transferring:
        return conv, max_status, install_ts, bind_ts, concur
    return conv, max_status, install_ts, bind_ts


def longest_run_ge(series, thresh):
    best = cur = 0
    for v in series:
        if v >= thresh:
            cur += 1; best = max(best, cur)
        else:
            cur = 0
    return best


def verify_copies(run, want, ips, expect_mib):
    """Digest + length + flow-provenance for every requested copy."""
    nbytes = expect_mib * 1024 * 1024
    digests_ok = length_ok = flow_ok = True
    for objkey, (src, targets) in want.items():
        expect = gen_digest(objkey, nbytes)
        for tgt in targets:
            ip = ips.get(tgt, "")
            if copy_digest(ip, run, objkey) != expect:
                digests_ok = False
            rec = installed_record(ip, run, objkey)
            if not rec or rec.get("bytes") != nbytes:
                length_ok = False
            if not rec or rec.get("fromNode") not in (src, None) or \
                    rec.get("source") != "remote":
                flow_ok = False
    return digests_ok, length_ok, flow_ok


def evict_and_verify(run, want, ips):
    """Evict every requested copy and confirm it is gone from status and disk."""
    entries = [{"object": o, "copies": [], "servingCopy": "",
                "evict": t[1]} for o, t in want.items()]
    patch_realization(run, entries)
    t0 = time.time()
    while time.time() - t0 < 120:
        objs, _ = status_objects(run)
        by = {o.get("object"): o for o in objs}
        remaining = 0
        for objkey, (src, targets) in want.items():
            states = {c.get("node"): c.get("state")
                      for c in by.get(objkey, {}).get("copies", [])}
            for tgt in targets:
                if states.get(tgt) in ("Installed", "Transferring"):
                    remaining += 1
        if remaining == 0:
            break
        time.sleep(2)
    # confirm on disk via /installed 404
    gone = True
    for objkey, (src, targets) in want.items():
        for tgt in targets:
            if installed_record(ips.get(tgt, ""), run, objkey) is not None:
                gone = False
    return gone


def cleanup_run(run, ips):
    kubectl(f"delete odag {run} --ignore-not-found >/dev/null 2>&1")
    for n, ip in ips.items():
        sh(f"curl -s -m30 -X DELETE http://{ip}:8082/data/{run} >/dev/null",
           timeout=40)
    time.sleep(3)


def verify_no_qdisc(cap_pairs):
    srcs = sorted({p[1] for p in cap_pairs})
    ok = True
    for sname in srcs:
        fw = f"e8-fw-{sname}"
        out = kubectl(f"exec {fw} -- sh -c 'tc qdisc show 2>/dev/null | "
                      f"grep -c htb || true'").stdout.strip()
        if out and out != "0":
            ok = False
    return ok


def max_push_inflight_from(samp, nodes=None):
    """Peak push.inflight across the sampled window (all agents, or a subset)."""
    nodes = nodes or WORKERS
    peak = 0
    for row in samp:
        for n in nodes:
            a = row["agents"].get(n) or {}
            v = a.get("push_inflight", 0)
            if isinstance(v, (int, float)) and v > peak:
                peak = int(v)
    return peak


def agent_cpu_coreseconds(samp):
    total_cs, peak_rss = 0.0, 0
    for n in WORKERS:
        ac = cpu_millicores(samp, "agent", n)
        ar = rss_series(samp, "agent", n)
        if ac:
            total_cs += sum(ac) / 1000.0
        if ar:
            peak_rss = max(peak_rss, max(ar))
    return round(total_cs, 2), round(peak_rss / 1e6, 1)


def pctl_l(sorted_list, p):
    if not sorted_list:
        return None
    i = min(len(sorted_list) - 1, int(p * len(sorted_list)))
    return sorted_list[i]


def rnd(x):
    return round(x, 2) if x is not None else None


def c_cap(pairs, action):
    """Cap each selected src->dst at 59mbit (tcp/8082) via a fw pod on each
    source; action in {apply, clear}. Each source gets one HTB qdisc with an
    unlimited default class and one INDEPENDENT 59mbit class + u32 filter per
    destination (so concurrent flows to different targets do not split one
    class). Every action asserts its own effect; returns True iff all sources
    reached the expected state."""
    ips = agent_node_internal_ips()
    srcs = sorted({p[1] for p in pairs})
    all_ok = True
    for sname in srcs:
        fw = f"e8-fw-{sname}"
        if not _ensure_fw(fw, sname):
            all_ok = False
            continue
        dests = [p[2] for p in pairs if p[1] == sname]
        anchor = ips[dests[0]]
        # NOTE: this string is parsed by anrg-2's host /bin/sh (shell=True)
        # before reaching the pod, so every '$' that must evaluate INSIDE the
        # pod is escaped as \$ (same discipline as e7_net.sh); literal IPs are
        # spliced in directly.
        # Extract the egress interface with cut (not awk): awk's $2 does not
        # survive the host-shell -> kubectl -> pod-shell quoting layers.
        ifexpr = (f"IF=\\$(ip route get {anchor} | grep -o 'dev [^ ]*' | "
                  "cut -d' ' -f2)")
        if action == "apply":
            cmds = (f"{ifexpr}; "
                    "tc qdisc del dev \\$IF root 2>/dev/null; "
                    "tc qdisc add dev \\$IF root handle 1: htb default 30; "
                    "tc class add dev \\$IF parent 1: classid 1:30 htb rate 10gbit ceil 10gbit; ")
            cid = 11
            for d in dests:
                cmds += (f"tc class add dev \\$IF parent 1: classid 1:{cid} htb rate 59mbit ceil 59mbit; "
                         f"tc filter add dev \\$IF parent 1: protocol ip prio 1 u32 "
                         f"match ip dst {ips[d]}/32 match ip dport 8082 0xffff flowid 1:{cid}; ")
                cid += 1
            cmds += "true"
            kubectl(f"exec {fw} -- sh -c \"{cmds}\"", timeout=60)
            # verify: one 59Mbit class + one flowid filter per destination
            v = kubectl(f"exec {fw} -- sh -c \"{ifexpr}; "
                        "echo NCLS=\\$(tc class show dev \\$IF | grep -c 'rate 59Mbit') "
                        "NFILT=\\$(tc filter show dev \\$IF | grep -c 'flowid 1:1')\"",
                        timeout=30).stdout
            import re as _re
            ncls = _re.search(r"NCLS=(\d+)", v)
            nfilt = _re.search(r"NFILT=(\d+)", v)
            if not (ncls and nfilt and int(ncls.group(1)) == len(dests)
                    and int(nfilt.group(1)) == len(dests)):
                print(f"[e8] c_cap apply {sname}: want {len(dests)}/{len(dests)} "
                      f"got {v.strip()}", flush=True)
                all_ok = False
        else:
            kubectl(f"exec {fw} -- sh -c \"{ifexpr}; "
                    "tc qdisc del dev \\$IF root 2>/dev/null; true\"", timeout=60)
            v = kubectl(f"exec {fw} -- sh -c \"{ifexpr}; "
                        "tc filter show dev \\$IF | grep -c 'flowid 1:1' || true\"",
                        timeout=30).stdout.strip()
            n = "".join(c for c in v.splitlines()[-1] if c.isdigit()) if v else "0"
            if n not in ("", "0"):
                all_ok = False
    return all_ok



def agent_node_internal_ips():
    out = {}
    raw = kubectl("get nodes -o jsonpath="
                  "'{range .items[*]}{.metadata.name}={.status.addresses[?(@.type==\"InternalIP\")].address} {end}'").stdout
    for tok in raw.split():
        if "=" in tok:
            n, ip = tok.split("=", 1)
            out[n] = ip
    return out



def _ensure_fw(name, node):
    ph = kubectl(f"get pod {name} -o jsonpath='{{.status.phase}}'").stdout.strip()
    if ph == "Running":
        return True
    kubectl(f"delete pod {name} --ignore-not-found --wait=false >/dev/null 2>&1")
    spec = {"spec": {"nodeName": node, "hostNetwork": True, "restartPolicy": "Never",
            "containers": [{"name": "c", "image": "alpine", "command": ["sh", "-c",
                "apk add -q iproute2 >/dev/null && sleep infinity"],
                "securityContext": {"privileged": True}}]}}
    sh(f"kubectl -n {NS} run {name} --restart=Never --image=alpine "
       f"--overrides='{json.dumps(spec)}' >/dev/null 2>&1")
    for _ in range(40):
        if kubectl(f"exec {name} -- sh -c 'tc -V 2>/dev/null'").returncode == 0:
            time.sleep(1)
            return True
        time.sleep(3)
    return False



def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "A-before"
    if mode in ("A-before", "A-after"):
        part_idle(mode)
    elif mode == "B":
        for b in range(1, BLOCKS + 1):
            part_b(b)
        print("[e8] B DONE", flush=True)
    elif mode == "C":
        for b in range(1, BLOCKS + 1):
            part_c(b)
        print("[e8] C DONE", flush=True)
    elif mode == "D":
        for b in range(1, BLOCKS + 1):
            part_d(b)
        print("[e8] D DONE", flush=True)
    else:
        print(f"part {mode} not yet implemented", flush=True)


if __name__ == "__main__":
    main()
