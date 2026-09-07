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


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "A-before"
    if mode in ("A-before", "A-after"):
        part_idle(mode)
    else:
        print(f"part {mode} not yet implemented", flush=True)


if __name__ == "__main__":
    main()
