#!/usr/bin/env python3
"""E8 resource sampler. Runs ON anrg-2 (root). Samples once per second:

  * controller cgroup-v2 cpu.stat/memory.current/memory.peak, read from
    the anrg-2 host (the controller runs here; no exec, no worker SSH);
  * each of the eight data agents' /metrics (cgroup cpu/mem, goroutines,
    heap alloc, push.inflight, transfer/byte counters), polled in
    parallel.

CPU is a monotonic usec counter; the analyzer deltas consecutive samples
into millicores. Writes one JSON object per second to <out>, and stops
cleanly when it receives SIGTERM (the harness sends it to bound a window).

Usage: e8_sample.py <out.jsonl> [poll_hz=1]
"""
import json
import os
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

OUT = sys.argv[1]
HZ = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0
NS = "wl-system"
WORKERS = ["anrg-1", "anrg-3", "anrg-4", "anrg-5",
           "anrg-6", "anrg-7", "anrg-8", "anrg-9"]
_stop = False


def sh(cmd, timeout=8):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True,
                              text=True, timeout=timeout).stdout.strip()
    except subprocess.SubprocessError:
        return ""


def controller_cgroup_dir():
    """Locate the controller container's cgroup-v2 scope dir on the host."""
    cid = sh("crictl ps --name odag-controller -q 2>/dev/null | head -1")
    if not cid:
        return None
    d = sh(f"find /sys/fs/cgroup -maxdepth 6 -type d -name '*{cid[:32]}*' "
           f"2>/dev/null | head -1")
    return d or None


def read_controller(cgdir):
    out = {"cpu_usage_usec": -1, "memory_current": -1, "memory_peak": -1}
    if not cgdir:
        return out
    try:
        with open(f"{cgdir}/cpu.stat") as f:
            for ln in f:
                if ln.startswith("usage_usec "):
                    out["cpu_usage_usec"] = int(ln.split()[1])
        with open(f"{cgdir}/memory.current") as f:
            out["memory_current"] = int(f.read().strip())
        for pk in ("memory.peak",):
            try:
                with open(f"{cgdir}/{pk}") as f:
                    out["memory_peak"] = int(f.read().strip())
            except OSError:
                pass
    except OSError:
        pass
    return out


def agent_ips():
    raw = sh("kubectl -n %s get pods -l app=data-agent -o jsonpath="
             "'{range .items[*]}{.spec.nodeName}={.status.podIP} {end}'" % NS)
    ips = {}
    for tok in raw.split():
        if "=" in tok:
            n, ip = tok.split("=", 1)
            if n in WORKERS and ip:
                ips[n] = ip
    return ips


def poll_agent(node_ip):
    node, ip = node_ip
    raw = sh(f"curl -s -m0.8 http://{ip}:8082/metrics", timeout=1.2)
    try:
        d = json.loads(raw)
    except (ValueError, TypeError):
        return node, None
    return node, {
        "cpu_usage_usec": d.get("cgroup", {}).get("cpu_usage_usec", -1),
        "memory_current": d.get("cgroup", {}).get("memory_current", -1),
        "memory_peak": d.get("cgroup", {}).get("memory_peak", -1),
        "goroutines": d.get("goroutines", -1),
        "heap_alloc": d.get("memory", {}).get("alloc_bytes", -1),
        "push_inflight": d.get("push", {}).get("inflight", -1),
        "bytes_in": d.get("transfers", {}).get("bytes_in", -1),
        "bytes_out": d.get("push", {}).get("bytes_out", -1),
        "push_success": d.get("push", {}).get("success", -1),
        "run_count": d.get("disk", {}).get("run_count", -1),
        "disk_bytes": d.get("disk", {}).get("bytes_used", -1),
    }


def _term(*_):
    global _stop
    _stop = True


def main():
    signal.signal(signal.SIGTERM, _term)
    signal.signal(signal.SIGINT, _term)
    cgdir = controller_cgroup_dir()
    ips = agent_ips()
    period = 1.0 / HZ
    n = 0
    with open(OUT, "w") as f, ThreadPoolExecutor(max_workers=8) as pool:
        # refresh controller cgroup dir occasionally in case of a restart
        while not _stop:
            t0 = time.time()
            if n % 30 == 0:
                cgdir = controller_cgroup_dir() or cgdir
                ips = agent_ips() or ips
            sample = {"t": round(t0, 3),
                      "controller": read_controller(cgdir),
                      "agents": {}}
            for node, m in pool.map(poll_agent, list(ips.items())):
                sample["agents"][node] = m
            f.write(json.dumps(sample) + "\n")
            f.flush()
            n += 1
            dt = period - (time.time() - t0)
            if dt > 0:
                time.sleep(dt)
    print(f"[e8-sample] wrote {n} samples to {OUT}", flush=True)


if __name__ == "__main__":
    main()
