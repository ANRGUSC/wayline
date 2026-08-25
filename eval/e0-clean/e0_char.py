#!/usr/bin/env python3
"""E0: clean-testbed characterization. Runs ON anrg-2.

One privileged hostNetwork pod per worker (alpine + iperf3 + iproute2 +
iputils), everything exec'd into them strictly sequentially. No shaping
is touched, ever; qdiscs are only dumped for the record.
"""

import argparse
import csv
import json
import os
import statistics as st
import subprocess
import time

WORKERS = ["anrg-1", "anrg-3", "anrg-4", "anrg-5",
           "anrg-6", "anrg-7", "anrg-8", "anrg-9"]
NS = "wl-system"
BW_SECS, BW_REPS = 20, 5
PING_COUNT, PING_IVL, PING_REPS = 100, 0.1, 5
FAN_SECS, FAN_REPS = 20, 10
FANOUT_CASES = [
    ("producer-fanout", "anrg-3", ["anrg-6", "anrg-7", "anrg-8"]),
    ("rebound-serving", "anrg-7", ["anrg-6", "anrg-8"]),
]


def sh(cmd, timeout=120):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout)


def kexec(node, cmd, timeout=120):
    return sh(f"kubectl -n {NS} exec char-{node} -- sh -c '{cmd}'",
              timeout=timeout)


def node_ips():
    r = sh("kubectl get nodes -o json")
    out = {}
    for n in json.loads(r.stdout)["items"]:
        name = n["metadata"]["name"]
        for a in n["status"]["addresses"]:
            if a["type"] == "InternalIP":
                out[name] = a["address"]
    return out


def pods_up(ips):
    for w in WORKERS:
        spec = {"spec": {
            "nodeName": w, "hostNetwork": True, "hostPID": True,
            "restartPolicy": "Never",
            "containers": [{
                "name": "c", "image": "alpine",
                "command": ["sh", "-c",
                            "apk add -q iperf3 iproute2 iputils >/dev/null "
                            "&& iperf3 -s -D && sleep 86400"],
                "securityContext": {"privileged": True}}]}}
        sh(f"kubectl -n {NS} delete pod char-{w} --ignore-not-found "
           f">/dev/null 2>&1")
        sh(f"kubectl run char-{w} -n {NS} --restart=Never --image=alpine "
           f"--overrides='{json.dumps(spec)}' >/dev/null 2>&1")
    for w in WORKERS:
        for _ in range(30):
            r = sh(f"kubectl -n {NS} get pod char-{w} "
                   f"-o jsonpath='{{.status.phase}}'")
            if r.stdout.strip() == "Running":
                if kexec(w, "iperf3 -v >/dev/null && echo ok").stdout.strip() == "ok":
                    break
            time.sleep(4)
        else:
            raise SystemExit(f"char-{w} did not come up")
        print(f"[e0] char-{w} up", flush=True)


def pods_down():
    for w in WORKERS:
        sh(f"kubectl -n {NS} delete pod char-{w} --ignore-not-found "
           f">/dev/null 2>&1")


def phase_inventory(out, ips):
    with open(f"{out}/inventory.csv", "w", newline="") as f:
        wcsv = csv.writer(f)
        wcsv.writerow(["node", "internal_ip", "cpu_cores", "mem_kb",
                       "kernel", "interfaces_with_addrs"])
        for w in WORKERS:
            cores = kexec(w, "nproc").stdout.strip()
            mem = kexec(w, "awk '/MemTotal/{print $2}' /proc/meminfo").stdout.strip()
            kern = kexec(w, "uname -r").stdout.strip()
            ifs = kexec(w, "ip -o addr show | awk '{print $2\"=\"$4}' "
                           "| tr '\\n' ';'").stdout.strip()
            wcsv.writerow([w, ips[w], cores, mem, kern, ifs])
    # interfaces.csv: which interface routes to every other worker.
    with open(f"{out}/interfaces.csv", "w", newline="") as f:
        wcsv = csv.writer(f)
        wcsv.writerow(["src_node", "dst_node", "dst_ip", "out_iface", "src_ip"])
        for s in WORKERS:
            for d in WORKERS:
                if s == d:
                    continue
                r = kexec(s, f"ip route get {ips[d]}").stdout
                dev, src = "", ""
                toks = r.split()
                if "dev" in toks:
                    dev = toks[toks.index("dev") + 1]
                if "src" in toks:
                    src = toks[toks.index("src") + 1]
                wcsv.writerow([s, d, ips[d], dev, src])
    print("[e0] inventory + interfaces written", flush=True)


def phase_qdisc(out, fname):
    with open(f"{out}/{fname}", "w") as f:
        shaped = 0
        for w in WORKERS:
            dump = kexec(w, "tc qdisc show").stdout
            f.write(f"===== {w}\n{dump}\n")
            shaped += sum(dump.count(k) for k in ("htb", "tbf", "netem"))
        f.write(f"===== TOTAL shaping qdiscs across workers: {shaped}\n")
    print(f"[e0] {fname}: shaping qdiscs = {shaped}", flush=True)
    return shaped


def clock_row(node, phase, raw):
    sync, offset_ms, source = "", "", ""
    for line in raw.splitlines():
        ls = line.strip()
        if ls.startswith("NTPSynchronized="):
            sync = ls.split("=", 1)[1]
        if ls.startswith("Offset:"):
            v = ls.split(":", 1)[1].strip()
            offset_ms = v
        if "System time" in ls and "of NTP time" in ls:  # chronyc tracking
            offset_ms = ls.split(":", 1)[1].strip()
        if ls.startswith("Server:") or ls.startswith("Reference ID"):
            source = ls.split(":", 1)[1].strip()
    return [node, phase, sync, offset_ms, source, raw.replace("\n", " | ")[:400]]


def phase_clock(out, phase, rows):
    # Workers: nsenter into the host from the hostPID pod.
    cmd = ("nsenter -t 1 -m -u -- sh -c "
           "\"timedatectl show; timedatectl timesync-status 2>/dev/null; "
           "chronyc tracking 2>/dev/null\" 2>/dev/null || true")
    for w in WORKERS:
        raw = kexec(w, cmd).stdout
        rows.append(clock_row(w, phase, raw))
    # Controller (anrg-2): local commands.
    raw = sh("timedatectl show; timedatectl timesync-status 2>/dev/null; "
             "chronyc tracking 2>/dev/null").stdout
    rows.append(clock_row("anrg-2", phase, raw))
    print(f"[e0] clock sync recorded ({phase})", flush=True)


def phase_bandwidth(out, ips, routes):
    with open(f"{out}/bandwidth-clean.csv", "w", newline="") as f:
        wcsv = csv.writer(f)
        wcsv.writerow(["src", "dst", "src_iface", "dst_iface", "rep",
                       "mbits_per_sec", "retransmits", "timestamp"])
        pair_medians = {}
        for s in WORKERS:
            for d in WORKERS:
                if s == d:
                    continue
                vals = []
                for rep in range(1, BW_REPS + 1):
                    r = kexec(s, f"iperf3 -c {ips[d]} -t {BW_SECS} -J",
                              timeout=BW_SECS + 40)
                    try:
                        j = json.loads(r.stdout)
                        mbps = j["end"]["sum_sent"]["bits_per_second"] / 1e6
                        retr = j["end"]["sum_sent"].get("retransmits", "")
                    except (json.JSONDecodeError, KeyError):
                        mbps, retr = "", ""
                        print(f"[e0] WARN iperf {s}->{d} rep{rep} unparsable",
                              flush=True)
                    if mbps != "":
                        vals.append(mbps)
                    wcsv.writerow([s, d, routes.get((s, d), ""),
                                   routes.get((d, s), ""), rep,
                                   round(mbps, 1) if mbps != "" else "",
                                   retr, round(time.time(), 1)])
                    f.flush()
                if vals:
                    pair_medians[(s, d)] = st.median(vals)
                print(f"[e0] bw {s}->{d}: median "
                      f"{pair_medians.get((s, d), float('nan')):.0f} Mbit/s",
                      flush=True)
    return pair_medians


def phase_latency(out, ips):
    with open(f"{out}/latency-clean.csv", "w", newline="") as f:
        wcsv = csv.writer(f)
        wcsv.writerow(["src", "dst", "rep", "rtt_median_ms", "rtt_p95_ms",
                       "rtt_max_ms", "loss_pct", "timestamp"])
        for s in WORKERS:
            for d in WORKERS:
                if s == d:
                    continue
                for rep in range(1, PING_REPS + 1):
                    r = kexec(s, f"ping -c {PING_COUNT} -i {PING_IVL} "
                                 f"{ips[d]}",
                              timeout=int(PING_COUNT * PING_IVL) + 30)
                    rtts, loss = [], ""
                    for line in r.stdout.splitlines():
                        if "time=" in line:
                            try:
                                rtts.append(float(
                                    line.split("time=")[1].split()[0]))
                            except (IndexError, ValueError):
                                pass
                        if "packet loss" in line:
                            loss = line.split("%")[0].split()[-1]
                    if rtts:
                        rtts.sort()
                        med = st.median(rtts)
                        p95 = rtts[min(len(rtts) - 1,
                                       int(0.95 * len(rtts)))]
                        mx = rtts[-1]
                    else:
                        med = p95 = mx = ""
                    wcsv.writerow([s, d, rep,
                                   round(med, 3) if med != "" else "",
                                   round(p95, 3) if p95 != "" else "",
                                   round(mx, 3) if mx != "" else "",
                                   loss, round(time.time(), 1)])
                    f.flush()
                print(f"[e0] ping {s}->{d} done", flush=True)


def phase_fanout(out, ips):
    with open(f"{out}/fanout-clean.csv", "w", newline="") as f:
        wcsv = csv.writer(f)
        wcsv.writerow(["case", "src", "dst", "rep", "mbits_per_sec",
                       "aggregate_mbits_per_sec", "timestamp"])
        for case, src, dsts in FANOUT_CASES:
            for rep in range(1, FAN_REPS + 1):
                cmds = " ".join(
                    f"iperf3 -c {ips[d]} -t {FAN_SECS} -J > /tmp/f{i}.json 2>&1 &"
                    for i, d in enumerate(dsts))
                kexec(src, f"{cmds} wait", timeout=FAN_SECS + 60)
                per, agg = {}, 0.0
                for i, d in enumerate(dsts):
                    raw = kexec(src, f"cat /tmp/f{i}.json").stdout
                    try:
                        mbps = json.loads(raw)["end"]["sum_sent"][
                            "bits_per_second"] / 1e6
                    except (json.JSONDecodeError, KeyError):
                        mbps = 0.0
                        print(f"[e0] WARN fanout {case} rep{rep} "
                              f"{src}->{d} unparsable", flush=True)
                    per[d] = mbps
                    agg += mbps
                ts = round(time.time(), 1)
                for d in dsts:
                    wcsv.writerow([case, src, d, rep, round(per[d], 1),
                                   round(agg, 1), ts])
                f.flush()
                print(f"[e0] fanout {case} rep {rep}: "
                      f"agg {agg:.0f} Mbit/s", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.expanduser("~/e0-clean-results"))
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    ips = node_ips()

    print("[e0] bringing up measurement pods", flush=True)
    pods_up(ips)
    try:
        phase_inventory(args.out, ips)
        routes = {}
        with open(f"{args.out}/interfaces.csv") as f:
            for row in csv.DictReader(f):
                routes[(row["src_node"], row["dst_node"])] = row["out_iface"]
        shaped = phase_qdisc(args.out, "qdisc-before.txt")
        if shaped:
            print(f"[e0] WARNING: {shaped} shaping qdisc(s) present; "
                  f"E0 requires a clean network", flush=True)
        clock_rows = []
        phase_clock(args.out, "before", clock_rows)
        pair_medians = phase_bandwidth(args.out, ips, routes)
        phase_latency(args.out, ips)
        phase_fanout(args.out, ips)
        phase_clock(args.out, "after", clock_rows)
        with open(f"{args.out}/clock-sync.csv", "w", newline="") as f:
            wcsv = csv.writer(f)
            wcsv.writerow(["node", "phase", "ntp_synchronized", "offset",
                           "source", "raw"])
            wcsv.writerows(clock_rows)
        phase_qdisc(args.out, "qdisc-after.txt")
        if pair_medians:
            B = st.median(sorted(pair_medians.values()))
            print(f"[e0] B (median of {len(pair_medians)} per-path medians) "
                  f"= {B:.0f} Mbit/s", flush=True)
            print(f"[e0] candidate treatments: B/2={B/2:.0f}  B/4={B/4:.0f} "
                  f" B/8={B/8:.0f}  B/16={B/16:.0f} Mbit/s", flush=True)
    finally:
        pods_down()
    print("E0 CAMPAIGN DONE", flush=True)


if __name__ == "__main__":
    main()
