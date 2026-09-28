#!/usr/bin/env python3
"""E13 network shaping: per-node link classes, pairwise rate = the slower end.

Each worker node shapes its egress per destination worker IP with HTB, at
the pair's rate. Matching on destination IP alone covers every data path
between workers: Wayline agents (hostPort 8082), pod-to-pod VXLAN, and Ray's
object transfers (host network). Traffic to the control node (API server,
registry) is not shaped. Every direction of a pair carries the same rate.

Run ON anrg-2 with SUDO_PASS set (never written anywhere):
  python3 net.py apply | verify | measure | clear
"""
import os
import shlex
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

B_MBIT = 942.0                       # measured link rate (E0)
# Link class per worker, chosen to cut across the CPU classes of E11
# (fast 1,6,7; medium 3,5,8; slow 4,9) so fast CPUs do not always have fast links.
LINK = {"anrg-1": 1, "anrg-6": 4, "anrg-7": 8,
        "anrg-3": 1, "anrg-5": 2, "anrg-8": 8,
        "anrg-4": 2, "anrg-9": 4}
NODES = list(LINK)


def pair_mbit(u, v):
    """Rate between two workers: the slower end's class."""
    return B_MBIT / max(LINK[u], LINK[v])


def bw_matrix():
    """{(u, v): bytes/s} for every ordered pair of distinct workers."""
    return {(u, v): pair_mbit(u, v) * 1e6 / 8 for u in NODES for v in NODES if u != v}


def root(node, script, timeout=60):
    """Run `script` as root on `node` (ssh from anrg-2; password via stdin)."""
    remote = f"read -r P; echo \"$P\" | sudo -S sh -c {shlex.quote(script)} 2>&1"
    r = subprocess.run(["sshpass", "-e", "ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10",
                        f"anrg@{node}.lan", remote], input=os.environ["SUDO_PASS"] + "\n",
                       env={**os.environ, "SSHPASS": os.environ["SUDO_PASS"]},
                       capture_output=True, text=True, timeout=timeout)
    return r.stdout.replace("[sudo] password for anrg: ", "")


def user(node, script, timeout=120):
    r = subprocess.run(["sshpass", "-e", "ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10",
                        f"anrg@{node}.lan", script],
                       env={**os.environ, "SSHPASS": os.environ["SUDO_PASS"]},
                       capture_output=True, text=True, timeout=timeout)
    return r.stdout + r.stderr


IP = {}


def ips():
    """IPv4 InternalIP of each worker, as Kubernetes and Ray use them. (The
    .lan names resolve to IPv6 ULA addresses, which the data paths do not use.)"""
    if not IP:
        for n in NODES:
            IP[n] = subprocess.run(
                ["kubectl", "get", "node", n, "-o",
                 "jsonpath={.status.addresses[?(@.type=='InternalIP')].address}"],
                capture_output=True, text=True).stdout.strip()
            if not IP[n].startswith("192.168."):
                raise RuntimeError(f"{n}: unexpected InternalIP {IP[n]!r}")
    return IP


def rates():
    """Distinct pair rates in Mbit -> HTB class id."""
    return {r: 10 + i for i, r in enumerate(sorted({pair_mbit(u, v) for u in NODES for v in NODES if u != v}, reverse=True))}


def apply_node(u):
    ip = ips()
    cls = rates()
    peer = next(v for v in NODES if v != u)
    lines = [f"IF=$(ip route get {ip[peer]} | grep -o 'dev [^ ]*' | awk '{{print $2}}')",
             "tc qdisc del dev $IF root 2>/dev/null",
             "tc qdisc add dev $IF root handle 1: htb default 99",
             "tc class add dev $IF parent 1: classid 1:99 htb rate 10gbit ceil 10gbit"]
    lines += [f"tc class add dev $IF parent 1: classid 1:{c} htb rate {r:.0f}mbit ceil {r:.0f}mbit"
              for r, c in cls.items()]
    lines += [f"tc filter add dev $IF parent 1: protocol ip prio 1 u32 match ip dst {ip[v]}/32 "
              f"flowid 1:{cls[pair_mbit(u, v)]}" for v in NODES if v != u]
    lines.append("echo FILTERS=$(tc filter show dev $IF | grep -c 'flowid 1:')")
    return root(u, "; ".join(lines))


def apply():
    with ThreadPoolExecutor(len(NODES)) as ex:
        outs = dict(zip(NODES, ex.map(apply_node, NODES)))
    bad = {n: o for n, o in outs.items() if f"FILTERS={len(NODES) - 1}" not in o}
    if bad:
        raise RuntimeError(f"shaping not applied: {bad}")
    return {n: f"B/{LINK[n]}" for n in NODES}


def clear():
    def one(n):
        return root(n, "for i in $(ls /sys/class/net); do tc qdisc del dev $i root 2>/dev/null; done; "
                       "echo LEFT=$(tc qdisc show | grep -c htb)")
    with ThreadPoolExecutor(len(NODES)) as ex:
        outs = dict(zip(NODES, ex.map(one, NODES)))
    bad = {n: o for n, o in outs.items() if "LEFT=0" not in o}
    if bad:
        raise RuntimeError(f"shaping not cleared: {bad}")


HERE = os.path.dirname(os.path.abspath(__file__))


def measure(u, v, mbytes=24):
    """Throughput u -> v in Mbit/s, sending `mbytes` MB over one TCP stream."""
    env = {**os.environ, "SSHPASS": os.environ["SUDO_PASS"]}
    for n in (u, v):
        subprocess.run(["sshpass", "-e", "scp", "-q", "-o", "StrictHostKeyChecking=no",
                        os.path.join(HERE, "bwtest.py"), f"anrg@{n}.lan:/tmp/bwtest.py"], env=env)
    server = subprocess.Popen(["sshpass", "-e", "ssh", "-o", "StrictHostKeyChecking=no", f"anrg@{v}.lan",
                               "python3 /tmp/bwtest.py server"], env=env,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(1.5)
    out = user(u, f"python3 /tmp/bwtest.py client {ips()[v]} {mbytes}").strip().splitlines()
    server.wait(timeout=90)
    try:
        return float(out[-1])
    except (IndexError, ValueError):
        raise RuntimeError(f"measure {u}->{v} failed: {out}")


def verify(pairs=(("anrg-1", "anrg-3"), ("anrg-1", "anrg-5"), ("anrg-1", "anrg-6"), ("anrg-1", "anrg-7"))):
    res = []
    for u, v in pairs:
        want = pair_mbit(u, v)
        got = measure(u, v, mbytes=max(8, int(want / 942 * 48)))
        res.append((u, v, round(want), got))
    return res


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "apply":
        print(apply())
    elif cmd == "clear":
        clear(); print("cleared")
    elif cmd == "verify":
        for u, v, want, got in verify():
            print(f"{u} -> {v}: planned {want} Mbit/s, measured {got} Mbit/s")
    else:
        sys.exit("usage: net.py apply|verify|clear")
