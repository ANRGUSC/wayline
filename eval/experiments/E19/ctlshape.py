#!/usr/bin/env python3
"""Shape a lane's control traffic like its radio data links (the control-plane experiment, 2026-11-11).

The testbed carries Kubernetes, the controller's requests to the data agents and runners, and the overlay network
on an untagged control network that is never shaped; data moves on VLAN 20, which the experiments shape. This script
puts the lane's control traffic through the same kind of link as its data: every directed pair between a lane node
and the control node, and between two lane nodes, gets its own HTB class at --mbit with netem delay, jitter and loss.
ssh is exempt (the harness drives the nodes over it), everything else (and every VLAN-tagged data frame) keeps the
interface's default class, unshaped. On the control node only traffic toward the lane's nodes is shaped.

  printf '%s\\n' "$PW" | python3 ctlshape.py apply  --lane anrg-7,rpi4-1,rpi4-2,rpi51,rpi52
  printf '%s\\n' "$PW" | python3 ctlshape.py remove --lane anrg-7,rpi4-1,rpi4-2,rpi51,rpi52
  printf '%s\\n' "$PW" | python3 ctlshape.py show   --lane ...

remove deletes the root qdisc this script installed (handle 7:), which restores the interface's default.
"""
import argparse
import json
import os
import subprocess
import time
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "E18"))
import netem  # noqa: E402  (RootShell)

HANDLE = "7"
MASTER = "eclipse"


def control_ips():
    """node name -> control IP (the Kubernetes InternalIP) for every node."""
    out = subprocess.run(["k3s", "kubectl", "get", "nodes", "-o", "json"], capture_output=True, text=True, check=True)
    ips = {}
    for n in json.loads(out.stdout)["items"]:
        for a in n["status"]["addresses"]:
            if a["type"] == "InternalIP":
                ips[n["metadata"]["name"]] = a["address"]
    return ips


def dev_of(sh, ip):
    """The interface that carries ip. ip's stderr is dropped: while interfaces churn (the overlay's veths) it may
    print "Dump was interrupted and may be inconsistent", which the shell would otherwise merge into the answer."""
    for _ in range(5):
        out = sh.run(f"ip -o -4 addr show 2>/dev/null | awk '$4 ~ /^{ip}\\// {{print $2}}'").strip().split()
        if out and sh.run(f"test -e /sys/class/net/{out[0]} && echo yes", check=False).strip() == "yes":
            return out[0]
        time.sleep(1)
    raise SystemExit(f"no interface carries {ip}")


def apply_cmds(dev, dsts, mbit, delay, jitter, loss):
    cmds = [f"tc qdisc del dev {dev} root 2>/dev/null; true",
            f"tc qdisc add dev {dev} root handle {HANDLE}: htb default 1",
            f"tc class add dev {dev} parent {HANDLE}: classid {HANDLE}:1 htb rate 10gbit",
            f"tc qdisc add dev {dev} parent {HANDLE}:1 fq_codel",
            # ssh, both directions, stays in the default class
            f"tc filter add dev {dev} parent {HANDLE}: protocol ip prio 1 u32 match ip dport 22 0xffff flowid {HANDLE}:1",
            f"tc filter add dev {dev} parent {HANDLE}: protocol ip prio 1 u32 match ip sport 22 0xffff flowid {HANDLE}:1"]
    for k, ip in enumerate(dsts):
        cid = f"{HANDLE}:{10 + k:x}"
        cmds += [f"tc class add dev {dev} parent {HANDLE}: classid {cid} htb rate {mbit}mbit ceil {mbit}mbit",
                 f"tc qdisc add dev {dev} parent {cid} netem delay {delay}ms {jitter}ms loss {loss}% limit 10000",
                 f"tc filter add dev {dev} parent {HANDLE}: protocol ip prio 2 u32 match ip dst {ip}/32 flowid {cid}"]
    return cmds


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["apply", "remove", "show"])
    ap.add_argument("--lane", required=True)
    ap.add_argument("--mbit", type=float, default=2.0)
    ap.add_argument("--delay", type=float, default=10.0)
    ap.add_argument("--jitter", type=float, default=2.0)
    ap.add_argument("--loss", type=float, default=0.2)
    a = ap.parse_args()
    password = sys.stdin.readline().rstrip("\n")
    lane = a.lane.split(",")
    ips = control_ips()
    shells = {n: netem.RootShell(n, ips[n], password) for n in lane}
    shells[MASTER] = netem.RootShell(MASTER, ips.get(MASTER, "10.10.0.1"), password, local=True)
    try:
        for n, sh in shells.items():
            dev = dev_of(sh, ips.get(n, "10.10.0.1"))
            if a.action == "apply":
                dsts = [ips[m] for m in lane if m != n] + ([] if n == MASTER else [ips.get(MASTER, "10.10.0.1")])
                for c in apply_cmds(dev, dsts, a.mbit, a.delay, a.jitter, a.loss):
                    sh.run(c)
                print(f"{n}: {dev} shaped toward {len(dsts)} control peers")
            elif a.action == "remove":
                sh.run(f"if tc qdisc show dev {dev} root | grep -q 'htb {HANDLE}: root'; then tc qdisc del dev {dev} root; fi; true")
                print(f"{n}: {dev} restored")
            else:
                print(f"== {n} {dev}\n" + sh.run(f"tc -s class show dev {dev} | head -40"))
    finally:
        for sh in shells.values():
            sh.close()


if __name__ == "__main__":
    main()
