#!/usr/bin/env python3
"""Fault injection for Wayline runs on the testbed.

Faults
  crash <node>          Kill the node's workload: k3s-killall.sh stops the
                        kubelet and every container (the data agent too).
                        Files on disk survive, as after a power loss.
  restore <node>        Start k3s-agent again and wait for Ready and a
                        running data agent.
  partition <node>      Cut the node off the cluster but keep ssh: drop the
                        data agent (8082, hostPort: dropped before DNAT and in
                        FORWARD), kubelet (10250), VXLAN (8472/udp) and
                        API-server (6443) traffic. Processes keep running.
  heal <node>           Remove the partition rules.
  link <node> <mbit>    Shape all traffic between <node> and the other
                        workers to <mbit> (both directions); 0 cuts it.
                        Composes with E13's link classes.
  link-pair <u> <v> <mbit>
                        Shape only the link between u and v.
  unlink <node>         Remove that shaping on every worker; E13's link
                        classes, if applied, stay.

Trigger
  after <run> <task> <delay_s> <fault> <args...>
                        Wait until <task> of ODAG <run> has Succeeded, wait
                        <delay_s> more, then inject the fault. Prints the
                        timestamps of each step.

Run on anrg-2 with SUDO_PASS set in the environment (never written anywhere):
  SUDO_PASS=... python3 fault.py crash anrg-7
"""
import json
import os
import shlex
import subprocess
import sys
import time

NS = "wl-system"
WORKERS = [f"anrg-{i}" for i in (1, 3, 4, 5, 6, 7, 8, 9)]
MARK = "wl-fault"   # comment on every iptables rule this tool adds


def ssh(node, script, sudo=False, timeout=120):
    """Run `script` on `node` (password from SUDO_PASS, via stdin/env only)."""
    pw = os.environ["SUDO_PASS"]
    if sudo:
        script = f"read -r P; echo \"$P\" | sudo -S sh -c {shlex.quote(script)} 2>&1"
    r = subprocess.run(
        ["sshpass", "-e", "ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10",
         f"anrg@{node}.lan", script],
        input=(pw + "\n") if sudo else None, env={**os.environ, "SSHPASS": pw},
        capture_output=True, text=True, timeout=timeout)
    return (r.stdout + r.stderr).replace("[sudo] password for anrg: ", "")


def kubectl(*args):
    return subprocess.run(["kubectl", *args], capture_output=True, text=True).stdout


def node_ip(node):
    return kubectl("get", "node", node, "-o",
                   "jsonpath={.status.addresses[?(@.type=='InternalIP')].address}").strip()


def stamp(msg):
    print(f"[fault] {time.time():.3f} {msg}", flush=True)


def crash(node):
    ssh(node, "/usr/local/bin/k3s-killall.sh >/tmp/wl-fault-killall.log 2>&1; echo done", sudo=True)
    stamp(f"crashed {node}")


def restore(node, timeout=180, settle=8):
    ssh(node, "systemctl start k3s-agent", sudo=True)
    t0 = time.time()
    while time.time() - t0 < timeout:
        ready = kubectl("get", "node", node, "-o",
                        "jsonpath={.status.conditions[?(@.type=='Ready')].status}").strip()
        agents = kubectl("-n", NS, "get", "pods", "-l", "app=data-agent", "--field-selector",
                         f"spec.nodeName={node}", "-o", "jsonpath={.items[*].status.phase}")
        if ready == "True" and "Running" in agents:
            stamp(f"restored {node} (Ready, data agent Running)")
            # The controller re-checks a lost node's agent every second and
            # its Ready condition every 2 s; give it time to take the node
            # back before the next run is planned.
            time.sleep(settle)
            return
        time.sleep(2)
    raise SystemExit(f"{node} not back after {timeout}s")


def partition(node):
    master = node_ip("anrg-2")
    c = f"-m comment --comment {MARK}"
    rules = [
        # Keep ssh from the control node so heal can reach us.
        f"iptables -I INPUT -p tcp --dport 22 -s {master} -j ACCEPT {c}",
        # The data agent listens on a hostPort: inbound traffic is DNAT'ed
        # to its pod and never reaches INPUT, so drop it in raw PREROUTING
        # (before DNAT), and its outbound pushes in FORWARD.
        f"iptables -t raw -A PREROUTING -p tcp --dport 8082 -j DROP {c}",
        f"iptables -t raw -A PREROUTING -p tcp --dport 10250 -j DROP {c}",
        f"iptables -t raw -A PREROUTING -p udp --dport 8472 -j DROP {c}",
        f"iptables -I FORWARD -p tcp --dport 8082 -j DROP {c}",
        f"iptables -A OUTPUT -p tcp --dport 8082 -j DROP {c}",
        f"iptables -A OUTPUT -p udp --dport 8472 -j DROP {c}",
        f"iptables -A OUTPUT -p tcp --dport 6443 -j DROP {c}",
    ]
    ssh(node, "; ".join(rules), sudo=True)
    stamp(f"partitioned {node}")


def heal(node):
    script = ("iptables-save | grep -v -- '--comment " + MARK + "' | iptables-restore; "
              "echo LEFT=$(iptables-save | grep -c " + MARK + ")")
    out = ssh(node, script, sudo=True)
    if "LEFT=0" not in out:
        raise SystemExit(f"heal {node}: {out}")
    stamp(f"healed {node}")


def _shape(u, peers_ip, mbit):
    """On u's egress, send traffic to each peer IP through class 1:77 at
    `mbit` (0 = drop), with priority-1 filters. An existing HTB tree (E13's
    link classes, priority-2 filters) is kept; without one, a pass-through
    root is created."""
    peer = next(iter(peers_ip.values()))
    lines = [f"IF=$(ip route get {peer} | grep -o 'dev [^ ]*' | awk '{{print $2}}')",
             "if ! tc qdisc show dev $IF | grep -q 'htb 1: root'; then "
             "tc qdisc del dev $IF root 2>/dev/null; "
             "tc qdisc add dev $IF root handle 1: htb default 99; "
             "tc class add dev $IF parent 1: classid 1:99 htb rate 10gbit ceil 10gbit; fi",
             "tc filter del dev $IF parent 1: prio 1 2>/dev/null",
             "tc class del dev $IF classid 1:77 2>/dev/null"]
    if mbit > 0:
        lines.append(f"tc class add dev $IF parent 1: classid 1:77 htb rate {mbit}mbit ceil {mbit}mbit")
        action = "flowid 1:77"
    else:
        action = "action drop"
    lines += [f"tc filter add dev $IF parent 1: protocol ip prio 1 u32 match ip dst {ip}/32 {action}"
              for ip in peers_ip.values()]
    lines.append("echo FAULTFILTERS=$(tc filter show dev $IF parent 1: prio 1 | grep -c 'match')")
    return ssh(u, "; ".join(lines), sudo=True)


def link(node, mbit):
    ips = {n: node_ip(n) for n in WORKERS}
    others = {n: ip for n, ip in ips.items() if n != node}
    _shape(node, others, mbit)
    for n in others:
        _shape(n, {node: ips[node]}, mbit)
    stamp(f"link {node} <-> workers at {mbit} Mbit/s" if mbit else f"link {node} cut")


def link_pair(u, v, mbit):
    """Shape only the link between u and v (both directions)."""
    _shape(u, {v: node_ip(v)}, mbit)
    _shape(v, {u: node_ip(u)}, mbit)
    stamp(f"link {u} <-> {v} at {mbit} Mbit/s" if mbit else f"link {u} <-> {v} cut")


def unlink(node):
    """Remove the fault's filters and class on every worker; E13's link
    classes, if applied, stay."""
    script = ("for i in $(ls /sys/class/net); do tc filter del dev $i parent 1: prio 1 2>/dev/null; "
              "tc class del dev $i classid 1:77 2>/dev/null; done; echo done")
    for n in WORKERS:
        ssh(n, script, sudo=True)
    stamp(f"unlinked {node}")


def task_phase(run, task):
    raw = kubectl("-n", NS, "get", "odag", run, "-o", "json")
    try:
        st = json.loads(raw).get("status", {})
    except json.JSONDecodeError:
        return ""
    for t in st.get("tasks", []):
        if t.get("name") == task:
            return t.get("phase", "")
    return ""


FAULTS = {"crash": crash, "restore": restore, "partition": partition, "heal": heal,
          "link": lambda n, m: link(n, float(m)), "unlink": unlink,
          "link-pair": lambda u, v, m: link_pair(u, v, float(m))}


def after(run, task, delay, fault, *args):
    stamp(f"waiting for {run}/{task} to succeed")
    while task_phase(run, task) != "Succeeded":
        time.sleep(0.2)
    stamp(f"{run}/{task} succeeded; injecting {fault} in {delay}s")
    time.sleep(float(delay))
    FAULTS[fault](*args)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    cmd, rest = sys.argv[1], sys.argv[2:]
    if cmd == "after":
        after(*rest)
    elif cmd in FAULTS:
        FAULTS[cmd](*rest)
    else:
        sys.exit(__doc__)
