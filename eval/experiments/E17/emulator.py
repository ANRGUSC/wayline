#!/usr/bin/env python3
"""E17 link emulator: a lane of testbed nodes as the simulated radio mesh.

A lane is five testbed nodes standing in for the simulator's n0..n4. Every
ordered pair (i, j) is a directed radio link, emulated on node i's data
interface (the data VLAN) as one HTB class, rate R, matched by j's MAC, with
an fq_codel leaf so concurrent transfers share the link fairly, as the
simulator's per-link fair share does. Traffic to anything outside the lane
(the control plane on eclipse, image pulls) takes an unshaped default class.

Link states, per directed link, mirror the simulator's:
  up        rate R
  degrade   rate R * factor; in-flight transfers slow down
  down      every frame to j's MAC is dropped (the radio link is gone), and
            TCP to j's data IP is answered with a reset (nftables, ahead of
            the cluster's own chains), so in-flight
            transfers abort and are retried from scratch, as the simulator
            aborts and retransmits them. (Without the reset TCP would sit in
            exponential retransmission backoff, a behaviour the simulator
            does not model.)
  recovery  rate R again; drop and reset removed

Rerouting (the simulator's WidestPathRouting): per-node host routes send a
destination whose direct link is down through a relay on the data VLAN
(`ip route replace <dst>/32 via <relay>`); relays forward. In-flight
transfers on a link that goes down are still reset first, so they restart
on the detour as in the simulator.

Commands run in one persistent root shell per node (ssh + sudo -S), so a link
event costs one round trip, not an ssh handshake. The sudo password comes
from the SUDO_PASS environment variable and is written only to the remote
sudo's stdin.
"""
import os
import queue
import select
import subprocess
import threading
import time

SSH_OPTS = ["-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null", "-o", "LogLevel=ERROR",
            "-o", "ServerAliveInterval=15", "-o", "ConnectTimeout=10"]
DEFAULT_CLASS = 999
QUANTUM = 3028


class RemoteError(RuntimeError):
    pass


class RootShell:
    """A long-lived root shell on one node; run() sends one command line."""

    TIMEOUT = 30.0   # a command that has not answered by then: the node or its link is in trouble

    def __init__(self, name, ctl_ip, password):
        self.name = name
        self.lock = threading.Lock()
        self.seq = 0
        self._buf = b""
        self.p = subprocess.Popen(
            ["ssh", *SSH_OPTS, f"anrg@{ctl_ip}", "sudo -k -S -p '' sh"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
        self.p.stdin.write((password + "\n").encode())
        self.p.stdin.flush()
        out = self.run("id -u; export PATH=/usr/sbin:/sbin:/usr/bin:/bin:$PATH; "
                       "IPT=$(command -v iptables || echo /var/lib/rancher/k3s/data/current/bin/aux/iptables); "
                       "echo $IPT")
        if "0" not in out.splitlines():
            raise RemoteError(f"{name}: no root shell: {out!r}")

    def _readline(self, deadline):
        fd = self.p.stdout.fileno()
        while b"\n" not in self._buf:
            left = deadline - time.time()
            if left <= 0 or not select.select([fd], [], [], left)[0]:
                raise RemoteError(f"{self.name}: no answer within {self.TIMEOUT:g} s")
            chunk = os.read(fd, 65536)
            if not chunk:
                return ""
            self._buf += chunk
        line, _, self._buf = self._buf.partition(b"\n")
        return line.decode(errors="replace") + "\n"

    def run(self, cmd, check=True):
        with self.lock:
            self.seq += 1
            mark = f"__wl_{self.seq}__"
            try:
                self.p.stdin.write(f"{{ {cmd} ; }} 2>&1; echo {mark} $?\n".encode())
                self.p.stdin.flush()
            except OSError as e:
                raise RemoteError(f"{self.name}: shell gone: {e}")
            lines = []
            deadline = time.time() + self.TIMEOUT
            while True:
                line = self._readline(deadline)
                if not line:
                    raise RemoteError(f"{self.name}: shell closed; output so far: {''.join(lines)!r}")
                if line.startswith(mark):
                    rc = int(line.split()[1])
                    break
                lines.append(line)
        out = "".join(lines).strip()
        if check and rc != 0:
            raise RemoteError(f"{self.name}: `{cmd}` exited {rc}: {out}")
        return out

    def close(self):
        try:
            self.p.stdin.write(b"exit\n")
            self.p.stdin.flush()
            self.p.wait(timeout=5)
        except Exception:  # noqa: BLE001 - best effort
            self.p.kill()


class Lane:
    """Five nodes as n0..n4 with emulated links between every pair.

    nodes: list of dicts {name, ctl_ip, data_ip, mac, dif}, in n-index order.
    rate_mbit: R, the undisrupted per-link rate.
    """

    def __init__(self, nodes, rate_mbit, password=None, log=None, wired=(), netem=None, bw=None):
        """wired: unordered index pairs left unshaped (real switch links,
        never disrupted); netem: a netem spec for the radio links' leaf
        qdisc (delay, jitter, loss), else fq_codel; bw: {(i, j): pristine
        bandwidth} for routing (default 1 for every link)."""
        self.nodes = nodes
        self.n = len(nodes)
        self.rate = float(rate_mbit)
        self.log = log or (lambda m: None)
        self.wired = {frozenset(p) for p in wired}
        self.netem = netem
        self.bw = bw or {}
        pw = password if password is not None else os.environ["SUDO_PASS"]
        self.sh = [None] * self.n
        errs = []

        def open_shell(i):
            try:
                self.sh[i] = RootShell(nodes[i]["name"], nodes[i]["ctl_ip"], pw)
            except Exception as e:  # noqa: BLE001 - reported below
                errs.append(e)
        ts = [threading.Thread(target=open_shell, args=(i,)) for i in range(self.n)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        if errs:
            self.close()
            raise errs[0]
        # Current emulated state per directed link and per-node routes.
        self.state = {}      # (i, j) -> ("up"|"degrade"|"down", factor)
        self.routes = {}     # (i, j) -> relay index or None (direct)
        self.resets = set()  # (i, j) whose TCP to j is being reset on i
        self._jobs = [queue.Queue() for _ in range(self.n)]
        self._workers = [threading.Thread(target=self._worker, args=(i,), daemon=True)
                         for i in range(self.n)]
        [w.start() for w in self._workers]

    # ------------------------------------------------------------- plumbing
    def _worker(self, i):
        while True:
            job = self._jobs[i].get()
            if job is None:
                return
            cmds, done, errs = job
            try:
                for c in cmds:
                    self.sh[i].run(c)
            except Exception as e:  # noqa: BLE001 - surfaced to the caller
                errs.append(e)
            done.set()

    def on_nodes(self, per_node):
        """Run {i: [cmd, ...]} on every node concurrently; raise the first error."""
        waits, errs = [], []
        for i, cmds in per_node.items():
            if not cmds:
                continue
            done = threading.Event()
            self._jobs[i].put((cmds, done, errs))
            waits.append(done)
        for w in waits:
            w.wait()
        if errs:
            raise errs[0]

    def classid(self, j):
        return 10 + j

    def prio(self, j):
        return 1 + j        # drop filters; class filters use prio 100

    def _rate(self, factor):
        return f"{max(self.rate * factor, 0.01):.4f}mbit"

    # ---------------------------------------------------------------- setup
    def setup(self):
        per = {}
        for i, me in enumerate(self.nodes):
            d = me["dif"]
            cmds = [
                f"tc qdisc del dev {d} root 2>/dev/null; true",
                f"ip link set dev {d} txqueuelen 1000",
                f"tc qdisc add dev {d} root handle 1: htb default {DEFAULT_CLASS}",
                f"tc class add dev {d} parent 1: classid 1:{DEFAULT_CLASS} htb rate 1gbit quantum {QUANTUM}",
                f"tc qdisc add dev {d} parent 1:{DEFAULT_CLASS} handle {DEFAULT_CLASS}: fq_codel",
            ]
            for j, peer in enumerate(self.nodes):
                if j == i or frozenset((i, j)) in self.wired:
                    continue    # a wired pair rides the unshaped default class
                c = self.classid(j)
                cmds.append(f"tc class add dev {d} parent 1: classid 1:{c} htb rate {self._rate(1)} "
                            f"ceil {self._rate(1)} quantum {QUANTUM}")
                if self.netem:
                    # Radio impairments, then fq_codel for the queue the rate
                    # limit builds: a deep FIFO here (netem's own queue) cut
                    # the aggregate TCP goodput of 13 flows to a quarter of
                    # the link (E17 Tier 2, verified in namespaces).
                    cmds += [f"tc qdisc add dev {d} parent 1:{c} handle {c}: netem {self.netem} limit 1000",
                             f"tc qdisc add dev {d} parent {c}:1 handle {100 + c}: fq_codel"]
                else:
                    cmds.append(f"tc qdisc add dev {d} parent 1:{c} handle {c}: fq_codel")
                cmds.append(f"tc filter add dev {d} parent 1: protocol all prio 100 flower "
                            f"dst_mac {peer['mac']} classid 1:{c}")
            cmds += [
                # Relays forward on the data VLAN; no redirects, which would
                # tell the sender to bypass the relay over the downed link.
                "sysctl -qw net.ipv4.ip_forward=1",
                "sysctl -qw net.ipv4.conf.all.send_redirects=0",
                f"sysctl -qw net.ipv4.conf.{d.replace('.', '/')}.send_redirects=0",
                "sysctl -qw net.ipv4.conf.all.accept_redirects=0",
                f"sysctl -qw net.ipv4.conf.{d.replace('.', '/')}.accept_redirects=0",
                # Resets for downed peers live in a private nftables table
                # whose output and forward chains run at priority -10, before
                # every iptables-managed chain: kube-router keeps its own
                # FORWARD jump first and accepts pod traffic there, so a rule
                # in the iptables FORWARD chain never sees the data agent's
                # traffic (a hostPort pod: its pushes are forwarded). A reject
                # is final whichever table issues it.
                "nft delete table inet wlemu 2>/dev/null; true",
                "nft add table inet wlemu",
                "nft add set inet wlemu peers_down '{ type ipv4_addr; }'",
                "nft add chain inet wlemu wlfwd '{ type filter hook forward priority -10; policy accept; }'",
                "nft add chain inet wlemu wlout '{ type filter hook output priority -10; policy accept; }'",
                "nft add rule inet wlemu wlfwd ip daddr @peers_down meta l4proto tcp reject with tcp reset",
                "nft add rule inet wlemu wlout ip daddr @peers_down meta l4proto tcp reject with tcp reset",
                # Relay traffic on the data VLAN is accepted ahead of any
                # FORWARD DROP (Docker's, on the UP boards).
                f"$IPT -D FORWARD -i {d} -o {d} -j ACCEPT 2>/dev/null; $IPT -I FORWARD 1 -i {d} -o {d} -j ACCEPT",
            ]
            for j, peer in enumerate(self.nodes):
                if j != i:
                    cmds.append(f"ip route del {peer['data_ip']}/32 2>/dev/null; true")
            per[i] = cmds
        self.on_nodes(per)
        self.state = {(i, j): ("up", 1.0) for i in range(self.n) for j in range(self.n) if i != j}
        self.routes = {(i, j): None for i in range(self.n) for j in range(self.n) if i != j}
        self.resets = set()
        self.log(f"lane setup: {self.n} nodes, {self.rate:g} Mbit/s per link")

    def reset(self):
        """Every link up at full rate, direct routes, no resets (between runs)."""
        per = {i: [] for i in range(self.n)}
        for (i, j), (kind, f) in self.state.items():
            d = self.nodes[i]["dif"]
            if frozenset((i, j)) in self.wired:
                continue
            if kind == "down":
                per[i].append(f"tc filter del dev {d} parent 1: prio {self.prio(j)} 2>/dev/null; true")
            if kind != "up" or f != 1.0:
                per[i].append(f"tc class change dev {d} parent 1: classid 1:{self.classid(j)} htb "
                              f"rate {self._rate(1)} ceil {self._rate(1)} quantum {QUANTUM}")
        for (i, j), relay in self.routes.items():
            if relay is not None:
                per[i].append(f"ip route del {self.nodes[j]['data_ip']}/32 2>/dev/null; true")
        for i in range(self.n):
            per[i].append("nft flush set inet wlemu peers_down")
        self.on_nodes(per)
        self.state = {k: ("up", 1.0) for k in self.state}
        self.routes = {k: None for k in self.routes}
        self.resets = set()

    def teardown(self):
        per = {}
        for i, me in enumerate(self.nodes):
            d = me["dif"]
            cmds = [f"tc qdisc del dev {d} root 2>/dev/null; true",
                    "nft delete table inet wlemu 2>/dev/null; true",
                    f"$IPT -D FORWARD -i {d} -o {d} -j ACCEPT 2>/dev/null; true"]
            for j, peer in enumerate(self.nodes):
                if j != i:
                    cmds.append(f"ip route del {peer['data_ip']}/32 2>/dev/null; true")
            per[i] = cmds
        self.on_nodes(per)

    def close(self):
        for q in getattr(self, "_jobs", []):
            q.put(None)
        for s in self.sh:
            if s is not None:
                s.close()

    # ---------------------------------------------------------- link events
    def _reset_add(self, j):
        return f"nft add element inet wlemu peers_down '{{ {self.nodes[j]['data_ip']} }}'"

    def _reset_del(self, j):
        return f"nft delete element inet wlemu peers_down '{{ {self.nodes[j]['data_ip']} }}' 2>/dev/null; true"

    def apply(self, changes, reroute=False, reset_window=0.15):
        """Apply simultaneous directed-link changes [(i, j, kind, factor)].

        kind: down | degrade | recovery. With reroute, host routes are then
        recomputed (routes_fn) so destinations behind a downed link go via a
        relay; the resets for newly downed links are held for reset_window
        seconds first, so in-flight transfers abort before the detour opens.
        """
        per = {i: [] for i in range(self.n)}
        newly_down = []
        for i, j, kind, factor in changes:
            if frozenset((i, j)) in self.wired:
                raise ValueError(f"disruption on wired pair n{i}-n{j}")
            d = self.nodes[i]["dif"]
            old_kind, _ = self.state[(i, j)]
            cls = self.classid(j)
            if kind == "down":
                if old_kind != "down":
                    per[i].append(f"tc filter add dev {d} parent 1: protocol all prio {self.prio(j)} "
                                  f"flower dst_mac {self.nodes[j]['mac']} action drop")
                    per[i].append(self._reset_add(j))
                    self.resets.add((i, j))
                    newly_down.append((i, j))
                self.state[(i, j)] = ("down", 0.0)
            else:
                f = 1.0 if kind == "recovery" else float(factor)
                if old_kind == "down":
                    per[i].append(f"tc filter del dev {d} parent 1: prio {self.prio(j)}")
                    if (i, j) in self.resets:
                        per[i].append(self._reset_del(j))
                        self.resets.discard((i, j))
                per[i].append(f"tc class change dev {d} parent 1: classid 1:{cls} htb "
                              f"rate {self._rate(f)} ceil {self._rate(f)} quantum {QUANTUM}")
                self.state[(i, j)] = ("up" if f == 1.0 else "degrade", f)
        self.on_nodes(per)
        if not reroute:
            return
        if newly_down:
            time.sleep(reset_window)
        per = {i: [] for i in range(self.n)}
        want = self.widest_routes()
        for (i, j), relay in want.items():
            if self.routes[(i, j)] == relay:
                continue
            dst = self.nodes[j]["data_ip"]
            d = self.nodes[i]["dif"]
            if relay is None:
                per[i].append(f"ip route del {dst}/32 2>/dev/null; true")
            else:
                per[i].append(f"ip route replace {dst}/32 via {self.nodes[relay]['data_ip']} dev {d}")
            self.routes[(i, j)] = relay
        # A downed link whose destination now has a detour drops its reset
        # (the direct frames stay dropped by tc); one with no route keeps or
        # regains it, so connection attempts fail fast instead of hanging in
        # SYN backoff until the link recovers.
        for (i, j), (kind, _) in self.state.items():
            if kind != "down":
                continue
            if want[(i, j)] is not None and (i, j) in self.resets:
                per[i].append(self._reset_del(j))
                self.resets.discard((i, j))
            elif want[(i, j)] is None and (i, j) not in self.resets:
                per[i].append(self._reset_add(j))
                self.resets.add((i, j))
        self.on_nodes(per)

    def widest_routes(self):
        """Per-node next hops, as each node's own widest-path search would
        choose them on the links that are up (WidestPathRouting semantics:
        pristine bandwidths, so equal-width ties keep the first-found path;
        with uniform links, the direct link whenever it is up)."""
        import heapq
        up = {(i, j) for (i, j), (k, _) in self.state.items() if k != "down"}
        names = [f"n{i}" for i in range(self.n)]
        nexthop = {}
        for s in range(self.n):
            bw = {i: 0.0 for i in range(self.n)}
            bw[s] = float("inf")
            pred = {i: None for i in range(self.n)}
            heap = [(-float("inf"), names[s], s)]
            seen = set()
            while heap:
                nb, _, cur = heapq.heappop(heap)
                if cur in seen:
                    continue
                seen.add(cur)
                cb = -nb
                if cb < bw[cur]:
                    continue
                for j in range(self.n):       # links from cur, in link-id order
                    if (cur, j) not in up or j in seen:
                        continue
                    nbw = min(cb, self.bw.get((cur, j), 1.0))
                    if nbw > bw[j]:
                        bw[j] = nbw
                        pred[j] = cur
                        heapq.heappush(heap, (-nbw, names[j], j))
            for t in range(self.n):
                if t == s:
                    continue
                if pred[t] is None:
                    nexthop[(s, t)] = None    # unreachable: leave direct (dropped)
                    continue
                hop = t
                while pred[hop] != s:
                    hop = pred[hop]
                nexthop[(s, t)] = None if hop == t else hop
        return nexthop

    # ---------------------------------------------------------------- probes
    def show(self, i):
        d = self.nodes[i]["dif"]
        return self.sh[i].run(f"tc -s class show dev {d}; tc filter show dev {d}; "
                              f"nft list table inet wlemu; ip route show | grep /32 || true", check=False)
