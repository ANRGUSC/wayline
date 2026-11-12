"""E18 link emulator: a lane's data-VLAN links, shaped per directed pair.

Adapted from E17's emulator.py. Every ordered pair (i, j) of lane nodes, and
every node's link to and from eclipse (the Argo repository, the Ray head, the
controller), is one HTB class on node i's data VLAN interface, matched by j's
MAC, with an fq_codel leaf (concurrent transfers share a link fairly). The
rate is the lane's pair rate (lanes.py: E13 link classes, NIC caps).
Anything else on the VLAN takes an unshaped default class.

Only the data VLAN interface is ever touched (enp1s0.20, eth0.20,
enp4s0f0.20). The root qdisc uses handle 18: so an E17 lane's handle 1:
tree is recognized and never replaced: setup refuses a VLAN interface whose
root is an HTB that is not E18's.

Link states mirror E17's: up (pristine rate), degrade (another rate), down
(frames to j's MAC dropped, TCP to j's data IP answered with a reset from
nftables table inet wle18 at priority -10, ahead of the cluster's chains, so
in-flight transfers fail fast instead of backing off). A partition is every
link of one node down, both directions, eclipse included: the node keeps its
control network (kubelet, ssh), loses the data plane.

Commands run in one persistent root shell per node (ssh to the control
address, sudo -S with the password written once to its stdin; eclipse runs
a local `sudo -n sh`). The password is never put in an environment variable,
a file or a command line.
"""
import os
import queue
import re
import select
import subprocess
import threading
import time

SSH_OPTS = ["-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null", "-o", "LogLevel=ERROR",
            "-o", "ServerAliveInterval=15", "-o", "ConnectTimeout=10"]
HANDLE = "18"           # root qdisc handle (E17 uses 1:)
DEFAULT_MINOR = 0xffe   # unshaped default class
QUANTUM = 3028
NFT_TABLE = "wle18"


class RemoteError(RuntimeError):
    pass


class RootShell:
    """A long-lived root shell on one node; run() sends one command line.
    E17's RootShell, plus a local variant for eclipse and per-call timeouts."""

    TIMEOUT = 30.0

    def __init__(self, name, ctl_ip, password, user="anrg", local=False):
        self.name = name
        self.lock = threading.Lock()
        self.seq = 0
        self._buf = b""
        # Kept so a shell lost with its node (crashed, rebooted, out of
        # memory) can be reopened instead of failing every later run.
        self._args = (ctl_ip, password, user, local)
        self._open()

    def _open(self):
        ctl_ip, password, user, local = self._args
        name = self.name
        self._buf = b""
        if local:
            nopw = subprocess.run(["sudo", "-n", "true"], capture_output=True).returncode == 0
            cmd = ["sudo", "-n", "sh"] if nopw else ["sudo", "-k", "-S", "-p", "", "sh"]
            send_pw = not nopw
        else:
            cmd = ["ssh", *SSH_OPTS, f"{user}@{ctl_ip}", "sudo -k -S -p '' sh"]
            send_pw = True
        self.p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, bufsize=0)
        if send_pw:
            if password is None:
                raise RemoteError(f"{name}: a root shell needs the sudo password (stdin's first line)")
            self.p.stdin.write((password + "\n").encode())
            self.p.stdin.flush()
        out = self._run("id -u; export PATH=/usr/sbin:/sbin:/usr/bin:/bin:/usr/local/bin:$PATH", True, None)
        if "0" not in out.splitlines():
            raise RemoteError(f"{name}: no root shell: {out!r}")

    def run(self, cmd, check=True, timeout=None):
        """One command; a shell that is gone is reopened once and the command retried."""
        try:
            return self._run(cmd, check, timeout)
        except RemoteError as e:
            if "shell gone" not in str(e) and "shell closed" not in str(e):
                raise
            try:
                self.p.kill()
            except Exception:  # noqa: BLE001
                pass
            self._open()
            return self._run(cmd, check, timeout)

    def _readline(self, deadline, timeout):
        fd = self.p.stdout.fileno()
        while b"\n" not in self._buf:
            left = deadline - time.time()
            if left <= 0 or not select.select([fd], [], [], left)[0]:
                raise RemoteError(f"{self.name}: no answer within {timeout:g} s")
            chunk = os.read(fd, 65536)
            if not chunk:
                return ""
            self._buf += chunk
        line, _, self._buf = self._buf.partition(b"\n")
        return line.decode(errors="replace") + "\n"

    def _run(self, cmd, check=True, timeout=None):
        timeout = timeout or self.TIMEOUT
        with self.lock:
            self.seq += 1
            mark = f"__wl_{self.seq}__"
            try:
                self.p.stdin.write(f"{{ {cmd} ; }} 2>&1; echo {mark} $?\n".encode())
                self.p.stdin.flush()
            except OSError as e:
                raise RemoteError(f"{self.name}: shell gone: {e}")
            lines = []
            deadline = time.time() + timeout
            while True:
                line = self._readline(deadline, timeout)
                if not line:
                    raise RemoteError(f"{self.name}: shell closed; output so far: {''.join(lines)!r}")
                if line.startswith(mark):
                    rc = int(line.split()[1])
                    break
                lines.append(line)
        out = "".join(lines).strip()
        if check and rc != 0:
            raise RemoteError(f"{self.name}: `{cmd[:200]}` exited {rc}: {out[:2000]}")
        return out

    def close(self):
        try:
            self.p.stdin.write(b"exit\n")
            self.p.stdin.flush()
            self.p.wait(timeout=5)
        except Exception:  # noqa: BLE001 - best effort
            self.p.kill()


def open_shells(nodes, password, log=print):
    """{name: RootShell} for nodes (dicts with name, ctl_ip, kind), opened in
    parallel; eclipse gets a local shell."""
    shells, errs = {}, []

    def one(n):
        try:
            shells[n["name"]] = RootShell(n["name"], n["ctl_ip"], password, local=(n["kind"] == "master"))
        except Exception as e:  # noqa: BLE001 - reported below
            errs.append(f"{n['name']}: {e}")
    ts = [threading.Thread(target=one, args=(n,)) for n in nodes]
    [t.start() for t in ts]
    [t.join() for t in ts]
    if errs:
        for s in shells.values():
            s.close()
        raise RemoteError("root shells failed: " + "; ".join(errs[:10]))
    log(f"[netem] {len(shells)} root shells open")
    return shells


# ------------------------------------------------------------- rate parsing
_UNIT = {"": 1, "K": 1000, "M": 1000 ** 2, "G": 1000 ** 3, "T": 1000 ** 4}


def tc_rate_ok(printed, intended_mbit):
    """Whether tc's printed rate ("117750Kbit") is the rate we set. tc keeps
    whole bytes/s and prints the largest unit it can (iproute2 print_rate),
    truncating only when the value is at least 1000 of that unit: the set
    rate must lie in [v, v + 1) units."""
    m = re.fullmatch(r"(\d+)([KMGT]?)bit", printed)
    if not m:
        return False
    v, u = int(m.group(1)), _UNIT[m.group(2)]
    bits = int(intended_mbit * 1e6 / 8) * 8
    return v * u <= bits < (v + 1) * u


class Emulator:
    """The lane's links. peers: lane nodes, then eclipse (index n)."""

    def __init__(self, lane, shells, log=print, shape_master=True, pair_mbit=None):
        """shape_master=False leaves eclipse's links alone: suites whose data
        never crosses eclipse (no Argo repository) can then run on parallel
        lanes, which would otherwise overwrite each other's root qdisc on
        eclipse's single data interface."""
        self.lane = lane
        self.peers = list(lane.nodes) + ([lane.master] if lane.master and shape_master else [])
        self.n = len(self.peers)
        self.idx = {p["name"]: i for i, p in enumerate(self.peers)}
        self.sh = shells
        self.log = log
        # pair_mbit {(u, v): mbit} overrides the lane's class-based rate for those directed pairs.
        over = pair_mbit or {}
        self.pristine = {(i, j): over.get((self.peers[i]["name"], self.peers[j]["name"]),
                                          lane.pair_mbit(self.peers[i]["name"], self.peers[j]["name"]))
                         for i in range(self.n) for j in range(self.n) if i != j}
        self.groups = {}        # src name -> (dst names, mbit): one shared class for src's traffic to dsts
        self.state = {k: ("up", r) for k, r in self.pristine.items()}
        self._jobs = {p["name"]: queue.Queue() for p in self.peers}
        self._workers = [threading.Thread(target=self._worker, args=(p["name"],), daemon=True) for p in self.peers]
        [w.start() for w in self._workers]

    # ------------------------------------------------------------- plumbing
    def _worker(self, name):
        while True:
            job = self._jobs[name].get()
            if job is None:
                return
            cmds, done, errs, outs = job
            try:
                outs[name] = [self.sh[name].run(c) for c in cmds]
            except Exception as e:  # noqa: BLE001 - surfaced to the caller
                errs.append(e)
            done.set()

    def on_nodes(self, per_node):
        """Run {name: [cmd, ...]} on every node concurrently; raise the first
        error; return {name: [outputs]}."""
        waits, errs, outs = [], [], {}
        for name, cmds in per_node.items():
            if not cmds:
                continue
            done = threading.Event()
            self._jobs[name].put((cmds, done, errs, outs))
            waits.append(done)
        for w in waits:
            w.wait()
        if errs:
            raise errs[0]
        return outs

    def minor(self, j):
        return 0x100 + j

    def classid(self, j):
        return f"{HANDLE}:{self.minor(j):x}"

    def prio(self, j):
        return 1 + j        # drop filters; class filters use prio 200

    @staticmethod
    def _rate(mbit):
        return f"{max(mbit, 0.01):.4f}mbit"

    def dif(self, i):
        return self.peers[i]["dif"]

    # ---------------------------------------------------------------- setup
    def preflight(self):
        """Refuse to touch a VLAN interface that carries someone else's HTB
        (an E17 lane): returns {name: problem}."""
        per = {p["name"]: [f"tc qdisc show dev {p['dif']} root"] for p in self.peers}
        outs = self.on_nodes(per)
        bad = {}
        for name, (out,) in outs.items():
            if "htb" in out and f"htb {HANDLE}: root" not in out:
                bad[name] = out.strip()
            if "does not exist" in out or "Cannot find device" in out:
                bad[name] = out.strip()
        return bad

    def _node_setup_cmds(self, i):
        d = self.dif(i)
        cmds = [
            f"tc qdisc del dev {d} root 2>/dev/null; true",
            f"tc qdisc add dev {d} root handle {HANDLE}: htb default {DEFAULT_MINOR:x}",
            f"tc class add dev {d} parent {HANDLE}: classid {HANDLE}:{DEFAULT_MINOR:x} htb rate 10gbit quantum {QUANTUM}",
            f"tc qdisc add dev {d} parent {HANDLE}:{DEFAULT_MINOR:x} fq_codel",
        ]
        for j, peer in enumerate(self.peers):
            if j == i:
                continue
            kind, rate = self.state[(i, j)]
            r = self._rate(rate)
            cmds += [f"tc class add dev {d} parent {HANDLE}: classid {self.classid(j)} htb rate {r} ceil {r} quantum {QUANTUM}",
                     f"tc qdisc add dev {d} parent {self.classid(j)} fq_codel",
                     f"tc filter add dev {d} parent {HANDLE}: protocol all prio 200 flower dst_mac {peer['mac']} "
                     f"classid {self.classid(j)}"]
            if kind in ("down", "stall"):
                cmds.append(self._drop_add(i, j))
        cmds += [
            f"nft delete table inet {NFT_TABLE} 2>/dev/null; true",
            f"nft add table inet {NFT_TABLE}",
            f"nft add set inet {NFT_TABLE} peers_down '{{ type ipv4_addr; }}'",
            f"nft add chain inet {NFT_TABLE} wlfwd '{{ type filter hook forward priority -10; policy accept; }}'",
            f"nft add chain inet {NFT_TABLE} out '{{ type filter hook output priority -10; policy accept; }}'",
            f"nft add rule inet {NFT_TABLE} wlfwd ip daddr @peers_down meta l4proto tcp reject with tcp reset",
            f"nft add rule inet {NFT_TABLE} out ip daddr @peers_down meta l4proto tcp reject with tcp reset",
        ]
        cmds += [self._reset_add(j) for j in range(self.n) if j != i and self.state[(i, j)][0] == "down"]
        return cmds

    def setup(self):
        bad = self.preflight()
        if bad:
            raise RemoteError(f"VLAN interfaces with a foreign root qdisc (E17 lane still set up?): {bad}")
        self.on_nodes({p["name"]: self._node_setup_cmds(i) for i, p in enumerate(self.peers)})
        self.log(f"[netem] {self.n} nodes shaped ({self.n * (self.n - 1)} directed links)")

    def ensure(self, name):
        """Re-create a node's tree if it is gone (reboot, link reset) with
        the current state; True if it had to."""
        i = self.idx[name]
        out = self.sh[name].run(f"tc qdisc show dev {self.dif(i)} root; nft list table inet {NFT_TABLE} >/dev/null "
                                f"2>&1 && echo NFT_OK", check=False)
        if f"htb {HANDLE}: root" in out and "NFT_OK" in out:
            return False
        self.log(f"[netem] {name}: emulation state missing, setting it up again")
        self.on_nodes({name: self._node_setup_cmds(i)})
        return True

    def teardown(self):
        per = {}
        for i, p in enumerate(self.peers):
            d = self.dif(i)
            per[p["name"]] = [f"if tc qdisc show dev {d} root | grep -q 'htb {HANDLE}: root'; then "
                              f"tc qdisc del dev {d} root; fi; true",
                              f"nft delete table inet {NFT_TABLE} 2>/dev/null; true"]
        self.on_nodes(per)
        self.log("[netem] emulation removed")

    def close(self):
        for q in self._jobs.values():
            q.put(None)

    # ---------------------------------------------------------- link changes
    def _drop_add(self, i, j):
        return (f"tc filter add dev {self.dif(i)} parent {HANDLE}: protocol all prio {self.prio(j)} "
                f"flower dst_mac {self.peers[j]['mac']} action drop")

    def _reset_add(self, j):
        return f"nft add element inet {NFT_TABLE} peers_down '{{ {self.peers[j]['data_ip']} }}'"

    def _reset_del(self, j):
        return f"nft delete element inet {NFT_TABLE} peers_down '{{ {self.peers[j]['data_ip']} }}' 2>/dev/null; true"

    def apply(self, changes):
        """Simultaneous directed-link changes [(i, j, kind, mbit)], kind in
        down | degrade | up (up = pristine rate)."""
        per = {p["name"]: [] for p in self.peers}
        for i, j, kind, mbit in changes:
            name, d = self.peers[i]["name"], self.dif(i)
            old, _ = self.state[(i, j)]
            if kind in ("down", "stall"):
                # down: frames dropped and TCP reset (transfers fail fast); stall: frames silently dropped, so a
                # transfer waits and resumes when the link comes back (a contact window closing and reopening).
                if old not in ("down", "stall"):
                    per[name].append(self._drop_add(i, j))
                if kind == "down" and old != "down":
                    per[name].append(self._reset_add(j))
                if kind == "stall" and old == "down":
                    per[name].append(self._reset_del(j))
                self.state[(i, j)] = (kind, self.state[(i, j)][1])   # the class keeps its rate
                continue
            rate = self.pristine[(i, j)] if kind == "up" else float(mbit)
            if old in ("down", "stall"):
                per[name] += [f"tc filter del dev {d} parent {HANDLE}: prio {self.prio(j)}", self._reset_del(j)]
            per[name].append(f"tc class change dev {d} parent {HANDLE}: classid {self.classid(j)} htb "
                             f"rate {self._rate(rate)} ceil {self._rate(rate)} quantum {QUANTUM}")
            self.state[(i, j)] = ("up" if kind == "up" else "degrade", rate)
        self.on_nodes(per)

    GROUP_MINOR = 0xf00
    GROUP_PRIO = 150    # after the drop filters (1 + j), before the per-destination class filters (200)

    def cap_group(self, src, dsts, mbit):
        """One shared rate for src's traffic to every node in dsts (a degraded uplink serving several
        consumers): those destinations' frames go to one class instead of their own. Cuts still apply."""
        i = self.idx[src]
        d, cid = self.dif(i), f"{HANDLE}:{self.GROUP_MINOR:x}"
        r = self._rate(mbit)
        if src in self.groups:
            cmds = [f"tc filter del dev {d} parent {HANDLE}: prio {self.GROUP_PRIO} 2>/dev/null; true",
                    f"tc class change dev {d} parent {HANDLE}: classid {cid} htb rate {r} ceil {r} quantum {QUANTUM}"]
        else:
            cmds = [f"tc class add dev {d} parent {HANDLE}: classid {cid} htb rate {r} ceil {r} quantum {QUANTUM}",
                    f"tc qdisc add dev {d} parent {cid} fq_codel"]
        cmds += [f"tc filter add dev {d} parent {HANDLE}: protocol all prio {self.GROUP_PRIO} flower "
                 f"dst_mac {self.peers[self.idx[n]]['mac']} classid {cid}" for n in dsts]
        self.on_nodes({src: cmds})
        self.groups[src] = (list(dsts), float(mbit))

    def uncap_group(self, src):
        if src not in self.groups:
            return
        i = self.idx[src]
        d, cid = self.dif(i), f"{HANDLE}:{self.GROUP_MINOR:x}"
        self.on_nodes({src: [f"tc filter del dev {d} parent {HANDLE}: prio {self.GROUP_PRIO} 2>/dev/null; true",
                             f"tc qdisc del dev {d} parent {cid} 2>/dev/null; true",
                             f"tc class del dev {d} classid {cid} 2>/dev/null; true"]})
        del self.groups[src]

    def reset(self):
        """Every link back to its pristine rate (between runs)."""
        for src in list(self.groups):
            self.uncap_group(src)
        changes = [(i, j, "up", None) for (i, j), (k, r) in self.state.items()
                   if k != "up" or abs(r - self.pristine[(i, j)]) > 1e-9]
        if changes:
            self.apply(changes)
        # A crashed and restored node keeps its qdisc (k3s-killall leaves the
        # VLAN alone), but check the nft table and the tree are still there.
        for p in self.peers:
            self.ensure(p["name"])

    def _node_links(self, name, peers=None):
        i = self.idx[name]
        js = [self.idx[p] for p in peers] if peers else [j for j in range(self.n) if j != i]
        return i, js

    def degrade_node(self, name, mbit, include_master=True):
        """Every link between `name` and the other peers at `mbit` (0 cuts)."""
        i, js = self._node_links(name)
        if not include_master and self.lane.master:
            js = [j for j in js if self.peers[j]["name"] != self.lane.master["name"]]
        kind = "down" if mbit <= 0 else "degrade"
        self.apply([(a, b, kind, mbit) for j in js for a, b in ((i, j), (j, i))])

    def degrade_pair(self, u, v, mbit):
        i, j = self.idx[u], self.idx[v]
        kind = "down" if mbit <= 0 else "degrade"
        self.apply([(i, j, kind, mbit), (j, i, kind, mbit)])

    def partition(self, name):
        self.degrade_node(name, 0, include_master=True)

    # ------------------------------------------------------------ readback
    def verify(self):
        """Read every node's classes and filters back and compare with the
        intended state. Returns {"ok": bool, "problems": [...]}."""
        per = {p["name"]: [f"tc class show dev {p['dif']}; echo ===; tc filter show dev {p['dif']} parent {HANDLE}:; "
                           f"echo ===; nft list set inet {NFT_TABLE} peers_down 2>&1"] for p in self.peers}
        outs = self.on_nodes(per)
        problems = []
        for i, p in enumerate(self.peers):
            classes, filters, nftset = (outs[p["name"]][0].split("===") + ["", "", ""])[:3]
            rates = dict(re.findall(rf"class htb {HANDLE}:([0-9a-f]+) .*?rate (\S+)", classes))
            drops = drop_prefs(filters)
            resets = set(re.findall(r"\d+\.\d+\.\d+\.\d+", nftset))
            for j in range(self.n):
                if j == i:
                    continue
                kind, rate = self.state[(i, j)]
                printed = rates.get(f"{self.minor(j):x}")
                if printed is None or not tc_rate_ok(printed, rate):
                    problems.append(f"{p['name']}->{self.peers[j]['name']}: rate {printed} != {rate:.4f} Mbit")
                ip = self.peers[j]["data_ip"]
                dropped = str(self.prio(j)) in drops
                if kind == "stall":
                    if not dropped:
                        problems.append(f"{p['name']}->{self.peers[j]['name']}: not stalled (no drop filter)")
                    continue
                down_now = dropped and ip in resets
                if (kind == "down") != down_now:
                    problems.append(f"{p['name']}->{self.peers[j]['name']}: {'not ' if kind == 'down' else ''}cut "
                                    f"(drop filter {str(self.prio(j)) in drops}, reset {ip in resets})")
        return {"ok": not problems, "problems": problems}

    def counters(self):
        """Bytes sent per directed link and per node's default class:
        {"u>v": bytes, "u>*": default-class bytes}."""
        per = {p["name"]: [f"tc -s class show dev {p['dif']}"] for p in self.peers}
        outs = self.on_nodes(per)
        res = {}
        for i, p in enumerate(self.peers):
            for minor, sent in re.findall(rf"class htb {HANDLE}:([0-9a-f]+) .*?\n\s*Sent (\d+) bytes", outs[p["name"]][0]):
                m = int(minor, 16)
                if m == DEFAULT_MINOR:
                    res[f"{p['name']}>*"] = int(sent)
                elif 0 <= m - 0x100 < self.n:
                    res[f"{p['name']}>{self.peers[m - 0x100]['name']}"] = int(sent)
                elif m == self.GROUP_MINOR:
                    res[f"{p['name']}>group"] = int(sent)
        return res

    def snapshot_state(self):
        out = {f"{self.peers[i]['name']}>{self.peers[j]['name']}": [k, round(r, 4)]
               for (i, j), (k, r) in self.state.items() if k != "up"}
        out.update({f"{src}>group{{{','.join(d)}}}": ["cap", m] for src, (d, m) in self.groups.items()})
        return out


def drop_prefs(filters):
    """Prefs of the flower filters whose action is drop, from `tc filter show`."""
    prefs, cur = set(), None
    for line in filters.splitlines():
        m = re.match(r"filter .*?pref (\d+)", line)
        if m:
            cur = m.group(1)
        elif cur and "action drop" in line:
            prefs.add(cur)
    return prefs


def counter_delta(before, after):
    return {k: after[k] - before.get(k, 0) for k in after if after[k] - before.get(k, 0) != 0}
