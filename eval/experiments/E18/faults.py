"""E18 node faults: crash and restore through the node's root shell, and the
out-of-service taint automation of the Argo+oos arm. Link faults and the
data-plane partition are the emulator's (netem.py).

crash    k3s-killall.sh: the kubelet and every container stop (data agent,
         runners, task pods); files on disk survive, as after a power loss.
         The data VLAN shaping survives too (k3s-killall leaves it alone).
restore  start k3s-agent; wait until the node is Ready and its data agent
         answers on the data IP (Ready alone can be stale after a crash
         shorter than the node grace), then a settle time for the
         controller to take the node back (it re-checks every second).
"""
import json
import threading
import time
import urllib.request

import kube

KILLALL = "/usr/local/bin/k3s-killall.sh"
OOS_TAINT = "node.kubernetes.io/out-of-service"


def stamp(msg):
    print(f"[fault] {time.time():.3f} {msg}", flush=True)


def resolve(fault, lane):
    """A fault tuple with roles replaced by node names (trigger tasks and
    numbers unchanged)."""
    if not fault:
        return None
    kind = fault[0]
    if kind in ("crash", "partition"):
        return (kind, lane.resolve(fault[1])) + tuple(fault[2:])
    if kind == "crash2":
        return (kind, lane.resolve(fault[1])) + tuple(fault[2:])
    if kind == "link":
        return (kind, lane.resolve(fault[1]), fault[2])
    if kind == "link-pair":
        return (kind, lane.resolve(fault[1]), lane.resolve(fault[2]), fault[3])
    raise ValueError(f"unknown fault {fault!r}")


def agent_ok(data_ip, timeout=2):
    try:
        with urllib.request.urlopen(f"http://{data_ip}:8082/healthz", timeout=timeout) as r:
            return r.status == 200
    except Exception:  # noqa: BLE001 - unreachable
        return False


def crash(sh, node):
    sh.run(f"{KILLALL} >/tmp/wl-fault-killall.log 2>&1; echo done", timeout=180)
    stamp(f"crashed {node}")
    return time.time()


def restore(sh, node, data_ip, api, timeout=360, settle=8):
    t0 = time.time()
    sh.run("systemctl start k3s-agent", timeout=180)
    while time.time() - t0 < timeout:
        try:
            n = api.get(f"/api/v1/nodes/{node}")
            ready = next((c["status"] for c in n["status"].get("conditions", []) if c["type"] == "Ready"), "")
        except Exception:  # noqa: BLE001 - API hiccup: try again
            ready = ""
        if ready == "True" and agent_ok(data_ip):
            stamp(f"restored {node} (Ready, data agent answers)")
            time.sleep(settle)
            return time.time()
        time.sleep(2)
    raise RuntimeError(f"{node} not back after {timeout}s")


class OutOfService:
    """What an operator (or node-lifecycle automation) does for a crashed
    node: once Kubernetes marks it NotReady, taint it out-of-service, which
    force-deletes its pods (non-graceful node shutdown). Without it, pods of
    an unreachable node stay Terminating and a workflow waiting on them never
    retries (E16)."""

    def __init__(self):
        self.stop = threading.Event()
        self.marked = {}

    def watch(self, node):
        def run():
            while not self.stop.is_set():
                r = kube.kubectl(["get", "node", node, "-o",
                                  "jsonpath={.status.conditions[?(@.type=='Ready')].status}"], ns=None)
                ready = r.stdout.strip()
                if ready and ready != "True":
                    kube.kubectl(["taint", "node", node, f"{OOS_TAINT}=nodeshutdown:NoExecute", "--overwrite"], ns=None)
                    self.marked[node] = time.time()
                    stamp(f"tainted {node} out-of-service")
                    return
                time.sleep(1)
        threading.Thread(target=run, daemon=True).start()

    def close(self):
        self.stop.set()


def clear_out_of_service(node):
    kube.kubectl(["taint", "node", node, f"{OOS_TAINT}-"], ns=None)


def agent_metrics(data_ip):
    try:
        with urllib.request.urlopen(f"http://{data_ip}:8082/metrics", timeout=3) as r:
            return json.loads(r.read())
    except Exception:  # noqa: BLE001 - unreachable agent
        return None
