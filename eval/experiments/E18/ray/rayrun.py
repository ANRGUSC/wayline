"""E18 Ray arms: cluster lifecycle on a lane, and one run with fault injection.

E16's Ray setup on the rebuilt testbed: one hostNetwork Ray node per lane
worker, started with --node-ip-address <data IP>, so Ray's object transfers
run on the data VLAN through the same emulated links as Wayline's; the head
runs on eclipse with no CPUs (GCS on 10.20.0.1:6379; its link to every
worker is shaped like any other). Node-failure detection is tuned to about
5 s (health check every second, five misses), as fast as Wayline's 5 s grace.

Ray nodes are found by their data IP (NodeManagerAddress), never by host
name. The drivers run inside the head pod (copied there per run) and get
{node: data IP} with the plan.

Every Ray pod lives in namespace ray-e18 and mounts a memory-backed
/dev/shm: a container's own /dev/shm is 64 MB, and Ray then puts its object
store in /tmp (E16 ran that way). The head's object store is capped at 2 GB
(it runs no tasks; only the driver's few small objects land there), the
workers keep Ray's default (30% of the node's memory).
"""
import json
import os
import subprocess
import threading
import time

import kube
from hostlock import HostLock

HERE = os.path.dirname(os.path.abspath(__file__))
NS = "ray-e18"
LOCK = HostLock("ray")   # this process's hold on the control node's Ray cluster
HEAD = "e18-ray-head"
LABEL = "e18-ray"
# Worker ports stay off 10010: containerd's stream server holds 127.0.0.1:10010 on every node.
WORKER_PORTS = ("--object-manager-port 8076 --node-manager-port 8077 "
                "--min-worker-port 10011 --max-worker-port 10100")
# eclipse is a shared server: its head keeps worker ports away from 10010 (in use there).
HEAD_PORTS = ("--object-manager-port 8076 --node-manager-port 8077 --ray-client-server-port 10001 "
              "--min-worker-port 20002 --max-worker-port 20100 --dashboard-host 127.0.0.1 --dashboard-port 8265 "
              "--object-store-memory 2000000000")
HEAD_TCP_PORTS = [6379, 8076, 8077, 8265, 10001]
HEALTH = [{"name": "RAY_health_check_period_ms", "value": "1000"},
          {"name": "RAY_health_check_timeout_ms", "value": "1000"},
          {"name": "RAY_health_check_failure_threshold", "value": "5"}]
DRIVERS = {"gdtm": "e18_ray_gdtm.py", "focal": "e18_ray_focal.py"}


def _k(args, stdin=None, timeout=120):
    return kube.kubectl(args, stdin=stdin, ns=NS, timeout=timeout)


def ensure_namespace():
    if _k(["get", "namespace", NS, "-o", "name"]).returncode != 0:
        kube.kubectl(["create", "namespace", NS], ns=None)


def _pod(name, node, image, cmd, head=False):
    spec = {"nodeName": node, "hostNetwork": True, "restartPolicy": "Never",
            "containers": [{"name": "ray", "image": image, "imagePullPolicy": "IfNotPresent",
                            "command": ["sh", "-c", cmd], "env": HEALTH if head else [],
                            "volumeMounts": [{"name": "shm", "mountPath": "/dev/shm"}]}],
            "volumes": [{"name": "shm", "emptyDir": {"medium": "Memory"}}]}
    if head:
        spec["tolerations"] = [{"key": "node-role.kubernetes.io/control-plane", "operator": "Exists",
                                "effect": "NoSchedule"}]
    return {"apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": NS, "labels": {"app": LABEL}}, "spec": spec}


def _phase(name):
    return _k(["get", "pod", name, "-o", "jsonpath={.status.phase}"]).stdout.strip()


def worker_name(node):
    return f"e18-ray-{node}"


class Cluster:
    """Ray on the lane nodes `workers` ({name: node dict}), head on eclipse."""

    def __init__(self, image, workers, master):
        self.image = image
        self.workers = workers
        self.master = master
        self.ip2node = {n["data_ip"]: name for name, n in workers.items()}
        self.ip2node[master["data_ip"]] = master["name"]

    def alive_ips(self):
        code = ("import json, ray; ray.init(address='auto', log_to_driver=False); "
                "print(json.dumps(sorted(n['NodeManagerAddress'] for n in ray.nodes() if n['Alive'])))")
        r = _k(["exec", HEAD, "--", "python", "-c", code], timeout=60)
        try:
            return set(json.loads(r.stdout.strip().splitlines()[-1]))
        except (IndexError, ValueError):
            return set()

    def up(self):
        """Create the head and every worker that is not running and alive."""
        fresh = LOCK.f is None
        LOCK.acquire()
        ensure_namespace()
        if fresh:   # pods left by a process that no longer holds the lock are no one's cluster
            _k(["delete", "pod", "-l", f"app={LABEL}", "--wait=true", "--grace-period=0", "--force"])
        if _phase(HEAD) != "Running":
            _k(["delete", "pod", HEAD, "--wait=true", "--ignore-not-found"])
            cmd = (f"ray start --head --node-ip-address {self.master['data_ip']} --port 6379 --num-cpus 0 "
                   f"{HEAD_PORTS} --block")
            _k(["apply", "-f", "-"], stdin=json.dumps(_pod(HEAD, self.master["name"], self.image, cmd, head=True)))
            for _ in range(150):
                if _phase(HEAD) == "Running":
                    break
                time.sleep(2)
            time.sleep(5)
        alive = self.alive_ips()
        for name, n in self.workers.items():
            pod = worker_name(name)
            if _phase(pod) == "Running" and n["data_ip"] in alive:
                continue
            _k(["delete", "pod", pod, "--wait=true", "--ignore-not-found", "--grace-period=0", "--force"])
            cpus = max(1, n["cores"] - 1)   # one core left to the system, as E16 (7 of 8)
            cmd = (f"ray start --address {self.master['data_ip']}:6379 --node-ip-address {n['data_ip']} "
                   f"--num-cpus {cpus} {WORKER_PORTS} --block")
            _k(["apply", "-f", "-"], stdin=json.dumps(_pod(pod, name, self.image, cmd)))
        want = {n["data_ip"] for n in self.workers.values()}
        for _ in range(150):
            alive = self.alive_ips()
            if want <= alive:
                return
            time.sleep(2)
        raise RuntimeError(f"Ray workers not alive: {sorted(self.ip2node[i] for i in want - alive)}")

    def down(self):
        if not LOCK.try_acquire():
            print(f"[{LOCK.name}] another experiment holds the cluster; leaving it up", flush=True)
            return
        _k(["delete", "pod", "-l", f"app={LABEL}", "--wait=true", "--grace-period=0", "--force"])
        LOCK.release()

    def running_on(self, task):
        """The lane node a Ray task named `task` runs on ("" if none), from
        Ray's state API on the head."""
        code = ("from ray.util.state import list_tasks, list_nodes; import ray; "
                "ray.init(address='auto', log_to_driver=False); "
                "ips = {n.node_id: n.node_ip for n in list_nodes()}; "
                f"ts = list_tasks(filters=[('name', '=', '{task}'), ('state', '=', 'RUNNING')]); "
                "print(' '.join(ips.get(t.node_id, '') for t in ts))")
        ips = _k(["exec", HEAD, "--", "python", "-c", code], timeout=60).stdout.strip().split()
        return self.ip2node.get(ips[-1], "") if ips else ""

    def put(self, path, data):
        r = _k(["exec", "-i", HEAD, "--", "sh", "-c", f"cat > {path}"], stdin=data)
        if r.returncode != 0:
            raise RuntimeError(f"copy to {HEAD}:{path}: {r.stderr}")

    def run(self, workload, plan, copies, pins, iot_base, fault_fn=None, trigger=None, delay=0.0,
            second=None, timeout=900, repairs=None):
        """One Ray run. fault_fn fires `delay` s after task `trigger`
        finishes (right after submission when trigger is None). second:
        (consumer, delay2, crash_fn, first_victim): a second loss on the node
        Ray re-runs the consumer on, once it runs there. Returns the
        driver's RESULT plus fault times."""
        kind = {"gdtm": "gdtm", "focal": "focal"}.get(workload, "plain")
        driver = DRIVERS.get(kind, "e18_ray.py")
        self.put(f"/tmp/{driver}", open(os.path.join(HERE, driver)).read())
        nodes = {name: n["data_ip"] for name, n in self.workers.items()}
        self.put("/tmp/plan.json", json.dumps(plan))
        self.put("/tmp/nodes.json", json.dumps(nodes))
        self.put("/tmp/pins.json", json.dumps(pins))
        args = ["--plan", "/tmp/plan.json", "--nodes", "/tmp/nodes.json", "--pins", "/tmp/pins.json"]
        if kind == "plain":
            self.put("/tmp/iot-base.json", json.dumps(iot_base))
            args += ["--workload", workload, "--iot-base", "/tmp/iot-base.json"]
        if copies:
            self.put("/tmp/copies.json", json.dumps(copies))
            args += ["--copies", "/tmp/copies.json"]
        if repairs:
            self.put("/tmp/repairs.json", json.dumps(repairs))
            args += ["--repairs", "/tmp/repairs.json"]
        t0 = time.time()
        p = subprocess.Popen(list(kube.KUBECTL) + ["-n", NS, "exec", HEAD, "--", "python", "-u", f"/tmp/{driver}"] + args,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        out = {"submitted": t0, "events": [], "driver": driver}
        fired = threading.Event()
        killer = threading.Timer(timeout, lambda: (out.__setitem__("timedOut", True), p.kill()))
        killer.daemon = True
        killer.start()

        def fire():
            out["faultAt"] = time.time()
            fault_fn()
            if second:
                threading.Thread(target=fire_second, daemon=True).start()

        def fire_second():
            consumer, delay2, crash, first_victim = second
            while p.poll() is None:   # as long as the run is in progress
                node = self.running_on(consumer)
                if node and node != first_victim:
                    time.sleep(delay2)
                    out["victim2"], out["fault2At"] = node, time.time()
                    crash(node)
                    return
                time.sleep(0.5)

        if fault_fn and trigger is None:
            fired.set()
            threading.Thread(target=fire, daemon=True).start()
        tail = []
        for line in p.stdout:
            line = line.rstrip()
            if line.startswith(("DONE ", "FAILED ")):
                out["events"].append([time.time(), line])
                if fault_fn and not fired.is_set() and line.startswith(f"DONE {trigger} "):
                    fired.set()
                    threading.Timer(delay, fire).start()
            elif line.startswith("RESULT "):
                out.update(json.loads(line[7:]))
            else:
                tail = (tail + [line])[-40:]
        p.wait()
        killer.cancel()
        out["wall"] = time.time() - t0
        out["driverTail"] = tail
        return out
