"""E18 Dask arms: a Dask cluster on a lane, and one run with fault injection.

Dask distributed as a third baseline, deployed like Ray (../ray/rayrun.py):
one hostNetwork pod per lane worker runs `dask worker` bound to the node's
data IP, so worker-to-worker transfers run on the data VLAN through the
emulated links; the scheduler runs on eclipse bound to 10.20.0.1:8786 (its
link to every worker is shaped like any other). The driver runs inside the
scheduler pod (copied there per run): only task submissions and small
completion messages go between client and scheduler, and no task data goes
through the client.

Every pod runs the same image (the client, scheduler and workers must agree
on versions), in namespace dask-e18, with this configuration:

  - Active Memory Manager off. Its default policy (ReduceReplicas, every
    2 s) drops replicas the scheduler does not need, which would undo
    client.replicate(), the copies of Dask+copies.
  - Work stealing off. A task pinned softly to its planned node (a loose
    restriction) then stays there unless that node is gone, as with Ray's
    soft node affinity; stealing would move it to an idle worker.
  - worker-ttl 5 s. A worker whose heartbeats stop is removed after 5 s
    (Ray here: 1 s health checks x 5 misses; Wayline: 5 s grace). A
    crashed worker process is removed at once, when its connections reset.

Workers: nthreads = the node's CPUs, resource CPU = CPUs - 1 (the same
accounting as Ray's --num-cpus here, one core left to the system; each
task asks for its template CPU request), one worker process under a nanny
(Dask's default), no worker dashboard. With procs=True: one single-threaded
worker process per CPU instead (Dask's advice for pure-Python CPU-bound
code, which holds the GIL), without the CPU resource (each process runs
one task at a time). Tasks are pinned by host (the data IP), which matches
every worker process on the node, and a worker restarted on the node.

Workers are found by their host (the data IP they bind), never by name.
"""
import importlib.util
import json
import os
import subprocess
import threading
import time

import images
import kube
from hostlock import HostLock

HERE = os.path.dirname(os.path.abspath(__file__))
E18 = os.path.dirname(HERE)
NS = "dask-e18"
LOCK = HostLock("dask")   # this process's hold on the control node's Dask cluster
# python:3.12-slim + dask[distributed] 2026.8.0 (dask/image.sh), amd64 + arm64. Not the upstream
# ghcr.io/dask/dask image (build.sh dask mirrors it as dask:2026.8.0-py3.12): its conda CPython and
# OpenSSL hash E11's loop about 30% faster than the python:3.12-slim of the Ray, Argo and Wayline tasks.
IMAGE = images.ref("wl-dask-baseline")
SCHED = "e18-dask-scheduler"
LABEL = "e18-dask"
PORT = 8786
DASHBOARD = 8787        # scheduler dashboard, on eclipse's 127.0.0.1 only
WORKER_PORT, NANNY_PORT = 8790, 8791
WORKER_PORTS, NANNY_PORTS = "8800:8899", "8900:8999"   # procs mode
SCHED_TCP_PORTS = [PORT]
CONFIG = [{"name": "DASK_DISTRIBUTED__SCHEDULER__ACTIVE_MEMORY_MANAGER__START", "value": "False"},
          {"name": "DASK_DISTRIBUTED__SCHEDULER__WORK_STEALING", "value": "False"},
          {"name": "DASK_DISTRIBUTED__SCHEDULER__WORKER_TTL", "value": "5s"}]
DRIVER = "e18_dask.py"
BODY = os.path.join(E18, "argo", "e16_ray_body.py")   # the task bodies the Ray driver and Argo task run


def _argorun():
    spec = importlib.util.spec_from_file_location("argorun", os.path.join(E18, "argo", "argorun.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _k(args, stdin=None, timeout=120):
    return kube.kubectl(args, stdin=stdin, ns=NS, timeout=timeout)


def ensure_namespace():
    if _k(["get", "namespace", NS, "-o", "name"]).returncode != 0:
        kube.kubectl(["create", "namespace", NS], ns=None)


def _pod(name, node, image, cmd, sched=False):
    spec = {"nodeName": node, "hostNetwork": True, "restartPolicy": "Never",
            "containers": [{"name": "dask", "image": image, "imagePullPolicy": "IfNotPresent",
                            "command": ["sh", "-c", cmd], "env": CONFIG}]}
    if sched:
        spec["tolerations"] = [{"key": "node-role.kubernetes.io/control-plane", "operator": "Exists",
                                "effect": "NoSchedule"}]
    return {"apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": NS, "labels": {"app": LABEL}}, "spec": spec}


def _phase(name):
    return _k(["get", "pod", name, "-o", "jsonpath={.status.phase}"]).stdout.strip()


def worker_name(node):
    return f"e18-dask-{node}"


def task_list(workload, iot_base, pins):
    """The workload's tasks as the driver takes them, from the Argo arm's
    task list (the same names, bodies, runtimes, sizes and CPU requests;
    chain 8 MB, chain-link and fan 30 MB, as workloads.py)."""
    out = []
    for t in _argorun().tasks_of(workload, iot_base, pins):
        if t["command"] is not None:
            raise ValueError(f"{workload}: Dask runs the generic bodies only (iot, chain, chain-link, fan)")
        name, role, runtime, size = t["args"]
        out.append({"name": name, "role": role, "runtime": float(runtime), "size": int(size),
                    "cpu": parse_cpu(t["cpu"]), "deps": sorted({p for p, _ in t["inputs"]}),
                    "pinned": t["pin"] is not None or name in pins})
    return out


def parse_cpu(s):
    s = str(s)
    return float(s[:-1]) / 1000 if s.endswith("m") else float(s)


class Cluster:
    """Dask on the lane nodes `workers` ({name: node dict}), scheduler on eclipse."""

    def __init__(self, image, workers, master, procs=False):
        self.image = image or IMAGE
        self.workers = workers
        self.master = master
        self.procs = procs
        self.address = f"tcp://{master['data_ip']}:{PORT}"
        self.ip2node = {n["data_ip"]: name for name, n in workers.items()}
        self.ip2node[master["data_ip"]] = master["name"]

    def _on_scheduler(self, code, timeout=60):
        """Run `code` (a Python expression of the Scheduler `s`) on the
        scheduler through a short-lived client; returns its JSON value."""
        py = ("import json; from distributed import Client; "
              f"c = Client('{self.address}', timeout=10); "
              f"f = lambda dask_scheduler: (lambda s: {code})(dask_scheduler); "
              "print(json.dumps(c.run_on_scheduler(f))); c.close()")
        r = _k(["exec", SCHED, "--", "python", "-c", py], timeout=timeout)
        try:
            return json.loads(r.stdout.strip().splitlines()[-1])
        except (IndexError, ValueError):
            return None

    def worker_hosts(self):
        """{host: number of running worker processes} known to the scheduler."""
        return self._on_scheduler("__import__('collections').Counter(ws.host for ws in s.workers.values() "
                                  "if ws.status.name == 'running')") or {}

    def alive_ips(self):
        return set(self.worker_hosts())

    def _want(self, n):
        return n["cores"] if self.procs else 1

    def up(self):
        """Create the scheduler and every worker that is not running and registered."""
        fresh = LOCK.f is None
        LOCK.acquire()
        ensure_namespace()
        if fresh:   # pods left by a process that no longer holds the lock are no one's cluster
            _k(["delete", "pod", "-l", f"app={LABEL}", "--wait=true", "--grace-period=0", "--force"])
        if _phase(SCHED) != "Running":
            _k(["delete", "pod", SCHED, "--wait=true", "--ignore-not-found"])
            cmd = (f"exec python -m dask scheduler --host {self.master['data_ip']} --port {PORT} "
                   f"--dashboard-address 127.0.0.1:{DASHBOARD}")
            _k(["apply", "-f", "-"], stdin=json.dumps(_pod(SCHED, self.master["name"], self.image, cmd, sched=True)))
            for _ in range(150):
                if _phase(SCHED) == "Running" and self._on_scheduler("1") == 1:
                    break
                time.sleep(2)
            else:
                raise RuntimeError("Dask scheduler did not start")
        hosts = self.worker_hosts()
        for name, n in self.workers.items():
            pod = worker_name(name)
            if _phase(pod) == "Running" and hosts.get(n["data_ip"], 0) >= self._want(n):
                continue
            _k(["delete", "pod", pod, "--wait=true", "--ignore-not-found", "--grace-period=0", "--force"])
            if self.procs:
                shape = (f"--nworkers {n['cores']} --nthreads 1 --worker-port {WORKER_PORTS} "
                         f"--nanny-port {NANNY_PORTS}")
            else:
                cpus = max(1, n["cores"] - 1)   # one core left to the system, as the Ray workers
                shape = (f"--nworkers 1 --nthreads {n['cores']} --resources CPU={cpus} "
                         f"--worker-port {WORKER_PORT} --nanny-port {NANNY_PORT}")
            cmd = (f"exec python -m dask worker {self.address} --host {n['data_ip']} {shape} --name {name} "
                   f"--no-dashboard --local-directory /tmp/dask-worker")
            _k(["apply", "-f", "-"], stdin=json.dumps(_pod(pod, name, self.image, cmd)))
        for _ in range(150):
            hosts = self.worker_hosts()
            missing = [name for name, n in self.workers.items() if hosts.get(n["data_ip"], 0) < self._want(n)]
            if not missing:
                return
            time.sleep(2)
        raise RuntimeError(f"Dask workers not registered: {missing}")

    def down(self):
        if not LOCK.try_acquire():
            print(f"[{LOCK.name}] another experiment holds the cluster; leaving it up", flush=True)
            return
        _k(["delete", "pod", "-l", f"app={LABEL}", "--wait=true", "--grace-period=0", "--force"])
        LOCK.release()

    def running_on(self, task, run_id):
        """The lane node the task's key is processing on ("" if none). A policy re-run of the task is submitted
        under <key>-r<n> (e18_dask.py), so those keys count as the task too."""
        key = f"{task}-{run_id}"
        host = self._on_scheduler(f"next((ts.processing_on.host for k, ts in s.tasks.items() "
                                  f"if (k == {key!r} or str(k).startswith({key + '-r'!r})) "
                                  "and ts.processing_on is not None), '')")
        return self.ip2node.get(host or "", "")

    def put(self, path, data):
        r = _k(["exec", "-i", SCHED, "--", "sh", "-c", f"cat > {path}"], stdin=data)
        if r.returncode != 0:
            raise RuntimeError(f"copy to {SCHED}:{path}: {r.stderr}")

    def run(self, workload, plan, copies, pins, iot_base, fault_fn=None, trigger=None, delay=0.0,
            second=None, timeout=900, pin="soft", repairs=None):
        """One Dask run (the interface of rayrun.Cluster.run). fault_fn fires
        `delay` s after task `trigger` finishes (right after submission when
        trigger is None). second: (consumer, delay2, crash_fn, first_victim):
        a second loss on the node the consumer is rescheduled on, once it
        runs there. Returns the driver's RESULT plus fault times.

        A task the driver reports as no-worker after the fault is pinned
        hard to a node that is gone (without a fault every planned host has
        a worker): Dask waits for a worker on that node, which the harness
        restores only after the run. The run is stopped there and recorded
        as Failed, with the stuck task (Ray fails such a task at once as
        unschedulable)."""
        run_id = f"r{int(time.time() * 1000)}"
        self.put(f"/tmp/{DRIVER}", open(os.path.join(HERE, DRIVER)).read())
        self.put("/tmp/e16_ray_body.py", open(BODY).read())
        self.put("/tmp/tasks.json", json.dumps(task_list(workload, iot_base, pins)))
        self.put("/tmp/plan.json", json.dumps(plan))
        self.put("/tmp/nodes.json", json.dumps({name: n["data_ip"] for name, n in self.workers.items()}))
        self.put("/tmp/pins.json", json.dumps(pins))
        args = ["--scheduler", self.address, "--tasks", "/tmp/tasks.json", "--plan", "/tmp/plan.json",
                "--nodes", "/tmp/nodes.json", "--pins", "/tmp/pins.json", "--body", "/tmp/e16_ray_body.py",
                "--run-id", run_id, "--pin", pin] + ([] if self.procs else ["--cpu-resource"])
        if copies:
            self.put("/tmp/copies.json", json.dumps(copies))
            args += ["--copies", "/tmp/copies.json"]
        if repairs:
            self.put("/tmp/repairs.json", json.dumps(repairs))
            args += ["--repairs", "/tmp/repairs.json"]
        t0 = time.time()
        p = subprocess.Popen(list(kube.KUBECTL) + ["-n", NS, "exec", SCHED, "--", "python", "-u", f"/tmp/{DRIVER}"] + args,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        out = {"submitted": t0, "events": [], "driver": DRIVER, "runId": run_id, "pin": pin,
               "workerMode": "procs" if self.procs else "threads"}
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
                node = self.running_on(consumer, run_id)
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
            if line.startswith(("DONE ", "FAILED ", "NOWORKER ")):
                out["events"].append([time.time(), line])
                if fault_fn and not fired.is_set() and line.startswith(f"DONE {trigger} "):
                    fired.set()
                    threading.Timer(delay, fire).start()
                if line.startswith("NOWORKER ") and fired.is_set() and "noWorkerStop" not in out:
                    out["noWorkerStop"] = {"task": line.split()[1], "at": time.time()}
                    p.kill()
            elif line.startswith("RESULT "):
                out.update(json.loads(line[7:]))
            else:
                tail = (tail + [line])[-40:]
        p.wait()
        killer.cancel()
        out["wall"] = time.time() - t0
        out["driverTail"] = tail
        if "noWorkerStop" in out and "phase" not in out:
            task = out["noWorkerStop"]["task"]
            out.update(phase="Failed", errors={task: "no-worker: pinned to a lost node; Dask waits for a worker there"})
        return out
