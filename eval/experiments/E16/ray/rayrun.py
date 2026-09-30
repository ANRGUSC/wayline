"""E16 Ray arms: cluster lifecycle and one run with fault injection.

The cluster is E11's (head on anrg-2 with no CPUs, one hostNetwork worker
per node, so E13's per-destination shaping applies to Ray's transfers too),
with node-failure detection tuned to about 5 s (health check every second,
five misses) so Ray learns of a crash as fast as Wayline's 5 s grace.
"""
import json
import os
import subprocess
import threading
import time

NS = "wl-system"
IMAGE = "192.168.1.163:5000/wl-ray-baseline:2.58.0"
HEAD_NODE = "anrg-2"
HEAD_IP = "192.168.1.163"
HEAD_CPUS = 0   # a head on a worker node doubles as that node's worker


def configure(image, head_node="anrg-2"):
    """Use another image (all nodes run it: one Python across the cluster)
    and, for images too big for anrg-2, a head on a worker. hostNetwork allows
    one Ray node per host, so a head on a worker also serves as its worker."""
    global IMAGE, HEAD_NODE, HEAD_IP, HEAD_CPUS
    IMAGE, HEAD_NODE = image, head_node
    HEAD_IP = subprocess.run(["kubectl", "get", "node", head_node, "-o",
                              "jsonpath={.status.addresses[?(@.type=='InternalIP')].address}"],
                             capture_output=True, text=True).stdout.strip()
    HEAD_CPUS = 0 if head_node == "anrg-2" else 7
HERE = os.path.dirname(os.path.abspath(__file__))
PORTS = ("--object-manager-port 8076 --node-manager-port 8077 "
         "--min-worker-port 10002 --max-worker-port 10100")
HEALTH = [{"name": "RAY_health_check_period_ms", "value": "1000"},
          {"name": "RAY_health_check_timeout_ms", "value": "1000"},
          {"name": "RAY_health_check_failure_threshold", "value": "5"}]


def kubectl(args, stdin=None, timeout=120):
    return subprocess.run(["kubectl", "-n", NS] + args, input=stdin, capture_output=True, text=True,
                          timeout=timeout)


def pod(name, node, cmd, head=False):
    spec = {"nodeName": node, "hostNetwork": True, "restartPolicy": "Never",
            "containers": [{"name": "ray", "image": IMAGE, "command": ["sh", "-c", cmd],
                            "env": HEALTH if head else []}]}
    if head:
        spec["tolerations"] = [{"key": "node.kubernetes.io/unschedulable", "operator": "Exists",
                                "effect": "NoSchedule"}]
    return {"apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": NS, "labels": {"app": "e16-ray"}}, "spec": spec}


def phase(name):
    return kubectl(["get", "pod", name, "-o", "jsonpath={.status.phase}"]).stdout.strip()


def alive_nodes():
    r = kubectl(["exec", "e16-ray-head", "--", "python", "-c",
                 "import ray; ray.init(address='auto', log_to_driver=False); "
                 "print(sorted(n['NodeManagerHostname'] for n in ray.nodes() if n['Alive']))"], timeout=60)
    try:
        return set(eval(r.stdout.strip().splitlines()[-1]))
    except Exception:
        return set()


def up(workers):
    """Create or replace the head and every worker that is not running."""
    if phase("e16-ray-head") != "Running":
        kubectl(["delete", "pod", "e16-ray-head", "--wait=true", "--ignore-not-found"])
        cmd = f"ray start --head --port 6379 --num-cpus {HEAD_CPUS} {PORTS} --block"
        kubectl(["apply", "-f", "-"], stdin=json.dumps(pod("e16-ray-head", HEAD_NODE, cmd, head=True)))
        for _ in range(60):
            if phase("e16-ray-head") == "Running":
                break
            time.sleep(2)
        time.sleep(5)
    for n in workers:
        if n == HEAD_NODE:
            continue  # the head is this node's Ray node
        name = f"e16-ray-{n}"
        if phase(name) == "Running" and n in alive_nodes():
            continue
        kubectl(["delete", "pod", name, "--wait=true", "--ignore-not-found", "--grace-period=0", "--force"])
        cmd = f"ray start --address {HEAD_IP}:6379 --num-cpus 7 {PORTS} --block"
        kubectl(["apply", "-f", "-"], stdin=json.dumps(pod(name, n, cmd)))
    for _ in range(90):
        if set(workers) <= alive_nodes():
            return
        time.sleep(2)
    raise RuntimeError(f"Ray workers not alive: {set(workers) - alive_nodes()}")


def down():
    kubectl(["delete", "pod", "-l", "app=e16-ray", "--wait=false", "--grace-period=0", "--force"])


def run(workload, plan, copies, iot_base, fault_fn=None, trigger=None, delay=0.0):
    """One Ray run. fault_fn fires `delay` s after task `trigger` finishes
    (immediately after submission when trigger is None). Returns the
    driver's RESULT plus the fault time. GDTM uses the driver built into its
    image (e16_ray_gdtm.py); iot and chain the generic one."""
    if workload == "gdtm":
        driver, args = "/app/e16_ray_gdtm.py", ["--plan", "/tmp/plan.json"]
    else:
        for local, remote in ((os.path.join(HERE, "e16_ray.py"), "/tmp/e16_ray.py"), (iot_base, "/tmp/iot-base.json")):
            kubectl(["cp", local, f"e16-ray-head:{remote}"])
        driver, args = "/tmp/e16_ray.py", ["--workload", workload, "--plan", "/tmp/plan.json"]
    kubectl(["exec", "-i", "e16-ray-head", "--", "sh", "-c", "cat > /tmp/plan.json"], stdin=json.dumps(plan))
    if copies:
        kubectl(["exec", "-i", "e16-ray-head", "--", "sh", "-c", "cat > /tmp/copies.json"], stdin=json.dumps(copies))
        args += ["--copies", "/tmp/copies.json"]
    t0 = time.time()
    p = subprocess.Popen(["kubectl", "-n", NS, "exec", "e16-ray-head", "--", "python", "-u", driver] + args,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    out = {"submitted": t0, "events": []}
    fired = threading.Event()

    def fire():
        out["faultAt"] = time.time()
        fault_fn()

    if fault_fn and trigger is None:
        fired.set()
        threading.Thread(target=fire).start()
    for line in p.stdout:
        line = line.rstrip()
        if line.startswith(("DONE ", "FAILED ")):
            out["events"].append([time.time(), line])
            if fault_fn and not fired.is_set() and line.startswith(f"DONE {trigger} "):
                fired.set()
                threading.Timer(delay, fire).start()
        elif line.startswith("RESULT "):
            out.update(json.loads(line[7:]))
    p.wait()
    out["wall"] = time.time() - t0
    return out
