"""E18 Argo arms: a central S3 artifact repository on eclipse.

E16's Argo arm on the rebuilt testbed. Every output is uploaded to the
repository and every input downloaded from it, as Argo Workflows does with
an S3 artifact repository. The repository is `rclone serve s3` over
memory-backed storage (MinIO's images are no longer published), as a
hostNetwork pod on eclipse bound to eclipse's data address 10.20.0.1:9500:
tasks reach it over the data VLAN, through eclipse's emulated link like any
other link (E16's repository on anrg-2 was unshaped, which favored the arm).

Tasks are placed where Wayline's plan put them (required for pinned
sources, preferred for the rest, and required to stay in the lane), retry up
to three times on another node, and tolerate a lost node for only 5 s
before eviction (the Kubernetes default of 300 s would leave a crashed
node's pods Running for five minutes).

Needs Argo Workflows installed in namespace argo (install.sh).
"""
import json
import time

import images
import kube

NS = "argo"
REG = kube.REG
TASK_IMAGE = images.ref("wl-e16-task")      # images.py: mx1 is amd64 + arm64
S3_IMAGE = images.ref("rclone")
# Not 9000: a Docker swarm service on eclipse (traefik_traefik, 0 replicas) publishes *:9000, and the
# swarm's ingress mesh DNATs that port on every interface, so connections to 10.20.0.1:9000 are refused.
S3_PORT = 9500
IMAGES = {"glue": images.ref("wl-gdtm-argo"), "model": images.ref("wl-gdtm-model-argo"),
          "focal": images.ref("wl-focal-argo")}
NAME = "e18-s3"


def _k(args, stdin=None, timeout=120):
    return kube.kubectl(args, stdin=stdin, ns=NS, timeout=timeout)


def installed():
    r = kube.kubectl(["get", "deploy", "workflow-controller", "-o", "name"], ns=NS)
    return r.returncode == 0


def repo_up(master):
    """The S3 repository on eclipse (hostNetwork, data address) and the
    artifact-repository ConfigMap pointing at it."""
    endpoint = f"{master['data_ip']}:{S3_PORT}"
    objs = [
        {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": f"{NAME}-cred", "namespace": NS},
         "stringData": {"accesskey": "e18admin", "secretkey": "e18adminpw"}},
        {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": NAME, "namespace": NS},
         "spec": {"replicas": 1, "selector": {"matchLabels": {"app": NAME}},
                  "template": {"metadata": {"labels": {"app": NAME}},
                               "spec": {"nodeName": master["name"], "hostNetwork": True,
                                        "tolerations": [{"key": "node-role.kubernetes.io/control-plane",
                                                         "operator": "Exists", "effect": "NoSchedule"}],
                                        "containers": [{"name": "s3", "image": S3_IMAGE,
                                                        "command": ["sh", "-c",
                                                                    f"mkdir -p /data/e18 && exec rclone serve s3 /data "
                                                                    f"--addr {endpoint} --auth-key e18admin,e18adminpw"],
                                                        "volumeMounts": [{"name": "data", "mountPath": "/data"}]}],
                                        "volumes": [{"name": "data", "emptyDir": {"medium": "Memory",
                                                                                  "sizeLimit": "8Gi"}}]}}}},
        {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": f"{NAME}-artifacts", "namespace": NS},
         "data": {"s3": f"s3:\n  bucket: e18\n  endpoint: {endpoint}\n"
                        "  insecure: true\n  createBucketIfNotPresent:\n    objectLocking: false\n"
                        "  keyFormat: '{{workflow.name}}/{{pod.name}}'\n"
                        f"  accessKeySecret:\n    name: {NAME}-cred\n    key: accesskey\n"
                        f"  secretKeySecret:\n    name: {NAME}-cred\n    key: secretkey\n"}},
    ]
    for o in objs:
        r = _k(["apply", "-f", "-"], stdin=json.dumps(o))
        if r.returncode != 0:
            raise RuntimeError(f"argo repository: {r.stderr}")
    _k(["rollout", "status", f"deploy/{NAME}", "--timeout=180s"], timeout=200)


def repo_down():
    _k(["delete", f"deploy/{NAME}", f"cm/{NAME}-artifacts", f"secret/{NAME}-cred", "--ignore-not-found"])


def repo_clear(prefix):
    _k(["exec", f"deploy/{NAME}", "--", "sh", "-c", f"rm -rf /data/e18/{prefix}"])


def _size(s):
    s = str(s).upper()
    for k, v in (("GB", 10**9), ("MB", 10**6), ("KB", 10**3)):
        if s.endswith(k):
            return int(float(s[:-2]) * v)
    return int(float(s))


def _generic(name, role, runtime, size, cpu, deps, pin=None):
    """A task of the generic bodies (task.py): one output, "out"."""
    return {"name": name, "image": TASK_IMAGE, "command": None,
            "args": [name, role, str(runtime), str(size)], "cpu": str(cpu), "mem": "512Mi",
            "inputs": [(d, "out") for d in deps], "outputs": ["out"], "pin": pin}


def tasks_of(workload, iot_base, pins):
    """Argo tasks: name, image, command/args, resources, inputs as
    (producer, object), named outputs, and the pinned node if any."""
    if workload in ("chain", "chain-link", "fan"):
        mb = 8 if workload == "chain" else 30
        out = [_generic("p", "hash", 12.0, mb * 1_000_000, 2, []), _generic("c", "hash", 20.0, 0, 2, ["p"])]
        if workload == "fan":
            out.append(_generic("d", "hash", 1.0, 0, 1, ["p"]))
        return out
    if workload == "iot":
        out = []
        for t in iot_base["tasks"]:
            nn = (t.get("constraints") or {}).get("nodeNames") or [None]
            out.append(_generic(t["name"], t["name"].split("-")[0].replace("generate", "report"),
                                float(t.get("runtime", 1)), _size(t.get("dataSize", "1MB")),
                                t.get("resources", {}).get("cpu", "1"), t.get("dependencies", []), nn[0]))
        return out
    if workload == "gdtm":
        import gen_gdtm
        out = []
        for task, role, mod, node, kind, inputs, outputs, rt, cpu in gen_gdtm.stages(30):
            out.append({"name": task, "image": IMAGES[kind], "command": ["python", "/app/argo_stage.py"],
                        "args": ["gdtm", role, mod or "-", node or "-", str(cpu), "0", "30"], "cpu": str(cpu),
                        "mem": "2Gi" if kind == "model" else "1Gi", "inputs": list(inputs),
                        "outputs": list(outputs), "pin": pins.get(task)})
        return out
    if workload == "focal":
        import gen_focal
        out = []
        for task, role, mod, inputs, output, rt, cpu in gen_focal.stages():
            out.append({"name": task, "image": IMAGES["focal"], "command": ["python", "/app/argo_stage.py"],
                        "args": ["focal", role, mod or "-", "64", "0", str(cpu)], "cpu": str(cpu), "mem": "1Gi",
                        "inputs": list(inputs), "outputs": [output], "pin": pins.get(task)})
        return out
    raise ValueError(workload)


def images_of(workload):
    if workload in ("chain", "chain-link", "fan", "iot") or workload.startswith("iot-pi"):
        return [TASK_IMAGE]
    if workload == "gdtm":
        return [IMAGES["glue"], IMAGES["model"]]
    if workload == "focal":
        return [IMAGES["focal"]]
    raise ValueError(workload)


def workflow(workload, plan, iot_base, pins, lane_nodes):
    """lane_nodes: per image, the lane nodes that can run it (arch)."""
    tasks = tasks_of(workload, iot_base, pins)
    tol = [{"key": k, "operator": "Exists", "effect": "NoExecute", "tolerationSeconds": 5}
           for k in ("node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable")]
    templates, dag = [], []
    host = "kubernetes.io/hostname"
    for t in tasks:
        if t["pin"]:
            affinity = {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
                {"matchExpressions": [{"key": host, "operator": "In", "values": [t["pin"]]}]}]}}
        else:
            affinity = {
                "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
                    {"matchExpressions": [{"key": host, "operator": "In", "values": lane_nodes[t["image"]]}]}]},
                "preferredDuringSchedulingIgnoredDuringExecution": [{"weight": 100, "preference": {
                    "matchExpressions": [{"key": host, "operator": "In", "values": [plan[t["name"]]]}]}}]}
        generic = t["command"] is None
        container = {"image": t["image"], "imagePullPolicy": "Always", "args": t["args"],   # as E16 and Wayline pods
                     "env": [{"name": "NODE_NAME", "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}}],
                     "resources": {"requests": {"cpu": t["cpu"], "memory": t["mem"]}}}
        if not generic:
            container["command"] = t["command"]
        templates.append({
            "name": t["name"],
            "inputs": {"artifacts": [{"name": f"in{i}", "path": f"/in/{i:02d}-{p}.{o}"}
                                     for i, (p, o) in enumerate(t["inputs"])]},
            "outputs": {"artifacts": [{"name": o, "path": "/out/data" if generic else f"/out/{o}",
                                       "archive": {"none": {}}} for o in t["outputs"]]},
            "container": container,
            "affinity": {"nodeAffinity": affinity},
            "tolerations": tol,
            "retryStrategy": {"limit": 3, "retryPolicy": "Always", "affinity": {"nodeAntiAffinity": {}}},
        })
        deps = sorted({p for p, _ in t["inputs"]})
        dag.append({"name": t["name"], "template": t["name"], "dependencies": deps,
                    "arguments": {"artifacts": [{"name": f"in{i}", "from": f"{{{{tasks.{p}.outputs.artifacts.{o}}}}}"}
                                                for i, (p, o) in enumerate(t["inputs"])]}})
    return {"apiVersion": "argoproj.io/v1alpha1", "kind": "Workflow",
            "metadata": {"generateName": f"e18-{workload}-", "namespace": NS,
                         "labels": {kube.EXPERIMENT_LABEL: kube.EXPERIMENT}},
            "spec": {"entrypoint": "dag", "serviceAccountName": "argo",
                     "artifactRepositoryRef": {"configMap": f"{NAME}-artifacts", "key": "s3"},
                     "templates": [{"name": "dag", "dag": {"tasks": dag}}] + templates}}


def status(name):
    r = _k(["get", "wf", name, "-o", "json"])
    return json.loads(r.stdout).get("status", {}) if r.returncode == 0 else {}


def task_phase(st, task):
    """Phase of a task's latest attempt (Pod nodes named <wf>.<task>(<n>))."""
    phase = ""
    for n in (st.get("nodes") or {}).values():
        if n.get("type") == "Pod" and n.get("displayName", "").split("(")[0] == task:
            phase = n.get("phase", phase) if phase != "Succeeded" else phase
    return phase


def running_on(st, task):
    """The node a task's latest attempt is running on, or ""."""
    best = None
    for n in (st.get("nodes") or {}).values():
        if n.get("type") == "Pod" and n.get("displayName", "").split("(")[0] == task and n.get("phase") == "Running":
            if best is None or (n.get("startedAt") or "") > (best.get("startedAt") or ""):
                best = n
    return (best or {}).get("hostNodeName", "")


def run(wf, fault_fn=None, trigger=None, delay=0.0, timeout=900, oos=None, oos_node=None, second=None):
    """One workflow. fault_fn fires `delay` s after `trigger` succeeded
    (right away when trigger is None). oos: an OutOfService watcher to arm
    on the victim (Argo+oos). second: (consumer, delay2, crash_fn,
    first_victim) for a second loss on the node the consumer's retry runs on."""
    r = _k(["create", "-f", "-", "-o", "jsonpath={.metadata.name}"], stdin=json.dumps(wf))
    if r.returncode != 0:
        raise RuntimeError(f"argo create: {r.stderr}")
    name = r.stdout.strip()
    t0 = time.time()
    out = {"workflow": name, "submitted": t0}
    fired = False
    st = {}
    timed_out = True
    while time.time() - t0 < timeout:
        st = status(name)
        if fault_fn and not fired and (trigger is None or task_phase(st, trigger) == "Succeeded"):
            fired = True
            out["triggerAt"] = time.time()
            time.sleep(delay)
            out["faultAt"] = time.time()
            fault_fn()
            if oos and oos_node:
                oos.watch(oos_node)
        if second and fired and "fault2At" not in out:
            consumer, delay2, crash, first_victim = second
            node = running_on(st, consumer)
            if node and node != first_victim:
                time.sleep(delay2)
                out["victim2"], out["fault2At"] = node, time.time()
                crash(node)
                if oos:
                    oos.watch(node)
        if st.get("phase") in ("Succeeded", "Failed", "Error"):
            timed_out = False
            break
        time.sleep(0.5)
    out["wall"] = time.time() - t0
    out["timedOut"] = timed_out
    out["phase"] = st.get("phase")
    out["message"] = st.get("message", "")
    pods = [n for n in (st.get("nodes") or {}).values() if n.get("type") == "Pod"]
    out["tasks"] = [{"task": n.get("displayName"), "node": n.get("hostNodeName"), "phase": n.get("phase"),
                     "startedAt": n.get("startedAt"), "finishedAt": n.get("finishedAt"),
                     "message": n.get("message", "")} for n in pods]
    out["retries"] = sum(1 for n in pods if "(" in n.get("displayName", "") and not n["displayName"].endswith("(0)"))
    out["argoStatus"] = st
    _k(["delete", "wf", name, "--wait=false"])
    repo_clear(name)
    return out


def cleanup_stale():
    """Delete E18 workflows left by an interrupted run."""
    _k(["delete", "wf", "-l", f"{kube.EXPERIMENT_LABEL}={kube.EXPERIMENT}", "--wait=false"])
