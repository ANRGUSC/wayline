"""E16 Argo arm: a central S3 artifact repository.

Every output is uploaded to the repository and every input downloaded from
it, as Argo Workflows does with an S3 artifact repository. The repository
is `rclone serve s3` over memory-backed storage: MinIO's images are no
longer published (quay.io and Docker Hub both refuse them since 2025), and
any S3 endpoint serves Argo the same way. Tasks are placed where
Wayline's plan put them (required for pinned sources, preferred for the
rest), retry up to three times on another node, and tolerate a lost node
for only 5 s before eviction (the Kubernetes default is 300 s, which would
leave a crashed node's pods Running for five minutes).

The repository runs on anrg-2. Its traffic is not shaped by E13 (which
shapes worker-to-worker traffic only), which favors this arm on the slow
link classes; the analysis must say so. (Resource names keep "minio".)
"""
import json
import os
import subprocess
import time

NS = "argo"
REG = "192.168.1.163:5000"
TASK_IMAGE = f"{REG}/wl-e16-task:latest"
S3_IMAGE = f"{REG}/rclone:e16"
HERE = os.path.dirname(os.path.abspath(__file__))


def kubectl(args, stdin=None, timeout=120):
    return subprocess.run(["kubectl", "-n", NS] + args, input=stdin, capture_output=True, text=True,
                          timeout=timeout)


def minio_up():
    objs = [
        {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "e16-minio-cred", "namespace": NS},
         "stringData": {"accesskey": "e16admin", "secretkey": "e16adminpw"}},
        {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "e16-minio", "namespace": NS},
         "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "e16-minio"}},
                  "template": {"metadata": {"labels": {"app": "e16-minio"}},
                               "spec": {"nodeName": "anrg-2",
                                        "tolerations": [{"key": "node.kubernetes.io/unschedulable",
                                                         "operator": "Exists", "effect": "NoSchedule"}],
                                        "containers": [{"name": "s3", "image": S3_IMAGE,
                                                        "command": ["sh", "-c",
                                                                    "mkdir -p /data/e16 && exec rclone serve s3 /data "
                                                                    "--addr :9000 --auth-key e16admin,e16adminpw"],
                                                        "ports": [{"containerPort": 9000}],
                                                        "volumeMounts": [{"name": "data", "mountPath": "/data"}]}],
                                        "volumes": [{"name": "data", "emptyDir": {"medium": "Memory",
                                                                                  "sizeLimit": "4Gi"}}]}}}},
        {"apiVersion": "v1", "kind": "Service", "metadata": {"name": "e16-minio", "namespace": NS},
         "spec": {"selector": {"app": "e16-minio"}, "ports": [{"port": 9000, "targetPort": 9000}]}},
        {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "e16-artifacts", "namespace": NS},
         "data": {"minio": "s3:\n  bucket: e16\n  endpoint: e16-minio.argo.svc.cluster.local:9000\n"
                           "  insecure: true\n  createBucketIfNotPresent:\n    objectLocking: false\n"
                           "  keyFormat: '{{workflow.name}}/{{pod.name}}'\n"
                           "  accessKeySecret:\n    name: e16-minio-cred\n    key: accesskey\n"
                           "  secretKeySecret:\n    name: e16-minio-cred\n    key: secretkey\n"}},
    ]
    for o in objs:
        r = kubectl(["apply", "-f", "-"], stdin=json.dumps(o))
        assert r.returncode == 0, r.stderr
    kubectl(["rollout", "status", "deploy/e16-minio", "--timeout=180s"], timeout=200)


def minio_down():
    kubectl(["delete", "deploy/e16-minio", "svc/e16-minio", "cm/e16-artifacts", "secret/e16-minio-cred",
             "--ignore-not-found"])


def minio_clear(prefix):
    kubectl(["exec", "deploy/e16-minio", "--", "sh", "-c", f"rm -rf /data/e16/{prefix}"])


def tasks_of(workload, iot_base):
    if workload == "chain":
        return [{"name": "p", "role": "hash", "runtime": 12.0, "size": 8_000_000, "cpu": "2", "deps": [], "pinned": False},
                {"name": "c", "role": "hash", "runtime": 20.0, "size": 0, "cpu": "2", "deps": ["p"], "pinned": False}]
    out = []
    for t in json.load(open(iot_base))["tasks"]:
        size = t.get("dataSize", "1MB").upper()
        mul = {"GB": 10**9, "MB": 10**6, "KB": 10**3}
        b = next((int(float(size[:-2]) * v) for k, v in mul.items() if size.endswith(k)), None) or int(size)
        out.append({"name": t["name"], "role": t["name"].split("-")[0].replace("generate", "report"),
                    "runtime": float(t.get("runtime", 1)), "size": b,
                    "cpu": t.get("resources", {}).get("cpu", "1"), "deps": t.get("dependencies", []),
                    "pinned": bool((t.get("constraints") or {}).get("nodeNames"))})
    return out


def workflow(workload, plan, iot_base):
    tasks = tasks_of(workload, iot_base)
    tol = [{"key": k, "operator": "Exists", "effect": "NoExecute", "tolerationSeconds": 5}
           for k in ("node.kubernetes.io/not-ready", "node.kubernetes.io/unreachable")]
    templates, dag = [], []
    for t in tasks:
        term = {"matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "In", "values": [plan[t["name"]]]}]}
        affinity = ({"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [term]}} if t["pinned"]
                    else {"preferredDuringSchedulingIgnoredDuringExecution": [{"weight": 100, "preference": term}]})
        templates.append({
            "name": t["name"],
            "inputs": {"artifacts": [{"name": f"in{i}", "path": f"/in/{i:02d}-{d}"} for i, d in enumerate(t["deps"])]},
            "outputs": {"artifacts": [{"name": "out", "path": "/out/data", "archive": {"none": {}}}]},
            "container": {"image": TASK_IMAGE, "imagePullPolicy": "Always",
                          "args": [t["name"], t["role"], str(t["runtime"]), str(t["size"])],
                          "env": [{"name": "NODE_NAME", "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}}],
                          "resources": {"requests": {"cpu": t["cpu"], "memory": "512Mi"}}},
            "affinity": {"nodeAffinity": affinity},
            "tolerations": tol,
            "retryStrategy": {"limit": 3, "retryPolicy": "Always", "affinity": {"nodeAntiAffinity": {}}},
        })
        dag.append({"name": t["name"], "template": t["name"], "dependencies": t["deps"],
                    "arguments": {"artifacts": [{"name": f"in{i}", "from": f"{{{{tasks.{d}.outputs.artifacts.out}}}}"}
                                                for i, d in enumerate(t["deps"])]}})
    return {"apiVersion": "argoproj.io/v1alpha1", "kind": "Workflow",
            "metadata": {"generateName": f"e16-{workload}-", "namespace": NS},
            "spec": {"entrypoint": "dag", "serviceAccountName": "argo",
                     "artifactRepositoryRef": {"configMap": "e16-artifacts", "key": "minio"},
                     "templates": [{"name": "dag", "dag": {"tasks": dag}}] + templates}}


def status(name):
    r = kubectl(["get", "wf", name, "-o", "json"])
    return json.loads(r.stdout).get("status", {}) if r.returncode == 0 else {}


def task_phase(st, task):
    """Phase of a task's latest attempt (Pod nodes named <wf>.<task>(<n>))."""
    phase = ""
    for n in (st.get("nodes") or {}).values():
        if n.get("type") == "Pod" and n.get("displayName", "").split("(")[0] == task:
            phase = n.get("phase", phase) if phase != "Succeeded" else phase
    return phase


def mark_out_of_service(node, stop):
    """What an operator (or node-lifecycle automation) does for a crashed
    node: once Kubernetes marks it NotReady, taint it out-of-service
    (non-graceful node shutdown), which force-deletes its pods. Without it,
    pods evicted from an unreachable node stay Terminating until the node
    returns, and a workflow waiting on them never retries."""
    import threading

    def watch():
        while not stop.is_set():
            ready = subprocess.run(["kubectl", "get", "node", node, "-o",
                                    "jsonpath={.status.conditions[?(@.type=='Ready')].status}"],
                                   capture_output=True, text=True).stdout.strip()
            if ready and ready != "True":
                subprocess.run(["kubectl", "taint", "node", node,
                                "node.kubernetes.io/out-of-service=nodeshutdown:NoExecute", "--overwrite"],
                               capture_output=True, text=True)
                return time.time()
            time.sleep(1)
    t = threading.Thread(target=lambda: marked.append(watch()), daemon=True)
    marked = []
    t.start()
    return marked


def clear_out_of_service(node):
    subprocess.run(["kubectl", "taint", "node", node, "node.kubernetes.io/out-of-service-"],
                   capture_output=True, text=True)


def run(workload, plan, iot_base, fault_fn=None, trigger=None, delay=0.0, timeout=900,
        out_of_service=None):
    wf = workflow(workload, plan, iot_base)
    r = kubectl(["create", "-f", "-", "-o", "jsonpath={.metadata.name}"], stdin=json.dumps(wf))
    assert r.returncode == 0, r.stderr
    name = r.stdout.strip()
    t0 = time.time()
    out = {"workflow": name, "submitted": t0}
    fired = False
    st = {}
    import threading
    stop = threading.Event()
    marked = []
    while time.time() - t0 < timeout:
        st = status(name)
        if fault_fn and not fired and (trigger is None or task_phase(st, trigger) == "Succeeded"):
            fired = True
            time.sleep(delay)
            out["faultAt"] = time.time()
            fault_fn()
            if out_of_service:
                marked = mark_out_of_service(out_of_service, stop)
        if st.get("phase") in ("Succeeded", "Failed", "Error"):
            break
        time.sleep(0.5)
    stop.set()
    out["wall"] = time.time() - t0
    if marked and marked[0]:
        out["outOfServiceAt"] = marked[0]
    out["phase"] = st.get("phase")
    out["message"] = st.get("message", "")
    pods = [n for n in (st.get("nodes") or {}).values() if n.get("type") == "Pod"]
    out["tasks"] = [{"task": n.get("displayName"), "node": n.get("hostNodeName"), "phase": n.get("phase"),
                     "startedAt": n.get("startedAt"), "finishedAt": n.get("finishedAt"),
                     "message": n.get("message", "")} for n in pods]
    out["retries"] = sum(1 for n in pods if "(" in n.get("displayName", "") and not n["displayName"].endswith("(0)"))
    kubectl(["delete", "wf", name, "--wait=false"])
    minio_clear(name)
    return out
