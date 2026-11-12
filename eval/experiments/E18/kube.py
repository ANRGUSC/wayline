"""E18 cluster access: kubectl, the API through a private proxy, Wayline
ODAG helpers, and registry lookups.

On eclipse the kubectl on PATH is v1.16 and broken against this server; every
call goes through `k3s kubectl` (override with E18_KUBECTL).
"""
import json
import os
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

KUBECTL = os.environ.get("E18_KUBECTL", "k3s kubectl").split()
NS = "wl-system"
REG = os.environ.get("E18_REGISTRY", "10.10.0.1:5000")
OLD_REG = "192.168.1.163:5000"      # the E16-era registry baked into templates and examples
EXPERIMENT_LABEL = "wl.io/experiment"
EXPERIMENT = "e18"


def kubectl(args, stdin=None, ns=NS, timeout=120):
    """Run k3s kubectl; returns the CompletedProcess (never raises on exit code)."""
    cmd = list(KUBECTL) + (["-n", ns] if ns else []) + list(args)
    try:
        return subprocess.run(cmd, input=stdin, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as e:
        return subprocess.CompletedProcess(cmd, 124, e.stdout or "", f"timeout after {timeout}s")


def kubectl_json(args, ns=NS, timeout=120):
    r = kubectl(list(args) + ["-o", "json"], ns=ns, timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(args)}: {r.stderr.strip()}")
    return json.loads(r.stdout)


def apply(obj_or_text, ns=NS):
    """kubectl apply of a dict (sent as JSON) or YAML/JSON text."""
    text = obj_or_text if isinstance(obj_or_text, str) else json.dumps(obj_or_text)
    r = kubectl(["apply", "-f", "-"], stdin=text, ns=ns)
    if r.returncode != 0:
        raise RuntimeError(f"kubectl apply: {r.stderr.strip()}")
    return r.stdout


def rewrite_registry(x):
    """Replace the E16 registry with REG in every string of a nested object."""
    if isinstance(x, str):
        return x.replace(OLD_REG, REG)
    if isinstance(x, list):
        return [rewrite_registry(v) for v in x]
    if isinstance(x, dict):
        return {k: rewrite_registry(v) for k, v in x.items()}
    return x


# --------------------------------------------------------------- API proxy
class Api:
    """The API server through a private `k3s kubectl proxy` (as E17's driver)."""

    ODAGS = f"/apis/wl.io/v1/namespaces/{NS}/odags"
    TEMPLATES = f"/apis/wl.io/v1/namespaces/{NS}/odagtemplates"

    def __init__(self):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        self.port = s.getsockname()[1]
        s.close()
        self.proxy = subprocess.Popen(list(KUBECTL) + ["proxy", f"--port={self.port}"],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(100):
            try:
                self.req("GET", "/version")
                return
            except Exception:  # noqa: BLE001 - proxy still starting
                time.sleep(0.1)
        raise RuntimeError("kubectl proxy did not start")

    def req(self, method, path, body=None, ctype="application/json", timeout=15):
        data = json.dumps(body).encode() if body is not None else None
        r = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                   headers={"Content-Type": ctype})
        try:
            with urllib.request.urlopen(r, timeout=timeout) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            if e.code != 404:
                e.msg = f"{e.msg}: {e.read().decode(errors='replace')[:2000]}"
            raise

    def get(self, path):
        return self.req("GET", path)

    def exists(self, path):
        try:
            self.get(path)
            return True
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return False
            raise

    def close(self):
        self.proxy.terminate()

    # ---------------------------------------------------------- core objects
    def nodes(self):
        return self.get("/api/v1/nodes")["items"]

    def pods(self, ns, selector=None, field=None):
        q = []
        if selector:
            q.append("labelSelector=" + urllib.parse.quote(selector))
        if field:
            q.append("fieldSelector=" + urllib.parse.quote(field))
        return self.get(f"/api/v1/namespaces/{ns}/pods" + ("?" + "&".join(q) if q else ""))["items"]

    def configmap(self, ns, name):
        try:
            return self.get(f"/api/v1/namespaces/{ns}/configmaps/{name}")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            raise

    def put_configmap(self, ns, name, data):
        body = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name, "namespace": ns},
                "data": {k: str(v) for k, v in data.items()}}
        path = f"/api/v1/namespaces/{ns}/configmaps"
        if self.configmap(ns, name) is None:
            return self.req("POST", path, body)
        return self.req("PUT", f"{path}/{name}", body)

    def merge_configmap(self, ns, name, data):
        """Set (a value) or remove (None) the given keys and leave every other key as it is: a JSON merge patch,
        applied atomically by the API server, so writers of disjoint keys never undo each other."""
        path = f"/api/v1/namespaces/{ns}/configmaps"
        if self.configmap(ns, name) is None:
            return self.req("POST", path, {"apiVersion": "v1", "kind": "ConfigMap",
                                           "metadata": {"name": name, "namespace": ns},
                                           "data": {k: str(v) for k, v in data.items() if v is not None}})
        return self.req("PATCH", f"{path}/{name}", {"data": {k: None if v is None else str(v) for k, v in data.items()}},
                        "application/merge-patch+json")

    def patch_node(self, name, body):
        return self.req("PATCH", f"/api/v1/nodes/{name}", body, "application/merge-patch+json")

    # ------------------------------------------------------------- Wayline
    def put_template(self, tmpl):
        """Create or replace an ODAGTemplate (a dict)."""
        name = tmpl["metadata"]["name"]
        path = f"{self.TEMPLATES}/{name}"
        try:
            cur = self.get(path)
            tmpl = dict(tmpl, metadata=dict(tmpl["metadata"], resourceVersion=cur["metadata"]["resourceVersion"]))
            return self.req("PUT", path, tmpl)
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
        return self.req("POST", self.TEMPLATES, tmpl)

    def run_from_template(self, name, labels=None, extra_spec=None):
        """A new run of a template, exactly as `wayline run` makes it
        (cmd/cli odagRunFromTemplate): the template's spec without its
        template-only fields, generateName <tmpl>-run-, label wl.io/template.
        extra_spec: fields set on the run itself (spec.nodes)."""
        tmpl = self.get(f"{self.TEMPLATES}/{name}")
        spec = {k: v for k, v in tmpl["spec"].items() if k not in ("profiling", "defaults", "retention", "description")}
        spec.update(extra_spec or {})
        lab = {"wl.io/template": name, EXPERIMENT_LABEL: EXPERIMENT}
        lab.update(labels or {})
        obj = {"apiVersion": "wl.io/v1", "kind": "ODAG",
               "metadata": {"generateName": f"{name}-run-", "namespace": NS, "labels": lab}, "spec": spec}
        return self.req("POST", self.ODAGS, obj)["metadata"]["name"]

    def crd_spec_fields(self, crd="odags.wl.io"):
        """Top-level spec fields the installed CRD's schema knows (others are
        pruned by the API server)."""
        try:
            obj = self.get(f"/apis/apiextensions.k8s.io/v1/customresourcedefinitions/{crd}")
        except urllib.error.HTTPError:
            return set()
        v = obj["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
        return set((v.get("properties") or {}).keys())

    def odag(self, name):
        return self.get(f"{self.ODAGS}/{name}")

    def status(self, name):
        try:
            return self.odag(name).get("status") or {}
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return {}
            raise

    def odags(self, selector=f"{EXPERIMENT_LABEL}={EXPERIMENT}"):
        return self.get(f"{self.ODAGS}?labelSelector={urllib.parse.quote(selector)}")["items"]

    def delete_odag(self, name, wait_s=120):
        try:
            self.req("DELETE", f"{self.ODAGS}/{name}")
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
        t0 = time.time()
        while time.time() - t0 < wait_s:
            if not self.exists(f"{self.ODAGS}/{name}"):
                return True
            time.sleep(0.2)
        return False


def task_phase(st, task):
    for t in st.get("tasks") or []:
        if t.get("name") == task:
            return t.get("phase", "")
    return ""


# ------------------------------------------------------------ deployments
def container_info(obj, name=None):
    """image, env (plain values; valueFrom shown as such) and args of a
    Deployment's or DaemonSet's container."""
    cs = obj["spec"]["template"]["spec"]["containers"]
    c = next((c for c in cs if c["name"] == name), cs[0]) if name else cs[0]
    env = {e["name"]: e.get("value", "<valueFrom>") for e in c.get("env") or []}
    return {"image": c.get("image"), "env": env, "args": c.get("args") or [], "command": c.get("command") or []}


def wayline_deployment():
    """What runs the Wayline side: controller, sidecar and agent images and
    settings (recorded in every suite manifest)."""
    out = {}
    try:
        dep = kubectl_json(["get", "deploy", "odag-controller"])
        out["controller"] = container_info(dep, "odag-controller")
        names = [c["name"] for c in dep["spec"]["template"]["spec"]["containers"]]
        if "saga-sidecar" in names:
            out["sagaSidecar"] = container_info(dep, "saga-sidecar")
    except RuntimeError as e:
        out["controllerError"] = str(e)
    try:
        out["dataAgent"] = container_info(kubectl_json(["get", "ds", "data-agent"]))
    except RuntimeError as e:
        out["dataAgentError"] = str(e)
    try:
        ds = kubectl_json(["get", "ds", "-l", "wl.io/runner"])["items"]
    except RuntimeError:
        ds = []
    # Runner DaemonSets carry wl.io/runner on their pod template, not always
    # on themselves: list all and keep those whose pods are runners.
    if not ds:
        try:
            ds = [d for d in kubectl_json(["get", "ds"])["items"]
                  if "wl.io/runner" in (d["spec"]["template"]["metadata"].get("labels") or {})]
        except RuntimeError:
            ds = []
    out["runners"] = {d["metadata"]["name"]: {
        "runner": d["spec"]["template"]["metadata"]["labels"].get("wl.io/runner"),
        "image": container_info(d)["image"]} for d in ds}
    return out


def tag_of(image):
    return image.rsplit(":", 1)[1] if image and ":" in image.rsplit("/", 1)[-1] else "latest"


# ----------------------------------------------------------------- registry
_ACCEPT = ", ".join([
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
])
_PLATFORMS = {}


def _reg_get(url, accept=_ACCEPT):
    r = urllib.request.Request(url, headers={"Accept": accept})
    with urllib.request.urlopen(r, timeout=10) as resp:
        return json.loads(resp.read())


def image_archs(image):
    """The CPU architectures an image in our registry provides ({'amd64',
    'arm64'}), from its index or its single manifest's config. Empty set if
    the image is missing; images outside REG are assumed multi-arch
    (upstream images used by E18 are)."""
    if image in _PLATFORMS:
        return _PLATFORMS[image]
    if not image.startswith(REG + "/"):
        return {"amd64", "arm64"}
    repo_tag = image[len(REG) + 1:]
    repo, _, tag = repo_tag.rpartition(":") if ":" in repo_tag else (repo_tag, "", "latest")
    base = f"http://{REG}/v2/{repo}"
    archs = set()
    try:
        m = _reg_get(f"{base}/manifests/{tag}")
        if "manifests" in m:
            archs = {x["platform"]["architecture"] for x in m["manifests"]
                     if x.get("platform", {}).get("architecture") not in (None, "unknown")}
        else:
            cfg = _reg_get(f"{base}/blobs/{m['config']['digest']}", accept="*/*")
            archs = {cfg.get("architecture")} - {None}
    except Exception:  # noqa: BLE001 - missing image or registry down: no archs
        archs = set()
    _PLATFORMS[image] = archs
    return archs

