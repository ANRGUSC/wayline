#!/usr/bin/env python3
"""Warm-runner DaemonSets for E18's workloads, selected by node label.

One DaemonSet per (runner, node class): pods go to every node with
testbed.lab/class=<class> whose architecture the runner image provides
(kubernetes.io/arch, from the registry), never to a host list. All pods of
a runner carry wl.io/runner=<name>, which is how the controller finds them.
--cpus is the class's cores minus 0.6, as E16's 7.4 on the 8-core UP boards.
Images are workloads.RUNNER_IMAGES (images.py: the multi-arch mx1 set by
default, so e11, iot and focal runners also go to the Pis; E18_IMAGES=e16
for E16's amd64-only tags).

Prints a manifest; nothing is applied:
  python3 deploy/runners.py --runners e11,iot --classes up > runners.json
  k3s kubectl apply -f runners.json
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import kube  # noqa: E402
import workloads  # noqa: E402

CORES = {"up": 8, "rpi4": 4, "rpi3": 4}
RUNNERS = {
    "e11": {"script": "/app/task.py", "preload": "", "env": {}},
    "iot": {"script": "/app/stage.py", "preload": "", "env": {}},
    "gdtm": {"script": "/app/stage.py", "preload": "numpy,cv2,h5py,torch,gdtm_glue,tracker", "env": {}},
    "gdtm-model": {"script": "/app/stage.py", "preload": "numpy,torch,gdtm_model", "env": {"GDTM_PRELOAD": "1"}},
    "focal": {"script": "/app/stage.py", "preload": "torch,numpy,focal_stages", "env": {"FOCAL_PRELOAD": "1"}},
}


def daemonset(runner, cls, archs):
    spec = RUNNERS[runner]
    image = workloads.RUNNER_IMAGES[runner]
    cmd = ["python", "-m", "wl.runner", "--script", spec["script"]]
    if spec["preload"]:
        cmd += ["--preload", spec["preload"]]
    cmd += ["--cpus", f"{CORES[cls] - 0.6:.1f}"]
    env = [{"name": "NODE_NAME", "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}}]
    env += [{"name": k, "value": v} for k, v in spec["env"].items()]
    labels = {"wl.io/runner": runner, "e18.lab/runner-class": cls}
    return {
        "apiVersion": "apps/v1", "kind": "DaemonSet",
        "metadata": {"name": f"wl-runner-{runner}-{cls}", "namespace": kube.NS, "labels": labels},
        "spec": {
            "selector": {"matchLabels": labels},
            "updateStrategy": {"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": 20}},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [{"matchExpressions": [
                            {"key": "testbed.lab/class", "operator": "In", "values": [cls]},
                            {"key": "kubernetes.io/arch", "operator": "In", "values": sorted(archs)}]}]}}},
                    "containers": [{
                        "name": "runner", "image": image, "imagePullPolicy": "Always", "command": cmd, "env": env,
                        "ports": [{"containerPort": 8090, "name": "invoke"}],
                        "readinessProbe": {"httpGet": {"path": "/healthz", "port": 8090}, "periodSeconds": 2},
                        "volumeMounts": [{"name": "wl-outputs", "mountPath": "/data/wl-outputs"},
                                         {"name": "wl-shared", "mountPath": "/shared/wl-outputs"}]}],
                    "volumes": [{"name": "wl-outputs", "hostPath": {"path": "/data/wl-outputs", "type": "DirectoryOrCreate"}},
                                {"name": "wl-shared", "hostPath": {"path": "/shared/wl-outputs", "type": "DirectoryOrCreate"}}],
                }}}}


CLASS_ARCH = {"up": "amd64", "rpi4": "arm64", "rpi3": "arm64"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runners", default="e11,iot", help=f"of {sorted(RUNNERS)}")
    ap.add_argument("--classes", default="up", help="node classes (testbed.lab/class)")
    a = ap.parse_args()
    items = []
    for r in [x for x in a.runners.split(",") if x]:
        archs = kube.image_archs(workloads.RUNNER_IMAGES[r])
        for cls in [x for x in a.classes.split(",") if x]:
            if CLASS_ARCH[cls] not in archs:
                print(f"# skip {r} on {cls}: {workloads.RUNNER_IMAGES[r]} has no {CLASS_ARCH[cls]} build "
                      f"(has {sorted(archs) or 'nothing'})", file=sys.stderr)
                continue
            items.append(daemonset(r, cls, {CLASS_ARCH[cls]}))
    print(json.dumps({"apiVersion": "v1", "kind": "List", "items": items}, indent=1))


if __name__ == "__main__":
    main()
