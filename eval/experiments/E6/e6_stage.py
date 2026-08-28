#!/usr/bin/env python3
"""Stage AI City clips from the gateway onto each sensor node.

The original stager shells between nodes with sshpass; inter-node SSH is
not usable on this cluster (permission denied), and depending on a
hardcoded password is fragile besides. This moves the bytes through the
Kubernetes API instead: a reader pod on the gateway, a writer pod per
sensor node, and `kubectl cp` between them via anrg-2.

Each sensor node receives only its own camera, so the total transfer is
small (~76 MB for the four full-source clips).

Verifies the destination digest against the source and fails loudly on
mismatch: a truncated copy would otherwise surface much later as a
decode error inside a run.

Usage: e6_stage.py [clip_file]
"""
import json
import subprocess
import sys
import time

NS = "wl-system"
GW = "anrg-9"
SRC_ROOT = "/var/lib/wl-workloads/aicity-source"
DST_ROOT = "/var/lib/wl-workloads/aicity"
SENSORS = {"cam-1": "anrg-1", "cam-2": "anrg-3",
           "cam-3": "anrg-4", "cam-4": "anrg-5"}
CLIP = sys.argv[1] if len(sys.argv) > 1 else "clip_120s.mp4"
TMP = "/tmp/e6-stage"


def sh(cmd, timeout=900):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout)


def start_pod(name, node, host_path, mount):
    spec = {"spec": {"nodeName": node, "restartPolicy": "Never",
            "containers": [{"name": "c", "image": "alpine",
                            "command": ["sh", "-c", "sleep 3600"],
                            "volumeMounts": [{"name": "v",
                                              "mountPath": mount}]}],
            "volumes": [{"name": "v",
                         "hostPath": {"path": host_path,
                                      "type": "DirectoryOrCreate"}}]}}
    sh(f"kubectl -n {NS} delete pod {name} --ignore-not-found --wait=false "
       f">/dev/null 2>&1")
    time.sleep(2)
    sh(f"kubectl run {name} -n {NS} --restart=Never --image=alpine "
       f"--overrides='{json.dumps(spec)}' >/dev/null 2>&1")
    for _ in range(90):
        if sh(f"kubectl -n {NS} get pod {name} "
              f"-o jsonpath='{{.status.phase}}'").stdout.strip() == "Running":
            return True
        time.sleep(2)
    return False


def digest(pod, path):
    return sh(f"kubectl -n {NS} exec {pod} -- sha256sum {path} 2>/dev/null"
              ).stdout.split()[:1]


def main():
    sh(f"mkdir -p {TMP}")
    print(f"staging {CLIP}")
    if not start_pod("e6-src", GW, SRC_ROOT, "/src"):
        raise SystemExit("STOP: could not start reader pod on the gateway")

    failures = []
    try:
        for cam, node in SENSORS.items():
            wpod = f"e6-dst-{node}"
            if not start_pod(wpod, node, DST_ROOT, "/ds"):
                failures.append(f"{cam}: writer pod on {node} did not start")
                continue
            src = f"/src/{cam}/{CLIP}"
            sd = digest("e6-src", src)
            if not sd:
                failures.append(f"{cam}: missing on gateway ({src})")
                sh(f"kubectl -n {NS} delete pod {wpod} --wait=false "
                   f">/dev/null 2>&1")
                continue
            local = f"{TMP}/{cam}-{CLIP}"
            r = sh(f"kubectl -n {NS} cp e6-src:{src} {local}")
            if r.returncode != 0:
                failures.append(f"{cam}: pull failed")
            sh(f"kubectl -n {NS} exec {wpod} -- mkdir -p /ds/{cam}")
            r = sh(f"kubectl -n {NS} cp {local} {wpod}:/ds/{cam}/{CLIP}")
            dd = digest(wpod, f"/ds/{cam}/{CLIP}")
            ok = bool(sd) and sd == dd
            print(f"  {cam} -> {node}: "
                  f"{'OK' if ok else 'MISMATCH'} sha={(dd or ['?'])[0][:12]}")
            if not ok:
                failures.append(f"{cam}: digest mismatch after copy")
            sh(f"rm -f {local}")
            sh(f"kubectl -n {NS} delete pod {wpod} --wait=false >/dev/null 2>&1")
    finally:
        sh(f"kubectl -n {NS} delete pod e6-src --wait=false >/dev/null 2>&1")

    if failures:
        for f in failures:
            print(f"  FAIL {f}")
        raise SystemExit(f"STOP: staging failed for {len(failures)} camera(s)")
    print("all clips staged and digest-verified")


if __name__ == "__main__":
    main()
