#!/usr/bin/env python3
"""Record what the clips actually are, into the frozen manifest.

The condition is FULL-SOURCE PNG: each clip is the entire source camera
video, not a fixed-duration slice. `d120-png` survives only as a legacy
internal identifier for the historical dataset; it is not a duration and
must not be presented as one.

For every clip this records measured duration, frame count, byte size,
and SHA-256, then verifies the copy staged on each sensor node is
byte-identical to the source on the gateway. Staging is a plain copy, so
a silent truncation or a stale leftover would otherwise be invisible.

Writes <frozen-dir>/clip-manifest.json.

Usage: e6_clip_manifest.py <frozen-dir> [clip_tag]
"""
import hashlib
import json
import os
import subprocess
import sys
import time

NS = "wl-system"
GW = "anrg-9"
SENSORS = {1: "anrg-1", 2: "anrg-3", 3: "anrg-4", 4: "anrg-5"}
SRC_ROOT = "/var/lib/wl-workloads/aicity-source"
STAGED_ROOT = "/var/lib/wl-workloads/aicity"
FROZEN = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser("~/E6-frozen")
CLIP = sys.argv[2] if len(sys.argv) > 2 else "clip_120s.mp4"


def sh(cmd, timeout=600):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout)


def probe_pod(node, host_path, mount, script, image):
    pod = f"e6-cm-{node}-{int(time.time()) % 100000}"
    spec = {"spec": {"nodeName": node, "restartPolicy": "Never",
            "containers": [{"name": "c", "image": image,
                            "command": ["sh", "-c", script],
                            "volumeMounts": [{"name": "v",
                                              "mountPath": mount}]}],
            "volumes": [{"name": "v", "hostPath": {"path": host_path,
                                                   "type": "DirectoryOrCreate"}}]}}
    sh(f"kubectl -n {NS} delete pod {pod} --ignore-not-found >/dev/null 2>&1")
    sh(f"kubectl run {pod} -n {NS} --restart=Never --image={image} "
       f"--overrides='{json.dumps(spec)}' >/dev/null 2>&1")
    out = ""
    for _ in range(120):
        ph = sh(f"kubectl -n {NS} get pod {pod} -o jsonpath='{{.status.phase}}'"
                ).stdout.strip()
        if ph in ("Succeeded", "Failed"):
            out = sh(f"kubectl -n {NS} logs {pod}").stdout
            break
        time.sleep(3)
    sh(f"kubectl -n {NS} delete pod {pod} --ignore-not-found >/dev/null 2>&1")
    return out


def main():
    os.makedirs(FROZEN, exist_ok=True)
    # Duration + frame count + size + digest, measured on the gateway copy.
    script = (
        'for i in 1 2 3 4; do f=/src/cam-$i/' + CLIP + '; '
        '[ -f "$f" ] || { echo "cam-$i MISSING"; continue; }; '
        'd=$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$f"); '
        'n=$(ffprobe -v error -count_frames -select_streams v:0 '
        '-show_entries stream=nb_read_frames -of csv=p=0 "$f"); '
        's=$(stat -c %s "$f"); h=$(sha256sum "$f" | cut -d" " -f1); '
        'echo "cam-$i $d $n $s $h"; done')
    raw = probe_pod(GW, SRC_ROOT, "/src", script, "linuxserver/ffmpeg:latest")

    clips = {}
    for line in raw.splitlines():
        f = line.split()
        if len(f) == 5 and f[0].startswith("cam-"):
            clips[f[0]] = {"duration_s": float(f[1]),
                           "frames": int(f[2]) if f[2].isdigit() else None,
                           "bytes": int(f[3]), "sha256": f[4]}
        elif "MISSING" in line:
            clips[f[0]] = {"error": "missing on gateway"}

    if not clips:
        raise SystemExit(f"STOP: no clips probed. Raw output:\n{raw}")

    # Verify each sensor node's staged copy matches the source digest.
    for idx, node in SENSORS.items():
        cam = f"cam-{idx}"
        # Emit the raw sha256sum line and parse here: escaping a `cut`
        # pipeline through JSON into the pod spec produced an unparseable
        # result and flagged correct copies as mismatched.
        script = ("f=/ds/" + cam + "/" + CLIP + "; "
                  "if [ -f $f ]; then sha256sum $f; else echo MISSING; fi")
        out = probe_pod(node, STAGED_ROOT, "/ds", script, "alpine")
        rec = clips.setdefault(cam, {})
        rec["staged_node"] = node
        tok = out.split()
        rec["staged_sha256"] = tok[0] if tok and tok[0] != "MISSING" else None
        rec["staged_matches_source"] = (
            rec["staged_sha256"] is not None
            and rec["staged_sha256"] == rec.get("sha256"))

    durs = [c["duration_s"] for c in clips.values() if "duration_s" in c]
    manifest = {
        "condition": "full-source-png",
        "legacy_identifier": "n4-d120-png",
        "note": ("Each clip is the ENTIRE source camera video. The legacy "
                 "identifier is not a duration and must not be presented as "
                 "one. Footage is never looped or duplicated."),
        "clip_file": CLIP,
        "cameras": clips,
        "duration_span_s": [min(durs), max(durs)] if durs else None,
        "total_bytes": sum(c.get("bytes", 0) for c in clips.values()),
    }
    path = os.path.join(FROZEN, "clip-manifest.json")
    json.dump(manifest, open(path, "w"), indent=2, sort_keys=True)

    print(f"clip manifest -> {path}")
    for cam in sorted(clips):
        c = clips[cam]
        if "duration_s" not in c:
            print(f"  {cam}: {c}")
            continue
        print(f"  {cam}: {c['duration_s']:6.2f}s  frames={c['frames']}  "
              f"{c['bytes']/1e6:6.1f}MB  sha={c['sha256'][:12]}  "
              f"staged@{c.get('staged_node')} "
              f"{'OK' if c.get('staged_matches_source') else 'MISMATCH'}")
    bad = [c for c in clips.values() if not c.get("staged_matches_source")]
    if bad:
        raise SystemExit(f"STOP: {len(bad)} clip(s) not staged identically")
    print(f"  span {min(durs):.1f}-{max(durs):.1f}s; all staged copies match")


if __name__ == "__main__":
    main()
