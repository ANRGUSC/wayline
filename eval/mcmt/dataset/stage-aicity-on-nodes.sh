#!/usr/bin/env bash
#
# Distribute AI City clips from anrg-9's hostPath
# (/var/lib/wl-workloads/aicity-source) to each sensor node's hostPath
# (/var/lib/wl-workloads/aicity/cam-<i>). Uses ssh password "anrg" on
# every node (preconfigured on this testbed).
#
# Two-phase per sensor:
#   1. Stream tar via ssh→ssh (root reads on anrg-9, anrg user writes
#      to /tmp on sensor — /tmp is world-writable; no sudo needed for
#      the data transfer itself).
#   2. ssh + sudo -S on sensor: unpack /tmp/aicity-<cam>.tar into
#      /var/lib/wl-workloads/aicity, then rm the temp.
set -euo pipefail

PASS=anrg
SRC_NODE=anrg-9
SRC_DIR=/var/lib/wl-workloads/aicity-source

declare -a CAMS=(cam-1 cam-2 cam-3 cam-4)
declare -a SENSORS=(anrg-1 anrg-3 anrg-4 anrg-5)

SSH_FLAGS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

for i in "${!CAMS[@]}"; do
    cam="${CAMS[$i]}"
    sensor="${SENSORS[$i]}"
    tar_remote=/tmp/aicity-${cam}.tar
    echo "==> $cam (anrg-9 source → $sensor)"

    # Phase 1: stream tar from anrg-9 (sudo'd read) directly into a
    # /tmp file on the sensor (no sudo needed; world-writable /tmp).
    sshpass -p "$PASS" ssh $SSH_FLAGS "$SRC_NODE" \
        "echo $PASS | sudo -S tar -C $SRC_DIR -cf - $cam 2>/dev/null" \
      | sshpass -p "$PASS" ssh $SSH_FLAGS "$sensor" \
        "cat > $tar_remote"

    # Phase 2: on the sensor, sudo unpack + remove temp + verify.
    sshpass -p "$PASS" ssh $SSH_FLAGS "$sensor" \
        "echo $PASS | sudo -S sh -c '
            rm -rf /var/lib/wl-workloads/aicity/$cam &&
            mkdir -p /var/lib/wl-workloads/aicity &&
            tar -C /var/lib/wl-workloads/aicity -xf $tar_remote &&
            rm -f $tar_remote &&
            ls -lh /var/lib/wl-workloads/aicity/$cam
        '"
done

echo
echo "Done. Each sensor serves AI City clips at /var/lib/wl-workloads/aicity/cam-<i>/clip_<d>s.mp4."
