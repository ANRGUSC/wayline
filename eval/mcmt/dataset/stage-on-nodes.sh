#!/usr/bin/env bash
#
# Copy sliced clips to each sensor node's hostPath so Wayline and Argo task
# pods can mount them read-only without runtime download.
#
# Sensor-node mapping is whatever wayline/render.py and argo/render.py use:
#   cam-1 → anrg-1
#   cam-2 → anrg-3
#   cam-3 → anrg-4
#   cam-4 → anrg-5
#
# Destination on each node: /var/lib/wl-workloads/aicity/cam-<i>/clip_<d>s.mp4
#
# Requires passwordless ssh + sudo to each anrg-* node.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SRC="$HERE/clips"
DEST_ROOT="/var/lib/wl-workloads/aicity"

if [[ ! -d "$SRC" ]]; then
    echo "ERROR: clips not prepared yet. Run ./prepare.sh first." >&2
    exit 2
fi

# (camera_label, sensor_node) pairs. Wrap-around for N>4 cameras.
declare -A NODE_FOR_CAM=(
    [cam-1]=anrg-1
    [cam-2]=anrg-3
    [cam-3]=anrg-4
    [cam-4]=anrg-5
    [cam-5]=anrg-1
    [cam-6]=anrg-3
    [cam-7]=anrg-4
    [cam-8]=anrg-5
)

for cam_dir in "$SRC"/cam-*; do
    cam=$(basename "$cam_dir")
    node="${NODE_FOR_CAM[$cam]:-}"
    if [[ -z "$node" ]]; then
        echo "  skip $cam (no node mapping)"
        continue
    fi
    echo "==> $cam → $node"
    ssh "$node" "sudo mkdir -p $DEST_ROOT/$cam && sudo chmod 755 $DEST_ROOT/$cam"
    # rsync via stdin tar so we don't need sudo on the destination's rsync.
    tar -C "$cam_dir" -cf - . | ssh "$node" "sudo tar -C $DEST_ROOT/$cam -xf -"
    ssh "$node" "ls -lh $DEST_ROOT/$cam"
done

echo
echo "Done. Sanity check: each pod's decode task can read from /dataset/cam-<i>/clip_<d>s.mp4"
