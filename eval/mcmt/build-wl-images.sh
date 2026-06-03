#!/usr/bin/env bash
# Build + push the 6 wl-native MCMT workload images from the Wayline repo root.
set -uo pipefail
cd /home/anrg/wayline
R=192.168.1.163:5000
declare -A M=( [decode]=wl-vemcmt-decode [preprocess]=wl-vemcmt-preprocess [detect_embed]=wl-vemcmt-detect-embed [track]=wl-vemcmt-track [cross_camera_match]=wl-vemcmt-cross-camera-match [report]=wl-vemcmt-report )
for d in decode preprocess detect_embed track cross_camera_match report; do
  echo "[$(date +%T)] building ${M[$d]} ..."
  if docker build -q -f eval/mcmt/images/$d/Dockerfile -t "$R/${M[$d]}:latest" . >/tmp/wlbuild-$d.log 2>&1; then
    docker push -q "$R/${M[$d]}:latest" >/dev/null 2>&1 && echo "[$(date +%T)] ${M[$d]} OK + pushed"
  else
    echo "[$(date +%T)] ${M[$d]} BUILD FAILED (see /tmp/wlbuild-$d.log)"; tail -6 /tmp/wlbuild-$d.log
  fi
done
echo "[$(date +%T)] ALL MCMT IMAGE BUILDS DONE"
