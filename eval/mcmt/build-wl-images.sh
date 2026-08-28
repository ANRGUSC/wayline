#!/usr/bin/env bash
# Build + push the 6 wl-native MCMT workload images from the Wayline repo root.
set -uo pipefail
# Build from the tree that actually holds the current SDK and the ported
# wrappers. /home/anrg/wayline is a stale checkout: its SDK has no
# two-argument send() and its task scripts still emit unnamed outputs, so
# building there produced images that installed objects under the bare
# task name and wedged every consumer waiting on "producer.object".
BUILD_ROOT=${BUILD_ROOT:-/home/anrg/wayline-build-vertex}
cd "$BUILD_ROOT" || { echo "no build root at $BUILD_ROOT"; exit 1; }
grep -q "_payload" sdk/python/wl/api.py || {
  echo "STOP: $BUILD_ROOT has an SDK without two-argument send()"; exit 1; }
test -f eval/mcmt/lib/wlobj.py || {
  echo "STOP: $BUILD_ROOT is missing eval/mcmt/lib/wlobj.py"; exit 1; }
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
