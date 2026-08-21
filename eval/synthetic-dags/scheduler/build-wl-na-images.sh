#!/usr/bin/env bash
# Build + push wl-native network-aware synthetic images from the Wayline repo root.
set -uo pipefail
cd /home/anrg/wayline
R=192.168.1.163:5000
build(){ # <dockerfile> <image>
  echo "[$(date +%T)] building $2 ..."
  if docker build -q -f "$1" -t "$R/$2:latest" . >/tmp/wlna-$2.log 2>&1; then
    docker push -q "$R/$2:latest" >/dev/null 2>&1 && echo "[$(date +%T)] $2 OK"
  else echo "[$(date +%T)] $2 FAILED"; tail -5 /tmp/wlna-$2.log; fi
}
build eval/synthetic-dags/scheduler/iot/tasks/capture/Dockerfile     wl-iot-capture
build eval/synthetic-dags/scheduler/iot/tasks/preprocess/Dockerfile  wl-iot-preprocess
build eval/synthetic-dags/scheduler/iot/tasks/infer/Dockerfile       wl-iot-infer
build eval/synthetic-dags/scheduler/iot/tasks/fuse/Dockerfile        wl-iot-fuse
build eval/synthetic-dags/scheduler/iot/tasks/report/Dockerfile      wl-iot-report
build eval/synthetic-dags/scheduler/hetero-compute/tasks/Dockerfile   wl-hetero-compute-task
build eval/synthetic-dags/scheduler/wide-pipeline-flex/tasks/Dockerfile wl-multi-odag-task
echo "[$(date +%T)] ALL NETWORK-AWARE IMAGE BUILDS DONE"
