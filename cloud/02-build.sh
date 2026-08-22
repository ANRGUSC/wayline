#!/usr/bin/env bash
# Run on the server node from the wayline repo root: cloud/02-build.sh
# Builds every image the E9 campaign needs and pushes to the local
# registry. Native amd64, same procedure as anrg-2.
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
SERVER_IP=${SERVER_IP:-$(hostname -I | awk '{print $1}')}
. cloud/env.sh

build(){ # <dockerfile> <image>
  echo "[$(date +%T)] building $2 ..."
  if sudo docker build -q -f "$1" -t "$REG/$2:latest" . >/tmp/wl-$2.log 2>&1; then
    sudo docker push -q "$REG/$2:latest" >/dev/null && echo "[$(date +%T)] $2 OK"
  else echo "[$(date +%T)] $2 FAILED"; tail -5 /tmp/wl-$2.log; return 1; fi
}

# Runtime images.
build cmd/data-agent/Dockerfile       wl-data-agent
build cmd/odag-controller/Dockerfile  wl-odag-controller
build saga-sidecar/Dockerfile         wl-saga-sidecar
# Synthetic task images (iot, hetero, wpf).
build eval/synthetic-dags/scheduler/iot/tasks/capture/Dockerfile      wl-iot-capture
build eval/synthetic-dags/scheduler/iot/tasks/preprocess/Dockerfile   wl-iot-preprocess
build eval/synthetic-dags/scheduler/iot/tasks/infer/Dockerfile        wl-iot-infer
build eval/synthetic-dags/scheduler/iot/tasks/fuse/Dockerfile         wl-iot-fuse
build eval/synthetic-dags/scheduler/iot/tasks/report/Dockerfile       wl-iot-report
build eval/synthetic-dags/scheduler/hetero-compute/tasks/Dockerfile   wl-hetero-compute-task
build eval/synthetic-dags/scheduler/wide-pipeline-flex/tasks/Dockerfile wl-multi-odag-task
echo "[$(date +%T)] ALL BUILDS DONE"
