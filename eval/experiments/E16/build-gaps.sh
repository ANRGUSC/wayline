#!/usr/bin/env bash
# Images for the E16 gap cells, built on anrg-6 through a BuildKit pod
# (ray/buildkit.yml; anrg-2's disk cannot hold these bases). Run on anrg-2
# from a directory laid out like the repo root (~/wl-e16).
set -euo pipefail
export KUBECONFIG=$HOME/.kube/config
REG=192.168.1.163:5000
cd "$(dirname "$0")/../../.."
kubectl apply -f eval/experiments/E16/ray/buildkit.yml >/dev/null
kubectl -n wl-system wait --for=condition=Ready pod/e16-buildkit --timeout=300s >/dev/null
IP=$(kubectl -n wl-system get pod e16-buildkit -o jsonpath='{.status.podIP}')
docker buildx rm e16bk >/dev/null 2>&1 || true
docker buildx create --name e16bk --driver remote tcp://$IP:1234 >/dev/null
build() {  # dockerfile tag [build-arg]
  docker buildx build --builder e16bk --progress quiet -f "$1" -t "$REG/$2" ${3:+--build-arg "$3"} --push . && echo "built $2"
}
build examples/focal-mod/Dockerfile.ray wl-focal-ray:latest
build eval/experiments/E16/argo/Dockerfile.stage wl-gdtm-argo:latest BASE=$REG/wl-gdtm:latest
build eval/experiments/E16/argo/Dockerfile.stage wl-gdtm-model-argo:latest BASE=$REG/wl-gdtm-model:latest
build eval/experiments/E16/argo/Dockerfile.stage wl-focal-argo:latest BASE=$REG/wl-focal:latest
docker buildx rm e16bk >/dev/null 2>&1 || true
kubectl -n wl-system delete pod e16-buildkit cm/e16-buildkitd --wait=false >/dev/null 2>&1 || true
kubectl -n wl-system delete cm e16-buildkitd >/dev/null 2>&1 || true
