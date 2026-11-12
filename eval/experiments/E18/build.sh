#!/usr/bin/env bash
# E18's images. Run from anywhere in the repo copy on eclipse (sync.sh puts it
# at ~/testbed/wl-e18); it pushes to 10.10.0.1:5000 and skips a tag that
# exists (FORCE=1 rebuilds it; never on a tag a running deployment uses).
#
#   eval/experiments/E18/build.sh light   # small Python images, linux/amd64 + linux/arm64, :$TAG
#   eval/experiments/E18/build.sh ray     # wl-ray-baseline:2.58.0-$TAG, amd64 + arm64
#   eval/experiments/E18/build.sh dask    # ghcr.io/dask/dask mirrored (multi-arch upstream)
#   eval/experiments/E18/build.sh focal   # wl-focal:$TAG, amd64 + arm64, pinned to E16's library versions
#   eval/experiments/E18/build.sh heavy   # the missing amd64-only GDTM / FOCAL images (:latest)
#
# Multi-arch without QEMU on eclipse: two BuildKit pods (deploy/buildkit.yml),
# amd64 on eclipse and arm64 natively on a Pi 4, each used through a buildx
# "remote" builder on the pod's IP ($AMD_BUILDER, $ARM_BUILDER). (eclipse's
# Docker cannot run buildx's docker-container driver: its default runtime is
# nvidia, whose binary is missing.) Each architecture is pushed as
# <tag>-amd64 / <tag>-arm64 and the two are joined into the index <tag> with
# crane. Before `light`, `ray`, `focal` or `heavy`:
#   k3s kubectl apply -f eval/experiments/E18/deploy/buildkit.yml
#   k3s kubectl -n default rollout status deploy/e18-buildkit; k3s kubectl -n default rollout status deploy/e18-buildkit-amd64
# and afterwards (drops the build caches):
#   k3s kubectl -n default scale deploy/e18-buildkit deploy/e18-buildkit-amd64 --replicas=0
# While an experiment measures the control plane on eclipse, move the amd64
# builder to an idle UP board first (2026-11-08's wl-focal:mx1 built on anrg-4):
#   k3s kubectl -n default patch deploy e18-buildkit-amd64 -p '{"spec":{"template":{"spec":{"nodeSelector":{"kubernetes.io/hostname":"anrg-4"}}}}}'
#
# The E16-era tags (:latest, wl-ray-baseline:2.58.0) are amd64-only and stay
# as they were; E18_IMAGES=e16 selects them (images.py).
set -euo pipefail
REG=${REG:-10.10.0.1:5000}
TAG=${TAG:-mx1}
WHAT=${1:-light}
CRANE=${CRANE:-$HOME/testbed/bin/crane}
AMD=${AMD_BUILDER:-e18-amd64}
ARM=${ARM_BUILDER:-e18-arm64}
LOGS=${E18_BUILD_LOGS:-$HOME/E18-build-logs}
K=${E18_KUBECTL:-k3s kubectl}
DASK=${DASK:-2026.8.0-py3.12}
cd "$(dirname "$0")/../../.."
mkdir -p "$LOGS"

exists() { "$CRANE" digest --insecure "$1" >/dev/null 2>&1; }

remote_builder() {  # builder platform app-label: a buildx "remote" builder on that BuildKit pod's IP
  local name=$1 plat=$2 app=$3 ip ep
  ip=$($K -n default get pod -l app="$app" --field-selector status.phase=Running \
       -o jsonpath='{.items[0].status.podIP}' 2>/dev/null || true)
  [ -n "$ip" ] || { echo "no running $app pod (k3s kubectl apply -f eval/experiments/E18/deploy/buildkit.yml)" >&2; exit 1; }
  ep=$(docker buildx inspect "$name" 2>/dev/null | awk '/^Endpoint:/ {print $2; exit}' || true)
  if [ "$ep" != "tcp://$ip:1234" ]; then
    docker buildx rm "$name" >/dev/null 2>&1 || true
    docker buildx create --name "$name" --driver remote --platform "$plat" "tcp://$ip:1234" >/dev/null
  fi
}
amd_builder() { remote_builder "$AMD" linux/amd64 e18-buildkit-amd64; }
arm_builder() { remote_builder "$ARM" linux/arm64 e18-buildkit; }

one() {  # builder platform dockerfile context ref [build-arg ...]
  local b=$1 p=$2 f=$3 c=$4 r=$5; shift 5
  local args=(); for a in "$@"; do args+=(--build-arg "$a"); done
  docker buildx build --builder "$b" --platform "$p" --provenance=false --progress plain \
    -f "$f" -t "$r" ${args[@]+"${args[@]}"} --push "$c"
}

multi() {  # dockerfile context repo:tag [build-arg ...]: <tag>-amd64, <tag>-arm64, index <tag>
  local f=$1 c=$2 name=$3 ref=$REG/$3; shift 3
  if exists "$ref" && [ -z "${FORCE:-}" ]; then echo "skip $ref (exists)"; return; fi
  local log=$LOGS/${name//[:\/]/_}
  one "$AMD" linux/amd64 "$f" "$c" "$ref-amd64" "$@" > "$log.amd64.log" 2>&1 & local pa=$!
  one "$ARM" linux/arm64 "$f" "$c" "$ref-arm64" "$@" > "$log.arm64.log" 2>&1 & local pb=$!
  local ok=1
  wait $pa || { echo "FAIL $ref-amd64 (see $log.amd64.log)" >&2; ok=0; }
  wait $pb || { echo "FAIL $ref-arm64 (see $log.arm64.log)" >&2; ok=0; }
  [ $ok = 1 ] || return 1
  "$CRANE" index append --insecure -m "$ref-amd64" -m "$ref-arm64" -t "$ref" >/dev/null
  echo "built $ref: $("$CRANE" manifest --insecure "$ref" | python3 -c \
    'import json,sys; print(" ".join(sorted(m["platform"]["os"]+"/"+m["platform"]["architecture"] for m in json.load(sys.stdin)["manifests"])))')"
}

case "$WHAT" in
light)
  amd_builder; arm_builder
  multi eval/experiments/E11/tasks/Dockerfile . "wl-e11:$TAG"
  multi eval/experiments/E11/tasks/Dockerfile . "wl-runner-e11:$TAG"   # E16: E11's image as the e11 runner
  multi eval/experiments/E18/runner/Dockerfile.iot . "wl-runner-iot:$TAG"
  for t in capture preprocess infer fuse report; do
    multi "examples/iot-mission-snapshot-odag/tasks/$t/Dockerfile" . "wl-iot-$t:$TAG"
  done
  multi eval/experiments/E18/argo/Dockerfile eval/experiments/E18/argo "wl-e16-task:$TAG"
  ;;
ray)
  amd_builder; arm_builder
  multi eval/experiments/E10/ray/Dockerfile eval/experiments/E10 "wl-ray-baseline:2.58.0-$TAG"   # context has e10_ray.py
  ;;
dask)
  # Upstream ghcr.io/dask/dask is multi-arch (amd64, arm64); scheduler and worker are the same image.
  if exists "$REG/dask:$DASK"; then echo "skip $REG/dask:$DASK (exists)"
  else "$CRANE" copy --insecure "ghcr.io/dask/dask:$DASK" "$REG/dask:$DASK" && echo "mirrored $REG/dask:$DASK"; fi
  ;;
focal)
  # E16's wl-focal:latest (amd64): Python 3.12.14, torch 2.14.0+cpu, torchvision 0.29.0+cpu,
  # timm 1.0.30, einops 0.8.2, numpy 2.5.2, pyzmq 27.2.0, PyYAML 6.0.3. Pinned so both
  # architectures match it (the CPU index has the aarch64 wheels of both).
  amd_builder; arm_builder
  multi examples/focal-mod/Dockerfile . "wl-focal:$TAG" \
    "TORCH_PIN=torch==2.14.0 torchvision==0.29.0" \
    "PIP_PIN=timm==1.0.30 einops==0.8.2 pyyaml==6.0.3 numpy==2.5.2 pyzmq==27.2.0"
  ;;
heavy)
  amd_builder
  for spec in "examples/focal-mod/Dockerfile.ray . wl-focal-ray:latest BASE=$REG/wl-focal:latest" \
              "eval/experiments/E18/argo/Dockerfile.stage . wl-gdtm-argo:latest BASE=$REG/wl-gdtm:latest" \
              "eval/experiments/E18/argo/Dockerfile.stage . wl-gdtm-model-argo:latest BASE=$REG/wl-gdtm-model:latest" \
              "eval/experiments/E18/argo/Dockerfile.stage . wl-focal-argo:latest BASE=$REG/wl-focal:latest"; do
    set -- $spec
    if exists "$REG/$3"; then echo "skip $REG/$3 (exists)"; continue; fi
    one "$AMD" linux/amd64 "$1" "$2" "$REG/$3" "$4" && echo "built $REG/$3 (linux/amd64)"
  done
  ;;
*)
  echo "usage: build.sh light|ray|dask|focal|heavy   (TAG=$TAG, FORCE=1 to rebuild an existing tag)" >&2; exit 2 ;;
esac
