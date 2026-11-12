#!/bin/sh
# Build the controller on eclipse from this checkout and roll it out (the
# controller and the policy process share the image).
#   eval/tools/deploy_ctl.sh <tag>
set -eu
TAG=$1
ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
# Only what the controller build needs (the link to eclipse can be slow).
cd "$ROOT"
rsync -az --relative --delete --exclude '/cmd/odag-controller/odag-controller' --exclude '/cmd/data-agent/data-agent' --exclude '/cmd/*/node_modules' --exclude '*.pprof' \
  go.mod go.sum cmd pkg api eclipse:testbed/wl-src/
ssh eclipse "set -e; cd ~/testbed/wl-src && CGO_ENABLED=0 ~/go-sdk/go/bin/go build -o ~/testbed/build/ctl/odag-controller ./cmd/odag-controller && \
  cd ~/testbed/build && tar --owner=0 --group=0 -C ctl -cf ctl.tar odag-controller && \
  C=~/testbed/bin/crane; R=10.10.0.1:5000; \
  \$C append --insecure -b alpine:3.20 --platform linux/amd64 -f ctl.tar -t \$R/wl-odag-controller:$TAG >/dev/null 2>&1 && \
  \$C mutate --insecure \$R/wl-odag-controller:$TAG --entrypoint=/odag-controller >/dev/null 2>&1 && \
  k3s kubectl -n wl-system set image deploy/odag-controller odag-controller=\$R/wl-odag-controller:$TAG >/dev/null && \
  k3s kubectl -n wl-system set image deploy/wl-policy wl-policy=\$R/wl-odag-controller:$TAG >/dev/null && \
  k3s kubectl -n wl-system rollout status deploy/odag-controller --timeout=170s | tail -1 && \
  k3s kubectl -n wl-system rollout status deploy/wl-policy --timeout=120s | tail -1"
echo "deployed wl-odag-controller:$TAG"
