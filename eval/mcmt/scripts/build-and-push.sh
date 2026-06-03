#!/usr/bin/env bash
#
# Build + push all six mcmt stage images to the local registry.
#
# Prerequisites:
#   - ../models/fetch.sh has produced the OpenVINO IR files (yolov8n.{xml,bin},
#     osnet_x0_25.{xml,bin}) under ../models/. These are baked into the
#     detect_embed image.
#
# Build context is the REPO ROOT (../../..) so we can COPY the wl_sdk and
# the eval directory in one shot.
set -euo pipefail

REG="${REG:-192.168.1.163:5000}"
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"

STAGES=(decode preprocess detect_embed track cross_camera_match report)

# Models are baked into detect_embed — fail fast if they're missing.
for f in yolov8n.xml yolov8n.bin osnet_x0_25.xml osnet_x0_25.bin; do
    if [[ ! -f "$REPO/eval/mcmt/models/$f" ]]; then
        echo "ERROR: missing $f — run ../models/fetch.sh first" >&2
        exit 2
    fi
done

echo "=== building 6 stage images in parallel ==="
PIDS=""
for stage in "${STAGES[@]}"; do
    tag="${REG}/vemcmt-${stage//_/-}:latest"
    log="/tmp/vemcmt-build-${stage}.log"
    (cd "$REPO" && docker build --no-cache \
        -f "eval/mcmt/images/${stage}/Dockerfile" \
        -t "$tag" .) > "$log" 2>&1 &
    PIDS="$PIDS $!"
done
FAIL=0
for pid in $PIDS; do wait "$pid" || FAIL=1; done

for stage in "${STAGES[@]}"; do
    log="/tmp/vemcmt-build-${stage}.log"
    if tail -3 "$log" | grep -q "ERROR\|error:"; then
        echo "FAIL: $stage — see $log"
        FAIL=1
    else
        echo "  ok: $stage"
    fi
done
[[ $FAIL -eq 0 ]] || { echo "one or more builds failed"; exit 3; }

echo
echo "=== pushing 6 stage images in parallel ==="
PIDS=""
for stage in "${STAGES[@]}"; do
    tag="${REG}/vemcmt-${stage//_/-}:latest"
    docker push "$tag" > "/tmp/vemcmt-push-${stage}.log" 2>&1 &
    PIDS="$PIDS $!"
done
FAIL=0
for pid in $PIDS; do wait "$pid" || FAIL=1; done
for stage in "${STAGES[@]}"; do
    tag="${REG}/vemcmt-${stage//_/-}:latest"
    if tail -1 "/tmp/vemcmt-push-${stage}.log" | grep -q digest; then
        echo "  ok: $tag"
    else
        echo "FAIL: $tag"
        FAIL=1
    fi
done
[[ $FAIL -eq 0 ]] || exit 4

echo
echo "Done. Six images live at $REG/vemcmt-*:latest."
