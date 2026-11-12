#!/bin/sh
# Copy what E18 needs to eclipse, keeping the repo's layout (the harness
# imports examples/gdtm/gen_gdtm.py and examples/focal-mod/gen_focal.py from
# it, and build.sh builds from it), and record the commit it came from.
# Run from anywhere inside the repo on the Mac:   eval/experiments/E18/sync.sh [host] [dir]
set -eu
HOST=${1:-eclipse}
DIR=${2:-testbed/wl-e18}
ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
cd "$ROOT"
commit=$(git rev-parse HEAD)
dirty=$(git status --porcelain -- eval/experiments/E18 | wc -l | tr -d ' ')
printf '{"commit": "%s", "e18Dirty": %s, "syncedAt": "%s"}\n' "$commit" \
  "$([ "$dirty" = 0 ] && echo false || echo true)" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > /tmp/e18-GIT_COMMIT.json
rsync -a --relative --exclude __pycache__ \
  eval/experiments/E18 eval/experiments/E22 eval/experiments/E11/tasks eval/experiments/E10/ray eval/experiments/E10/e10_ray.py \
  eval/experiments/E16/ray/e16_ray_focal.py eval/experiments/E16/ray/e16_ray_gdtm.py \
  examples/gdtm examples/focal-mod examples/iot-mission-snapshot-odag sdk/python/wl \
  "$HOST:$DIR/"
rsync -a /tmp/e18-GIT_COMMIT.json "$HOST:$DIR/GIT_COMMIT.json"
echo "synced $commit (E18 dirty files: $dirty) to $HOST:$DIR"
