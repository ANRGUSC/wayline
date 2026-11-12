#!/bin/sh
# Copy E17, E18 and E19 (E19 imports both) to eclipse:~/testbed/wl-e19.
set -eu
ROOT=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
cd "$ROOT"
rsync -az --relative --exclude __pycache__ --exclude 'results*' eval/experiments/E17 eval/experiments/E18 \
  eval/experiments/E19 eval/experiments/E21 examples/gdtm examples/focal-mod eclipse:testbed/wl-e19/
echo "synced E17, E18, E19 to eclipse:testbed/wl-e19"
