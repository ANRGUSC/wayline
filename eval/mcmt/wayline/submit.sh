#!/usr/bin/env bash
#
# Render + apply + run a Wayline mcmt cell.
#
#   ./submit.sh <cameras> <duration_s> <scheduler>
#
# Examples:
#   ./submit.sh 4 60 heft
#   ./submit.sh 2 30 random
#
set -euo pipefail

CAM="${1:-4}"
DUR="${2:-60}"
SCHED="${3:-heft}"

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
NAME="vemcmt-n${CAM}-d${DUR}-${SCHED}"

# Render and apply the template.
python3 "$HERE/render.py" --cameras "$CAM" --duration "$DUR" --scheduler "$SCHED" \
    --name "$NAME" -o "/tmp/${NAME}.yml"
kubectl apply -f "/tmp/${NAME}.yml"

# Trigger a run.
"$REPO/bin/wayline" odag run "$NAME" -n wl-system
