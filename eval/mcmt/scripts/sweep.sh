#!/usr/bin/env bash
#
# Run the full mcmt sweep matrix: { N=2,4,8 } × { D=30,60,120 }
# × REPS reps per cell. Wraps scripts/run.sh.
#
#   REPS=10 ./sweep.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPS="${REPS:-10}"
CAM_LIST="${CAM_LIST:-2 4 8}"
DUR_LIST="${DUR_LIST:-30 60 120}"

for n in $CAM_LIST; do
    for d in $DUR_LIST; do
        echo "############################################"
        echo "# cell N=$n D=$d REPS=$REPS"
        echo "############################################"
        "$HERE/run.sh" "$n" "$d" "$REPS"
    done
done
