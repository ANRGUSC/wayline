#!/bin/sh
# After process PID exits, resume suites (fill in the cells their latest directory lacks).
# The workers' sudo password is the first line of stdin, handed to each suite on its stdin only.
#   printf '%s\n' "$PW" | ssh eclipse 'sh testbed/wl-e18/eval/experiments/E18/after.sh PID LOG REPS suite...'
set -u
IFS= read -r PW
pid=$1; log=$2; reps=$3; shift 3
cd "$(dirname "$0")"
(
    trap '' HUP
    while kill -0 "$pid" 2>/dev/null; do sleep 60; done
    for s in "$@"; do
        echo "=== after: resume $s ($(date -u +%FT%TZ))"
        printf '%s\n' "$PW" | ./run.sh run --suite "$s" --fence nodes --reps "$reps"
        echo "=== after: $s exited $? ($(date -u +%FT%TZ))"
    done
) > "$log" 2>&1 < /dev/null &
echo "after started: wait for pid $pid, then $*"
