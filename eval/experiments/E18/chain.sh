#!/bin/sh
# Run several suites one after another on one lane, detached from the ssh
# session that starts them. The workers' sudo password is read once from the
# first line of stdin and handed to each suite on its stdin; it is never put
# in the environment, a file or a command line.
#   printf '%s\n' "$PW" | ssh eclipse 'sh testbed/wl-e18/eval/experiments/E18/chain.sh LOG REPS suite...'
set -u
IFS= read -r PW
log=$1; reps=$2; shift 2
cd "$(dirname "$0")"
(
    trap '' HUP
    for s in "$@"; do
        echo "=== chain: suite $s ($(date -u +%FT%TZ))"
        printf '%s\n' "$PW" | ./run.sh run --suite "$s" --fence nodes --reps "$reps" --new
        echo "=== chain: suite $s exited $? ($(date -u +%FT%TZ))"
    done
) > "$log" 2>&1 < /dev/null &
echo "chain started: $* -> $log"
