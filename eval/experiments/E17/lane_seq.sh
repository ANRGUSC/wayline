#!/bin/sh
# lane_seq.sh LANE [--after QUEUE] STEP [STEP ...]   (sudo password: stdin's first line)
#   STEP = NAME:REPEAT:policy,policy,...
# Reads the password in the foreground, then runs in the background: waits for
# the lane's driver on queue QUEUE (if given) to exit, then takes the lane
# through the given campaign queues in order. (A background job's stdin is
# /dev/null, so the password must be read before backgrounding.)
cd "$(dirname "$0")"
read -r P
L=$1; shift
AFTER=""
if [ "$1" = "--after" ]; then AFTER=$2; shift 2; fi
(
  if [ -n "$AFTER" ]; then
    while pgrep -f "[d]river.py --lane $L --queue queue-$AFTER " >/dev/null; do sleep 20; done
  fi
  for step in "$@"; do
    name=${step%%:*}; rest=${step#*:}; rep=${rest%%:*}; pols=$(echo ${rest#*:} | tr , " ")
    mkdir -p logs-$name results-$name
    echo "$P" | ./run.sh --lane $L --queue queue-$name --out results-$name --repeat $rep --policies $pols >> logs-$name/${L%%,*}.log 2>&1
  done
) > /dev/null 2>&1 < /dev/null &
echo "lane $L scheduled${AFTER:+ after queue-$AFTER}: $*"
