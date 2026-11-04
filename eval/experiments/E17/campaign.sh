#!/bin/sh
# E17 campaign: one driver per lane, all claiming scenarios from one queue.
#   campaign.sh NAME REPEAT [driver args...]     (sudo password: stdin's first line)
# Scenarios are queued from scenarios/ (or $SCENARIOS), interleaved so every
# lane mixes structures; results go to results-NAME/, logs to logs-NAME/.
cd "$(dirname "$0")"
NAME=$1; REPEAT=$2; shift 2
read -r P
LANES=${LANES:-"rpi1,rpi2,rpi3,rpi4,rpi5 rpi6,rpi7,rpi8,rpi9,rpi10 rpi11,rpi12,rpi13,rpi14,rpi15 rpi16,rpi17,rpi18,rpi19,rpi20 rpi21,rpi22,rpi23,rpi24,rpi25 rpi26,rpi27,rpi28,rpi29,rpi30 rpi31,rpi32,rpi33,rpi34,rpi35 rpi36,rpi37,rpi38,rpi39,rpi40 rpi41,rpi42,rpi43,rpi44,rpi45 rpi46,rpi47,rpi48,rpi49,rpi50 anrg-1,anrg-3,anrg-4,anrg-5,anrg-6"}
Q=queue-$NAME; mkdir -p $Q logs-$NAME results-$NAME
if [ -z "$(ls $Q 2>/dev/null | grep json)" ] && [ ! -d $Q/claimed ]; then
  python3 - "${SCENARIOS:-scenarios}" $Q <<'PY'
import os, random, shutil, sys
src, q = sys.argv[1], sys.argv[2]
fs = sorted(f for f in os.listdir(src) if f.endswith(".json"))
random.Random(17).shuffle(fs)
for k, f in enumerate(fs):
    shutil.copy(os.path.join(src, f), os.path.join(q, f"{k:03d}-{f}"))
print(len(fs), "scenarios queued")
PY
fi
for L in $LANES; do
  echo "$P" | nohup ./run.sh --lane $L --queue $Q --out results-$NAME --repeat $REPEAT "$@" > logs-$NAME/${L%%,*}.log 2>&1 &
done
echo "launched $(echo $LANES | wc -w) lanes"
