#!/bin/sh
# The control-plane experiment: E19's plans on a lane whose control traffic is shaped like its radio data links
# (ctlshape.py), then the shaping removed, also when the run fails. The sudo password is stdin's first line.
#   printf '%s\n' "$PW" | ./run_ctlshaped.sh <lane> <out> [e19 args...]
set -u
IFS= read -r PW
lane=$1; out=$2; shift 2
cd "$(dirname "$0")"
PY="$HOME/testbed/venv/bin/python"
cleanup() { printf '%s\n' "$PW" | "$PY" ctlshape.py remove --lane "$lane"; }
trap cleanup EXIT INT TERM
printf '%s\n' "$PW" | "$PY" ctlshape.py apply --lane "$lane" || exit 1
printf '%s\n' "$PW" | ./run.sh --lane "$lane" --out "$out" "$@"
