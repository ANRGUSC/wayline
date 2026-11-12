#!/bin/sh
# Run e19.py on eclipse in E17's paper environment (ncsim, saga); the sudo
# password for the lane's link emulation is stdin's first line.
cd "$(dirname "$0")"
PYTHONHASHSEED=0 PYTHONPATH="$HOME/testbed/milcom/ncsim:$HOME/testbed/milcom/saga/src" \
  exec "$HOME/testbed/venv/bin/python" -u e19.py "$@"
