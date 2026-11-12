#!/bin/sh
# Run planners.py on eclipse in E17's environment (ncsim, saga); the sudo password is stdin's first line.
cd "$(dirname "$0")"
PYTHONHASHSEED=0 PYTHONPATH="$HOME/testbed/milcom/ncsim:$HOME/testbed/milcom/saga/src" \
  exec "$HOME/testbed/venv/bin/python" -u planners.py "$@"
