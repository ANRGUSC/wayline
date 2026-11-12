#!/bin/sh
# Run e18.py on eclipse in the testbed venv. Commands that touch the workers
# (run, verify-links, calibrate --host, cleanup) read the workers' sudo
# password from the first line of stdin; it is never put in the environment,
# a file or a command line.
#   ./run.sh run --suite smoke < /dev/stdin
cd "$(dirname "$0")"
exec "${E18_PYTHON:-$HOME/testbed/venv/bin/python}" -u e18.py "$@"
