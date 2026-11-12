#!/bin/sh
# Run e22.py on eclipse in the testbed venv; the workers' sudo password is stdin's first line.
cd "$(dirname "$0")"
exec "$HOME/testbed/venv/bin/python" -u e22.py "$@"
