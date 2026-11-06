#!/bin/sh
# Start Repair Lab in the paper environment, in the background. The sudo
# password is stdin's first line: read here, then handed to the server on its
# own stdin (a background job would otherwise get /dev/null).
#   echo <password> | ./run-demo.sh [server.py options]
# Stop it with: pkill -f "[r]epair-lab-server"   (the lane is torn down on exit)
cd "$(dirname "$0")"
read -r PW
LOG="$HOME/testbed/e17/demo.log"
printf '%s\n' "$PW" | PYTHONHASHSEED=0 PYTHONPATH="$HOME/testbed/milcom/ncsim:$HOME/testbed/milcom/saga/src" \
  nohup "$HOME/testbed/venv/bin/python" -u server.py --tag repair-lab-server "$@" > "$LOG" 2>&1 &
echo "Repair Lab starting (pid $!); log: $LOG"
