#!/usr/bin/env bash
# Heterogeneity benchmark campaign.
#
# Matrix: {ra, classical} HEFT  x  {net on, net off}  (CPU clocks pinned throughout).
# Each (mode,net) cell is an independent ODAGTemplate -> its own cold->converged
# profiling sequence. Reps run sequentially WITHOUT resetting profiles within a
# cell (that IS the convergence experiment). Resumable: skips reps already in
# the cell's makespan.csv.
#
# Per rep: submit run -> wait to completion -> snapshot profiler DB + ODAG status
# -> record -> delete run -> wait for cluster idle.
#
#   REPS=12 ./campaign.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO=/home/anrg/wayline
REPS="${REPS:-12}"
RESULTS="$HERE/results"
DB=/var/lib/wayline/wl-profiler.db
POLL_MAX=120     # *10s = 20min hard cap per run
IDLE_MAX=48      # *5s = 4min idle wait

mkdir -p "$RESULTS"

log(){ echo "[$(date +%H:%M:%S)] $*"; }

reps_done(){ # cell -> count of recorded reps
  local f="$RESULTS/$1/makespan.csv"
  [ -f "$f" ] && echo $(( $(wc -l < "$f") - 1 )) || echo 0
}

wait_idle(){
  for _ in $(seq 1 "$IDLE_MAX"); do
    local n
    n=$(kubectl -n wl-system get pods --no-headers 2>/dev/null \
         | grep -E 'het-(ra|classical)-' | grep -vE 'Completed' | wc -l)
    [ "$n" -eq 0 ] && return 0
    sleep 5
  done
  log "WARN: cluster not idle after wait; continuing"
}

snapshot_db(){ # -> /tmp/het-snap.db readable
  sudo cp "$DB" /tmp/het-snap.db 2>/dev/null
  sudo cp "$DB-wal" /tmp/het-snap.db-wal 2>/dev/null || true
  sudo chown "$USER" /tmp/het-snap.db /tmp/het-snap.db-wal 2>/dev/null || true
}

run_rep(){ # cell mode net tpl rep
  local cell=$1 mode=$2 net=$3 tpl=$4 rep=$5
  wait_idle
  local s out run
  s=$(date +%s)
  out=$("$REPO/bin/wayline" run "$tpl" -n wl-system 2>/dev/null)
  run=$(echo "$out" | sed -nE 's/.*[Cc]reated run ([^ ]+).*/\1/p')
  if [ -z "$run" ]; then log "ERR: no run created for $tpl"; return 1; fi
  local p=""
  for _ in $(seq 1 "$POLL_MAX"); do
    p=$(kubectl -n wl-system get odags.wl.io "$run" -o jsonpath='{.status.phase}' 2>/dev/null)
    case "$p" in Succeeded|Failed) break;; esac
    sleep 10
  done
  local wall=$(( $(date +%s) - s ))
  snapshot_db
  kubectl -n wl-system get odags.wl.io "$run" -o json 2>/dev/null \
    | python3 "$HERE/record.py" "$cell" "$rep" "$mode" "$net" "$wall" "$tpl" /tmp/het-snap.db "$RESULTS"
  kubectl -n wl-system delete odags.wl.io "$run" --wait=false >/dev/null 2>&1
}

run_cell(){ # mode net
  local mode=$1 net=$2
  local cell="${mode}-${net}" tpl="het-${mode}-${net}"
  local done; done=$(reps_done "$cell")
  log "CELL $cell: $done/$REPS reps already done"
  for rep in $(seq $((done + 1)) "$REPS"); do
    log "CELL $cell rep $rep/$REPS (run $tpl)"
    run_rep "$cell" "$mode" "$net" "$tpl" "$rep"
  done
  log "CELL $cell COMPLETE"
}

# ---- net=ON block (shaping already applied by netshape.sh apply) ----
log "===== NET=ON block ====="
"$HERE/netshape.sh" apply 0 >/dev/null 2>&1
run_cell ra net
run_cell classical net

# ---- net=OFF block ----
log "===== NET=OFF block (tearing down shaping) ====="
"$HERE/netshape.sh" teardown >/dev/null 2>&1
sleep 5
run_cell ra nonet
run_cell classical nonet

log "===== CAMPAIGN COMPLETE ====="
for c in ra-net classical-net ra-nonet classical-nonet; do
  echo "--- $c makespan ---"; cat "$RESULTS/$c/makespan.csv" 2>/dev/null
done
