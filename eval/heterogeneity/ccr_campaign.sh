#!/usr/bin/env bash
# CCR=1 classical-HEFT convergence campaign (single cell).
# Network shaping ON (required to realize CCR), clocks pinned, alpha=0.7.
# Cold start -> REPS reps, profiles accumulate (the convergence sequence).
# Per rep: submit -> wait -> snapshot profiler + ODAG status -> record -> delete.
#
#   REPS=18 ./ccr_campaign.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO=/home/anrg/wayline
REPS="${REPS:-18}"
TPL=het-ccr1
CELL=ccr1
RESULTS="$HERE/results"
DB=/var/lib/wayline/wl-profiler.db
POLL_MAX=150   # *10s
IDLE_MAX=48

log(){ echo "[$(date +%H:%M:%S)] $*"; }
reps_done(){ local f="$RESULTS/$CELL/makespan.csv"; [ -f "$f" ] && echo $(( $(wc -l < "$f") - 1 )) || echo 0; }
wait_idle(){ for _ in $(seq 1 "$IDLE_MAX"); do
    [ "$(kubectl -n wl-system get pods --no-headers 2>/dev/null | grep "$TPL-run" | grep -vc Completed)" -eq 0 ] && return
    sleep 5; done; }
snapshot_db(){ sudo cp "$DB" /tmp/het-snap.db 2>/dev/null; sudo cp "$DB-wal" /tmp/het-snap.db-wal 2>/dev/null || true
    sudo chown "$USER" /tmp/het-snap.db /tmp/het-snap.db-wal 2>/dev/null || true; }

log "CCR campaign: $TPL, $REPS reps (done so far: $(reps_done))"
for rep in $(seq $(( $(reps_done) + 1 )) "$REPS"); do
  wait_idle
  s=$(date +%s)
  out=$("$REPO/bin/wayline" run "$TPL" -n wl-system 2>/dev/null)
  run=$(echo "$out" | sed -nE 's/.*[Cc]reated run ([^ ]+).*/\1/p')
  [ -z "$run" ] && { log "ERR no run rep$rep"; continue; }
  p=""
  for _ in $(seq 1 "$POLL_MAX"); do
    p=$(kubectl -n wl-system get odags.wl.io "$run" -o jsonpath='{.status.phase}' 2>/dev/null)
    case "$p" in Succeeded|Failed) break;; esac; sleep 10
  done
  wall=$(( $(date +%s) - s ))
  snapshot_db
  kubectl -n wl-system get odags.wl.io "$run" -o json 2>/dev/null \
    | python3 "$HERE/record.py" "$CELL" "$rep" classical net "$wall" "$TPL" /tmp/het-snap.db "$RESULTS"
  log "rep $rep/$REPS: $p wall=${wall}s"
  kubectl -n wl-system delete odags.wl.io "$run" --wait=false >/dev/null 2>&1
done
log "CCR CAMPAIGN COMPLETE"
cat "$RESULTS/$CELL/makespan.csv"
