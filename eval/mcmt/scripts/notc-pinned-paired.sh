#!/usr/bin/env bash
# Pin-identical no-tc head-to-head: Wayline vs Argo, SAME placement on both
# (detect-embed/track round-robin 6,7,8,6). Isolates the data plane — any
# makespan difference is data-plane, not scheduling.
#   ./notc-pinned-paired.sh [REPS]
set -uo pipefail
REPS="${1:-5}"
REPO=/home/anrg/wayline
WL_TPL=vemcmt-n4-d120-png-spread
AR_TPL=vemcmt-n4-d120-png-spread-argo
OUT="$REPO/eval/mcmt/results/notc-pinned"
mkdir -p "$OUT"; SUM="$OUT/summary.csv"
echo "rep,system,run,phase,makespan_s,wall_s" > "$SUM"

wait_idle(){ for i in $(seq 1 40); do
  n=$(kubectl -n wl-system get pods -l wl-odag --no-headers 2>/dev/null|grep -vE 'Succeeded|Completed'|wc -l)
  m=$(kubectl -n argo get pods --no-headers 2>/dev/null|grep -ivE 'argo-server|workflow-controller|httpbin'|grep -vcE 'Succeeded|Completed')
  [ "${n:-0}" = 0 ]&&[ "${m:-0}" = 0 ]&&return; sleep 5; done; }

run_wl(){ local r=$1; local s=$(date +%s)
  local out; out=$("$REPO/bin/wayline" odag run $WL_TPL -n wl-system 2>&1)
  local run; run=$(echo "$out"|sed -nE 's/Created run ([^ ]+).*/\1/p')
  for i in $(seq 1 80); do p=$(kubectl -n wl-system get odag "$run" -o jsonpath='{.status.phase}' 2>/dev/null); case "$p" in Succeeded|Failed) break;; esac; sleep 5; done
  local w=$(( $(date +%s)-s )); local ms=$(kubectl -n wl-system get odag "$run" -o jsonpath='{.status.makespan}' 2>/dev/null)
  echo "$r,wayline,$run,$p,${ms:-?},$w">>"$SUM"; echo "  wayline rep$r: $p makespan=${ms}s wall=${w}s"
  kubectl -n wl-system delete odag "$run" --wait=false>/dev/null 2>&1; }

run_ar(){ local r=$1; local s=$(date +%s)
  local out; out=$(kubectl -n argo create -f <(cat <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata: { generateName: ${AR_TPL}-, namespace: argo }
spec: { workflowTemplateRef: { name: ${AR_TPL} } }
EOF
) 2>&1); local wf; wf=$(echo "$out"|sed -nE 's|workflow.argoproj.io/(.+) created.*|\1|p')
  for i in $(seq 1 100); do p=$(kubectl -n argo get workflow "$wf" -o jsonpath='{.status.phase}' 2>/dev/null); case "$p" in Succeeded|Failed|Error) break;; esac; sleep 5; done
  local w=$(( $(date +%s)-s )); local ms=$(kubectl -n argo get workflow "$wf" -o json 2>/dev/null|python3 -c "import json,sys,datetime as dt;d=json.load(sys.stdin);s=d['status'];a=dt.datetime.fromisoformat(s['startedAt'].replace('Z','+00:00'));b=dt.datetime.fromisoformat(s['finishedAt'].replace('Z','+00:00'));print(int((b-a).total_seconds()))" 2>/dev/null)
  echo "$r,argo,$wf,$p,${ms:-?},$w">>"$SUM"; echo "  argo    rep$r: $p makespan=${ms}s wall=${w}s"
  kubectl -n argo delete workflow "$wf" --wait=false>/dev/null 2>&1; }

for r in $(seq 1 "$REPS"); do
  echo "==== rep $r ===="; wait_idle
  if (( r%2==1 )); then run_wl "$r"; wait_idle; run_ar "$r"; else run_ar "$r"; wait_idle; run_wl "$r"; fi
done
echo "PINNED PAIRED DONE -> $SUM"
python3 - <<PY
import csv,statistics
rows=list(csv.DictReader(open("$SUM")))
def w(s): return [float(r['wall_s']) for r in rows if r['system']==s and r['phase']=='Succeeded']
wl,ar=w('wayline'),w('argo')
if wl and ar:
    print(f"Wayline wall: {wl} mean={statistics.mean(wl):.0f}s")
    print(f"Argo    wall: {ar} mean={statistics.mean(ar):.0f}s")
    d=statistics.mean(wl)-statistics.mean(ar)
    print(f"Wayline - Argo = {d:+.0f}s ({d/statistics.mean(ar)*100:+.1f}%)  [<0 = Wayline faster]")
PY
