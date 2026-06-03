#!/usr/bin/env bash
# Fair paired MCMT: 4 cells x REPS, matched placement + equal CPU + perf governor.
# Arg1 = network label (notc|tc). Appends to /tmp/fair-results.csv.
set -uo pipefail
REPO=/home/anrg/wayline; NET="${1:?need net label}"; REPS="${REPS:-6}"
OUT=/tmp/fair-results.csv
[ -f "$OUT" ] || echo "net,cell,rep,system,phase,makespan_s,wall_s" > "$OUT"
CELLS=("30 jpg" "60 jpg" "120 jpg" "120 png")
clearbucket(){ kubectl -n e0-bench exec mc-helper -- sh -c 'mc rm --recursive --force local/argo-bench/ >/dev/null 2>&1' >/dev/null 2>&1 || true; }
wait_idle(){ for i in $(seq 1 60); do n=$(kubectl -n wl-system get pods -l wl-odag --no-headers 2>/dev/null|grep -vcE 'Succeeded|Completed'); m=$(kubectl -n argo get pods --no-headers 2>/dev/null|grep -ivE 'argo-server|workflow-controller|httpbin'|grep -vcE 'Succeeded|Completed'); [ "${n:-0}" = 0 ]&&[ "${m:-0}" = 0 ]&&return; sleep 4; done; }
run_wl(){ local cell=$1 r=$2 tpl=$3 s=$(date +%s) out run p ms
  out=$("$REPO/bin/wayline" run "$tpl" -n wl-system 2>&1); run=$(echo "$out"|sed -nE 's/Created run ([^ ]+).*/\1/p')
  for i in $(seq 1 120); do p=$(kubectl -n wl-system get odags.wl.io "$run" -o jsonpath='{.status.phase}' 2>/dev/null); case "$p" in Succeeded|Failed) break;; esac; sleep 5; done
  ms=$(kubectl -n wl-system get odags.wl.io "$run" -o jsonpath='{.status.makespan}' 2>/dev/null); local w=$(( $(date +%s)-s ))
  echo "$NET,$cell,$r,wayline,$p,${ms:-?},$w">>"$OUT"; echo "  [$NET $cell] wayline rep$r: $p ms=${ms}s wall=${w}s"
  kubectl -n wl-system delete odags.wl.io "$run" --wait=false >/dev/null 2>&1; }
run_ar(){ local cell=$1 r=$2 tpl=$3 s=$(date +%s) out wf p ms
  out=$(kubectl -n argo create -f <(printf 'apiVersion: argoproj.io/v1alpha1\nkind: Workflow\nmetadata: { generateName: %s-, namespace: argo }\nspec: { workflowTemplateRef: { name: %s } }\n' "$tpl" "$tpl") 2>&1)
  wf=$(echo "$out"|sed -nE 's|workflow.argoproj.io/(.+) created.*|\1|p')
  for i in $(seq 1 150); do p=$(kubectl -n argo get workflow "$wf" -o jsonpath='{.status.phase}' 2>/dev/null); case "$p" in Succeeded|Failed|Error) break;; esac; sleep 5; done
  ms=$(kubectl -n argo get workflow "$wf" -o json 2>/dev/null|python3 -c "import json,sys,datetime as dt;d=json.load(sys.stdin)['status'];a=dt.datetime.fromisoformat(d['startedAt'].replace('Z','+00:00'));b=dt.datetime.fromisoformat(d['finishedAt'].replace('Z','+00:00'));print(int((b-a).total_seconds()))" 2>/dev/null); local w=$(( $(date +%s)-s ))
  echo "$NET,$cell,$r,argo,$p,${ms:-?},$w">>"$OUT"; echo "  [$NET $cell] argo    rep$r: $p ms=${ms}s wall=${w}s"
  kubectl -n argo delete workflow "$wf" --wait=false >/dev/null 2>&1; clearbucket; }
for spec in "${CELLS[@]}"; do
  read d fmt <<<"$spec"; cell="d${d}-${fmt}"; WL="wl-vemcmt-n4-d${d}-${fmt}-spread"; AR="wl-vemcmt-n4-d${d}-${fmt}-spread-argo"
  echo "######## [$NET] CELL $cell (REPS=$REPS) ########"
  for r in $(seq 1 "$REPS"); do wait_idle
    if (( r%2==1 )); then run_wl "$cell" "$r" "$WL"; wait_idle; run_ar "$cell" "$r" "$AR"; else run_ar "$cell" "$r" "$AR"; wait_idle; run_wl "$cell" "$r" "$WL"; fi
  done
done
echo "FAIR RUN [$NET] DONE"
