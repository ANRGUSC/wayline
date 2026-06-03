#!/usr/bin/env bash
# Robustness sweep: K seeded random edge networks, fair MCMT (matched placement
# 6,7,8,6 + equal CPU + perf governor) Wayline vs Argo. Reports speedup per seed.
#   SEEDS=10 REPS=2 ./random-sweep.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; REPO=/home/anrg/wayline
SEEDS="${SEEDS:-10}"; REPS="${REPS:-2}"; CELLS=("120 png" "30 jpg")
OUT=/tmp/random-results.csv; echo "seed,cell,rep,system,phase,makespan_s,wall_s" > "$OUT"
clearbucket(){ kubectl -n e0-bench exec mc-helper -- sh -c 'mc rm --recursive --force local/argo-bench/ >/dev/null 2>&1' >/dev/null 2>&1 || true; }
wait_idle(){ for i in $(seq 1 60); do n=$(kubectl -n wl-system get pods -l wl-odag --no-headers 2>/dev/null|grep -vcE 'Succeeded|Completed'); m=$(kubectl -n argo get pods --no-headers 2>/dev/null|grep -ivE 'argo-server|workflow-controller|httpbin'|grep -vcE 'Succeeded|Completed'); [ "${n:-0}" = 0 ]&&[ "${m:-0}" = 0 ]&&return; sleep 4; done; }
run_wl(){ local sd=$1 cell=$2 r=$3 tpl=$4 s=$(date +%s) out run p ms
  out=$("$REPO/bin/wayline" run "$tpl" -n wl-system 2>&1); run=$(echo "$out"|sed -nE 's/Created run ([^ ]+).*/\1/p')
  for i in $(seq 1 150); do p=$(kubectl -n wl-system get odags.wl.io "$run" -o jsonpath='{.status.phase}' 2>/dev/null); case "$p" in Succeeded|Failed) break;; esac; sleep 5; done
  ms=$(kubectl -n wl-system get odags.wl.io "$run" -o jsonpath='{.status.makespan}' 2>/dev/null); echo "$sd,$cell,$r,wayline,$p,${ms:-?},$(( $(date +%s)-s ))">>"$OUT"
  echo "  [seed$sd $cell] wayline rep$r: $p ms=${ms}s"; kubectl -n wl-system delete odags.wl.io "$run" --wait=false >/dev/null 2>&1; }
run_ar(){ local sd=$1 cell=$2 r=$3 tpl=$4 s=$(date +%s) out wf p ms
  out=$(kubectl -n argo create -f <(printf 'apiVersion: argoproj.io/v1alpha1\nkind: Workflow\nmetadata: { generateName: %s-, namespace: argo }\nspec: { workflowTemplateRef: { name: %s } }\n' "$tpl" "$tpl") 2>&1)
  wf=$(echo "$out"|sed -nE 's|workflow.argoproj.io/(.+) created.*|\1|p')
  for i in $(seq 1 200); do p=$(kubectl -n argo get workflow "$wf" -o jsonpath='{.status.phase}' 2>/dev/null); case "$p" in Succeeded|Failed|Error) break;; esac; sleep 5; done
  ms=$(kubectl -n argo get workflow "$wf" -o json 2>/dev/null|python3 -c "import json,sys,datetime as dt;d=json.load(sys.stdin)['status'];a=dt.datetime.fromisoformat(d['startedAt'].replace('Z','+00:00'));b=dt.datetime.fromisoformat(d['finishedAt'].replace('Z','+00:00'));print(int((b-a).total_seconds()))" 2>/dev/null)
  echo "$sd,$cell,$r,argo,$p,${ms:-?},$(( $(date +%s)-s ))">>"$OUT"
  echo "  [seed$sd $cell] argo    rep$r: $p ms=${ms}s"; kubectl -n argo delete workflow "$wf" --wait=false >/dev/null 2>&1; clearbucket; }
for sd in $(seq 1 "$SEEDS"); do
  echo "================= SEED $sd ================="
  "$HERE/setup-tc-random.sh" "$sd" >/dev/null 2>&1; sleep 5
  for spec in "${CELLS[@]}"; do read d fmt <<<"$spec"; cell="d${d}-${fmt}"
    WL="wl-vemcmt-n4-d${d}-${fmt}-spread"; AR="wl-vemcmt-n4-d${d}-${fmt}-spread-argo"
    for r in $(seq 1 "$REPS"); do wait_idle
      if (( r%2==1 )); then run_wl "$sd" "$cell" "$r" "$WL"; wait_idle; run_ar "$sd" "$cell" "$r" "$AR"
      else run_ar "$sd" "$cell" "$r" "$AR"; wait_idle; run_wl "$sd" "$cell" "$r" "$WL"; fi
    done
  done
  "$HERE/setup-tc-random.sh" "$sd" teardown >/dev/null 2>&1
done
echo "RANDOM SWEEP DONE -> $OUT"
python3 - <<PY
import csv,statistics
rows=[r for r in csv.DictReader(open("$OUT")) if r['phase']=='Succeeded' and r['makespan_s'] not in('','?')]
for cell in ('d120-png','d30-jpg'):
    sp=[]
    for sd in set(r['seed'] for r in rows if r['cell']==cell):
        w=[float(r['makespan_s']) for r in rows if r['seed']==sd and r['cell']==cell and r['system']=='wayline']
        a=[float(r['makespan_s']) for r in rows if r['seed']==sd and r['cell']==cell and r['system']=='argo']
        if w and a: sp.append((sd, statistics.mean(a)/statistics.mean(w)))
    if not sp: continue
    sp.sort(key=lambda x:int(x[0])); vals=[x[1] for x in sp]
    wins=sum(1 for v in vals if v>1.0)
    print(f"[{cell}] Wayline faster in {wins}/{len(vals)} seeds; speedup median={statistics.median(vals):.2f}x min={min(vals):.2f}x max={max(vals):.2f}x")
    print("   per-seed (argo/wayline): "+", ".join(f"s{sd}={v:.2f}" for sd,v in sp))
PY
