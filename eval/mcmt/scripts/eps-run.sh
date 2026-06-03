#!/usr/bin/env bash
set -uo pipefail
REPO=/home/anrg/wayline; N="${N:-4}"; OUT=/tmp/eps-results.csv
echo "eps,rep,phase,makespan_s,n_distinct_detect_nodes,placement" > "$OUT"
wait_idle(){ for i in $(seq 1 60); do n=$(kubectl -n wl-system get pods -l wl-odag --no-headers 2>/dev/null|grep -vcE 'Succeeded|Completed'); [ "${n:-0}" = 0 ]&&return; sleep 4; done; }
run_eps(){ local eps=$1 tpl=$2 rep=$3 run p ms
  out=$("$REPO/bin/wayline" run "$tpl" -n wl-system 2>&1); run=$(echo "$out"|sed -nE 's/Created run ([^ ]+).*/\1/p')
  for i in $(seq 1 150); do p=$(kubectl -n wl-system get odags.wl.io "$run" -o jsonpath='{.status.phase}' 2>/dev/null); case "$p" in Succeeded|Failed) break;; esac; sleep 5; done
  ms=$(kubectl -n wl-system get odags.wl.io "$run" -o jsonpath='{.status.makespan}' 2>/dev/null)
  pl=$(kubectl -n wl-system get odags.wl.io "$run" -o json 2>/dev/null | python3 -c "import json,sys;d=json.load(sys.stdin);n=[t.get('node') for t in d.get('status',{}).get('tasks',[]) if 'detect' in t['name']];import collections;print(len(set(n)),'|',dict(collections.Counter(n)))" 2>/dev/null)
  echo "$eps,$rep,$p,${ms:-?},$pl" >> "$OUT"; echo "  [eps=$eps rep$rep] $p ms=${ms}s detect-nodes=$pl"
  kubectl -n wl-system delete odags.wl.io "$run" --wait=false >/dev/null 2>&1; }
for spec in "0 wl-vemcmt-n4-d120-png-heft" "20 wl-vemcmt-n4-d120-png-heft-eps20" "40 wl-vemcmt-n4-d120-png-heft-eps40" "60 wl-vemcmt-n4-d120-png-heft-eps60"; do
  read eps tpl <<<"$spec"; echo "############ eps=$eps ############"
  for r in $(seq 1 "$N"); do wait_idle; run_eps "$eps" "$tpl" "$r"; done
done
echo "EPS SWEEP DONE"
python3 - <<'PY'
import csv,statistics
rows=[r for r in csv.DictReader(open("/tmp/eps-results.csv")) if r['phase']=='Succeeded' and r['makespan_s'] not in('','?')]
for e in ('0','20','40','60'):
    v=[float(r['makespan_s']) for r in rows if r['eps']==e]
    nd=[r['n_distinct_detect_nodes'] for r in rows if r['eps']==e]
    if v: print(f"  eps={e:>2}: mean={statistics.mean(v):.1f}s std={statistics.pstdev(v):.1f} distinct-detect-nodes={nd}")
PY
