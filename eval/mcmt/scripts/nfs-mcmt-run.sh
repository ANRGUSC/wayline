#!/usr/bin/env bash
set -uo pipefail
N="${N:-6}"; OUT=/tmp/nfs-mcmt-results.csv; echo "rep,phase,makespan_s" > "$OUT"
TPL=wl-vemcmt-n4-d120-png-argo-nfs
wait_idle(){ for i in $(seq 1 60); do m=$(kubectl -n argo get pods --no-headers 2>/dev/null|grep -ivE 'argo-server|workflow-controller|httpbin'|grep -vcE 'Succeeded|Completed'); [ "${m:-0}" = 0 ]&&return; sleep 4; done; }
for r in $(seq 1 "$N"); do
  wait_idle
  wf=$(kubectl -n argo create -f <(printf 'apiVersion: argoproj.io/v1alpha1\nkind: Workflow\nmetadata: { generateName: %s-, namespace: argo }\nspec: { workflowTemplateRef: { name: %s } }\n' "$TPL" "$TPL") 2>&1 | sed -nE 's|workflow.argoproj.io/(.+) created.*|\1|p')
  for i in $(seq 1 200); do p=$(kubectl -n argo get workflow "$wf" -o jsonpath='{.status.phase}' 2>/dev/null); case "$p" in Succeeded|Failed|Error) break;; esac; sleep 5; done
  ms=$(kubectl -n argo get workflow "$wf" -o json 2>/dev/null|python3 -c "import json,sys,datetime as dt;d=json.load(sys.stdin)['status'];a=dt.datetime.fromisoformat(d['startedAt'].replace('Z','+00:00'));b=dt.datetime.fromisoformat(d['finishedAt'].replace('Z','+00:00'));print(int((b-a).total_seconds()))" 2>/dev/null)
  echo "$r,$p,${ms:-?}">>"$OUT"; echo "  [nfs-mcmt rep$r] $p ms=${ms}s"
  kubectl -n argo delete workflow "$wf" --wait=false >/dev/null 2>&1
  kubectl -n e0-bench exec deploy/e0-nfs -- sh -c 'rm -rf /nfsshare/vemcmt-* 2>/dev/null' >/dev/null 2>&1 || true
done
echo "NFS-MCMT DONE"
python3 -c "import csv,statistics;v=[float(r['makespan_s']) for r in csv.DictReader(open('$OUT')) if r['phase']=='Succeeded' and r['makespan_s'] not in('','?')];print(f'NFS-MCMT d120-png tc: mean={statistics.mean(v):.0f}s std={statistics.pstdev(v):.0f} n={len(v)}') if v else print('no data')"
