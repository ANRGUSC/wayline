#!/usr/bin/env bash
# Static-placement ablation under the FAIR config (equal CPU, matched-where-applicable
# placement, perf governor, tc matrix). 3 configs x N reps on d120-png.
#   Argo (spread-argo) | Wayline-static (spread, round-robin pin) | Wayline-HEFT (heft)
# Captures makespan + report.json semantic fields (correctness) + HEFT placement.
set -uo pipefail
REPO=/home/anrg/wayline; N="${N:-8}"; OUT=/tmp/ablation-results.csv
echo "rep,config,phase,makespan_s,n_tracks,report_md5" > "$OUT"
RP=/tmp/ablation-reports; mkdir -p "$RP"
clearbucket(){ kubectl -n e0-bench exec mc-helper -- sh -c 'mc rm --recursive --force local/argo-bench/ >/dev/null 2>&1' >/dev/null 2>&1 || true; }
wait_idle(){ for i in $(seq 1 60); do n=$(kubectl -n wl-system get pods -l wl-odag --no-headers 2>/dev/null|grep -vcE 'Succeeded|Completed'); m=$(kubectl -n argo get pods --no-headers 2>/dev/null|grep -ivE 'argo-server|workflow-controller|httpbin'|grep -vcE 'Succeeded|Completed'); [ "${n:-0}" = 0 ]&&[ "${m:-0}" = 0 ]&&return; sleep 4; done; }
# grab newest report.json from anrg-9 hostPath -> prints "n_tracks md5"
grab_report(){ local tag=$1
  sshpass -p anrg ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10 anrg-9 \
    "echo anrg | sudo -S sh -c 'f=\$(find /var/lib/wl-workloads/reports -name report.json -printf \"%T@ %p\n\" 2>/dev/null|sort -rn|head -1|cut -d\" \" -f2); cat \$f'" 2>/dev/null | grep -av password > "$RP/$tag.json" || true
  python3 -c "import json;d=json.load(open('$RP/$tag.json'));print(d.get('n_global_tracks','?'))" 2>/dev/null || echo "?"
}
run_wl(){ local cfg=$1 rep=$2 tpl=$3 s p ms run nt md
  out=$("$REPO/bin/wayline" run "$tpl" -n wl-system 2>&1); run=$(echo "$out"|sed -nE 's/Created run ([^ ]+).*/\1/p')
  for i in $(seq 1 150); do p=$(kubectl -n wl-system get odags.wl.io "$run" -o jsonpath='{.status.phase}' 2>/dev/null); case "$p" in Succeeded|Failed) break;; esac; sleep 5; done
  ms=$(kubectl -n wl-system get odags.wl.io "$run" -o jsonpath='{.status.makespan}' 2>/dev/null)
  if [ "$cfg" = wlheft ]; then kubectl -n wl-system get odags.wl.io "$run" -o json 2>/dev/null | python3 -c "import json,sys;d=json.load(sys.stdin);print('  HEFT placement:',{t['name']:t.get('node') for t in d.get('status',{}).get('tasks',[]) if 'detect' in t['name']})" 2>/dev/null; fi
  nt=$(grab_report "$cfg-$rep"); md=$(md5sum "$RP/$cfg-$rep.json" 2>/dev/null|awk '{print $1}')
  echo "$rep,$cfg,$p,${ms:-?},$nt,$md">>"$OUT"; echo "  [$cfg rep$rep] $p ms=${ms}s tracks=$nt"
  kubectl -n wl-system delete odags.wl.io "$run" --wait=false >/dev/null 2>&1; }
run_ar(){ local rep=$1 tpl=$2 s p ms wf nt md
  out=$(kubectl -n argo create -f <(printf 'apiVersion: argoproj.io/v1alpha1\nkind: Workflow\nmetadata: { generateName: %s-, namespace: argo }\nspec: { workflowTemplateRef: { name: %s } }\n' "$tpl" "$tpl") 2>&1)
  wf=$(echo "$out"|sed -nE 's|workflow.argoproj.io/(.+) created.*|\1|p')
  for i in $(seq 1 200); do p=$(kubectl -n argo get workflow "$wf" -o jsonpath='{.status.phase}' 2>/dev/null); case "$p" in Succeeded|Failed|Error) break;; esac; sleep 5; done
  ms=$(kubectl -n argo get workflow "$wf" -o json 2>/dev/null|python3 -c "import json,sys,datetime as dt;d=json.load(sys.stdin)['status'];a=dt.datetime.fromisoformat(d['startedAt'].replace('Z','+00:00'));b=dt.datetime.fromisoformat(d['finishedAt'].replace('Z','+00:00'));print(int((b-a).total_seconds()))" 2>/dev/null)
  nt=$(grab_report "argo-$rep"); md=$(md5sum "$RP/argo-$rep.json" 2>/dev/null|awk '{print $1}')
  echo "$rep,argo,$p,${ms:-?},$nt,$md">>"$OUT"; echo "  [argo rep$rep] $p ms=${ms}s tracks=$nt"
  kubectl -n argo delete workflow "$wf" --wait=false >/dev/null 2>&1; clearbucket; }
for rep in $(seq 1 "$N"); do
  echo "============ ABLATION rep $rep ============"
  wait_idle; run_ar "$rep" wl-vemcmt-n4-d120-png-spread-argo
  wait_idle; run_wl wlstatic "$rep" wl-vemcmt-n4-d120-png-spread
  wait_idle; run_wl wlheft "$rep" wl-vemcmt-n4-d120-png-heft
done
echo "ABLATION DONE"
python3 - <<'PY'
import csv,statistics
rows=[r for r in csv.DictReader(open("/tmp/ablation-results.csv")) if r['phase']=='Succeeded' and r['makespan_s'] not in('','?')]
M={}
for c in ('argo','wlstatic','wlheft'):
    v=[float(r['makespan_s']) for r in rows if r['config']==c]
    if v: M[c]=statistics.mean(v); print(f"{c}: mean={statistics.mean(v):.1f}s std={statistics.pstdev(v):.1f} n={len(v)}")
if len(M)==3:
    dp=M['argo']-M['wlstatic']; heft=M['wlstatic']-M['wlheft']; tot=M['argo']-M['wlheft']
    print(f"data-plane gap (Argo-static)={dp:.1f}s  HEFT gap (static-heft)={heft:.1f}s  total={tot:.1f}s")
    if tot>0: print(f"  data-plane share={100*dp/tot:.0f}%  HEFT share={100*heft/tot:.0f}%")
# correctness: tracks agreement argo vs wlstatic per rep
print("correctness (n_global_tracks argo vs wlstatic vs wlheft):")
for rep in sorted(set(r['rep'] for r in rows),key=int):
    t={r['config']:r['n_tracks'] for r in rows if r['rep']==rep}
    print(f"  rep{rep}: {t}")
PY
