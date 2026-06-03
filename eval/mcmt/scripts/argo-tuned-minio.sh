#!/usr/bin/env bash
#
# Phase-2 reviewer ask: stronger Argo+MinIO baseline.
#
# Runs N Argo reps for one cell, clearing the MinIO bucket completely
# before each rep. This eliminates artifact accumulation / DiskPressure
# as a confounder of the Argo timing.
#
#   ./argo-tuned-minio.sh <argo_template> <out_dir> [N=10]
#
# Output: <out_dir>/summary.csv with columns
#   rep,wf_name,phase,makespan_s,wall_s,report_md5
# plus per-rep workflow.yaml / placement.json / report.json under
# <out_dir>/rep${i}-argo/.
#
# Does NOT change MinIO's location — that's the more invasive Phase 2
# variant. The bucket-reset variant addresses the specific reviewer
# concern that "MinIO accumulated and triggered DiskPressure" was
# what made the baseline weak in n4-d120-jpg.
set -euo pipefail

ARGO_TPL="${1:?usage: $0 <argo_template> <out_dir> [N=10]}"
OUT="${2:?usage: $0 <argo_template> <out_dir> [N=10]}"
N=${3:-10}
HERE="$(cd "$(dirname "$0")" && pwd)"

mkdir -p "$OUT"
SUM="$OUT/summary.csv"
[[ -f "$SUM" ]] || echo "rep,wf_name,phase,makespan_s,wall_s,bytes_in_total,bytes_out_total,report_md5" > "$SUM"

md5() { md5sum "$1" 2>/dev/null | awk '{print $1}'; }

# Clear the bucket by deleting all files under the MinIO data dir on
# anrg-9. We keep the bucket directory itself so MinIO doesn't need
# to re-create it.
clear_bucket() {
    sshpass -p anrg ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR anrg@anrg-9 \
        "echo anrg | sudo -S find /var/lib/wl-minio-e0/argo-bench/ -mindepth 1 -delete" </dev/null >/dev/null 2>&1
}

collect_argo_artifacts() {
    local rep=$1 wf=$2 dest=$3
    mkdir -p "$dest"
    kubectl -n argo get workflow "$wf" -o yaml > "$dest/workflow.yaml" 2>/dev/null
    kubectl -n argo get workflow "$wf" -o json \
      | python3 -c "
import json,sys
d=json.load(sys.stdin); out=[]
for nid,n in d['status'].get('nodes',{}).items():
    if n.get('type') != 'Pod': continue
    out.append({
      'task': n.get('displayName'),
      'node': n.get('hostNodeName'),
      'startedAt': n.get('startedAt'),
      'finishedAt': n.get('finishedAt'),
      'phase': n.get('phase'),
    })
print(json.dumps(out, indent=2))" > "$dest/placement.json"
    kubectl run probe-tuned-$rep --rm -i --restart=Never --image=busybox \
      --overrides='{"spec":{"nodeName":"anrg-9","containers":[{"name":"p","image":"busybox","command":["sh","-c","cat /reports/'$wf'/report.json 2>/dev/null || cat /reports/unknown/report.json 2>/dev/null"],"volumeMounts":[{"name":"r","mountPath":"/reports"}]}],"volumes":[{"name":"r","hostPath":{"path":"/var/lib/wl-workloads/reports"}}]}}' 2>&1 \
      | grep -v "^If you\|^warning\|^pod " | python3 -c "
import sys; t=sys.stdin.read(); b=t.find('{'); e=t.rfind('}')+1; print(t[b:e] if b>=0 else '')
" > "$dest/report.json" || true
    sshpass -p anrg ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR anrg@anrg-9 \
        "echo anrg | sudo -S rm -rf /var/lib/wl-workloads/reports/unknown" </dev/null >/dev/null 2>&1 || true
}

wait_for_idle() {
    for i in $(seq 1 30); do
        local k=$(kubectl -n argo get pods --no-headers 2>/dev/null | grep -vE "Completed|Succeeded" | wc -l)
        [ "$k" = "0" ] && return 0
        sleep 5
    done
}

run_one() {
    local rep=$1
    local dest="$OUT/rep${rep}-argo"
    echo "  clearing MinIO bucket..."
    clear_bucket
    wait_for_idle
    local start=$(date +%s)
    local out
    out=$(kubectl -n argo create -f <(cat <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: ${ARGO_TPL}-tuned-
  namespace: argo
spec:
  workflowTemplateRef: { name: ${ARGO_TPL} }
EOF
) 2>&1)
    local wf=$(echo "$out" | sed -nE 's|workflow.argoproj.io/(.+) created.*|\1|p')
    if [ -z "$wf" ]; then
        echo "  ERROR: $out"
        echo "$rep,?,Failed,?,?,NA,NA,?" >> "$SUM"
        return
    fi
    echo "  submitted $wf"
    for j in $(seq 1 120); do
        sleep 15
        local p=$(kubectl -n argo get workflow "$wf" -o jsonpath='{.status.phase}' 2>/dev/null)
        case "$p" in Succeeded|Failed|Error) break;; esac
    done
    local end=$(date +%s)
    local wall=$((end - start))
    local ms=$(kubectl -n argo get workflow "$wf" -o json 2>&1 | python3 -c "
import json,sys
from datetime import datetime
d=json.load(sys.stdin); sa=datetime.fromisoformat(d['status']['startedAt'].replace('Z','+00:00')); fa=datetime.fromisoformat(d['status']['finishedAt'].replace('Z','+00:00'))
print(int((fa-sa).total_seconds()))" 2>/dev/null)
    local phase=$(kubectl -n argo get workflow "$wf" -o jsonpath='{.status.phase}' 2>/dev/null)
    collect_argo_artifacts "$rep" "$wf" "$dest"
    local h=$(md5 "$dest/report.json")
    echo "$rep,$wf,$phase,$ms,$wall,NA,NA,$h" >> "$SUM"
    echo "  -> rep $rep: phase=$phase makespan=${ms}s wall=${wall}s"
    kubectl -n argo delete workflow "$wf" --wait=false >/dev/null 2>&1 || true
}

echo "Tuned Argo+MinIO baseline: $ARGO_TPL, N=$N reps, bucket cleared before each rep"
echo "Output: $OUT"
echo
for r in $(seq 1 "$N"); do
    echo "==================== rep $r ===================="
    run_one "$r"
done

echo
echo "##############################################"
echo "## SUMMARY  $SUM"
echo "##############################################"
column -t -s, < "$SUM"
echo
python3 - <<PY
import csv, statistics
rows = list(csv.DictReader(open("$SUM")))
ms = [int(r['makespan_s']) for r in rows if r['phase']=='Succeeded' and r['makespan_s'] not in ('','?')]
n = len(rows)
if ms:
    print(f"Argo tuned reps: {ms}")
    print(f"mean={statistics.mean(ms):.1f}s  std={statistics.pstdev(ms):.1f}s  n_ok={len(ms)}/{n}")
PY
