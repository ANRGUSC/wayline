#!/usr/bin/env bash
#
# Re-run N failed Argo reps for a given cell.
#
#   ./argo-retry.sh <argo_template> <out_dir> [n_retries=2]
#
# Adds n_retries fresh Argo workflows, captures the same artifacts the
# pilot does, and appends rows to <out_dir>/summary.csv tagged
# rep=retry-<i>. Designed to fill in Argo failures caused by transient
# infrastructure issues (e.g. MinIO disk pressure earlier in the cell);
# results are honest because the underlying workload and template are
# identical, just submitted at a later wall-clock time.
set -euo pipefail

ARGO_TPL="${1:?usage: $0 <argo_template> <out_dir> [n_retries=2]}"
OUT="${2:?usage: $0 <argo_template> <out_dir> [n_retries=2]}"
N=${3:-2}
HERE="$(cd "$(dirname "$0")" && pwd)"

[[ -d "$OUT" ]] || { echo "ERROR: $OUT does not exist"; exit 1; }
SUM="$OUT/summary.csv"
[[ -f "$SUM" ]] || { echo "ERROR: $SUM not found"; exit 1; }

md5() { md5sum "$1" 2>/dev/null | awk '{print $1}'; }

# Borrow the same artifact collector pattern from pilot-paired.sh so the
# retry rows have the same shape.
collect_argo_artifacts() {
    local label=$1 wf=$2 dest=$3
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
    echo "bytes_in_total=NA"  > "$dest/bytes-summary.txt"
    echo "bytes_out_total=NA" >> "$dest/bytes-summary.txt"
    kubectl run probe-retry-$label --rm -i --restart=Never --image=busybox \
      --overrides='{"spec":{"nodeName":"anrg-9","containers":[{"name":"p","image":"busybox","command":["sh","-c","cat /reports/'$wf'/report.json 2>/dev/null || cat /reports/unknown/report.json"],"volumeMounts":[{"name":"r","mountPath":"/reports"}]}],"volumes":[{"name":"r","hostPath":{"path":"/var/lib/wl-workloads/reports"}}]}}' 2>&1 \
      | grep -v "^If you\|^warning\|^pod " | python3 -c "
import sys; t=sys.stdin.read(); b=t.find('{'); e=t.rfind('}')+1; print(t[b:e] if b>=0 else '')
" > "$dest/report.json" || true
    sshpass -p anrg ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR anrg-9 \
        "echo anrg | sudo -S rm -rf /var/lib/wl-workloads/reports/unknown" >/dev/null 2>&1 || true
}

run_one() {
    local i=$1
    local label="retry-${i}"
    local dest="$OUT/${label}-argo"
    local start=$(date +%s)
    local out
    out=$(kubectl -n argo create -f <(cat <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: ${ARGO_TPL}-
  namespace: argo
spec:
  workflowTemplateRef: { name: ${ARGO_TPL} }
EOF
) 2>&1)
    local wf=$(echo "$out" | sed -nE 's|workflow.argoproj.io/(.+) created.*|\1|p')
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
    collect_argo_artifacts "$label" "$wf" "$dest"
    local h=$(md5 "$dest/report.json")
    echo "$label,argo,$wf,$phase,$ms,$wall,NA,NA,$h" >> "$SUM"
    echo "  -> $label: phase=$phase makespan=${ms}s wall=${wall}s"
    kubectl -n argo delete workflow "$wf" --wait=false >/dev/null 2>&1 || true
}

echo "Retrying Argo for $ARGO_TPL ($N times) → $OUT"
for i in $(seq 1 "$N"); do
    echo
    echo "==================== retry $i ===================="
    run_one "$i"
done

echo
echo "Done. Rows in $SUM:"
wc -l < "$SUM"
