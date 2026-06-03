#!/usr/bin/env bash
#
# Paired-rep pilot driver for the mcmt eval. For a given cell
# (default N=4, D=120), runs Wayline and Argo back-to-back in alternating
# order across REPS reps. Captures makespan, per-task placement, per-edge
# payload sizes, data-agent flow records, /metrics, and report hash so
# the result is reproducible end-to-end — not just a wall-clock number.
#
#   ./pilot-paired.sh [N=4] [D=120] [REPS=3] [OUT=results/d120-png-pilot]
#
# Order across reps: rep1 wayline-first, rep2 argo-first, rep3 wayline-first.
# Avoids systematic warm-cache bias toward whichever system always runs second.
set -euo pipefail

N=${1:-4}
D=${2:-120}
FMT=${3:-png}
REPS=${4:-3}
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
REPO="$(cd "$ROOT/../.." && pwd)"
OUT="${5:-$ROOT/results/n${N}-d${D}-${FMT}-pilot}"
mkdir -p "$OUT"

CELL="n${N}-d${D}-${FMT}"
WL_TPL="vemcmt-${CELL}-heft"
ARGO_TPL="vemcmt-${CELL}-argo"

# --- render + apply both templates idempotently ---------------------------
python3 "$ROOT/wayline/render.py"  --cameras "$N" --duration "$D" --scheduler heft \
    --preprocess-fmt "$FMT" --name "$WL_TPL"  -o "/tmp/${WL_TPL}.yml"
python3 "$ROOT/argo/render.py" --cameras "$N" --duration "$D" \
    --preprocess-fmt "$FMT" --name "$ARGO_TPL" -o "/tmp/${ARGO_TPL}.yml"
kubectl apply -f "/tmp/${WL_TPL}.yml"  >/dev/null
kubectl apply -f "/tmp/${ARGO_TPL}.yml" >/dev/null

SUM="$OUT/summary.csv"
[[ -f "$SUM" ]] || echo "rep,system,run_name,phase,makespan_s,wall_s,bytes_in_total,bytes_out_total,report_md5" > "$SUM"

# --- harvest helpers ------------------------------------------------------
collect_wl_artifacts() {
    local rep=$1 run=$2 dest=$3
    mkdir -p "$dest"
    kubectl -n wl-system get odag "$run" -o yaml > "$dest/odag.yaml" 2>/dev/null
    kubectl -n wl-system get pods -l "wl.io/odag=$run" -o json \
      | python3 -c "
import json,sys
d=json.load(sys.stdin)
out=[]
for p in d['items']:
    out.append({
      'task': p['metadata'].get('labels',{}).get('wl.io/task'),
      'node': p['spec'].get('nodeName'),
      'startedAt': p['status'].get('startTime'),
      'finishedAt': p['status'].get('containerStatuses',[{}])[0].get('state',{}).get('terminated',{}).get('finishedAt'),
    })
print(json.dumps(out, indent=2))" > "$dest/placement.json"
    # Per-node flow records + metrics
    for n in 1 3 4 5 6 7 8 9; do
        da=$(kubectl -n wl-system get pod -l app=data-agent -o jsonpath="{.items[?(@.spec.nodeName==\"anrg-$n\")].metadata.name}")
        [ -z "$da" ] && continue
        kubectl -n wl-system exec "$da" -- wget -qO- "http://localhost:8081/flows/$run"  2>/dev/null > "$dest/flows-anrg-$n.json" || true
        kubectl -n wl-system exec "$da" -- wget -qO- "http://localhost:8081/metrics"     2>/dev/null > "$dest/metrics-anrg-$n.json" || true
    done
    # Sum bytes_in / bytes_out across nodes (proxy for total data-plane work).
    python3 - <<PY > "$dest/bytes-summary.txt"
import json, glob
bi = bo = 0
for f in glob.glob("$dest/metrics-anrg-*.json"):
    try:
        d=json.load(open(f))
        bi += d['transfers']['bytes_in']
        bo += d['push']['bytes_out']
    except Exception: pass
print(f'bytes_in_total={bi}')
print(f'bytes_out_total={bo}')
PY
    # Report
    kubectl run probe-rep-$rep --rm -i --restart=Never --image=busybox \
      --overrides='{"spec":{"nodeName":"anrg-9","containers":[{"name":"p","image":"busybox","command":["cat","/reports/'$run'/report.json"],"volumeMounts":[{"name":"r","mountPath":"/reports"}]}],"volumes":[{"name":"r","hostPath":{"path":"/var/lib/wl-workloads/reports"}}]}}' 2>&1 \
      | grep -v "^If you\|^warning\|^pod " | python3 -c "
import sys; t=sys.stdin.read(); b=t.find('{'); e=t.rfind('}')+1; print(t[b:e] if b>=0 else '')
" > "$dest/report.json" || true
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
    # Argo doesn't have bytes counters per se; report N/A
    echo "bytes_in_total=NA"  > "$dest/bytes-summary.txt"
    echo "bytes_out_total=NA" >> "$dest/bytes-summary.txt"
    # Report — Argo report task lands under /reports/unknown (env var bug
    # in the rendered template), so prefer that; fall back to $wf path.
    kubectl run probe-arep-$rep --rm -i --restart=Never --image=busybox \
      --overrides='{"spec":{"nodeName":"anrg-9","containers":[{"name":"p","image":"busybox","command":["sh","-c","cat /reports/'$wf'/report.json 2>/dev/null || cat /reports/unknown/report.json"],"volumeMounts":[{"name":"r","mountPath":"/reports"}]}],"volumes":[{"name":"r","hostPath":{"path":"/var/lib/wl-workloads/reports"}}]}}' 2>&1 \
      | grep -v "^If you\|^warning\|^pod " | python3 -c "
import sys; t=sys.stdin.read(); b=t.find('{'); e=t.rfind('}')+1; print(t[b:e] if b>=0 else '')
" > "$dest/report.json" || true
    # Clear the 'unknown' dir so successive reps don't pick up stale.
    sshpass -p anrg ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR anrg-9 \
        "echo anrg | sudo -S rm -rf /var/lib/wl-workloads/reports/unknown" >/dev/null 2>&1 || true
}

md5() { md5sum "$1" 2>/dev/null | awk '{print $1}'; }

run_wl() {
    local rep=$1
    local dest="$OUT/rep${rep}-wayline"
    # No delete-before-run: the CLI now uses generateName so every rep gets
    # a unique ODAG name (e.g. vemcmt-...-run-x7k4p) and the controller stamps
    # wl.io/run from the SQL counter. Prior runs accumulate in the cluster
    # until the template's retention policy cleans them up — which is what
    # the UI's template-runs tab wants to see.
    local start=$(date +%s)
    local out
    out=$("$REPO/bin/wayline" odag run $WL_TPL -n wl-system 2>&1)
    echo "$out" | tail -1
    local run
    run=$(echo "$out" | sed -nE 's|Created run ([^ ]+).*|\1|p')
    if [ -z "$run" ]; then
        echo "  ERROR: could not parse run name from CLI output: $out"
        echo "$rep,wayline,?,Failed,?,?,?,?,?" >> "$SUM"
        return 1
    fi
    for i in $(seq 1 90); do
        sleep 15
        local p=$(kubectl -n wl-system get odag "$run" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
        [ "$p" = "Succeeded" ] || [ "$p" = "Failed" ] && break
    done
    local end=$(date +%s)
    local wall=$((end - start))
    local ms=$(kubectl -n wl-system get odag "$run" -o jsonpath='{.status.makespan}' 2>/dev/null)
    local phase=$(kubectl -n wl-system get odag "$run" -o jsonpath='{.status.phase}' 2>/dev/null)
    collect_wl_artifacts "$rep" "$run" "$dest"
    local bi=$(grep bytes_in_total  "$dest/bytes-summary.txt" | cut -d= -f2)
    local bo=$(grep bytes_out_total "$dest/bytes-summary.txt" | cut -d= -f2)
    local h=$(md5 "$dest/report.json")
    echo "$rep,wayline,$run,$phase,$ms,$wall,$bi,$bo,$h" >> "$SUM"
    echo "  -> Wayline rep $rep: phase=$phase makespan=${ms}s wall=${wall}s bi=$bi bo=$bo"
    # Post-capture cleanup: delete the completed ODAG and its pods so the
    # next rep starts in a clean cluster. History is preserved in wl-history.db.
    kubectl -n wl-system delete odag "$run" --wait=false >/dev/null 2>&1 || true
}

run_argo() {
    local rep=$1
    local dest="$OUT/rep${rep}-argo"
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
    for i in $(seq 1 120); do
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
    echo "$rep,argo,$wf,$phase,$ms,$wall,NA,NA,$h" >> "$SUM"
    echo "  -> Argo rep $rep: phase=$phase makespan=${ms}s wall=${wall}s"
    # Post-capture cleanup so the next rep starts clean.
    kubectl -n argo delete workflow "$wf" --wait=false >/dev/null 2>&1 || true
}

# Wait for the cluster to actually be idle (all task pods terminated).
wait_for_idle() {
    for i in $(seq 1 30); do
        local n=$(kubectl -n wl-system get pods -l wl-odag --no-headers 2>/dev/null | grep -vE "Succeeded|Completed" | wc -l)
        local m=$(kubectl -n argo get pods --no-headers 2>/dev/null | grep -vE "Succeeded|Completed" | wc -l)
        [ "$n" = "0" ] && [ "$m" = "0" ] && return 0
        sleep 5
    done
}

for r in $(seq 1 "$REPS"); do
    echo
    echo "==================== rep $r ===================="
    wait_for_idle
    if (( r % 2 == 1 )); then
        run_wl "$r" ; run_argo "$r"
    else
        run_argo "$r" ; run_wl "$r"
    fi
done

echo
echo "##############################################"
echo "## SUMMARY  $OUT/summary.csv"
echo "##############################################"
column -t -s, < "$SUM"
echo
python3 - <<PY
import csv, statistics
rows = list(csv.DictReader(open("$SUM")))
def stats(sys_name):
    ms = [int(r['makespan_s']) for r in rows if r['system']==sys_name and r['phase']=='Succeeded' and r['makespan_s'] not in ('','?')]
    return ms
d = stats('wayline'); a = stats('argo')
if d and a:
    print(f"Wayline  reps: {d}  mean={statistics.mean(d):.1f}s std={statistics.pstdev(d):.1f}s")
    print(f"Argo reps: {a}  mean={statistics.mean(a):.1f}s std={statistics.pstdev(a):.1f}s")
    deltas = [aa-dd for aa,dd in zip(a,d)]
    print(f"Argo - Wayline deltas (per rep, paired): {deltas}")
    print(f"mean delta: {statistics.mean(deltas):+.1f}s  ({statistics.mean(deltas)/statistics.mean(a)*100:+.1f}% of Argo)")
PY
