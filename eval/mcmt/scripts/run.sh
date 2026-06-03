#!/usr/bin/env bash
#
# Drive one paired (Wayline, Argo) mcmt cell.
#
#   ./run.sh <cameras> <duration_s> <reps>
#
# For each rep, submits a Wayline run and an Argo run sequentially (cluster
# idle between each — preflight enforced), waits for completion, harvests
# their report.json files, runs the correctness diff, and appends a row
# to results/<cell>/summary.csv.
#
# CSV columns: rep, system, run_name, phase, makespan_s, wall_s, report_ok
set -euo pipefail

CAM="${1:-4}"
DUR="${2:-60}"
REPS="${3:-3}"
TIMEOUT_S="${TIMEOUT_S:-1200}"

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
REPO="$(cd "$ROOT/../.." && pwd)"

CELL="n${CAM}-d${DUR}"
OUTDIR="$ROOT/results/$CELL"
mkdir -p "$OUTDIR"
SUM="$OUTDIR/summary.csv"
[[ -f "$SUM" ]] || echo "rep,system,run_name,phase,makespan_s,wall_s,report_ok" > "$SUM"

green(){ printf '\033[32m%s\033[0m\n' "$*"; }
red()  { printf '\033[31m%s\033[0m\n' "$*" >&2; }

# Render+apply both templates once.
python3 "$ROOT/wayline/render.py" --cameras "$CAM" --duration "$DUR" --scheduler heft \
    --name "vemcmt-n${CAM}-d${DUR}-heft" -o "/tmp/wl-${CELL}.yml"
python3 "$ROOT/argo/render.py" --cameras "$CAM" --duration "$DUR" \
    --name "vemcmt-n${CAM}-d${DUR}-argo" -o "/tmp/argo-${CELL}.yml"
kubectl apply -f "/tmp/wl-${CELL}.yml" >/dev/null
kubectl apply -f "/tmp/argo-${CELL}.yml" >/dev/null

# Preflight: cluster idle. Reuse two-hop's check.
PREFLIGHT="$REPO/eval/e0-microbench/preflight-idle.sh"

wait_wl() {
    local name="$1"; local end=$(( $(date +%s) + TIMEOUT_S ))
    while [[ $(date +%s) -lt $end ]]; do
        local phase
        phase=$(kubectl -n wl-system get odag "$name" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
        case "$phase" in Succeeded|Failed) echo "$phase"; return ;; esac
        sleep 3
    done
    echo "Timeout"
}
wait_argo() {
    local name="$1"; local end=$(( $(date +%s) + TIMEOUT_S ))
    while [[ $(date +%s) -lt $end ]]; do
        local phase
        phase=$(kubectl -n argo get workflow "$name" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
        case "$phase" in Succeeded|Failed|Error) echo "$phase"; return ;; esac
        sleep 3
    done
    echo "Timeout"
}

for r in $(seq 1 "$REPS"); do
    green "==> cell $CELL rep $r/$REPS"
    "$PREFLIGHT" || { red "preflight not idle, skipping rep $r"; continue; }

    # --- Wayline leg ---
    start=$(date +%s)
    wl_name=$("$REPO/bin/wayline" odag run "vemcmt-n${CAM}-d${DUR}-heft" -n wl-system \
        | sed -nE 's/^Created run ([^ ]+).*/\1/p')
    wl_phase=$(wait_wl "$wl_name")
    wl_wall=$(( $(date +%s) - start ))
    wl_makespan=$(kubectl -n wl-system get odag "$wl_name" -o jsonpath='{.status.makespan}' 2>/dev/null || echo "")
    cp -f /var/lib/wl-workloads/reports/$wl_name/report.json "$OUTDIR/wl-rep${r}.json" 2>/dev/null \
        || cp -f /shared/wl-outputs/reports/$wl_name/report.json "$OUTDIR/wl-rep${r}.json" 2>/dev/null \
        || true

    # --- Argo leg ---
    start=$(date +%s)
    argo_name=$(kubectl -n argo create -f <(cat <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: vemcmt-n${CAM}-d${DUR}-argo-
  namespace: argo
spec:
  workflowTemplateRef:
    name: vemcmt-n${CAM}-d${DUR}-argo
EOF
) | awk '{print $1}' | sed 's|workflow.argoproj.io/||')
    argo_phase=$(wait_argo "$argo_name")
    argo_wall=$(( $(date +%s) - start ))
    cp -f /var/lib/wl-workloads/reports/$argo_name/report.json "$OUTDIR/argo-rep${r}.json" 2>/dev/null || true

    # --- Correctness diff ---
    report_ok="?"
    if [[ -f "$OUTDIR/wl-rep${r}.json" && -f "$OUTDIR/argo-rep${r}.json" ]]; then
        if python3 "$HERE/verify_reports.py" \
                "$OUTDIR/wl-rep${r}.json" "$OUTDIR/argo-rep${r}.json" > "$OUTDIR/diff-rep${r}.log" 2>&1; then
            report_ok=true
        else
            report_ok=false
        fi
    fi

    echo "${r},wayline,${wl_name},${wl_phase},${wl_makespan:-?},${wl_wall},${report_ok}" >> "$SUM"
    echo "${r},argo,${argo_name},${argo_phase},?,${argo_wall},${report_ok}" >> "$SUM"
    green "rep $r done: wayline=${wl_phase}/${wl_makespan:-?}s argo=${argo_phase}/${argo_wall}s diff=${report_ok}"
done

green "Done. Summary in $SUM"
