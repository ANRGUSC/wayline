#!/usr/bin/env bash
# E2 sweep driver. Same pattern as eval/synthetic-dags/e1/run.sh — submit
# a workflow N times, capture makespan, clean up between runs.
#
#   ./run.sh <iobt|hetero|wpf> [N]
set -euo pipefail

E2_DIR="$(cd "$(dirname "$0")" && pwd)"
BM="${1:?usage: run.sh <iobt|hetero|wpf> [N]}"
N="${2:-20}"
TIMEOUT="${TIMEOUT:-360}"   # E2 is slower than E1 due to scheduler latency

case "$BM" in
  iobt)   TPL=e2-iobt    ;;
  hetero) TPL=e2-hetero  ;;
  wpf)    TPL=e2-wpf     ;;
  *) echo "unknown bench" >&2; exit 2 ;;
esac

RESULTS_DIR="${E2_DIR}/results/${BM}"
mkdir -p "$RESULTS_DIR"

green(){ printf '\033[32m%s\033[0m\n' "$*"; }
yellow(){ printf '\033[33m%s\033[0m\n' "$*"; }

preflight() {
  local nondone
  nondone=$(kubectl -n argo get workflows -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null \
            | grep -vE '^(Succeeded|Failed|Error)$' | grep -v '^$' | wc -l)
  if (( nondone > 0 )); then
    yellow "  preflight: $nondone non-terminal workflows alive"
    return 1
  fi
  return 0
}

cleanup() {
  kubectl -n argo delete workflows --all --field-selector='status.phase=Succeeded' --wait=false >/dev/null 2>&1 || true
  kubectl -n argo delete workflows --all --field-selector='status.phase=Failed'    --wait=false >/dev/null 2>&1 || true
  kubectl -n e0-bench exec deploy/minio -- sh -c '
    mc alias set local http://minio:9000 e0admin e0adminpw >/dev/null 2>&1
    mc rm --force --recursive local/argo-bench 2>/dev/null || true
  ' >/dev/null 2>&1 || true
}

run_one() {
  local i=$1
  local out name
  out=$(kubectl -n argo create -f <(cat <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: e2-${BM}-r${i}-
  namespace: argo
spec:
  workflowTemplateRef:
    name: ${TPL}
EOF
) 2>&1 | tail -1)
  name=$(echo "$out" | sed -nE 's|.*workflow.argoproj.io/(.+) created.*|\1|p')
  if [[ -z "$name" ]]; then echo "$i,,,SubmitFailed,?,?"; return 1; fi

  local end=$(( $(date +%s) + TIMEOUT ))
  while [[ $(date +%s) -lt $end ]]; do
    local phase
    phase=$(kubectl -n argo get workflow "$name" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    case "$phase" in Succeeded|Failed|Error) break ;; esac
    sleep 2
  done

  local s e ms
  s=$(kubectl -n argo get workflow "$name" -o jsonpath='{.status.startedAt}' 2>/dev/null)
  e=$(kubectl -n argo get workflow "$name" -o jsonpath='{.status.finishedAt}' 2>/dev/null)
  ms=$(python3 -c "
from datetime import datetime
try:
  s='${s}'.replace('Z','+00:00'); e='${e}'.replace('Z','+00:00')
  print(int((datetime.fromisoformat(e)-datetime.fromisoformat(s)).total_seconds()))
except Exception:
  print('?')" 2>/dev/null)
  echo "${i},${name},${phase:-Timeout},${ms},${ms}"
}

summary="${RESULTS_DIR}/summary.csv"
echo "iteration,run_name,phase,makespan,wall_s" > "$summary"

green ""
green "==============================="
green "E2 sweep: ${BM}  N=${N}"
green "==============================="

for i in $(seq 1 "$N"); do
  if ! preflight; then
    yellow "[$i] preflight not idle — forcing cleanup"
    cleanup
    sleep 5
  fi
  row=$(run_one "$i")
  echo "[$i/$N] $row"
  echo "$row" >> "$summary"
  cleanup
done

green "Done. Results: $summary"
