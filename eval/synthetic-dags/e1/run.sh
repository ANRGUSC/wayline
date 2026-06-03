#!/usr/bin/env bash
#
# E1 sweep driver.
#
#   ./run.sh <argo|wayline> <iobt|hetero|wpf> [N]
#
# Submits N runs sequentially. Captures makespan + wall-clock per run.
# Aggressive cleanup between runs so the cluster + MinIO bucket stay clean.
#
set -euo pipefail

E1_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$E1_DIR/../.." && pwd)"
Wayline="${REPO_ROOT}/bin/wayline"

SYS="${1:?usage: run.sh <argo|wayline> <iobt|hetero|wpf> [N]}"
BM="${2:?usage: run.sh <argo|wayline> <iobt|hetero|wpf> [N]}"
N="${3:-20}"
TIMEOUT="${TIMEOUT:-300}"

RESULTS_DIR="${E1_DIR}/results/${SYS}/${BM}"
mkdir -p "$RESULTS_DIR"

green(){ printf '\033[32m%s\033[0m\n' "$*"; }
yellow(){ printf '\033[33m%s\033[0m\n' "$*"; }
red(){ printf '\033[31m%s\033[0m\n' "$*" >&2; }

case "$SYS" in
  argo)
    case "$BM" in
      iobt)   TPL=e1-iobt    ;;
      hetero) TPL=e1-hetero  ;;
      wpf)    TPL=e1-wpf     ;;
      *) red "unknown bench"; exit 2 ;;
    esac
    ;;
  wayline)
    case "$BM" in
      iobt)   TPL=iobt-heft   ; ODAG_DIR=iobt ;;
      hetero) TPL=hetero-heft ; ODAG_DIR=hetero-compute ;;
      wpf)    TPL=wpf-heft    ; ODAG_DIR=wide-pipeline-flex ;;
      *) red "unknown bench"; exit 2 ;;
    esac
    # Make sure the Wayline template is applied (idempotent).
    kubectl apply -f "${REPO_ROOT}/eval/synthetic-dags/scheduler/${ODAG_DIR}/template-heft.yml" >/dev/null
    ;;
  *)
    red "unknown system: $SYS"
    exit 2
    ;;
esac

# Preflight: cluster idle (no in-flight workflows or ODAGs).
preflight() {
  local nondone_wf
  nondone_wf=$(kubectl -n argo get workflows -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null \
               | grep -vE '^(Succeeded|Failed|Error)$' | grep -v '^$' | wc -l)
  if (( nondone_wf > 0 )); then
    yellow "  preflight: $nondone_wf non-terminal workflows alive"
    return 1
  fi
  local nondone_odag
  nondone_odag=$(kubectl -A get odags.wl.io -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null \
                 | grep -vE '^(Succeeded|Failed)$' | grep -v '^$' | wc -l)
  if (( nondone_odag > 0 )); then
    yellow "  preflight: $nondone_odag non-terminal ODAGs alive"
    return 1
  fi
  return 0
}

# Aggressive cleanup between runs: delete completed workflows, purge MinIO
# bucket, delete old terminal ODAGs.
cleanup() {
  kubectl -n argo delete workflows --all --field-selector='status.phase=Succeeded' --wait=false >/dev/null 2>&1 || true
  kubectl -n argo delete workflows --all --field-selector='status.phase=Failed'    --wait=false >/dev/null 2>&1 || true
  kubectl -n wl-system delete odags.wl.io --all --field-selector='status.phase=Succeeded' --wait=false >/dev/null 2>&1 || true
  # Quick mc rm of all objects under argo-bench/. Background, ignore failures.
  kubectl -n e0-bench exec deploy/minio -- sh -c '
    mc alias set local http://minio:9000 e0admin e0adminpw >/dev/null 2>&1
    mc rm --force --recursive local/argo-bench 2>/dev/null || true
  ' >/dev/null 2>&1 || true
}

run_argo() {
  local i=$1
  local out name
  out=$(kubectl -n argo create -f <(cat <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: e1-${BM}-r${i}-
  namespace: argo
spec:
  workflowTemplateRef:
    name: ${TPL}
EOF
) 2>&1 | tail -1)
  name=$(echo "$out" | sed -nE 's|.*workflow.argoproj.io/(.+) created.*|\1|p')
  if [[ -z "$name" ]]; then
    red "[$i] submit failed: $out"
    return 1
  fi

  local end=$(( $(date +%s) + TIMEOUT ))
  while [[ $(date +%s) -lt $end ]]; do
    local phase
    phase=$(kubectl -n argo get workflow "$name" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    case "$phase" in
      Succeeded|Failed|Error) break ;;
    esac
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

run_wl() {
  local i=$1
  local out name
  out=$("$Wayline" run "$TPL" -n wl-system 2>&1)
  name=$(echo "$out" | sed -nE 's/^Created run ([^ ]+).*/\1/p')
  if [[ -z "$name" ]]; then red "[$i] submit failed"; return 1; fi

  local end=$(( $(date +%s) + TIMEOUT ))
  local start=$(date +%s)
  while [[ $(date +%s) -lt $end ]]; do
    local phase
    phase=$(kubectl -n wl-system get odags.wl.io "$name" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    case "$phase" in
      Succeeded|Failed) break ;;
    esac
    sleep 2
  done
  local makespan=$(kubectl -n wl-system get odags.wl.io "$name" -o jsonpath='{.status.makespan}' 2>/dev/null)
  local wall=$(( $(date +%s) - start ))
  echo "${i},${name},${phase:-Timeout},${makespan:-?},${wall}"
}

summary="${RESULTS_DIR}/summary.csv"
echo "iteration,run_name,phase,makespan,wall_s" > "$summary"

green ""
green "==============================="
green "E1 sweep: ${SYS} / ${BM}  N=${N}"
green "==============================="

for i in $(seq 1 "$N"); do
  if ! preflight; then
    yellow "[$i] preflight not idle — forcing cleanup"
    cleanup
    sleep 5
  fi

  start=$(date +%s)
  if [[ "$SYS" == "argo" ]]; then
    row=$(run_argo "$i")
  else
    row=$(run_wl "$i")
  fi
  elapsed=$(( $(date +%s) - start ))
  echo "[$i/$N] $row  (cell-elapsed=${elapsed}s)"
  echo "$row" >> "$summary"

  # Between-run cleanup to keep MinIO bucket bounded.
  cleanup
done

green "Done. Results in: $summary"
