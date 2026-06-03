#!/usr/bin/env bash
#
# E0 cluster-idle gate. Exits 0 only if the cluster is in a state where
# a benchmark cell can run without contention:
#
#   1. No active ODAG/CDAG resources (Pending / Scheduling / Running).
#   2. No task pods (labels wl-odag, wl-cdag) in any namespace.
#   3. No two-hop microbenchmark Jobs (label app=two-hop) anywhere.
#   4. No leftover Services for two-hop runs.
#
# Drivers MUST call this before every cell, not just at sweep start, so
# that a half-cleaned cell from a previous iteration does not poison the
# next measurement.
#
# Usage:
#   ./preflight-idle.sh            # check, exit 0 if idle, 1 otherwise
#   FORCE_CLEAN=1 ./preflight-idle.sh   # try to clean anything found, then re-check
#
# Exit codes:
#   0  cluster is idle
#   1  cluster has active work (and FORCE_CLEAN failed to drain it)
#   2  prerequisite missing (controllers down, kubectl unreachable)

set -euo pipefail

WL_NS=${WL_NS:-wl-system}
E0_NS=${E0_NS:-e0-bench}
TIMEOUT=${TIMEOUT:-90}

red()   { printf '\033[31m%s\033[0m\n' "$*" >&2; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
yellow(){ printf '\033[33m%s\033[0m\n' "$*"; }

# --- prerequisite checks ---

if ! kubectl get nodes >/dev/null 2>&1; then
  red "[preflight] kubectl cannot reach the cluster"
  exit 2
fi

# Controllers should be healthy. Don't gate on UI/ui-server.
for app in odag-controller data-agent; do
  ready=$(kubectl get pods -n "$WL_NS" -l "app=$app" -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
  if [[ -z "$ready" ]]; then
    red "[preflight] no pods for app=$app in $WL_NS — controller stack is not deployed"
    exit 2
  fi
  if grep -q False <<<"$ready"; then
    red "[preflight] $app has non-ready pods (statuses: $ready)"
    exit 2
  fi
done

# --- idle checks ---

check_idle() {
  local fail=0

  # 1. Non-terminal ODAGs / CDAGs anywhere.
  local non_terminal
  non_terminal=$(kubectl get odags.wl.io -A -o json 2>/dev/null \
    | python3 -c '
import json, sys
d = json.load(sys.stdin)
out = []
for item in d.get("items", []):
    phase = (item.get("status") or {}).get("phase", "Unknown")
    if phase in ("Pending", "Scheduling", "Running", "Unknown"):
        out.append(f"{item['metadata']['namespace']}/{item['metadata']['name']}:{phase}")
print(",".join(out))
' 2>/dev/null || true)
  if [[ -n "$non_terminal" ]]; then
    yellow "[preflight] non-terminal ODAGs: $non_terminal"
    fail=1
  fi

  local non_terminal_cdag
  non_terminal_cdag=$(kubectl get cdags.wl.io -A --no-headers 2>/dev/null | awk '{print $1"/"$2":"$3}' | grep -v 'Succeeded\|Failed' || true)
  if [[ -n "$non_terminal_cdag" ]]; then
    # CDAGs are always-on by design — flag but allow opt-in to proceed via ALLOW_CDAG=1.
    if [[ "${ALLOW_CDAG:-0}" == "1" ]]; then
      yellow "[preflight] CDAGs present (allowed via ALLOW_CDAG=1): $non_terminal_cdag"
    else
      yellow "[preflight] CDAGs present (set ALLOW_CDAG=1 to ignore): $non_terminal_cdag"
      fail=1
    fi
  fi

  # 2. Non-terminal ODAG task pods (Pending/Running). Completed/Failed
  #    pods that haven't been garbage-collected do not contend for
  #    resources and are ignored.
  local task_pods
  task_pods=$(kubectl get pods -A -l wl-odag --no-headers --field-selector=status.phase!=Succeeded,status.phase!=Failed 2>/dev/null | wc -l)
  if [[ "$task_pods" != "0" ]]; then
    yellow "[preflight] $task_pods non-terminal ODAG task pods alive"
    fail=1
  fi

  # 3. Two-hop benchmark Jobs/pods/services — exclude the MinIO
  #    infrastructure (component=minio) which is meant to stay up
  #    between cells.
  local sel='app=two-hop,component notin (minio)'

  local twohop_jobs
  twohop_jobs=$(kubectl get jobs -A -l "$sel" --no-headers 2>/dev/null | wc -l)
  if [[ "$twohop_jobs" != "0" ]]; then
    yellow "[preflight] $twohop_jobs two-hop benchmark Jobs still alive"
    fail=1
  fi

  local twohop_pods
  twohop_pods=$(kubectl get pods -A -l "$sel" --no-headers --field-selector=status.phase!=Succeeded,status.phase!=Failed 2>/dev/null | wc -l)
  if [[ "$twohop_pods" != "0" ]]; then
    yellow "[preflight] $twohop_pods non-terminal two-hop benchmark pods alive"
    fail=1
  fi

  local twohop_svcs
  twohop_svcs=$(kubectl get svc -A -l "$sel" --no-headers 2>/dev/null | wc -l)
  if [[ "$twohop_svcs" != "0" ]]; then
    yellow "[preflight] $twohop_svcs two-hop benchmark services still alive"
    fail=1
  fi

  return $fail
}

if check_idle; then
  green "[preflight] cluster is idle — OK to start a cell"
  exit 0
fi

if [[ "${FORCE_CLEAN:-0}" != "1" ]]; then
  red "[preflight] cluster not idle (rerun with FORCE_CLEAN=1 to attempt cleanup)"
  exit 1
fi

# --- force-clean path ---

yellow "[preflight] FORCE_CLEAN=1 — attempting to drain..."

kubectl delete odags.wl.io -A --all --ignore-not-found --wait=false >/dev/null 2>&1 || true
kubectl delete pods -A -l wl-odag --ignore-not-found --wait=false >/dev/null 2>&1 || true
kubectl delete jobs -A -l app=two-hop --ignore-not-found --wait=false >/dev/null 2>&1 || true
kubectl delete pods -A -l app=two-hop --ignore-not-found --wait=false >/dev/null 2>&1 || true
kubectl delete svc -A -l app=two-hop --ignore-not-found --wait=false >/dev/null 2>&1 || true

# Wait up to TIMEOUT seconds for everything to drain.
end=$(( $(date +%s) + TIMEOUT ))
while [[ $(date +%s) -lt $end ]]; do
  if check_idle 2>/dev/null; then
    green "[preflight] cluster drained after force-clean"
    exit 0
  fi
  sleep 2
done

red "[preflight] still not idle after ${TIMEOUT}s of force-clean — manual intervention needed"
check_idle || true
exit 1
