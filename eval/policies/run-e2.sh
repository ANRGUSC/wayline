#!/usr/bin/env bash
# E2 campaign: expressiveness. 6 schedulers x 3 freed workloads x N runs.
# Requires the shaped matrix (same condition as E1-shaped / E3).
set -uo pipefail
cd "$(dirname "$0")"
ROOT=$(cd ../.. && pwd)
NS=wl-system
N=${N:-20}
RES=${RES:-$HOME/e2-results}
REPEAT=$ROOT/eval/scripts/benchmarks/repeat-template.sh
SCHED=$ROOT/eval/synthetic-dags/scheduler
mkdir -p "$RES"

# Shaped-network guard (same as run-e3.sh).
q=$(kubectl run tc-check --rm -i --restart=Never -n "$NS" --image=alpine \
    --overrides='{"spec":{"nodeName":"anrg-9","hostNetwork":true,"containers":[{"name":"tc-check","image":"alpine","command":["sh","-c","apk add -q iproute2 >/dev/null && tc qdisc show"],"securityContext":{"privileged":true}}]}}' \
    2>/dev/null)
echo "$q" | grep -q htb || { echo "ABORT: network is not shaped. Run setup-tc-matrix.sh first."; exit 1; }

# User scheduler: ship datagravity.py to the sidecar's ConfigMap mount.
kubectl -n "$NS" create configmap wl-user-schedulers \
  --from-file=schedulers/datagravity.py \
  -o yaml --dry-run=client | kubectl apply -f - >/dev/null
echo "wl-user-schedulers ConfigMap applied; waiting for mount propagation"
sleep 70   # kubelet configmap sync period

kubectl delete odags -n "$NS" --all --ignore-not-found >/dev/null 2>&1
sleep 3

arm() { # <arm-name> <template-file> <template-name>
  local arm=$1 tpl=$2 name=$3
  echo "=== ARM $arm ($name) start $(date +%F' '%T) ==="
  kubectl apply -f "$tpl" >/dev/null
  "$SCHED/reset-profiler.sh" >/dev/null 2>&1 || true
  sleep 8
  "$REPEAT" "$name" "$N" "$NS" 420 2>&1 | tee "$RES/$arm.log"
  kubectl get odags -n "$NS" -l wl.io/template="$name" -o json > "$RES/$arm.json"
  kubectl delete odags -n "$NS" -l wl.io/template="$name" --ignore-not-found >/dev/null 2>&1
  echo "=== ARM $arm done $(date +%F' '%T) ==="
}

for f in e2/e2-*.yml; do
  name=$(basename "$f" .yml)
  arm "$name" "$f" "$name"
done
echo "E2 CAMPAIGN DONE $(date +%F' '%T)"
