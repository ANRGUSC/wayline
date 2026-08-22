#!/usr/bin/env bash
# E3 campaign: checkpoint-count sweep (mixed DAGs), N runs per arm.
# Endpoints k=0 and k=all come from the E1 shaped campaign, so this MUST run
# under the same shaped matrix. The guard below refuses to start on an
# unshaped network.
set -uo pipefail
cd "$(dirname "$0")"
ROOT=$(cd ../.. && pwd)
NS=wl-system
N=${N:-20}
RES=${RES:-$HOME/e3-results}
REPEAT=$ROOT/eval/scripts/benchmarks/repeat-template.sh
SCHED=$ROOT/eval/synthetic-dags/scheduler
mkdir -p "$RES"

# Shaped-network guard: anrg-9's root qdisc must be htb (matrix applied).
q=$(kubectl run tc-check --rm -i --restart=Never -n "$NS" --image=alpine \
    --overrides='{"spec":{"nodeName":"anrg-9","hostNetwork":true,"containers":[{"name":"tc-check","image":"alpine","command":["sh","-c","apk add -q iproute2 >/dev/null && tc qdisc show | head -2"],"securityContext":{"privileged":true}}]}}' \
    2>/dev/null | head -2)
echo "$q" | grep -q htb || { echo "ABORT: network is not shaped (no htb qdisc). Run setup-tc-matrix.sh first."; exit 1; }
echo "shaped network confirmed: $q"

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

arm hetero-ckpt1 "hetero-compute-ckpt1.yml" hetero-heft-ckpt1
arm hetero-ckpt2 "hetero-compute-ckpt2.yml" hetero-heft-ckpt2
arm wpf-ckpt1    "wpf-ckpt1.yml"            wpf-heft-ckpt1
arm wpf-ckpt3    "wpf-ckpt3.yml"            wpf-heft-ckpt3
arm wpf-ckpt5    "wpf-ckpt5.yml"            wpf-heft-ckpt5
echo "E3 CAMPAIGN DONE $(date +%F' '%T)"
