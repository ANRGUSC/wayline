#!/usr/bin/env bash
# E1 campaign: direct vs store-mediated data plane, N runs per arm, one
# substrate version throughout.
#
# Arms:
#   *-direct     the three benchmarks on the direct data plane (re-run so
#                every arm shares the same controller/agent build and, for
#                iot, the fixed fuse/report images)
#   *-store      all-store augmentation (Argo-mode; data vertices, no pods)
#   hetero-store-pod  same physical DAG, vertices as passthrough containers
#                     (E6: the pod-dispatch overhead arm)
#
# Run on the cluster head (root kubeconfig), inside tmux:
#   sudo N=20 ./run-e1.sh
set -uo pipefail
cd "$(dirname "$0")"
ROOT=$(cd ../.. && pwd)
NS=wl-system
N=${N:-20}
RES=${RES:-$HOME/e1-results}
REPEAT=$ROOT/eval/scripts/benchmarks/repeat-template.sh
SCHED=$ROOT/eval/synthetic-dags/scheduler
mkdir -p "$RES"

echo "E1: $N runs/arm, results -> $RES"
# Clean slate: no leftover runs from smokes or earlier arms.
kubectl delete odags -n "$NS" --all --ignore-not-found >/dev/null 2>&1
sleep 3

arm() { # <arm-name> <template-file> <template-name>
  local arm=$1 tpl=$2 name=$3
  echo "=== ARM $arm ($name) start $(date +%F' '%T) ==="
  kubectl apply -f "$tpl" >/dev/null
  # Fresh profiler per arm so EMA state from one arm never leaks into the
  # next (same hygiene as sweep-scheduler.sh). Restarts the controller.
  "$SCHED/reset-profiler.sh" >/dev/null 2>&1 || true
  sleep 8
  "$REPEAT" "$name" "$N" "$NS" 420 2>&1 | tee "$RES/$arm.log"
  kubectl get odags -n "$NS" -l wl.io/template="$name" -o json > "$RES/$arm.json"
  kubectl delete odags -n "$NS" -l wl.io/template="$name" \
      --ignore-not-found >/dev/null 2>&1
  echo "=== ARM $arm done $(date +%F' '%T) ==="
}

arm iot-direct       "$SCHED/iot/template-instr.yml"                 iot-heft-instr
arm hetero-direct    "$SCHED/hetero-compute/template-instr.yml"      hetero-heft-instr
arm wpf-direct       "$SCHED/wide-pipeline-flex/template-instr.yml"  wpf-heft-instr
arm iot-store        "iot-store.yml"                                 iot-heft-store
arm hetero-store     "hetero-compute-store.yml"                      hetero-heft-store
arm wpf-store        "wide-pipeline-flex-store.yml"                  wpf-heft-store
arm hetero-store-pod "hetero-compute-store-pod.yml"                  hetero-heft-store-pod

echo "E1 CAMPAIGN DONE $(date +%F' '%T)"
