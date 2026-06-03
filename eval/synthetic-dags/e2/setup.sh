#!/usr/bin/env bash
# Idempotent one-time setup for E2: scheduler-plugins, CRDs, node labels,
# AppGroups, NetworkTopology, and the WorkflowTemplates.
#
# Apply this AFTER E1 completes — it doesn't interfere with anything
# running, but doing it before E1 is done is unnecessary risk.
set -euo pipefail

E2_DIR="$(cd "$(dirname "$0")" && pwd)"
green(){ printf '\033[32m%s\033[0m\n' "$*"; }
yellow(){ printf '\033[33m%s\033[0m\n' "$*"; }
red(){ printf '\033[31m%s\033[0m\n' "$*" >&2; }

green "=== 1. Install scheduler-plugins CRDs ==="
kubectl apply -f "${E2_DIR}/install/crds.yml" 2>&1 | tail -5

green ""
green "=== 2. Install scheduler-plugins controller (manages AppGroup CRs) ==="
kubectl apply -f "${E2_DIR}/install/scheduler-plugins.yml" 2>&1 | tail -5

green ""
green "=== 3. Install scheduler-plugins-scheduler (the actual scheduler) ==="
kubectl apply -f "${E2_DIR}/install/scheduler.yml" 2>&1 | tail -5

green ""
green "=== 4. Wait for scheduler-plugins pods to be ready ==="
kubectl -n scheduler-plugins wait --for=condition=Available deploy --all --timeout=120s 2>&1

green ""
green "=== 5. Label nodes with topology zones ==="
"${E2_DIR}/topology/node-labels.sh"

green ""
green "=== 6. Apply NetworkTopology CR ==="
kubectl apply -f "${E2_DIR}/topology/network-topology.yml" 2>&1 | tail -3

green ""
green "=== 7. Apply AppGroups for all 3 benchmarks ==="
kubectl apply -f "${E2_DIR}/appgroups/iobt.yml" -f "${E2_DIR}/appgroups/hetero.yml" -f "${E2_DIR}/appgroups/wpf.yml" 2>&1 | tail -5

green ""
green "=== 8. Apply WorkflowTemplates ==="
kubectl apply -f "${E2_DIR}/workflows/iobt.yml" -f "${E2_DIR}/workflows/hetero-compute.yml" -f "${E2_DIR}/workflows/wpf.yml" 2>&1 | tail -5

green ""
green "=== setup done ==="
echo ""
echo "scheduler-plugins pods:"
kubectl -n scheduler-plugins get pods --no-headers 2>&1
echo ""
echo "Node zones:"
kubectl get nodes -L topology.kubernetes.io/zone --no-headers 2>&1 | awk '{printf "  %-10s %s\n", $1, $NF}'
