#!/usr/bin/env bash
# Label each worker node with topology.kubernetes.io/zone so
# NetworkOverhead can compute inter-zone costs from the NetworkTopology CR.
#
# Idempotent: --overwrite ensures re-applying just updates the label.
set -euo pipefail

echo "[labels] tagging nodes with zones (edge / compute / gateway)..."
for node in anrg-1 anrg-3 anrg-4 anrg-5; do
  kubectl label node "$node" topology.kubernetes.io/zone=edge --overwrite
done
for node in anrg-6 anrg-7 anrg-8; do
  kubectl label node "$node" topology.kubernetes.io/zone=compute --overwrite
done
kubectl label node anrg-9 topology.kubernetes.io/zone=gateway --overwrite

echo ""
echo "[labels] current zone labels:"
kubectl get nodes -L topology.kubernetes.io/zone --no-headers | awk '{printf "  %-10s %s\n", $1, $NF}'
