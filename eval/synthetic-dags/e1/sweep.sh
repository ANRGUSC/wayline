#!/usr/bin/env bash
# Top-level orchestrator for the E1 sweep.
# Order: Wayline iobt/hetero/wpf, then Argo iobt/hetero/wpf, 20 reps each.
set -euo pipefail

E1_DIR="$(cd "$(dirname "$0")" && pwd)"
N="${N:-20}"

date_label="$(date +'%Y%m%d-%H%M%S')"
ALL_LOG="${E1_DIR}/results/sweep-${date_label}.log"

echo "================================================================"
echo " E1 sweep starting at $(date)"
echo " N=$N reps per (system,benchmark) cell"
echo " 6 cells × $N runs = $((6 * N)) runs total"
echo "================================================================"

for sys in wayline argo; do
  for bm in iobt hetero wpf; do
    echo ""
    echo "############################################"
    echo "# $sys / $bm"
    echo "############################################"
    N=$N "${E1_DIR}/run.sh" "$sys" "$bm" "$N"
  done
done

echo ""
echo "================================================================"
echo " E1 sweep done at $(date)"
echo "================================================================"
echo ""
echo "[sweep] done."
