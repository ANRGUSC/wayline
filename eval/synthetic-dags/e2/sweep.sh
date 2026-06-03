#!/usr/bin/env bash
# E2 orchestrator: run scheduler-plugins setup once, then sweep all 3
# benchmarks. Designed to be called by the E1->E2 chain watcher.
set -euo pipefail

E2_DIR="$(cd "$(dirname "$0")" && pwd)"
N="${N:-20}"

echo "================================================================"
echo " E2 sweep starting at $(date)"
echo "================================================================"

echo "[E2] applying scheduler-plugins + CRDs + node labels + AppGroups + workflows..."
"${E2_DIR}/setup.sh"

echo ""
echo "[E2] smoke-test: one workflow of each benchmark"
for bm in iobt hetero wpf; do
  echo ""
  echo "------- smoke $bm -------"
  N=1 "${E2_DIR}/run.sh" "$bm" 1
done

echo ""
echo "[E2] full sweep: N=$N reps × 3 benchmarks"
for bm in iobt hetero wpf; do
  echo ""
  echo "############################################"
  echo "# e2 / $bm"
  echo "############################################"
  N=$N "${E2_DIR}/run.sh" "$bm" "$N"
done

echo ""
echo "================================================================"
echo " E2 sweep done at $(date)"
echo "================================================================"
echo ""
echo "[sweep] done."
