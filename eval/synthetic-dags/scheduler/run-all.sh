#!/usr/bin/env bash
#
# Master driver: sweeps all three ODAGs with the full config set.
# Cleans the cluster between ODAGs so each sweep starts from zero.
#
# Usage:  ./run-all.sh
#
# Override via env:
#   N_RUNS         runs per config (default 20)
#   IOBT_CONFIGS   configs for iobt (default: "random heft heft-eps05 heft-eps heft-eps20")
#   OTHER_CONFIGS  configs for hetero-compute / wide-pipeline-flex
#                  (default: "random heft heft-eps")
set -euo pipefail

EVAL="$(cd "$(dirname "$0")" && pwd)"
N_RUNS="${N_RUNS:-20}"
IOBT_CONFIGS="${IOBT_CONFIGS:-random heft heft-eps05 heft-eps heft-eps20}"
OTHER_CONFIGS="${OTHER_CONFIGS:-random heft heft-eps}"

overall_start=$(date +%s)
echo "################################################################"
echo "#  Full network-aware ODAG benchmark sweep"
echo "#  $(date)"
echo "#  Runs per config: $N_RUNS"
echo "################################################################"

for odag in iobt hetero-compute wide-pipeline-flex; do
  echo ""
  echo "================================================================"
  echo " Cleaning cluster before $odag sweep"
  echo "================================================================"
  "$EVAL/cleanup-cluster.sh"

  if [[ "$odag" == "iobt" ]]; then
    CONFIGS="$IOBT_CONFIGS" "$EVAL/sweep-scheduler.sh" "$odag" "$N_RUNS"
  else
    CONFIGS="$OTHER_CONFIGS" "$EVAL/sweep-scheduler.sh" "$odag" "$N_RUNS"
  fi
done

overall_wall=$(( $(date +%s) - overall_start ))
echo ""
echo "################################################################"
echo "#  All sweeps complete in ${overall_wall}s ($(( overall_wall / 60 )) min)"
echo "#  $(date)"
echo "################################################################"

echo ""
echo "Summary tables:"
for odag in iobt hetero-compute wide-pipeline-flex; do
  echo ""
  echo "=== $odag ==="
  for cfg_dir in "$EVAL/results/$odag"/*/; do
    cfg=$(basename "$cfg_dir")
    n=$(tail -n +2 "$cfg_dir/summary.csv" 2>/dev/null | wc -l)
    mean=$(awk -F, 'NR>1 && $4 != "" && $4 != "?" {sum+=$4; n++} END {if(n) printf "%.2f", sum/n}' "$cfg_dir/summary.csv")
    echo "  $cfg: n=$n  mean=${mean}s"
  done
done

echo ""
echo "Next: python3 $EVAL/plot-results.py"
