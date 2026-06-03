#!/usr/bin/env bash
#
# E0 top-level orchestrator. Calls preflight, runs the Wayline sweep, runs
# the MinIO sweep, harvests, plots.
#
# Env knobs:
#   N        reps per cell (default 20; SMOKE=1 forces 2)
#   SMOKE    smoke mode — N=2, ONLY="same-10mb"
#   ONLY     cell tag filter (e.g. ONLY="same-10mb cross-100mb")
#
set -euo pipefail

E0_DIR="$(cd "$(dirname "$0")" && pwd)"

if [[ "${SMOKE:-0}" == "1" && -z "${ONLY:-}" ]]; then
  export ONLY="same-10mb"
fi

echo "================================================================"
echo " E0 sweep starting"
echo "   SMOKE=${SMOKE:-0}"
echo "   ONLY=${ONLY:-<all>}"
echo "   N=${N:-20}"
echo "================================================================"

# Preflight at the outermost level too; drivers will re-check per cell.
"${E0_DIR}/preflight-idle.sh"

echo ""
echo "[sweep] running Wayline cells..."
"${E0_DIR}/wayline/run.sh"

echo ""
echo "[sweep] running MinIO cells..."
"${E0_DIR}/minio/run.sh"

echo ""
echo "[sweep] harvesting timestamps..."
python3 "${E0_DIR}/harvest.py" "${E0_DIR}/results"

echo ""
echo "[sweep] plotting..."
python3 "${E0_DIR}/plot.py"

echo ""
echo "[sweep] done. See:"
echo "   ${E0_DIR}/results/all.csv"
echo "   ${E0_DIR}/figures/e0-e2e.png"
echo "   ${E0_DIR}/figures/e0-decomposition.png"
echo "   ${E0_DIR}/figures/e0-summary.md"
