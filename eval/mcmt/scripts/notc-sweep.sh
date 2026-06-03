#!/usr/bin/env bash
#
# No-tc paired sweep: re-run the 4 paper MCMT cells (Wayline vs Argo) on the
# unshaped 1Gbps LAN, after the tc HTB matrix was torn down (2026-05-21).
#
# Reuses the converged profiler runtimes (template names match the DB keys
# vemcmt-n4-dD-FMT-heft), so HEFT placement converges from rep 1.
#
#   REPS=6 ./notc-sweep.sh
#
# Results land under results/notc-<cell>/ so the tc-era paper pilots
# (results/n4-*-pilot) are not clobbered.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
REPS="${REPS:-6}"

# cell = "N D FMT"
CELLS=(
  "4 30 jpg"
  "4 60 jpg"
  "4 120 jpg"
  "4 120 png"
)

for cell in "${CELLS[@]}"; do
  read -r N D FMT <<<"$cell"
  OUT="$ROOT/results/notc-n${N}-d${D}-${FMT}"
  mkdir -p "$OUT"
  echo "############################################################"
  echo "# NO-TC cell N=$N D=$D FMT=$FMT REPS=$REPS  -> $OUT"
  echo "############################################################"
  "$HERE/pilot-paired.sh" "$N" "$D" "$FMT" "$REPS" "$OUT"
done

echo "ALL NO-TC CELLS DONE"
