#!/usr/bin/env bash
#
# Run the paired-rep curve across {(D, fmt)} cells × N reps each.
#
#   ./pilot-matrix.sh [REPS=3]
#
# Cells: D=30 jpg, D=60 jpg, D=120 jpg, D=120 png  (all N=4)
# Each cell's results land in results/<cell>-pilot/. After all cells
# finish, an aggregate curve.csv ties bytes_in_total to makespan and
# Argo↔Wayline deltas — that's the figure for the paper.
set -euo pipefail

REPS=${1:-3}
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"

declare -a CELLS=(
    "4 30  jpg"
    "4 60  jpg"
    "4 120 jpg"
    "4 120 png"
)

for cell in "${CELLS[@]}"; do
    read -r N D FMT <<< "$cell"
    echo
    echo "##############################################"
    echo "## cell: N=$N D=$D fmt=$FMT  reps=$REPS"
    echo "##############################################"
    "$HERE/pilot-paired.sh" "$N" "$D" "$FMT" "$REPS"
done

echo
echo "##############################################"
echo "## aggregate curve"
echo "##############################################"
python3 - <<'PY'
import csv, glob, statistics, os
from pathlib import Path

root = Path("$ROOT".rstrip()) / "results"
rows_out = []
for cell_dir in sorted(root.glob("n*-d*-*-pilot")):
    summary = cell_dir / "summary.csv"
    if not summary.is_file():
        continue
    cell = cell_dir.name.replace("-pilot", "")
    rs = list(csv.DictReader(open(summary)))
    wayline  = [int(r['makespan_s']) for r in rs if r['system']=='wayline'  and r['phase']=='Succeeded' and r['makespan_s'] not in ('','?')]
    argo = [int(r['makespan_s']) for r in rs if r['system']=='argo' and r['phase']=='Succeeded' and r['makespan_s'] not in ('','?')]
    # Wayline bytes_in_total is cumulative across reps on the data-agent; per-run
    # we need to diff successive rows. For now report the per-rep value range
    # as a proxy of payload scale.
    bytes_in = [int(r['bytes_in_total']) for r in rs if r['system']=='wayline' and r['bytes_in_total'] not in ('','NA')]
    if wayline and argo:
        wl_mean  = statistics.mean(wayline)
        argo_mean = statistics.mean(argo)
        deltas = [a - d for d, a in zip(wayline, argo)]
        rows_out.append({
            'cell': cell,
            'n_reps': len(wayline),
            'wl_mean':  f"{wl_mean:.1f}",
            'wl_std':   f"{statistics.pstdev(wayline):.1f}",
            'argo_mean': f"{argo_mean:.1f}",
            'argo_std':  f"{statistics.pstdev(argo):.1f}",
            'mean_delta_s': f"{statistics.mean(deltas):+.1f}",
            'speedup_pct':  f"{statistics.mean(deltas)/argo_mean*100:+.1f}",
            'wl_bytes_in_max': bytes_in[-1] if bytes_in else "",
        })

out = root / "curve.csv"
with out.open("w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=rows_out[0].keys() if rows_out else ['cell'])
    w.writeheader()
    w.writerows(rows_out)
print(f"wrote {out}")
print()
for r in rows_out:
    print(r)
PY
