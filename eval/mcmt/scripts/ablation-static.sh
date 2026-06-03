#!/usr/bin/env bash
#
# Block 4 — Wayline static-placement ablation.
#
# Runs the static template N times, captures the same artifacts as the
# pilot driver (placement.json, flow records, /metrics, report.json),
# and writes to results/ablation-static-<cell>/. Only Wayline; the matched
# Argo baseline is already in the cell's pilot results so the comparison
# is paired across the same workload.
#
#   ./ablation-static.sh [D=120] [FMT=png] [N=10]
#
# The static template is rendered from eval/mcmt/wayline/render-static.py
# (pre-baked placement from the modal HEFT decision over the 20-rep matrix).
set -euo pipefail

D=${1:-120}
FMT=${2:-png}
N=${3:-10}
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
REPO="$(cd "$ROOT/../.." && pwd)"

CELL="n4-d${D}-${FMT}"
STATIC_TPL="vemcmt-${CELL}-static"
OUT="$ROOT/results/ablation-static-${CELL}"
mkdir -p "$OUT"

python3 "$ROOT/wayline/render-static.py" --cameras 4 --duration "$D" \
    --preprocess-fmt "$FMT" --name "$STATIC_TPL" -o "/tmp/${STATIC_TPL}.yml"
kubectl apply -f "/tmp/${STATIC_TPL}.yml" >/dev/null

SUM="$OUT/summary.csv"
[[ -f "$SUM" ]] || echo "rep,run_name,phase,makespan_s,wall_s,bytes_in_total,bytes_out_total,report_md5" > "$SUM"

md5() { md5sum "$1" 2>/dev/null | awk '{print $1}'; }

collect_artifacts() {
    local rep=$1 run=$2 dest=$3
    mkdir -p "$dest"
    kubectl -n wl-system get odag "$run" -o yaml > "$dest/odag.yaml" 2>/dev/null
    for n in 1 3 4 5 6 7 8 9; do
        local da=$(kubectl -n wl-system get pod -l app=data-agent \
            -o jsonpath="{.items[?(@.spec.nodeName==\"anrg-$n\")].metadata.name}")
        [ -z "$da" ] && continue
        kubectl -n wl-system exec "$da" -- wget -qO- "http://localhost:8081/flows/$run"  2>/dev/null > "$dest/flows-anrg-$n.json" || true
        kubectl -n wl-system exec "$da" -- wget -qO- "http://localhost:8081/metrics"     2>/dev/null > "$dest/metrics-anrg-$n.json" || true
    done
    python3 - <<PY > "$dest/bytes-summary.txt"
import json, glob
bi = bo = 0
for f in glob.glob("$dest/metrics-anrg-*.json"):
    try:
        d=json.load(open(f))
        bi += d['transfers']['bytes_in']
        bo += d['push']['bytes_out']
    except Exception: pass
print(f'bytes_in_total={bi}')
print(f'bytes_out_total={bo}')
PY
    kubectl run probe-static-$rep --rm -i --restart=Never --image=busybox \
      --overrides='{"spec":{"nodeName":"anrg-9","containers":[{"name":"p","image":"busybox","command":["cat","/reports/'$run'/report.json"],"volumeMounts":[{"name":"r","mountPath":"/reports"}]}],"volumes":[{"name":"r","hostPath":{"path":"/var/lib/wl-workloads/reports"}}]}}' 2>&1 \
      | grep -v "^If you\|^warning\|^pod " | python3 -c "
import sys; t=sys.stdin.read(); b=t.find('{'); e=t.rfind('}')+1; print(t[b:e] if b>=0 else '')
" > "$dest/report.json" || true
}

wait_for_idle() {
    for i in $(seq 1 30); do
        local k=$(kubectl -n wl-system get pods -l wl-odag --no-headers 2>/dev/null \
            | grep -vE "Succeeded|Completed" | wc -l)
        [ "$k" = "0" ] && return 0
        sleep 5
    done
}

echo "Static-placement ablation: cell=$CELL, N=$N reps"
echo "Template: $STATIC_TPL"
echo "Output:   $OUT"
echo

for r in $(seq 1 "$N"); do
    echo "==================== rep $r ===================="
    wait_for_idle
    local_dest="$OUT/rep${r}-wayline"
    start=$(date +%s)
    out=$("$REPO/bin/wayline" odag run "$STATIC_TPL" -n wl-system 2>&1)
    echo "$out" | tail -1
    run=$(echo "$out" | sed -nE 's|Created run ([^ ]+).*|\1|p')
    if [ -z "$run" ]; then
        echo "  ERROR: could not parse run name"
        echo "$r,?,Failed,?,?,?,?,?" >> "$SUM"
        continue
    fi
    for i in $(seq 1 90); do
        sleep 15
        p=$(kubectl -n wl-system get odag "$run" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
        case "$p" in Succeeded|Failed) break ;; esac
    done
    end=$(date +%s)
    wall=$((end - start))
    ms=$(kubectl -n wl-system get odag "$run" -o jsonpath='{.status.makespan}' 2>/dev/null)
    phase=$(kubectl -n wl-system get odag "$run" -o jsonpath='{.status.phase}' 2>/dev/null)
    collect_artifacts "$r" "$run" "$local_dest"
    bi=$(grep bytes_in_total  "$local_dest/bytes-summary.txt" | cut -d= -f2)
    bo=$(grep bytes_out_total "$local_dest/bytes-summary.txt" | cut -d= -f2)
    h=$(md5 "$local_dest/report.json")
    echo "$r,$run,$phase,$ms,$wall,$bi,$bo,$h" >> "$SUM"
    echo "  -> rep $r: phase=$phase makespan=${ms}s wall=${wall}s"
    kubectl -n wl-system delete odag "$run" --wait=false >/dev/null 2>&1 || true
done

echo
echo "##############################################"
echo "## SUMMARY  $SUM"
echo "##############################################"
column -t -s, < "$SUM"
echo
python3 - <<PY
import csv, statistics
rows = list(csv.DictReader(open("$SUM")))
ms = [int(r['makespan_s']) for r in rows if r['phase']=='Succeeded' and r['makespan_s'] not in ('','?')]
if ms:
    print(f"Wayline-static reps: {ms}")
    print(f"mean={statistics.mean(ms):.1f}s  std={statistics.pstdev(ms):.1f}s  n={len(ms)}")
PY
