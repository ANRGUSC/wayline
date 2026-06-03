#!/usr/bin/env bash
#
# Block 6b — concurrent-ODAGs stress.
#
# Launches K Wayline ODAGs of the same template at the same instant and
# waits for all of them to complete. The poller (poll-agents.sh) should
# already be running in another terminal/process to capture resource
# pressure during the run.
#
#   ./run-stress.sh [K=3] [TEMPLATE=wl-vemcmt-n4-d60-jpg-heft]
#
# Emits one row per ODAG to results.csv: K, run_idx, run_name, phase,
# makespan_s, wall_s. The interesting metric is whether all K finish
# successfully and how much each is slowed down vs. a solo run.
set -euo pipefail

K=${1:-3}
TEMPLATE=${2:-wl-vemcmt-n4-d60-jpg-heft}
NS=wl-system
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
OUT="$HERE/results/concurrent-K${K}-${TEMPLATE}"
mkdir -p "$OUT"
SUM="$OUT/results.csv"
[[ -f "$SUM" ]] || echo "K,run_idx,run_name,phase,makespan_s,wall_s" > "$SUM"

# Sanity check — template must exist.
if ! kubectl -n "$NS" get odagtemplates.wl.io "$TEMPLATE" >/dev/null 2>&1; then
    echo "ERROR: template $TEMPLATE not found in $NS"
    exit 1
fi

# Idle preflight.
bash "$REPO/eval/e0-microbench/preflight-idle.sh" 2>&1 | tail -3

echo
echo "Launching $K concurrent ODAGs from $TEMPLATE..."
start=$(date +%s)

# Submit K runs in parallel so the controller sees them roughly simultaneously.
declare -a RUNS=()
TMP=$(mktemp -d)
for i in $(seq 1 "$K"); do
    "$REPO/bin/wayline" run "$TEMPLATE" -n "$NS" > "$TMP/sub-$i.txt" 2>&1 &
done
wait

for i in $(seq 1 "$K"); do
    name=$(sed -nE 's|Created run ([^ ]+).*|\1|p' "$TMP/sub-$i.txt")
    if [ -z "$name" ]; then
        echo "ERROR: failed to parse run name from $TMP/sub-$i.txt"
        cat "$TMP/sub-$i.txt"
        continue
    fi
    RUNS+=("$name")
    echo "  $i: $name"
done

# Wait for all to reach a terminal phase.
echo
echo "Waiting for all $K runs to complete..."
declare -a DONE=()
for ((iter=0; iter<200; iter++)); do
    sleep 15
    DONE=()
    for name in "${RUNS[@]}"; do
        p=$(kubectl -n "$NS" get odags.wl.io "$name" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
        case "$p" in Succeeded|Failed) DONE+=("$name:$p") ;; esac
    done
    n_done=${#DONE[@]}
    n_total=${#RUNS[@]}
    echo "  [t+$((iter*15))s] $n_done/$n_total done"
    if [ "$n_done" -ge "$n_total" ]; then break; fi
done

end=$(date +%s)
wall_total=$((end - start))

echo
echo "Collecting per-run stats..."
for idx in "${!RUNS[@]}"; do
    name="${RUNS[$idx]}"
    phase=$(kubectl -n "$NS" get odags.wl.io "$name" -o jsonpath='{.status.phase}' 2>/dev/null)
    ms=$(kubectl -n "$NS" get odags.wl.io "$name" -o jsonpath='{.status.makespan}' 2>/dev/null)
    echo "$K,$((idx+1)),$name,$phase,$ms,$wall_total" >> "$SUM"
    echo "  $name: phase=$phase makespan=${ms}s"
done

echo
echo "Total wall: ${wall_total}s"
echo "Saved to $SUM"
echo
column -t -s, < "$SUM"

# Cleanup
for name in "${RUNS[@]}"; do
    kubectl -n "$NS" delete odags.wl.io "$name" --wait=false >/dev/null 2>&1 || true
done
rm -rf "$TMP"
