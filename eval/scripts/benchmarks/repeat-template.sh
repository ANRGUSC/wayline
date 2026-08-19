#!/usr/bin/env bash
# Run an ODAGTemplate N times sequentially. Each iteration submits a new run
# and waits for it to reach Succeeded or Failed before starting the next, so
# runs never overlap and network/resource measurements are not polluted by
# concurrent activity.
#
# Usage: repeat-template.sh [template] [count] [namespace] [per-run-timeout-s]
#   defaults: wide-pipeline 20 wl-system 300

set -euo pipefail

TEMPLATE="${1:-wide-pipeline}"
COUNT="${2:-20}"
NS="${3:-wl-system}"
TIMEOUT="${4:-300}"

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
WAYLINE="${REPO_ROOT}/bin/wayline"
[[ -x "$WAYLINE" ]] || DSF="wayline"

echo "Template:    $TEMPLATE"
echo "Namespace:   $NS"
echo "Iterations:  $COUNT"
echo "Timeout:     ${TIMEOUT}s per run"
echo "Binary:      $WAYLINE"
echo

results=()
overall_start=$(date +%s)

for i in $(seq 1 "$COUNT"); do
    echo "=== [$i/$COUNT] submitting ==="
    out=$("$WAYLINE" run "$TEMPLATE" -n "$NS")
    echo "$out"
    name=$(printf '%s\n' "$out" | sed -nE 's/^Created run ([^ ]+).*/\1/p')
    if [[ -z "$name" ]]; then
        echo "ERROR: could not parse run name from CLI output" >&2
        exit 1
    fi

    start=$(date +%s)
    phase=""
    while :; do
        phase=$(kubectl -n "$NS" get odag "$name" -o jsonpath='{.status.phase}' 2>/dev/null || true)
        case "$phase" in
            Succeeded|Failed) break ;;
        esac
        elapsed=$(( $(date +%s) - start ))
        if (( elapsed > TIMEOUT )); then
            phase="Timeout(${phase:-unknown})"
            break
        fi
        sleep 2
    done

    makespan=$(kubectl -n "$NS" get odag "$name" -o jsonpath='{.status.makespan}' 2>/dev/null || true)
    wall=$(( $(date +%s) - start ))
    printf -- '-> %s  phase=%s  makespan=%ss  wall=%ss\n\n' \
        "$name" "$phase" "${makespan:-?}" "$wall"
    results+=("$i|$name|$phase|${makespan:-?}|$wall")
done

overall_wall=$(( $(date +%s) - overall_start ))

echo "======================== Summary ========================"
printf '%-4s %-32s %-12s %-10s %-8s\n' "#" "RUN" "PHASE" "MAKESPAN" "WALL"
for r in "${results[@]}"; do
    IFS='|' read -r idx name phase ms wall <<<"$r"
    printf '%-4s %-32s %-12s %-10s %-8s\n' "$idx" "$name" "$phase" "$ms" "$wall"
done
echo "---------------------------------------------------------"
echo "Total wall time: ${overall_wall}s"
