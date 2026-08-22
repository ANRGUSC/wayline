#!/usr/bin/env bash
# E7b: placement steering toward the cached copy when locality dominates.
#   e7-nocache  same DAG without cacheKey (baseline), N runs
#   e7-cache    with cacheKey: run 1 is the cold miss, runs 2..N are warm
#               hits (steady-state reuse). The consumer is UNCONSTRAINED, so
#               warm runs also demonstrate placement steering toward the
#               cached copy through the ordinary cost model.
set -uo pipefail
cd "$(dirname "$0")"
ROOT=$(cd ../.. && pwd)
NS=wl-system
N=${N:-8}
RES=${RES:-$HOME/e7b-results}
REPEAT=$ROOT/eval/scripts/benchmarks/repeat-template.sh
mkdir -p "$RES"

# Cold start must be genuine: restart the controller to clear the cache
# registry and delete every prior run (registry rebuild finds nothing).
kubectl delete odags -n "$NS" --all --ignore-not-found >/dev/null 2>&1
kubectl -n "$NS" rollout restart deploy/odag-controller >/dev/null
kubectl -n "$NS" rollout status deploy/odag-controller --timeout=120s >/dev/null
sleep 5

arm() { # <arm-name> <template-file> <template-name>
  local arm=$1 tpl=$2 name=$3
  echo "=== ARM $arm ($name) start $(date +%F' '%T) ==="
  kubectl apply -f "$tpl" >/dev/null
  sleep 3
  "$REPEAT" "$name" "$N" "$NS" 420 2>&1 | tee "$RES/$arm.log"
  kubectl get odags -n "$NS" -l wl.io/template="$name" -o json > "$RES/$arm.json"
  echo "=== ARM $arm done $(date +%F' '%T) ==="
}

arm e7b-near "e7/e7b-near.yml" e7b-near
kubectl delete odags -n "$NS" --all --ignore-not-found >/dev/null 2>&1
arm e7b-far  "e7/e7b-far.yml"  e7b-far
echo "E7B CAMPAIGN DONE $(date +%F' '%T)"
