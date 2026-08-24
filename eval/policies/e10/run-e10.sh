#!/usr/bin/env bash
# E10: serving-copy migration under link degradation. Four arms x N:
#   fixed-clean     no degradation, no policy      (baseline)
#   fixed-deg       degradation at T_DEG, no policy
#   adaptive-deg    degradation at T_DEG, policy migrates serving copy
#   adaptive-clean  no degradation, policy running (must equal baseline)
# The static-best referent (serve pinned on the safe node) is e10-static.
set -uo pipefail
cd "$(dirname "$0")"
NS=wl-system
N=${N:-3}
T_DEG=${T_DEG:-15}
RES=${RES:-$HOME/e10-results}
mkdir -p "$RES"

# Shaped-network guard.
q=$(kubectl run tc-check --rm -i --restart=Never -n "$NS" --image=alpine \
    --overrides='{"spec":{"nodeName":"anrg-3","hostNetwork":true,"containers":[{"name":"tc-check","image":"alpine","command":["sh","-c","apk add -q iproute2 >/dev/null && tc qdisc show"],"securityContext":{"privileged":true}}]}}' \
    2>/dev/null)
echo "$q" | grep -q htb || { echo "ABORT: network is not shaped."; exit 1; }

kubectl apply -f e10.yml >/dev/null
kubectl apply -f e10-static.yml >/dev/null 2>&1 || true
./degrade.sh off >/dev/null

run_one() { # <arm> <template> <degrade:0|1> <policy:0|1>
  local arm=$1 tpl=$2 deg=$3 pol=$4
  local run
  run=$(/home/anrg/wayline/bin/wayline run "$tpl" -n "$NS" 2>&1 | grep -o "${tpl}-run-[a-z0-9]*" | head -1)
  [ -n "$run" ] || { echo "$arm: SUBMIT FAILED"; return 1; }
  local polpid=""
  if [ "$pol" = 1 ]; then python3 policy.py "$run" >> "$RES/$arm-policy.log" 2>&1 & polpid=$!; fi
  if [ "$deg" = 1 ]; then ( sleep "$T_DEG"; ./degrade.sh on >/dev/null ) & fi
  local t=0
  while [ $t -lt 900 ]; do
    p=$(kubectl -n "$NS" get odag "$run" -o jsonpath='{.status.phase}' 2>/dev/null)
    [ "$p" = Succeeded ] || [ "$p" = Failed ] && break
    sleep 5; t=$((t+5))
  done
  mk=$(kubectl -n "$NS" get odag "$run" -o jsonpath='{.status.makespan}' 2>/dev/null)
  echo "-> $arm $run phase=$p makespan=${mk}s" | tee -a "$RES/$arm.log"
  kubectl -n "$NS" get odag "$run" -o json > "$RES/$arm-$run.json"
  [ -n "$polpid" ] && kill "$polpid" 2>/dev/null
  ./degrade.sh off >/dev/null
  kubectl -n "$NS" delete odag "$run" >/dev/null 2>&1
  sleep 5
}

for i in $(seq 1 "$N"); do
  echo "=== round $i/$N $(date +%T) ==="
  run_one fixed-clean    e10        0 0
  run_one adaptive-clean e10        0 1
  run_one fixed-deg      e10        1 0
  run_one adaptive-deg   e10        1 1
  run_one static-deg     e10-static 1 0
done
echo "E10 CAMPAIGN DONE $(date +%F' '%T)"
