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

# Shaped-network guard (wait+logs; kubectl run -i attach output is racy).
POD=tc-check-$$
kubectl run "$POD" --restart=Never -n "$NS" --image=alpine \
  --overrides='{"spec":{"nodeName":"anrg-3","hostNetwork":true,"containers":[{"name":"c","image":"alpine","command":["sh","-c","apk add -q iproute2 >/dev/null && tc qdisc show"],"securityContext":{"privileged":true}}]}}' \
  >/dev/null 2>&1
kubectl -n "$NS" wait "pod/$POD" --for=jsonpath='{.status.phase}'=Succeeded --timeout=90s >/dev/null 2>&1
q=$(kubectl -n "$NS" logs "$POD" 2>/dev/null)
kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1
echo "$q" | grep -q htb || { echo "ABORT: network is not shaped."; exit 1; }

kubectl apply -f e10.yml >/dev/null
kubectl apply -f e10-static.yml >/dev/null 2>&1 || true
./degrade.sh off >/dev/null

run_one() { # <arm> <template> <degrade:0|1> <policy:0|1> [t_deg] [timeout]
  local arm=$1 tpl=$2 deg=$3 pol=$4 tdeg=${5:-$T_DEG} tmax=${6:-900}
  local run
  run=$(/home/anrg/wayline/bin/wayline run "$tpl" -n "$NS" 2>&1 | grep -o "${tpl}-run-[a-z0-9]*" | head -1)
  [ -n "$run" ] || { echo "$arm: SUBMIT FAILED"; return 1; }
  local polpid=""
  if [ "$pol" = 1 ]; then python3 policy.py "$run" >> "$RES/$arm-policy.log" 2>&1 & polpid=$!; fi
  if [ "$deg" = 1 ]; then ( sleep "$tdeg"; ./degrade.sh on >/dev/null ) & fi
  local t=0
  while [ $t -lt "$tmax" ]; do
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

relax_deadline() { # on|off — patient transport deadline for fixed-deg-relaxed
  if [ "$1" = on ]; then
    kubectl -n "$NS" set env ds/data-agent WL_PUSH_MIN_THROUGHPUT_KBS=256 >/dev/null
  else
    kubectl -n "$NS" set env ds/data-agent WL_PUSH_MIN_THROUGHPUT_KBS- >/dev/null
  fi
  kubectl -n "$NS" rollout status ds/data-agent --timeout=300s >/dev/null
}

for i in $(seq 1 "$N"); do
  echo "=== round $i/$N $(date +%T) ==="
  run_one fixed-clean    e10        0 0
  run_one adaptive-clean e10        0 1
  run_one fixed-deg      e10        1 0
  run_one adaptive-deg   e10        1 1
  # adaptive-late: degradation strikes MID-FAN-OUT (t=45), after the
  # object exists and its delivery is under way: the policy revises an
  # existing object's realization; the controller cancels the stale
  # flows and re-serves from the new copy.
  run_one adaptive-late  e10        1 1 45
  run_one static-deg     e10-static 1 0
  # fixed-deg-relaxed: same degradation, patient transport deadline —
  # answers "the failure is just a timeout artifact": even with
  # unlimited patience, fixed pays three bottleneck transfers.
  relax_deadline on
  run_one fixed-deg-relaxed e10     1 0 "$T_DEG" 1800
  relax_deadline off
done
echo "E10 CAMPAIGN DONE $(date +%F' '%T)"
