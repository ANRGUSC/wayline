#!/usr/bin/env bash
# E9 campaign on the Chameleon fabric: direct + store x {iot, hetero,
# wpf}, N runs per arm, plus one iperf3 measurement so the simulator's
# 10G model has a measured capacity instead of a nominal one.
# Run on the server node from the repo root: cloud/04-campaign.sh
set -uo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
NS=wl-system
N=${N:-20}
RES=${RES:-$HOME/e9-results}
REPEAT=$ROOT/eval/scripts/benchmarks/repeat-template.sh
mkdir -p "$RES"

# --- fabric capacity: one iperf3 pair (anrg-9 server, anrg-1 client) ---
echo "=== measuring fabric capacity (iperf3 anrg-1 -> anrg-9) ==="
kubectl run iperf-srv -n "$NS" --restart=Never --image=networkstatic/iperf3 \
  --overrides='{"spec":{"nodeName":"anrg-9","hostNetwork":true,"containers":[{"name":"iperf-srv","image":"networkstatic/iperf3","args":["-s","-1"],"securityContext":{"privileged":true}}]}}' >/dev/null 2>&1
sleep 8
SRV_IP=$(kubectl get node anrg-9 -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
kubectl run iperf-cli -n "$NS" --restart=Never --image=networkstatic/iperf3 \
  --overrides='{"spec":{"nodeName":"anrg-1","hostNetwork":true,"containers":[{"name":"iperf-cli","image":"networkstatic/iperf3","args":["-c","'"$SRV_IP"'","-J","-t","10"],"securityContext":{"privileged":true}}]}}' >/dev/null 2>&1
kubectl wait pod/iperf-cli -n "$NS" --for=jsonpath='{.status.phase}'=Succeeded --timeout=120s >/dev/null 2>&1
kubectl logs iperf-cli -n "$NS" 2>/dev/null > "$RES/iperf3.json"
BPS=$(python3 -c "import json;d=json.load(open('$RES/iperf3.json'));print(int(d['end']['sum_received']['bits_per_second']/8))" 2>/dev/null || echo "")
kubectl delete pod iperf-srv iperf-cli -n "$NS" --ignore-not-found >/dev/null 2>&1
if [ -n "$BPS" ]; then
  echo "measured capacity: $BPS bytes/s — updating wl-network-profile"
  kubectl -n "$NS" patch cm wl-network-profile --type merge -p "{\"data\":{\"defaultBandwidth\":\"$BPS\"}}"
else
  echo "WARN: iperf3 failed; keeping the nominal bandwidth profile"
fi

# --- the six arms ---
arm(){ # <arm-name> <template-name>
  echo "=== ARM $1 start $(date +%F' '%T) ==="
  kubectl delete odags -n "$NS" --all --ignore-not-found >/dev/null 2>&1
  sleep 3
  "$REPEAT" "$2" "$N" "$NS" 420 2>&1 | tee "$RES/$1.log"
  kubectl get odags -n "$NS" -l wl.io/template="$2" -o json > "$RES/$1.json"
  echo "=== ARM $1 done $(date +%F' '%T) ==="
}
arm e9-iot-direct    iot-heft
arm e9-iot-store     iot-heft-store
arm e9-hetero-direct hetero-heft
arm e9-hetero-store  hetero-heft-store
arm e9-wpf-direct    wpf-heft
arm e9-wpf-store     wpf-heft-store
echo "E9 CAMPAIGN DONE $(date +%F' '%T)"
echo "scp $RES back, then DELETE THE LEASE."
