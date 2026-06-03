#!/usr/bin/env bash
#
# Apply the 8-node bandwidth matrix used for the network-aware
# scheduling evaluation. Runs a privileged "tc-setup" pod on each
# worker that installs HTB shaping on its host interface.
#
# Matrix v2 (symmetric, narrower escape routes):
#   F = 1 Gbps   — same-tier (edge↔edge, compute↔compute)
#   M = 100 Mbps — cross-tier generic
#   S =  50 Mbps — engineered bottleneck pairs
#
# v1 (archived in results/archive-matrix-v1/) used M=300Mbps, S=100Mbps; the
# wider gap between F and M here makes HEFT's avoidance of cross-tier
# traffic more consequential, reviving the 25–30% HEFT-vs-random gap the
# original paper claimed.
#
#            a-1   a-3   a-4   a-5   a-6   a-7   a-8   a-9
#   a-1       —    F     F     F     M     M     M     M
#   a-3       F    —     F     F     S     M     M     M
#   a-4       F    F     —     F     M     S     M     M
#   a-5       F    F     F     —     M     M     S     M
#   a-6       M    S     M     M     —     F     F     M
#   a-7       M    M     S     M     F     —     F     M
#   a-8       M    M     M     S     F     F     —     M
#   a-9       M    M     M     M     M     M     M     —
#
# Bottleneck pairs: anrg-3↔anrg-6, anrg-4↔anrg-7, anrg-5↔anrg-8.
#
# Usage:  ./setup-tc-matrix.sh
# Undo:   ./teardown-tc-matrix.sh
set -euo pipefail

NS=wl-system

# Node IP map (InternalIP, per `kubectl get nodes -o wide`).
declare -A IP=(
  [anrg-1]=192.168.1.189
  [anrg-3]=192.168.1.164
  [anrg-4]=192.168.1.156
  [anrg-5]=192.168.1.154
  [anrg-6]=192.168.1.208
  [anrg-7]=192.168.1.193
  [anrg-8]=192.168.1.168
  [anrg-9]=192.168.1.166
)

echo "Cleaning up any previous tc-setup pods..."
kubectl delete pods -n "$NS" -l app=tc-setup --ignore-not-found >/dev/null 2>&1 || true
sleep 2

# apply_tc <node> <ip> <per-destination rule block>
apply_tc() {
  local NODE=$1 SELF_IP=$2 RULES=$3
  cat <<EOF | kubectl apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata:
  name: tc-setup-${NODE}
  namespace: ${NS}
  labels: { app: tc-setup }
spec:
  nodeName: ${NODE}
  hostNetwork: true
  restartPolicy: Never
  containers:
  - name: tc
    image: alpine
    securityContext: { privileged: true }
    command: ["sh","-c"]
    args:
    - |
      apk add -q iproute2 >/dev/null
      IFACE=\$(ip -o addr show | grep '${SELF_IP}' | awk '{print \$2}')
      tc qdisc del dev \$IFACE root 2>/dev/null || true
      tc qdisc add dev \$IFACE root handle 1: htb default 10
      # class 1:10 = F (1 Gbps default), class 1:20 = M (300 Mbps), class 1:30 = S (100 Mbps)
      tc class add dev \$IFACE parent 1: classid 1:10 htb rate 1gbit   ceil 1gbit
      tc class add dev \$IFACE parent 1: classid 1:20 htb rate 100mbit ceil 100mbit
      tc class add dev \$IFACE parent 1: classid 1:30 htb rate 50mbit  ceil 50mbit
      ${RULES}
      echo "${NODE}: tc rules applied on \$IFACE"
      tc class show dev \$IFACE
      sleep 86400
EOF
}

# Helper: emit a single u32 filter for IP dst → classid, `;`-separated so
# multiple rules compose into a single YAML-safe line.
f() { local IP_DST=$1 CLASS=$2; printf 'tc filter add dev $IFACE parent 1: protocol ip prio 1 u32 match ip dst %s/32 flowid 1:%s; ' "$IP_DST" "$CLASS"; }

echo "Applying tc matrix..."

# anrg-1 (edge): F to anrg-3/4/5 (default), M to anrg-6/7/8/9, no S.
apply_tc anrg-1 ${IP[anrg-1]} "$(f ${IP[anrg-6]} 20)$(f ${IP[anrg-7]} 20)$(f ${IP[anrg-8]} 20)$(f ${IP[anrg-9]} 20)"

# anrg-3 (edge): F default; M to anrg-7/8/9; S to anrg-6.
apply_tc anrg-3 ${IP[anrg-3]} "$(f ${IP[anrg-6]} 30)$(f ${IP[anrg-7]} 20)$(f ${IP[anrg-8]} 20)$(f ${IP[anrg-9]} 20)"

# anrg-4 (edge): F default; M to anrg-6/8/9; S to anrg-7.
apply_tc anrg-4 ${IP[anrg-4]} "$(f ${IP[anrg-6]} 20)$(f ${IP[anrg-7]} 30)$(f ${IP[anrg-8]} 20)$(f ${IP[anrg-9]} 20)"

# anrg-5 (edge): F default; M to anrg-6/7/9; S to anrg-8.
apply_tc anrg-5 ${IP[anrg-5]} "$(f ${IP[anrg-6]} 20)$(f ${IP[anrg-7]} 20)$(f ${IP[anrg-8]} 30)$(f ${IP[anrg-9]} 20)"

# anrg-6 (compute): F to anrg-7/8 (default); M to anrg-1/4/5/9; S to anrg-3.
apply_tc anrg-6 ${IP[anrg-6]} "$(f ${IP[anrg-1]} 20)$(f ${IP[anrg-3]} 30)$(f ${IP[anrg-4]} 20)$(f ${IP[anrg-5]} 20)$(f ${IP[anrg-9]} 20)"

# anrg-7 (compute): F to anrg-6/8 (default); M to anrg-1/3/5/9; S to anrg-4.
apply_tc anrg-7 ${IP[anrg-7]} "$(f ${IP[anrg-1]} 20)$(f ${IP[anrg-3]} 20)$(f ${IP[anrg-4]} 30)$(f ${IP[anrg-5]} 20)$(f ${IP[anrg-9]} 20)"

# anrg-8 (compute): F to anrg-6/7 (default); M to anrg-1/3/4/9; S to anrg-5.
apply_tc anrg-8 ${IP[anrg-8]} "$(f ${IP[anrg-1]} 20)$(f ${IP[anrg-3]} 20)$(f ${IP[anrg-4]} 20)$(f ${IP[anrg-5]} 30)$(f ${IP[anrg-9]} 20)"

# anrg-9 (gateway): M to everyone.
apply_tc anrg-9 ${IP[anrg-9]} "$(f ${IP[anrg-1]} 20)$(f ${IP[anrg-3]} 20)$(f ${IP[anrg-4]} 20)$(f ${IP[anrg-5]} 20)$(f ${IP[anrg-6]} 20)$(f ${IP[anrg-7]} 20)$(f ${IP[anrg-8]} 20)"

echo "Waiting for tc pods to initialize..."
for node in "${!IP[@]}"; do
  kubectl wait -n "$NS" --for=condition=Ready pod/tc-setup-${node} --timeout=60s >/dev/null || true
done
sleep 3

echo ""
echo "=== Verification ==="
for node in anrg-1 anrg-3 anrg-4 anrg-5 anrg-6 anrg-7 anrg-8 anrg-9; do
  echo "--- $node ---"
  kubectl logs -n "$NS" tc-setup-${node} 2>&1 | grep -E "applied|rate" | head -4
done
echo ""
echo "Applying bandwidth ConfigMap (so HEFT's predictions match the tc matrix)..."
kubectl apply -f "$(dirname "$0")/bandwidth-configmap.yml" >/dev/null
echo ""
echo "Bandwidth matrix active (tc + ConfigMap)."
