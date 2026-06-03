#!/usr/bin/env bash
# Apply / tear down the 20-node heterogeneity network matrix.
# Egress HTB + netem per node (u32 match on destination NODE IP — flannel VXLAN
# outer dst is the peer node IP, so this shapes inter-node pod traffic), plus
# the wl-network-profile ConfigMap that feeds the same matrix to HEFT.
#
#   netshape.sh apply [seed]     netshape.sh verify     netshape.sh teardown
#
# Uses the offline wl-netshaper:multi image (iproute2 baked in) so it works on
# the internet-less arm64 Pis. Shapes eth0 (the InternalIP interface).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
NS=wl-system
IMG=192.168.1.163:5000/wl-netshaper:multi
MODE="${1:-}"; SEED="${2:-0}"

declare -A IP
while read -r n ip; do IP[$n]=$ip; done < <(python3 "$HERE/gen_net.py" ipmap)

launch() { # $1=node $2=script
  cat <<EOF | kubectl apply -f - >/dev/null 2>&1
apiVersion: v1
kind: Pod
metadata: {name: netshape-$1, namespace: $NS, labels: {app: netshape}}
spec:
  hostNetwork: true
  nodeName: $1
  containers:
  - name: tc
    image: $IMG
    command: ["sh","-c","$2"]
    securityContext: {privileged: true}
  restartPolicy: Never
  tolerations: [{operator: Exists}]
EOF
}

clear_pods() { kubectl delete pods -n "$NS" -l app=netshape --force --grace-period=0 >/dev/null 2>&1; }

case "$MODE" in
  apply)
    echo "[netshape] applying ConfigMap (seed=$SEED) for HEFT..."
    python3 "$HERE/gen_net.py" configmap "$SEED" | kubectl apply -f - >/dev/null
    MATRIX=$(python3 "$HERE/gen_net.py" rules "$SEED")
    clear_pods; sleep 2
    echo "[netshape] launching shaper pods on ${#IP[@]} nodes..."
    for src in "${!IP[@]}"; do
      selfip=${IP[$src]}
      s="IFACE=\$(ip -o -4 addr show | grep ' ${selfip}/' | awk '{print \$2}' | head -1); "
      s+=": \${IFACE:=eth0}; "
      s+="tc qdisc del dev \$IFACE root 2>/dev/null; "
      s+="tc qdisc add dev \$IFACE root handle 1: htb default 9999; "
      s+="tc class add dev \$IFACE parent 1: classid 1:9999 htb rate 1000mbit ceil 1000mbit; "
      id=10
      while read -r a b rate delay jitter; do
        [ "$a" = "$src" ] || continue
        dip=${IP[$b]}
        s+="tc class add dev \$IFACE parent 1: classid 1:$id htb rate ${rate}mbit ceil ${rate}mbit; "
        s+="tc qdisc add dev \$IFACE parent 1:$id handle $id: netem delay ${delay}ms ${jitter}ms; "
        s+="tc filter add dev \$IFACE parent 1: protocol ip prio 1 u32 match ip dst $dip/32 flowid 1:$id; "
        id=$((id+1))
      done <<< "$MATRIX"
      s+="echo \$IFACE \$(tc -s class show dev \$IFACE | grep -c htb) classes"
      launch "$src" "$s"
    done
    echo "[netshape] applied on ${#IP[@]} nodes (seed=$SEED); waiting to collect status..."
    sleep 12
    for n in $(echo "${!IP[@]}" | tr ' ' '\n' | sort -V); do
      echo -n "  $n: "; kubectl logs netshape-"$n" -n "$NS" 2>/dev/null | tail -1
    done
    clear_pods
    ;;
  verify)
    # spot-check one intel + one pi egress qdisc
    for n in anrg-1 rpi11; do
      selfip=${IP[$n]}
      launch "$n" "IFACE=\$(ip -o -4 addr show | grep ' ${selfip}/' | awk '{print \$2}' | head -1); echo \$IFACE; tc class show dev \$IFACE | head -6; echo '---filters:'; tc filter show dev \$IFACE | grep -c match"
    done
    sleep 10
    for n in anrg-1 rpi11; do echo "=== $n ==="; kubectl logs netshape-"$n" -n "$NS" 2>/dev/null; done
    clear_pods
    ;;
  teardown)
    echo "[netshape] removing tc qdiscs + ConfigMap..."
    clear_pods; sleep 2
    for src in "${!IP[@]}"; do
      selfip=${IP[$src]}
      launch "$src" "IFACE=\$(ip -o -4 addr show | grep ' ${selfip}/' | awk '{print \$2}' | head -1); : \${IFACE:=eth0}; tc qdisc del dev \$IFACE root 2>/dev/null; echo \$IFACE cleared"
    done
    sleep 10
    clear_pods
    kubectl delete configmap wl-network-profile -n "$NS" --ignore-not-found >/dev/null 2>&1
    echo "[netshape] torn down"
    ;;
  *)
    echo "usage: $0 {apply [seed]|verify|teardown}"; exit 1 ;;
esac
