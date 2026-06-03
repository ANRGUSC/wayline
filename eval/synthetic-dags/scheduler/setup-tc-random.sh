#!/usr/bin/env bash
# Apply a seeded random edge network: per-pair HTB rate + netem delay/jitter,
# via a privileged pod on each worker. Saves the matrix (gen-tc-random.py).
#   ./setup-tc-random.sh <seed>     ./setup-tc-random.sh <seed> teardown
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; NS=wl-system
SEED="${1:?usage: setup-tc-random.sh <seed> [teardown]}"; MODE="${2:-apply}"
declare -A IP=([anrg-1]=192.168.1.189 [anrg-3]=192.168.1.164 [anrg-4]=192.168.1.156 [anrg-5]=192.168.1.154 \
               [anrg-6]=192.168.1.208 [anrg-7]=192.168.1.193 [anrg-8]=192.168.1.168 [anrg-9]=192.168.1.166)
kubectl delete pods -n "$NS" -l app=tc-rand --ignore-not-found >/dev/null 2>&1; sleep 2
launch(){ # $1=node  $2=script
  cat <<EOF | kubectl apply -f - >/dev/null 2>&1
apiVersion: v1
kind: Pod
metadata: {name: tc-rand-$1, namespace: $NS, labels: {app: tc-rand}}
spec:
  hostNetwork: true
  nodeName: $1
  containers:
  - {name: tc, image: alpine, command: ["sh","-c","apk add -q iproute2 >/dev/null 2>&1; $2"], securityContext: {privileged: true}}
  restartPolicy: Never
  tolerations: [{operator: Exists}]
EOF
}
if [ "$MODE" = teardown ]; then
  for n in "${!IP[@]}"; do launch "$n" "IFACE=\$(ip -o addr show|grep '${IP[$n]}'|awk '{print \$2}'); tc qdisc del dev \$IFACE root 2>/dev/null; echo done"; done
  sleep 8; kubectl delete pods -n "$NS" -l app=tc-rand --force --grace-period=0 >/dev/null 2>&1
  echo "random tc seed $SEED torn down"; exit 0
fi
MATRIX=$(python3 "$HERE/gen-tc-random.py" "$SEED")  # SRC DST RATE DELAY JITTER ; also saves JSON
for src in "${!IP[@]}"; do
  selfip=${IP[$src]}
  s="IFACE=\$(ip -o addr show|grep '$selfip'|awk '{print \$2}'); "
  s+="tc qdisc del dev \$IFACE root 2>/dev/null; tc qdisc add dev \$IFACE root handle 1: htb default 9999; "
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
  s+="echo $src applied; tc -s class show dev \$IFACE | grep -c htb"
  launch "$src" "$s"
done
echo "applied random tc (seed $SEED) on 8 workers; matrix saved to results/random-nets/seed-$SEED.json"
sleep 10
for n in "${!IP[@]}"; do echo -n "$n: "; kubectl logs tc-rand-$n -n "$NS" 2>/dev/null | tail -1; done
kubectl delete pods -n "$NS" -l app=tc-rand --force --grace-period=0 >/dev/null 2>&1
