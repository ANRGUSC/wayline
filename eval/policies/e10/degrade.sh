#!/usr/bin/env bash
# Degrade or restore anrg-3's shaped egress classes (M: 100->16 Mbit,
# S: 50->16 Mbit). Applies via a one-shot privileged pod using the
# wait+logs pattern (kubectl run -i attach output is racy). The policy's
# observation signal is /tmp/wl-linkstate on this host.
#   ./degrade.sh on | off
set -uo pipefail
MODE=${1:?usage: degrade.sh on|off}
NS=wl-system
if [ "$MODE" = on ]; then M=16mbit; S=16mbit; else M=100mbit; S=50mbit; fi
POD=tc-degrade-$$
kubectl run "$POD" --restart=Never --image=alpine -n "$NS" \
  --overrides='{"spec":{"nodeName":"anrg-3","hostNetwork":true,"containers":[{"name":"c","image":"alpine","command":["sh","-c","apk add -q iproute2 >/dev/null; IFACE=; for i in $(ls /sys/class/net); do tc qdisc show dev $i 2>/dev/null | grep -q htb && IFACE=$i; done; [ -n \"$IFACE\" ] || { echo no-htb-iface; exit 1; }; tc class change dev $IFACE parent 1: classid 1:20 htb rate '"$M"' ceil '"$M"' && tc class change dev $IFACE parent 1: classid 1:30 htb rate '"$S"' ceil '"$S"' && echo degrade-done $IFACE"],"securityContext":{"privileged":true}}]}}' \
  >/dev/null 2>&1
kubectl -n "$NS" wait "pod/$POD" --for=jsonpath='{.status.phase}'=Succeeded --timeout=90s >/dev/null 2>&1
OUT=$(kubectl -n "$NS" logs "$POD" 2>/dev/null)
kubectl -n "$NS" delete pod "$POD" --ignore-not-found >/dev/null 2>&1
if echo "$OUT" | grep -q degrade-done; then
  echo "anrg-3 egress: M=$M S=$S ($OUT)"
  echo "$MODE" > /tmp/wl-linkstate
else
  echo "DEGRADE FAILED: $OUT"
  exit 1
fi
