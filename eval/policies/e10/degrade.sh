#!/usr/bin/env bash
# Degrade or restore anrg-3's shaped egress classes (M: 100->16 Mbit,
# S: 50->16 Mbit) via a privileged pod. Ground-truth observable: the
# policy reads the same signal we write in /tmp/wl-linkstate on anrg-2.
#   ./degrade.sh on | off
set -euo pipefail
MODE=${1:?usage: degrade.sh on|off}
if [ "$MODE" = on ]; then M=16mbit; S=16mbit; else M=100mbit; S=50mbit; fi
kubectl run tc-degrade --rm -i --restart=Never --image=alpine -n wl-system \
  --overrides='{"spec":{"nodeName":"anrg-3","hostNetwork":true,"containers":[{"name":"c","image":"alpine","command":["sh","-c","apk add -q iproute2 >/dev/null; IFACE=$(ls /sys/class/net | grep -E \"^(eno|enp|eth)\" | head -1); tc class change dev $IFACE parent 1: classid 1:20 htb rate '"$M"' ceil '"$M"'; tc class change dev $IFACE parent 1: classid 1:30 htb rate '"$S"' ceil '"$S"'; echo degrade-done"],"securityContext":{"privileged":true}}]}}' \
  2>/dev/null | grep -q degrade-done && echo "anrg-3 egress: M=$M S=$S" || echo "DEGRADE FAILED"
echo "$MODE" > /tmp/wl-linkstate
