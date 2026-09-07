#!/usr/bin/env bash
# E7 network: cap anrg-3 egress (data-agent tcp/8082) toward anrg-5,6,7,8,9
# at 59 Mbit/s EACH; leave everything else unshaped. One HTB qdisc on
# anrg-3 with an unlimited default class and one INDEPENDENT 59mbit class
# per destination (1:11..1:15), each with its own u32 filter. Per-dest
# (not one shared class): the spec wants a ~43s window for "a 300 MiB
# transfer" on any one path, so concurrent transfers to different targets
# must not starve each other by splitting a single 59mbit class -- that
# starvation blew the deadline for the third consumer in an early build.
# 5x59=295mbit << anrg-3's ~942mbit NIC, so each class gets its full rate.
# Every action asserts its own effect (E2/E3 lesson: a control action
# that silently no-ops is the failure mode to design out). The reverse
# fabric stays 1 GbE so a revised serving point can fan out fast.
#   ./e7_net.sh apply | verify | clear
set -uo pipefail
CMD=${1:?usage: e7_net.sh apply|verify|clear}
NS=wl-system
RATE=${RATE:-59mbit}
SRC=anrg-3
DESTS="anrg-5 anrg-6 anrg-7 anrg-8 anrg-9"
FW="e7-fw-$SRC"

ip_of() { kubectl -n "$NS" get node "$1" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}'; }
declare -A IP
for n in $DESTS; do IP[$n]=$(ip_of "$n"); done

ensure_pod() {
  ph=$(kubectl -n "$NS" get pod "$FW" -o jsonpath='{.status.phase}' 2>/dev/null)
  [ "$ph" = "Running" ] && return 0
  kubectl -n "$NS" delete pod "$FW" --ignore-not-found --wait=false >/dev/null 2>&1
  local spec
  spec=$(cat <<JSON
{"spec":{"nodeName":"$SRC","hostNetwork":true,"restartPolicy":"Never",
"containers":[{"name":"c","image":"alpine","command":["sh","-c",
"apk add -q iproute2 >/dev/null && sleep infinity"],
"securityContext":{"privileged":true}}]}}
JSON
)
  kubectl run "$FW" -n "$NS" --restart=Never --image=alpine \
    --overrides="$spec" >/dev/null 2>&1
  for _ in $(seq 1 40); do
    kubectl -n "$NS" exec "$FW" -- sh -c 'tc -V' >/dev/null 2>&1 && return 0
    sleep 3
  done
  return 1
}

fw() { kubectl -n "$NS" exec "$FW" -- sh -c "$1" 2>&1; }

case "$CMD" in
  apply)
    ensure_pod || { echo "STOP: fw pod on $SRC not ready"; exit 1; }
    # interface toward the cluster (route to the first dest)
    F="IF=\$(ip route get ${IP[anrg-5]} | grep -o 'dev [^ ]*' | awk '{print \$2}'); \
       tc qdisc del dev \$IF root 2>/dev/null; \
       tc qdisc add dev \$IF root handle 1: htb default 30; \
       tc class add dev \$IF parent 1: classid 1:30 htb rate 10gbit ceil 10gbit; "
    cls=11
    for n in $DESTS; do
      F="${F}tc class add dev \$IF parent 1: classid 1:$cls htb rate $RATE ceil $RATE; \
         tc filter add dev \$IF parent 1: protocol ip prio 1 u32 \
         match ip dst ${IP[$n]}/32 match ip dport 8082 0xffff flowid 1:$cls; "
      cls=$((cls+1))
    done
    F="${F}true"
    fw "$F"
    "$0" verify
    ;;
  verify)
    ensure_pod || { echo "STOP: fw pod on $SRC not ready"; exit 1; }
    out=$(fw "IF=\$(ip route get ${IP[anrg-5]} | grep -o 'dev [^ ]*' | awk '{print \$2}'); \
      echo NCLS=\$(tc class show dev \$IF | grep -cE 'htb 1:1[1-5] .*rate 59Mbit') \
           NFILT=\$(tc filter show dev \$IF | grep -cE 'flowid 1:1[1-5]')")
    ncls=$(printf '%s' "$out" | sed -n 's/.*NCLS=\([0-9]*\).*/\1/p')
    nfilt=$(printf '%s' "$out" | sed -n 's/.*NFILT=\([0-9]*\).*/\1/p')
    if [ "$ncls" != "5" ] || [ "$nfilt" != "5" ]; then
      echo "CAP ERROR on $SRC: 59Mbit-classes='$ncls' filters='$nfilt' (want 5/5) raw:$out"
      exit 1
    fi
    echo "cap live on $SRC: 5 independent 59Mbit classes, filters=$nfilt/5 -> {$DESTS}"
    ;;
  clear)
    ensure_pod >/dev/null 2>&1 || true
    fw "for i in \$(ls /sys/class/net); do tc qdisc del dev \$i root 2>/dev/null; done; true" >/dev/null
    out=$(fw "IF=\$(ip route get ${IP[anrg-5]} | grep -o 'dev [^ ]*' | awk '{print \$2}'); tc filter show dev \$IF | grep -cE 'flowid 1:1[1-5]' || true")
    n=$(printf '%s' "$out" | tail -1 | tr -dc 0-9)
    [ "${n:-0}" = "0" ] && echo "cleared on $SRC (0 filters)" || echo "CLEAR WARN on $SRC: $n filters remain"
    ;;
  *) echo "usage: e7_net.sh apply|verify|clear"; exit 2 ;;
esac
