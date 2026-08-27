#!/usr/bin/env bash
# E4 network: ONE aggregate 59 Mbit/s HTB class on anrg-3's egress, fed
# by three destination filters (anrg-7, anrg-8, anrg-9) matched on
# tcp/8082 only. anrg-4 and anrg-6 stay clean via the unlimited default
# class; anrg-7's own egress is never shaped. Direct feature fan-out and
# any migration to anrg-7 therefore contend for the same bottleneck.
#   ./e4_net.sh apply | verify | stats | clear
set -uo pipefail
CMD=${1:?usage: e4_net.sh apply|verify|stats|clear}
NS=wl-system
RATE=${RATE:-59mbit}
ip_of() { kubectl get node "$1" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}'; }
IP7=$(ip_of anrg-7); IP8=$(ip_of anrg-8); IP9=$(ip_of anrg-9)
fw() { local c=$1 out rc; out=$(kubectl -n "$NS" exec e4-fw-anrg-3 -- sh -c "$c" 2>&1); rc=$?; printf '%s' "$out"; return $rc; }

case "$CMD" in
  apply)
    F=""
    for d in "$IP7" "$IP8" "$IP9"; do
      F="${F}tc filter add dev \$IF parent 1: protocol ip prio 1 u32 match ip dst ${d}/32 match ip dport 8082 0xffff flowid 1:10; "
    done
    fw "IF=\$(ip route get $IP8 | grep -o 'dev [^ ]*' | awk '{print \$2}'); \
        tc qdisc del dev \$IF root 2>/dev/null; \
        tc qdisc add dev \$IF root handle 1: htb default 30; \
        tc class add dev \$IF parent 1: classid 1:30 htb rate 10gbit ceil 10gbit; \
        tc class add dev \$IF parent 1: classid 1:10 htb rate $RATE ceil $RATE; \
        ${F}true" >/dev/null
    exec "$0" verify ;;
  verify)
    out=$(fw "IF=\$(ip route get $IP8 | grep -o 'dev [^ ]*' | awk '{print \$2}'); \
      echo RATE=\$(tc class show dev \$IF | sed -n 's/.*htb 1:10 .*rate \([^ ]*\).*/\1/p') \
           FILTERS=\$(tc filter show dev \$IF | grep -c 'flowid 1:10')")
    rate=$(printf '%s' "$out" | sed -n 's/.*RATE=\([^ ]*\).*/\1/p')
    filt=$(printf '%s' "$out" | sed -n 's/.*FILTERS=\([0-9]*\).*/\1/p')
    if [ "$rate" != "59Mbit" ] || [ "$filt" != "3" ]; then
      echo "CAP ERROR: rate='$rate' filters='$filt' (want 59Mbit / 3)  raw: $out"; exit 1
    fi
    echo "cap live (verified): one $RATE class, 3 destination filters" ;;
  stats)
    o=$(fw "IF=\$(ip route get $IP8 | grep -o 'dev [^ ]*' | awk '{print \$2}'); \
      echo BYTES=\$(tc -s class show dev \$IF classid 1:10 | sed -n 's/.*Sent \([0-9]*\) bytes.*/\1/p')")
    printf '%s' "$o" | sed -n 's/.*BYTES=\([0-9]*\).*/\1/p' ;;
  clear)
    fw "for i in \$(ls /sys/class/net); do tc qdisc del dev \$i root 2>/dev/null; done; true" >/dev/null
    o=$(fw "echo LEFT=\$(tc qdisc show | grep -cE 'htb|netem')")
    left=$(printf '%s' "$o" | sed -n 's/.*LEFT=\([0-9]*\).*/\1/p')
    if [ "${left:-1}" != "0" ]; then echo "QDISC HYGIENE ERROR: $left remain"; exit 1; fi
    echo "cleared (verified)" ;;
esac
