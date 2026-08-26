#!/usr/bin/env bash
# E3 network: cap 3->8 and 7->8 data-agent traffic (tcp/8082 only) at
# 59 Mbit/s; leave 3->7 and everything else unshaped. Sender-side tc
# htb with an unlimited default class, so only the matched destination
# is capped. Every action verifies its own effect (E2 lesson: a control
# action that silently does nothing is the failure mode to design out).
#   ./e3_net.sh apply | verify | stats | clear
set -uo pipefail
CMD=${1:?usage: e3_net.sh apply|verify|stats|clear}
NS=wl-system
RATE=${RATE:-59mbit}
ip_of() { kubectl get node "$1" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}'; }
IP8=$(ip_of anrg-8)
fw() { local n=$1 c=$2 out rc; out=$(kubectl -n "$NS" exec "e3-fw-$n" -- sh -c "$c" 2>&1); rc=$?; printf '%s' "$out"; return $rc; }

apply_cap() { # <node>
  fw "$1" "IF=\$(ip route get $IP8 | grep -o 'dev [^ ]*' | awk '{print \$2}'); \
    tc qdisc del dev \$IF root 2>/dev/null; \
    tc qdisc add dev \$IF root handle 1: htb default 30; \
    tc class add dev \$IF parent 1: classid 1:30 htb rate 10gbit ceil 10gbit; \
    tc class add dev \$IF parent 1: classid 1:10 htb rate $RATE ceil $RATE; \
    tc filter add dev \$IF parent 1: protocol ip prio 1 u32 \
       match ip dst $IP8/32 match ip dport 8082 0xffff flowid 1:10; true"
}
check_cap() { # <node> -> prints RATE=<r> FILTERS=<n>
  fw "$1" "IF=\$(ip route get $IP8 | grep -o 'dev [^ ]*' | awk '{print \$2}'); \
    echo RATE=\$(tc class show dev \$IF | sed -n 's/.*htb 1:10 .*rate \([^ ]*\).*/\1/p') \
         FILTERS=\$(tc filter show dev \$IF | grep -c 'flowid 1:10')"
}
verify_one() { # <node>
  local out rate filt
  out=$(check_cap "$1")
  rate=$(printf '%s' "$out" | sed -n 's/.*RATE=\([^ ]*\).*/\1/p')
  filt=$(printf '%s' "$out" | sed -n 's/.*FILTERS=\([0-9]*\).*/\1/p')
  if [ "$rate" != "59Mbit" ] || [ "$filt" != "1" ]; then
    echo "CAP ERROR on $1: rate='$rate' filters='$filt' (raw: $out)"; return 1
  fi
  echo "  $1 capped: rate=$rate filters=$filt"; return 0
}

case "$CMD" in
  apply)
    apply_cap anrg-3 >/dev/null; apply_cap anrg-7 >/dev/null
    verify_one anrg-3 || exit 1
    verify_one anrg-7 || exit 1
    echo "caps applied (verified): 3->8 and 7->8 at $RATE, 3->7 unshaped" ;;
  verify)
    verify_one anrg-3 || exit 1
    verify_one anrg-7 || exit 1
    echo "caps live (verified)" ;;
  stats)   # bytes through each capped class — live proof traffic used it
    for n in anrg-3 anrg-7; do
      b=$(fw "$n" "IF=\$(ip route get $IP8 | grep -o 'dev [^ ]*' | awk '{print \$2}'); \
          echo BYTES=\$(tc -s class show dev \$IF classid 1:10 | sed -n 's/.*Sent \([0-9]*\) bytes.*/\1/p')")
      echo "$n $(printf '%s' "$b" | sed -n 's/.*BYTES=\([0-9]*\).*/\1/p')"
    done ;;
  clear)
    for n in anrg-3 anrg-7; do
      fw "$n" "for i in \$(ls /sys/class/net); do tc qdisc del dev \$i root 2>/dev/null; done; true" >/dev/null
    done
    left=0
    for n in anrg-3 anrg-7; do
      o=$(fw "$n" "echo LEFT=\$(tc qdisc show | grep -cE 'htb|netem|prio 1')")
      c=$(printf '%s' "$o" | sed -n 's/.*LEFT=\([0-9]*\).*/\1/p'); left=$((left + ${c:-0}))
    done
    if [ "$left" != "0" ]; then echo "QDISC HYGIENE ERROR: $left shaping qdisc(s) remain"; exit 1; fi
    echo "cleared (verified: 0 shaping qdiscs)" ;;
esac
