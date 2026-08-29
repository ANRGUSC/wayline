#!/usr/bin/env bash
# E6 network (identical matrix to E5, per the campaign spec): per-node egress HTB with three rate tiers,
# matched on data-agent traffic (tcp/8082) per destination IP.
#   within edge {1,3,4,5}: 942 Mbit    within compute {6,7,8}: 942 Mbit
#   edge <-> compute:      118 Mbit    any pair with anrg-9:      59 Mbit
# Every reverse direction carries the identical rate by construction:
# the rate is a pure function of the unordered node pair.
#   ./e6_net.sh apply | verify | clear
set -uo pipefail
CMD=${1:?usage: e6_net.sh apply|verify|clear}
NS=wl-system
EDGE="anrg-1 anrg-3 anrg-4 anrg-5"
COMPUTE="anrg-6 anrg-7 anrg-8"
GW=anrg-9
ALL="$EDGE $COMPUTE $GW"
ip_of() { kubectl get node "$1" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}'; }
declare -A IP
for n in $ALL; do IP[$n]=$(ip_of "$n"); done
fw() { kubectl -n "$NS" exec "e6-fw-$1" -- sh -c "$2" 2>&1; }
group() { case " $EDGE " in *" $1 "*) echo edge;; esac
          case " $COMPUTE " in *" $1 "*) echo compute;; esac
          [ "$1" = "$GW" ] && echo gw; }
# classid for the u->v pair: 10 = 942, 20 = 118, 30 = 59
cls() {
  [ "$1" = "$GW" ] || [ "$2" = "$GW" ] && { echo 30; return; }
  [ "$(group "$1")" = "$(group "$2")" ] && echo 10 || echo 20
}

# The tc commands run inside a privileged host-network pod per node.
# E5 created these from its harness; doing it here instead makes the
# script self-sufficient, so `apply` cannot fail merely because nothing
# made the pods first.
ensure_pods() {
  # A pod that exists but is not Running (its sleep expired after 24h,
  # or it was evicted) cannot be exec'd, which fails every verify while
  # the host qdiscs are actually still live. Recreate on any non-Running
  # phase, and sleep infinity so expiry cannot recur.
  local need=0
  for n in $ALL; do
    ph=$(kubectl -n "$NS" get pod "e6-fw-$n" -o jsonpath='{.status.phase}' 2>/dev/null)
    [ "$ph" = "Running" ] || need=1
  done
  [ "$need" = 0 ] && return 0
  for n in $ALL; do
    ph=$(kubectl -n "$NS" get pod "e6-fw-$n" -o jsonpath='{.status.phase}' 2>/dev/null)
    [ "$ph" = "Running" ] && continue
    kubectl -n "$NS" delete pod "e6-fw-$n" --ignore-not-found --wait=false >/dev/null 2>&1
    kubectl run "e6-fw-$n" -n "$NS" --restart=Never --image=alpine \
      --overrides="{\"spec\":{\"nodeName\":\"$n\",\"hostNetwork\":true,\"restartPolicy\":\"Never\",\"containers\":[{\"name\":\"c\",\"image\":\"alpine\",\"command\":[\"sh\",\"-c\",\"apk add -q iproute2 >/dev/null && sleep infinity\"],\"securityContext\":{\"privileged\":true}}]}}" \
      >/dev/null 2>&1
  done
  for n in $ALL; do
    for i in $(seq 1 60); do
      kubectl -n "$NS" exec "e6-fw-$n" -- sh -c 'tc -V' >/dev/null 2>&1 && break
      sleep 3
    done
  done
}

case "$CMD" in
  apply)
    ensure_pods
    for u in $ALL; do
      F=""
      for v in $ALL; do
        [ "$u" = "$v" ] && continue
        F="${F}tc filter add dev \$IF parent 1: protocol ip prio 1 u32 match ip dst ${IP[$v]}/32 match ip dport 8082 0xffff flowid 1:$(cls "$u" "$v"); "
      done
      fw "$u" "IF=\$(ip route get ${IP[$GW]} | grep -o 'dev [^ ]*' | awk '{print \$2}'); \
        tc qdisc del dev \$IF root 2>/dev/null; \
        tc qdisc add dev \$IF root handle 1: htb default 99; \
        tc class add dev \$IF parent 1: classid 1:99 htb rate 10gbit ceil 10gbit; \
        tc class add dev \$IF parent 1: classid 1:10 htb rate 942mbit ceil 942mbit; \
        tc class add dev \$IF parent 1: classid 1:20 htb rate 118mbit ceil 118mbit; \
        tc class add dev \$IF parent 1: classid 1:30 htb rate 59mbit ceil 59mbit; \
        ${F}true" >/dev/null
    done
    exec "$0" verify ;;
  verify)
    ensure_pods
    bad=0
    for u in $ALL; do
      out=$(fw "$u" "IF=\$(ip route get ${IP[$GW]} | grep -o 'dev [^ ]*' | awk '{print \$2}'); \
        echo R10=\$(tc class show dev \$IF | sed -n 's/.*htb 1:10 .*rate \([^ ]*\).*/\1/p') \
             R20=\$(tc class show dev \$IF | sed -n 's/.*htb 1:20 .*rate \([^ ]*\).*/\1/p') \
             R30=\$(tc class show dev \$IF | sed -n 's/.*htb 1:30 .*rate \([^ ]*\).*/\1/p') \
             F=\$(tc filter show dev \$IF | grep -c 'flowid 1:')")
      r10=$(printf '%s' "$out" | sed -n 's/.*R10=\([^ ]*\).*/\1/p')
      r20=$(printf '%s' "$out" | sed -n 's/.*R20=\([^ ]*\).*/\1/p')
      r30=$(printf '%s' "$out" | sed -n 's/.*R30=\([^ ]*\).*/\1/p')
      nf=$(printf '%s'  "$out" | sed -n 's/.*F=\([0-9]*\).*/\1/p')
      if [ "$r10" != "942Mbit" ] || [ "$r20" != "118Mbit" ] || \
         [ "$r30" != "59Mbit" ] || [ "$nf" != "7" ]; then
        echo "RATE ERROR on $u: 942='$r10' 118='$r20' 59='$r30' filters='$nf'"
        bad=1
      fi
    done
    [ "$bad" = 0 ] || exit 1
    echo "symmetric matrix live (verified on 8 nodes: 942/118/59 Mbit, 7 filters each)" ;;
  clear)
    CLEANUP_PODS=${CLEANUP_PODS:-0}
    left=0
    for u in $ALL; do
      fw "$u" "for i in \$(ls /sys/class/net); do tc qdisc del dev \$i root 2>/dev/null; done; true" >/dev/null
      o=$(fw "$u" "echo LEFT=\$(tc qdisc show | grep -cE 'htb|netem')")
      c=$(printf '%s' "$o" | sed -n 's/.*LEFT=\([0-9]*\).*/\1/p'); left=$((left + ${c:-0}))
    done
    [ "$left" = 0 ] || { echo "QDISC HYGIENE ERROR: $left remain"; exit 1; }
    echo "cleared (verified: 0 shaping qdiscs on 8 nodes)" ;;
esac
