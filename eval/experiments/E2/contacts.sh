#!/usr/bin/env bash
# Contact emulation for E2, restricted to data-agent traffic (TCP 8082).
# Rules live in a dedicated WL_E2 chain on the RECEIVING node's INPUT so
# "clear" is one flush. REJECT with tcp-reset so blocked transfers fail
# fast (a dropped SYN would hang past the contact windows).
#   ./contacts.sh init | close-3-7 | open-7-8 | close-7-8 | clear | status
set -uo pipefail
CMD=${1:?usage: contacts.sh init|close-3-7|open-7-8|close-7-8|clear|status}
NS=wl-system
IP3=$(kubectl get node anrg-3 -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
IP7=$(kubectl get node anrg-7 -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
PC3=$(kubectl get node anrg-3 -o jsonpath='{.spec.podCIDR}')
PC7=$(kubectl get node anrg-7 -o jsonpath='{.spec.podCIDR}')

# block <node> <srcIP> <srcCIDR>: agents push from the POD network, so a
# source is blocked by BOTH its node IP (host-network paths) and its
# flannel pod subnet (pod-network paths).
block() {
  fw $1 "iptables -A WL_E2 -s $2 -p tcp --dport 8082 -j REJECT --reject-with tcp-reset; iptables -A WL_E2 -s $3 -p tcp --dport 8082 -j REJECT --reject-with tcp-reset"
}
unblock() {
  fw $1 "iptables -D WL_E2 -s $2 -p tcp --dport 8082 -j REJECT --reject-with tcp-reset; iptables -D WL_E2 -s $3 -p tcp --dport 8082 -j REJECT --reject-with tcp-reset"
}

fw() { # <node> <iptables-cmds>
  kubectl -n "$NS" exec e2-fw-$1 -- sh -c "$2"
}
ensure_chain() { # <node>
  # Agent traffic to nodeIP:8082 is DNATed by the hostPort portmap in
  # PREROUTING and traverses FORWARD, not INPUT; hook both.
  fw $1 "iptables -N WL_E2 2>/dev/null; iptables -C INPUT -j WL_E2 2>/dev/null || iptables -I INPUT 1 -j WL_E2; iptables -C FORWARD -j WL_E2 2>/dev/null || iptables -I FORWARD 1 -j WL_E2"
}
verify() { # <expect-fail-ip> <expect-ok-ip>
  # Active verification from BOTH network identities of anrg-3: the
  # host (e2-fw pod, hostNetwork) and the pod network (e2-probe pod).
  bad=$(fw anrg-3 "wget -q -T 2 -O /dev/null http://$1:8082/healthz 2>/dev/null && echo REACHABLE || echo blocked")
  pbad=$(kubectl -n "$NS" exec e2-probe-anrg-3 -- sh -c "wget -q -T 2 -O /dev/null http://$1:8082/healthz 2>/dev/null && echo REACHABLE || echo blocked")
  good=$(fw anrg-3 "wget -q -T 2 -O /dev/null http://$2:8082/healthz 2>/dev/null && echo ok || echo UNREACHABLE")
  pgood=$(kubectl -n "$NS" exec e2-probe-anrg-3 -- sh -c "wget -q -T 2 -O /dev/null http://$2:8082/healthz 2>/dev/null && echo ok || echo UNREACHABLE")
  echo "verify: host-src=$bad/$good pod-src=$pbad/$pgood"
  [ "$bad" = blocked ] && [ "$pbad" = blocked ] && [ "$good" = ok ] && [ "$pgood" = ok ]
}
case "$CMD" in
  init)
    ensure_chain anrg-8; ensure_chain anrg-7
    # blocked for the whole run: 3->8; blocked until opened: 7->8. 3->7 open.
    fw anrg-8 "iptables -F WL_E2"
    fw anrg-7 "iptables -F WL_E2"
    block anrg-8 "$IP3" "$PC3"
    block anrg-8 "$IP7" "$PC7"
    IP8=$(kubectl get node anrg-8 -o jsonpath="{.status.addresses[?(@.type==\"InternalIP\")].address}")
    verify "$IP8" "$IP7" || { echo "INIT VERIFY FAILED"; exit 1; }
    echo "init: 3->8 blocked, 7->8 blocked, 3->7 open (verified)" ;;
  close-3-7)
    block anrg-7 "$IP3" "$PC3"
    echo "3->7 closed" ;;
  open-7-8)
    unblock anrg-8 "$IP7" "$PC7"
    echo "7->8 open" ;;
  close-7-8)
    block anrg-8 "$IP7" "$PC7"
    echo "7->8 closed" ;;
  clear)
    for n in anrg-7 anrg-8; do
      fw $n "iptables -F WL_E2 2>/dev/null; iptables -D INPUT -j WL_E2 2>/dev/null; iptables -D FORWARD -j WL_E2 2>/dev/null; iptables -X WL_E2 2>/dev/null; true"
    done
    echo cleared ;;
  status)
    for n in anrg-7 anrg-8; do echo "== $n"; fw $n "iptables -S WL_E2 2>/dev/null; true"; done ;;
esac
