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

fw() { # <node> <iptables-cmds>
  kubectl -n "$NS" exec e2-fw-$1 -- sh -c "$2"
}
ensure_chain() { # <node>
  fw $1 "iptables -N WL_E2 2>/dev/null; iptables -C INPUT -j WL_E2 2>/dev/null || iptables -I INPUT 1 -j WL_E2"
}
case "$CMD" in
  init)
    ensure_chain anrg-8; ensure_chain anrg-7
    # blocked for the whole run: 3->8; blocked until opened: 7->8. 3->7 open.
    fw anrg-8 "iptables -F WL_E2; iptables -A WL_E2 -s $IP3 -p tcp --dport 8082 -j REJECT --reject-with tcp-reset; iptables -A WL_E2 -s $IP7 -p tcp --dport 8082 -j REJECT --reject-with tcp-reset"
    fw anrg-7 "iptables -F WL_E2"
    echo "init: 3->8 blocked, 7->8 blocked, 3->7 open" ;;
  close-3-7)
    fw anrg-7 "iptables -A WL_E2 -s $IP3 -p tcp --dport 8082 -j REJECT --reject-with tcp-reset"
    echo "3->7 closed" ;;
  open-7-8)
    fw anrg-8 "iptables -D WL_E2 -s $IP7 -p tcp --dport 8082 -j REJECT --reject-with tcp-reset"
    echo "7->8 open" ;;
  close-7-8)
    fw anrg-8 "iptables -A WL_E2 -s $IP7 -p tcp --dport 8082 -j REJECT --reject-with tcp-reset"
    echo "7->8 closed" ;;
  clear)
    for n in anrg-7 anrg-8; do
      fw $n "iptables -F WL_E2 2>/dev/null; iptables -D INPUT -j WL_E2 2>/dev/null; iptables -X WL_E2 2>/dev/null; true"
    done
    echo cleared ;;
  status)
    for n in anrg-7 anrg-8; do echo "== $n"; fw $n "iptables -S WL_E2 2>/dev/null; true"; done ;;
esac
