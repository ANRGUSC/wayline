#!/usr/bin/env bash
# Contact emulation for E2, on the SENDER's egress via tc u32 + action
# drop, matched on destination IP AND tcp dport 8082 (data-agent traffic
# only; API 6443, registry 5000, and everything else are untouched).
#
# Why not iptables: agent traffic reaches a node through the hostPort
# portmap DNAT and never traverses the filter INPUT/FORWARD hooks a
# privileged pod can see — three iptables variants "verified" clean and
# still passed 300 MB. tc egress sees the packets (E1's caps measured
# exactly this traffic), and a dropped transfer STALLS in Transferring
# and completes when the contact reopens: temporal-relay semantics.
#
# contacts.sh owns the root qdisc on anrg-3 and anrg-7 for the duration.
#   ./contacts.sh init | close-3-7 | open-7-8 | close-7-8 | clear | status
set -uo pipefail
CMD=${1:?usage: contacts.sh init|close-3-7|open-7-8|close-7-8|clear|status}
NS=wl-system
ip_of() { kubectl get node "$1" -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}'; }
IP7=$(ip_of anrg-7); IP8=$(ip_of anrg-8)
agent_ip() { kubectl -n "$NS" get pods -o wide --no-headers -l app=data-agent | awk -v n="$1" '$7==n {print $6}'; }
# fw runs a command in the node's privileged pod. Exit status and stderr
# are PRESERVED: a silently-failed exec once let a "closed" contact stay
# open for a whole run while the log said otherwise.
fw() {
  local node=$1 cmd=$2 out rc
  out=$(kubectl -n "$NS" exec "e2-fw-$node" -- sh -c "$cmd" 2>&1); rc=$?
  printf '%s' "$out"
  return $rc
}

# assert_drops <node> <expected-count> — structural verification that the
# node's egress carries exactly the expected number of drop filters.
assert_drops() {
  local node=$1 want=$2 got
  got=$(fw "$node" "IF=\$(ip route get $IP8 | grep -o 'dev [^ ]*' | awk '{print \$2}'); tc filter show dev \$IF 2>/dev/null | grep -c 'action order'")
  got=$(printf '%s' "$got" | tr -dc '0-9')
  [ -z "$got" ] && got=-1
  if [ "$got" != "$want" ]; then
    echo "CONTACT STATE ERROR on $node: drop filters=$got expected=$want"
    return 1
  fi
  return 0
}

# block_set <node> [dst-ip ...] — rebuild that node's egress drop set.
block_set() {
  local node=$1; shift
  local ref="${1:-$IP8}"
  local filters=""
  for dst in "$@"; do
    filters+="tc filter add dev \$IF parent 1: protocol ip prio 1 u32 match ip dst $dst/32 match ip dport 8082 0xffff action drop; "
  done
  fw "$node" "IF=\$(ip route get $ref | grep -o 'dev [^ ]*' | awk '{print \$2}'); \
              tc qdisc del dev \$IF root 2>/dev/null; \
              if [ -n \"$filters\" ]; then tc qdisc add dev \$IF root handle 1: prio && $filters fi; true"
}

# verify_contacts — functional proof with a REAL agent push: a small
# object must fail to reach the blocked peer and succeed to the open one.
verify_contacts() {
  local blocked_ip=$1 open_ip=$2 A3
  A3=$(agent_ip anrg-3)
  [ -n "$A3" ] || { echo "verify: no agent on anrg-3"; return 1; }
  fw anrg-3 "head -c 200000 /dev/urandom > /tmp/p.bin; true" >/dev/null
  local D
  D=$(fw anrg-3 "sha256sum /tmp/p.bin | cut -d' ' -f1")
  fw anrg-3 "wget -q -O /dev/null --method=PUT --body-file=/tmp/p.bin \
             --header='X-Wayline-Content-SHA256: $D' \
             http://$A3:8082/e2probe/obj/output" >/dev/null
  curl -s -m 5 -X POST -H 'Content-Type: application/json' \
    -d "{\"successors\":[{\"name\":\"pblocked\",\"host\":\"$blocked_ip\",\"node\":\"x\"},{\"name\":\"popen\",\"host\":\"$open_ip\",\"node\":\"y\"}]}" \
    "http://$A3:8082/push/e2probe/obj" >/dev/null
  sleep 8
  local sb so
  sb=$(curl -s -m 5 "http://$A3:8082/transfers/e2probe/obj/pblocked")
  so=$(curl -s -m 5 "http://$A3:8082/transfers/e2probe/obj/popen")
  curl -s -m 10 -X DELETE "http://$A3:8082/data/e2probe" >/dev/null
  curl -s -m 10 -X DELETE "http://$(agent_ip anrg-7):8082/data/e2probe" >/dev/null 2>&1
  curl -s -m 10 -X DELETE "http://$(agent_ip anrg-8):8082/data/e2probe" >/dev/null 2>&1
  echo "verify: blocked-peer=$sb open-peer=$so"
  [ "$sb" != "ReadyRemote" ] && [ "$so" = "ReadyRemote" ]
}

case "$CMD" in
  init)        # 3->8 blocked (whole run), 7->8 blocked, 3->7 open
    block_set anrg-3 "$IP8"
    block_set anrg-7 "$IP8"
    assert_drops anrg-3 1 || exit 1
    assert_drops anrg-7 1 || exit 1
    if verify_contacts "$IP8" "$IP7"; then
      echo "init: 3->8 blocked, 7->8 blocked, 3->7 open (verified)"
    else
      echo "INIT VERIFY FAILED"; exit 1
    fi ;;
  close-3-7)
    block_set anrg-3 "$IP8" "$IP7"
    assert_drops anrg-3 2 || exit 1
    echo "3->7 closed (verified)" ;;
  open-7-8)
    block_set anrg-7
    assert_drops anrg-7 0 || exit 1
    echo "7->8 open (verified)" ;;
  close-7-8)
    block_set anrg-7 "$IP8"
    assert_drops anrg-7 1 || exit 1
    echo "7->8 closed (verified)" ;;
  clear)
    for n in anrg-3 anrg-7; do
      fw "$n" "for i in \$(ls /sys/class/net); do tc qdisc del dev \$i root 2>/dev/null; done; true"
    done
    left=$(for n in anrg-3 anrg-7; do fw "$n" "tc qdisc show | grep -c 'action drop\|prio 1'"; done | paste -sd+ | bc 2>/dev/null || echo 0)
    echo "cleared" ;;
  status)
    for n in anrg-3 anrg-7; do echo "== $n"; fw "$n" "tc filter show dev \$(ip route get $IP8 | grep -o 'dev [^ ]*' | awk '{print \$2}') 2>/dev/null | head -4; true"; done ;;
esac
