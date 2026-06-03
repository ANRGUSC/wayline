#!/usr/bin/env bash
# Fix stale hostPort DNAT on the Pis left over from the k3s-pi -> main cluster
# merge. Before the merge the Pis used the 10.52.0.0/16 pod CIDR; the data-agent
# hostPort (8082) DNAT rule to the old 10.52.x pod IP was never cleaned, and it
# is matched BEFORE the correct 10.42.x rule, so a pod's local install PUT to
# its own node (WL_NODE_IP:8082) DNATs to a dead IP and times out.
#
# Fix: flush (empty) any CNI-DN-* chain whose DNAT targets a 10.52.x address.
# An emptied chain is a harmless no-op; traffic falls through to the correct
# 10.42.x rule. Idempotent.
#
#   fix-pi-hostport.sh            # fix the 12 benchmark Pis
#   fix-pi-hostport.sh <rpiN...>  # fix specific Pis
set -uo pipefail
SSHP="sshpass -p anrgrpi ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10"
PIS=("$@")
if [ ${#PIS[@]} -eq 0 ]; then
  PIS=(rpi11 rpi14 rpi15 rpi17 rpi26 rpi27 rpi28 rpi39 rpi44 rpi47 rpi50 rpi52)
fi

fix_one() {
  local n=$1 ip="192.168.1.$((100 + ${1#rpi}))"
  local out
  out=$($SSHP pi@"$ip" "echo anrgrpi | sudo -S -p '' sh -c '
    for ch in \$(iptables -t nat -S 2>/dev/null | grep -oE \"CNI-DN-[0-9a-f]+\" | sort -u); do
      if iptables -t nat -S \$ch 2>/dev/null | grep -q \"to-destination 10.52\"; then
        iptables -t nat -F \$ch 2>/dev/null && echo flushed \$ch;
      fi
    done
    (timeout 4 bash -c \"echo > /dev/tcp/$ip/8082\" 2>/dev/null && echo REACHABLE || echo UNREACHABLE)'" 2>&1)
  echo "$n: $(echo "$out" | tr '\n' ' ')"
}

echo "[fix-pi-hostport] fixing ${#PIS[@]} Pis..."
for n in "${PIS[@]}"; do fix_one "$n" & done
wait
echo "[fix-pi-hostport] done"
