#!/usr/bin/env bash
#
# Block 5b — adversarial failure injection on the data-agent.
#
# Three injection tests reviewers explicitly asked for:
#   (1) Kill sender data-agent AFTER /push returns 202. Expected:
#       agent restarts via DaemonSet, scans transfers/*.state,
#       resumes the Pending transfer; receiver gets the payload.
#   (2) Kill receiver data-agent MID-TRANSFER. Expected: no
#       partial .tmp survives the restart; no .wl-ready over
#       a partial file.
#   (3) Oversized PUT (claimed Content-Length 100 GB, 1 KB body).
#       Expected: PUT fails cleanly, no .wl-ready written.
#
#   ./failure-injection.sh [NODE=anrg-3] [PEER=anrg-6]
#
# A python:3.12-alpine helper pod is spawned to issue HTTP calls
# against the data-agent pods' IPs (the data-agent containers are
# Alpine-only and lack python3). Final line: SUMMARY: K/N pass.
set -uo pipefail

NODE="${1:-anrg-3}"
PEER="${2:-anrg-6}"
NS=wl-system

get_pod()    { kubectl -n "$NS" get pod -l app=data-agent -o jsonpath="{.items[?(@.spec.nodeName==\"$1\")].metadata.name}"; }
get_pod_ip() { kubectl -n "$NS" get pod "$1" -o jsonpath='{.status.podIP}'; }

DA_POD=$(get_pod "$NODE")
DA_IP=$(get_pod_ip "$DA_POD")
PEER_POD=$(get_pod "$PEER")
PEER_IP=$(get_pod_ip "$PEER_POD")
[ -z "$DA_POD" ] || [ -z "$PEER_POD" ] && { echo "ERROR: missing data-agent pods"; exit 99; }
echo "sender:   $DA_POD on $NODE ($DA_IP)"
echo "receiver: $PEER_POD on $PEER ($PEER_IP)"
echo

# --- spawn helper pod -------------------------------------------------------
HELPER="fij-helper-$(date +%s)"
echo "Spawning helper pod $HELPER on $NODE..."
kubectl -n "$NS" run "$HELPER" --image=python:3.12-alpine --restart=Never \
  --overrides='{"spec":{"nodeName":"'$NODE'","containers":[{"name":"'$HELPER'","image":"python:3.12-alpine","command":["sh","-c","sleep 900"]}]}}' \
  >/dev/null 2>&1
for i in $(seq 1 30); do
    p=$(kubectl -n "$NS" get pod "$HELPER" -o jsonpath='{.status.phase}' 2>/dev/null)
    [ "$p" = "Running" ] && break
    sleep 2
done
if [ "$p" != "Running" ]; then
    echo "ERROR: helper failed to start"
    kubectl -n "$NS" delete pod "$HELPER" --wait=false >/dev/null 2>&1
    exit 99
fi
echo "Helper ready."
echo

PY_HELPER=$(cat <<'PYEOF'
import sys, json, urllib.request, urllib.error
def http(method, url, body=None, hdrs=None, timeout=15):
    req = urllib.request.Request(url, data=body, method=method, headers=hdrs or {})
    try:
        r = urllib.request.urlopen(req, timeout=timeout); return r.status, r.read()
    except urllib.error.HTTPError as e: return e.code, e.read()
    except Exception as e: return -1, str(e).encode()
cmd = sys.argv[1]
if cmd == "push":
    base, odag, task, peer_host, peer_node = sys.argv[2:7]
    body = json.dumps({"successors":[{"name":"consume","host":peer_host,"node":peer_node}]}).encode()
    s, b = http("POST", f"{base}/push/{odag}/{task}", body, {"Content-Type":"application/json"})
    print(f"STATUS={s}"); print(b.decode(errors="replace"))
elif cmd == "oversized":
    import socket
    host, port, odag = sys.argv[2], int(sys.argv[3]), sys.argv[4]
    sk = socket.socket(); sk.settimeout(30); sk.connect((host, port))
    req = (f"PUT /{odag}/consume/output HTTP/1.1\r\n"
           f"Host: {host}\r\n"
           f"Content-Length: 107374182400\r\n"
           f"X-Wayline-Content-SHA256: " + "0"*64 + "\r\n"
           f"X-Wayline-Uncompressed-Length: 107374182400\r\n"
           f"\r\n")
    sk.sendall(req.encode()); sk.sendall(b"A"*1024)
    try:
        resp = sk.recv(2048).decode(errors="replace")
        code = resp.split(" ",2)[1] if "HTTP" in resp else "?"
        print(f"STATUS={code}")
    except Exception: print("STATUS=TIMEOUT")
    sk.close()
PYEOF
)

py() { kubectl -n "$NS" exec "$HELPER" -- python3 -c "$PY_HELPER" "$@" 2>&1; }

PASS=0; FAIL=0
report() {
    local s="$1" n="$2" d="${3:-}"
    if [ "$s" = "PASS" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi
    printf "  %-4s  %-58s  %s\n" "$s" "$n" "$d"
}

ts=$(date +%s)

# ---------------------------------------------------------------------------
echo "== test 1: kill sender after /push 202 (durable-queue recovery) ========"
ODAG="fij-${ts}-t1"; TASK="produce"

# Stage payload + readiness state on the sender
kubectl -n "$NS" exec "$DA_POD" -- sh -c "
  mkdir -p /data/wl-outputs/${ODAG}/${TASK}
  head -c 16384 /dev/urandom > /data/wl-outputs/${ODAG}/${TASK}/output
  echo -n ComputeDone > /data/wl-outputs/${ODAG}/${TASK}/.wl-task-state
  touch /data/wl-outputs/${ODAG}/${TASK}/.wl-ready
  sha=\$(sha256sum /data/wl-outputs/${ODAG}/${TASK}/output | awk '{print \$1}')
  echo -n \"\$sha\" > /data/wl-outputs/${ODAG}/${TASK}/.wl-sha256
  echo 16384 > /data/wl-outputs/${ODAG}/${TASK}/.wl-bytes
" >/dev/null 2>&1

# /push from helper to sender (cross-node successor target: receiver)
push_out=$(py push "http://${DA_IP}:8082" "$ODAG" "$TASK" "$PEER_IP" "$PEER")
push_status=$(echo "$push_out" | grep -oP 'STATUS=\K-?\d+')
if [ "$push_status" = "202" ]; then
    report PASS "/push returns 202 (durable enqueue committed)"
else
    report FAIL "/push returns 202 (durable enqueue committed)" "status=$push_status"
fi

state=$(kubectl -n "$NS" exec "$DA_POD" -- cat /data/wl-outputs/${ODAG}/${TASK}/transfers/consume.state 2>/dev/null)
if [ -n "$state" ]; then
    report PASS "Sender persisted transfers/consume.state before 202" "state=$state"
else
    report FAIL "Sender persisted transfers/consume.state before 202" "missing"
fi

echo "  killing sender agent pod $DA_POD..."
kubectl -n "$NS" delete pod "$DA_POD" --grace-period=0 --force >/dev/null 2>&1

NEW_DA_POD=""
for i in $(seq 1 30); do
    sleep 2
    NEW_DA_POD=$(get_pod "$NODE")
    if [ -n "$NEW_DA_POD" ] && [ "$NEW_DA_POD" != "$DA_POD" ]; then
        ph=$(kubectl -n "$NS" get pod "$NEW_DA_POD" -o jsonpath='{.status.phase}')
        [ "$ph" = "Running" ] && break
    fi
done
if [ -z "$NEW_DA_POD" ] || [ "$NEW_DA_POD" = "$DA_POD" ]; then
    report FAIL "Sender agent restarted by DaemonSet" "still=$NEW_DA_POD"
else
    report PASS "Sender agent restarted by DaemonSet" "new=$NEW_DA_POD"
fi
DA_POD="$NEW_DA_POD"
DA_IP=$(get_pod_ip "$DA_POD")

sleep 10  # let recoverTransfers run

recv_ok=$(kubectl -n "$NS" exec "$PEER_POD" -- sh -c "
  test -f /data/wl-outputs/${ODAG}/${TASK}/output && test -f /data/wl-outputs/${ODAG}/${TASK}/.wl-ready && echo OK
" 2>/dev/null)
if [ "$recv_ok" = "OK" ]; then
    report PASS "Receiver got payload + .wl-ready after sender restart"
else
    report FAIL "Receiver got payload + .wl-ready after sender restart" "got=$recv_ok"
fi

state_after=$(kubectl -n "$NS" exec "$DA_POD" -- cat /data/wl-outputs/${ODAG}/${TASK}/transfers/consume.state 2>/dev/null)
case "$state_after" in
    ReadyRemote|Sent|Acknowledged)
        report PASS "Sender transfers/consume.state == terminal after recovery" "state=$state_after" ;;
    *)  report FAIL "Sender transfers/consume.state == terminal after recovery" "state=$state_after" ;;
esac

kubectl -n "$NS" exec "$DA_POD"   -- rm -rf /data/wl-outputs/${ODAG} >/dev/null 2>&1 || true
kubectl -n "$NS" exec "$PEER_POD" -- rm -rf /data/wl-outputs/${ODAG} >/dev/null 2>&1 || true

echo
# ---------------------------------------------------------------------------
echo "== test 2: kill receiver mid-transfer (no partial bytes visible) ======="
ODAG="fij-${ts}-t2"

kubectl -n "$NS" exec "$DA_POD" -- sh -c "
  mkdir -p /data/wl-outputs/${ODAG}/${TASK}
  dd if=/dev/urandom of=/data/wl-outputs/${ODAG}/${TASK}/output bs=1M count=10 status=none
  sha=\$(sha256sum /data/wl-outputs/${ODAG}/${TASK}/output | awk '{print \$1}')
  echo -n \"\$sha\" > /data/wl-outputs/${ODAG}/${TASK}/.wl-sha256
  echo 10485760 > /data/wl-outputs/${ODAG}/${TASK}/.wl-bytes
  echo -n ComputeDone > /data/wl-outputs/${ODAG}/${TASK}/.wl-task-state
  touch /data/wl-outputs/${ODAG}/${TASK}/.wl-ready
" >/dev/null 2>&1

py push "http://${DA_IP}:8082" "$ODAG" "$TASK" "$PEER_IP" "$PEER" >/dev/null 2>&1 &
sleep 0.05
kubectl -n "$NS" delete pod "$PEER_POD" --grace-period=0 --force >/dev/null 2>&1
wait

NEW_PEER_POD=""
for i in $(seq 1 30); do
    sleep 2
    NEW_PEER_POD=$(get_pod "$PEER")
    if [ -n "$NEW_PEER_POD" ] && [ "$NEW_PEER_POD" != "$PEER_POD" ]; then
        ph=$(kubectl -n "$NS" get pod "$NEW_PEER_POD" -o jsonpath='{.status.phase}')
        [ "$ph" = "Running" ] && break
    fi
done
PEER_POD="$NEW_PEER_POD"
PEER_IP=$(get_pod_ip "$PEER_POD")

tmp_files=$(kubectl -n "$NS" exec "$PEER_POD" -- find /data/wl-outputs/${ODAG} -name '*.tmp' 2>/dev/null | wc -l)
if [ "$tmp_files" = "0" ]; then
    report PASS "No partial *.tmp files after receiver restart"
else
    report FAIL "No partial *.tmp files after receiver restart" "found=$tmp_files"
fi

ready_ok=$(kubectl -n "$NS" exec "$PEER_POD" -- sh -c "
  if [ -f /data/wl-outputs/${ODAG}/${TASK}/.wl-ready ]; then
    size=\$(stat -c %s /data/wl-outputs/${ODAG}/${TASK}/output 2>/dev/null || echo 0)
    if [ \"\$size\" = \"10485760\" ]; then echo READY_FULL
    else echo READY_PARTIAL_\$size; fi
  else echo NOT_READY; fi
" 2>/dev/null)
case "$ready_ok" in
    READY_FULL)       report PASS "Either clean install via retry, or no .wl-ready" "(installed)" ;;
    NOT_READY)        report PASS "Either clean install via retry, or no .wl-ready" "(no leak)"   ;;
    READY_PARTIAL_*)  report FAIL "Either clean install via retry, or no .wl-ready" "$ready_ok"   ;;
    *)                report FAIL "Either clean install via retry, or no .wl-ready" "$ready_ok"   ;;
esac

kubectl -n "$NS" exec "$DA_POD"    -- rm -rf /data/wl-outputs/${ODAG} >/dev/null 2>&1 || true
kubectl -n "$NS" exec "$PEER_POD"  -- rm -rf /data/wl-outputs/${ODAG} >/dev/null 2>&1 || true

echo
# ---------------------------------------------------------------------------
echo "== test 3: oversized PUT (claimed 100 GB, only 1 KB) ==================="
ODAG="fij-${ts}-t3"

status3=$(py oversized "$PEER_IP" 8082 "$ODAG" | grep -oP 'STATUS=\K\S+')
case "$status3" in
    4*|5*)   report PASS "Oversized PUT failed cleanly" "status=$status3" ;;
    TIMEOUT) report PASS "Oversized PUT timed out (no leak observed)"      ;;
    *)       report FAIL "Oversized PUT failed cleanly" "status=$status3"  ;;
esac

leaked=$(kubectl -n "$NS" exec "$PEER_POD" -- sh -c "
  test -f /data/wl-outputs/${ODAG}/${TASK}/.wl-ready && echo LEAK || echo CLEAN
" 2>/dev/null)
if [ "$leaked" = "CLEAN" ]; then
    report PASS "Oversized PUT did not create .wl-ready"
else
    report FAIL "Oversized PUT did not create .wl-ready" "leaked=$leaked"
fi
kubectl -n "$NS" exec "$PEER_POD" -- rm -rf /data/wl-outputs/${ODAG} >/dev/null 2>&1 || true

echo
echo "Cleaning up helper pod..."
kubectl -n "$NS" delete pod "$HELPER" --wait=false >/dev/null 2>&1
echo
echo "======================================================================"
echo "SUMMARY: $PASS/$((PASS+FAIL)) pass"
echo "======================================================================"
exit $FAIL
