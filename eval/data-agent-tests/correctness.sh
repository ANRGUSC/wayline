#!/usr/bin/env bash
#
# Block 5 — data-agent correctness and failure tests.
#
# Spawns a python helper pod and hits the data-agent's HTTP API on
# anrg-3 (or NODE env var) over the cluster network. Each test verifies
# one documented invariant of the data-agent state machine and prints
# PASS or FAIL with a short reason.
#
#   ./correctness.sh [NODE=anrg-3]
#
# Requires: kubectl context with data-agents in wl-system. The helper
# pod is short-lived (deleted at end).
set -uo pipefail

NODE="${1:-anrg-3}"
NS=wl-system
DA_POD=$(kubectl -n "$NS" get pod -l app=data-agent -o jsonpath="{.items[?(@.spec.nodeName==\"$NODE\")].metadata.name}")
DA_IP=$(kubectl -n "$NS" get pod "$DA_POD" -o jsonpath='{.status.podIP}')
DA_URL="http://${DA_IP}:8082"
if [ -z "$DA_POD" ] || [ -z "$DA_IP" ]; then
    echo "ERROR: no data-agent on $NODE"; exit 99
fi
echo "data-agent pod: $DA_POD on $NODE ($DA_IP)"
echo "data-agent base URL: $DA_URL"

HELPER="datest-$(date +%s)"
ts=$(date +%s)
PASS=0; FAIL=0

# --- helper script: a tiny REPL that runs in the helper pod ----------
HELPER_SCRIPT=$(cat <<PYEOF
import sys, urllib.request, urllib.parse, urllib.error, hashlib, os, time

BASE = os.environ["BASE"]

def _do(method, url, body=None, hdrs=None, timeout=30):
    req = urllib.request.Request(url, data=body, method=method, headers=hdrs or {})
    try:
        r = urllib.request.urlopen(req, timeout=timeout)
        return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except Exception as e:
        return -1, str(e).encode()

def put(path, body, hdrs=None):
    return _do("PUT", BASE + path, body, hdrs)

def delete(path):
    return _do("DELETE", BASE + path)

def get(path):
    return _do("GET", BASE + path)

def sha256(b): return hashlib.sha256(b).hexdigest()

def step_to_path(odag, task, fname="output"):
    return f"/{urllib.parse.quote(odag, safe='')}/{urllib.parse.quote(task, safe='')}/{urllib.parse.quote(fname, safe='')}"

def run():
    cmd = sys.argv[1]
    if cmd == "roundtrip":
        odag, sz = sys.argv[2], int(sys.argv[3])
        body = os.urandom(sz)
        digest = sha256(body)
        # 1. PUT
        s1, b1 = put(step_to_path(odag, "produce"), body, {
            "X-Wayline-Content-SHA256": digest,
            "X-Wayline-Uncompressed-Length": str(sz),
        })
        # 2. GET
        s2, b2 = get(step_to_path(odag, "produce"))
        print(f"PUT={s1} GET={s2} GET_LEN={len(b2)} EXPECTED_LEN={sz}")
    elif cmd == "get-before-put":
        odag = sys.argv[2]
        s, _ = get(step_to_path(odag, "produce"))
        print(f"STATUS={s}")
    elif cmd == "idempotent":
        odag, sz = sys.argv[2], int(sys.argv[3])
        body = os.urandom(sz); digest = sha256(body)
        hdrs = {"X-Wayline-Content-SHA256": digest, "X-Wayline-Uncompressed-Length": str(sz)}
        s1, _ = put(step_to_path(odag, "produce"), body, hdrs)
        s2, _ = put(step_to_path(odag, "produce"), body, hdrs)
        print(f"PUT1={s1} PUT2={s2}")
    elif cmd == "conflict":
        odag = sys.argv[2]
        a = os.urandom(4096); b = os.urandom(4096)
        sa, sb = sha256(a), sha256(b)
        s1, _ = put(step_to_path(odag, "produce"), a, {"X-Wayline-Content-SHA256": sa, "X-Wayline-Uncompressed-Length": "4096"})
        s2, _ = put(step_to_path(odag, "produce"), b, {"X-Wayline-Content-SHA256": sb, "X-Wayline-Uncompressed-Length": "4096"})
        print(f"PUT1={s1} PUT2={s2}")
    elif cmd == "wrong-sha":
        odag = sys.argv[2]
        body = os.urandom(4096)
        s1, _ = put(step_to_path(odag, "produce"), body, {
            "X-Wayline-Content-SHA256": "0"*64, "X-Wayline-Uncompressed-Length": "4096"})
        s2, _ = get(step_to_path(odag, "produce"))
        print(f"PUT={s1} GET={s2}")
    elif cmd == "ready-put-rejected":
        odag = sys.argv[2]
        s, _ = put(f"/ready/{odag}/produce", b"true")
        print(f"STATUS={s}")
    elif cmd == "ready-delete":
        odag = sys.argv[2]
        # Install then clear-ready then check.
        body = os.urandom(2048); digest = sha256(body)
        put(step_to_path(odag, "produce"), body, {"X-Wayline-Content-SHA256": digest, "X-Wayline-Uncompressed-Length": "2048"})
        sd, _ = delete(f"/ready/{odag}/produce")
        sg, bg = get(f"/ready/{odag}/produce")
        print(f"DELETE={sd} GET_READY={sg} GET_BODY={bg.decode(errors='replace')}")
    elif cmd == "path-traversal":
        # Craft a relative path that would escape data dir if unvalidated.
        s, _ = get("/" + urllib.parse.quote("../etc", safe="") + "/x/output")
        print(f"STATUS={s}")
    elif cmd == "metrics":
        s, b = get("/metrics")
        print(f"STATUS={s}")
        print(b.decode())
    elif cmd == "installed-record":
        # An install must stamp a data-availability record, and must
        # attribute local (SDK) vs remote (agent-to-agent) installs.
        odag = sys.argv[2]
        body = os.urandom(4096); digest = sha256(body)
        hdrs = {"X-Wayline-Content-SHA256": digest, "X-Wayline-Uncompressed-Length": "4096"}
        t_before = time.time()
        put(step_to_path(odag, "local"), body, hdrs)
        s1, b1 = get(f"/installed/{odag}/local")
        put(step_to_path(odag, "remote"), body, dict(hdrs, **{"X-Wayline-Source-Node": "othernode"}))
        s2, b2 = get(f"/installed/{odag}/remote")
        s3, _ = get(f"/installed/{odag}/never-installed")
        print(f"LOCAL_STATUS={s1} LOCAL_BODY={b1.decode()}")
        print(f"REMOTE_STATUS={s2} REMOTE_BODY={b2.decode()}")
        print(f"MISSING_STATUS={s3} T_BEFORE={t_before}")
    elif cmd == "timings-roundtrip":
        # The agent stores SDK timing reports verbatim and rejects
        # non-JSON bodies; a missing report is a 404, not an empty object.
        odag = sys.argv[2]
        rec = b'{"computeSeconds":8.5,"handoffSeconds":0.4,"bytesOut":2048}'
        s1, _ = put(f"/timings/{odag}/t", rec, {"Content-Type": "application/json"})
        s2, b2 = get(f"/timings/{odag}/t")
        s3, _ = put(f"/timings/{odag}/t", b"not json", {"Content-Type": "application/json"})
        s4, _ = get(f"/timings/{odag}/absent")
        print(f"PUT_STATUS={s1} GET_STATUS={s2} GET_BODY={b2.decode()} BADJSON_STATUS={s3} MISSING_STATUS={s4}")

run()
PYEOF
)

# Spawn helper pod once
echo "Spawning helper pod $HELPER on $NODE..."
kubectl -n "$NS" run "$HELPER" --image=python:3.12-alpine --restart=Never \
  --overrides='{"spec":{"nodeName":"'$NODE'","containers":[{"name":"'$HELPER'","image":"python:3.12-alpine","command":["sh","-c","sleep 600"]}]}}' \
  >/dev/null 2>&1

# Wait for ready
for i in $(seq 1 30); do
    phase=$(kubectl -n "$NS" get pod "$HELPER" -o jsonpath='{.status.phase}' 2>/dev/null)
    [ "$phase" = "Running" ] && break
    sleep 2
done
if [ "$phase" != "Running" ]; then
    echo "ERROR: helper pod failed to start (phase=$phase)"
    kubectl -n "$NS" delete pod "$HELPER" --wait=false >/dev/null 2>&1
    exit 99
fi
echo "Helper pod ready."
echo

# Run a python script in the helper pod with BASE env set.
run_py() {
    kubectl -n "$NS" exec "$HELPER" -- env BASE="$DA_URL" python3 -c "$HELPER_SCRIPT" "$@" 2>&1
}

report() {
    local status="$1"; shift
    local name="$1"; shift
    local detail="$*"
    if [ "$status" = "PASS" ]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi
    printf "  %-4s  %-50s  %s\n" "$status" "$name" "$detail"
}

# ----- TESTS -------------------------------------------------------------

echo "== test 1: PUT then GET roundtrip ====================================="
ODAG="tst-${ts}-t1"
out=$(run_py roundtrip "$ODAG" 8192)
put_status=$(echo "$out" | grep -oP 'PUT=\K\d+')
get_status=$(echo "$out" | grep -oP 'GET=\K\d+')
get_len=$(echo "$out" | grep -oP 'GET_LEN=\K\d+')
if [ "$put_status" = "200" ]; then report PASS "PUT new payload returns 200"; else report FAIL "PUT new payload returns 200" "got=$put_status ($out)"; fi
if [ "$get_status" = "200" ] && [ "$get_len" = "8192" ]; then
    report PASS "GET after PUT returns 200 with correct size"
else
    report FAIL "GET after PUT returns 200 with correct size" "GET=$get_status len=$get_len"
fi

echo
echo "== test 2: GET before any PUT ========================================="
ODAG="tst-${ts}-t2"
out=$(run_py get-before-put "$ODAG")
status=$(echo "$out" | grep -oP 'STATUS=\K-?\d+')
case "$status" in 404|409) report PASS "GET before PUT returns 4xx" "status=$status" ;;
                  *)        report FAIL "GET before PUT returns 4xx" "status=$status" ;;
esac

echo
echo "== test 3: idempotent PUT with same SHA ==============================="
ODAG="tst-${ts}-t3"
out=$(run_py idempotent "$ODAG" 8192)
put1=$(echo "$out" | grep -oP 'PUT1=\K\d+')
put2=$(echo "$out" | grep -oP 'PUT2=\K\d+')
if [ "$put1" = "200" ] && [ "$put2" = "200" ]; then
    report PASS "Duplicate PUT with same SHA returns 200 idempotent"
else
    report FAIL "Duplicate PUT with same SHA returns 200 idempotent" "PUT1=$put1 PUT2=$put2"
fi
metrics=$(run_py metrics 2>&1 | tail -30)
idemp=$(echo "$metrics" | python3 -c "
import json, sys
raw = sys.stdin.read()
i = raw.find('{')
if i >= 0:
    try:
        d = json.loads(raw[i:])
        # data-agent exposes idempotent count under transfers.put_idempotent
        print(d.get('transfers', {}).get('put_idempotent', 0))
    except Exception:
        print(0)
else: print(0)" 2>/dev/null)
if [ -n "$idemp" ] && [ "$idemp" -gt 0 ]; then
    report PASS "metricPutIdempotent counter ticked" "idempotent=$idemp"
else
    report FAIL "metricPutIdempotent counter ticked" "got=$idemp"
fi

echo
echo "== test 4: PUT with different SHA → conflict =========================="
ODAG="tst-${ts}-t4"
out=$(run_py conflict "$ODAG")
put1=$(echo "$out" | grep -oP 'PUT1=\K\d+')
put2=$(echo "$out" | grep -oP 'PUT2=\K\d+')
if [ "$put1" = "200" ] && [ "$put2" = "409" ]; then
    report PASS "PUT with different SHA returns 409 conflict"
else
    report FAIL "PUT with different SHA returns 409 conflict" "PUT1=$put1 PUT2=$put2"
fi

echo
echo "== test 5: PUT with wrong SHA header → rejection ======================"
ODAG="tst-${ts}-t5"
out=$(run_py wrong-sha "$ODAG")
put_status=$(echo "$out" | grep -oP 'PUT=\K\d+')
get_status=$(echo "$out" | grep -oP 'GET=\K\d+')
if [ "$put_status" = "400" ]; then
    report PASS "PUT with wrong SHA header returns 400"
else
    report FAIL "PUT with wrong SHA header returns 400" "got=$put_status"
fi
case "$get_status" in 404|409) report PASS "After SHA mismatch, GET returns 4xx (no partial bytes)" "status=$get_status" ;;
                      *)       report FAIL "After SHA mismatch, GET returns 4xx (no partial bytes)" "status=$get_status" ;;
esac

echo
echo "== test 6: /ready/ PUT is rejected (install-driven only) =============="
ODAG="tst-${ts}-t6"
out=$(run_py ready-put-rejected "$ODAG")
status=$(echo "$out" | grep -oP 'STATUS=\K\d+')
if [ "$status" = "405" ]; then
    report PASS "PUT /ready/ rejected with 405"
else
    report FAIL "PUT /ready/ rejected with 405" "status=$status"
fi

echo
echo "== test 7: DELETE /ready/ clears marker ==============================="
ODAG="tst-${ts}-t7"
out=$(run_py ready-delete "$ODAG")
del_status=$(echo "$out" | grep -oP 'DELETE=\K\d+')
body=$(echo "$out" | grep -oP 'GET_BODY=\K.*')
if [ "$del_status" = "200" ] && [ "$body" = "false" ]; then
    report PASS "DELETE /ready/ + GET reports false"
else
    report FAIL "DELETE /ready/ + GET reports false" "DELETE=$del_status body=$body"
fi

echo
echo "== test 8: Path traversal cannot read host paths ======================"
# Go's net/http normalizes /../etc/x/output → /etc/x/output before the
# handler sees it, so the agent treats this as a normal (missing) lookup
# and returns 409 "data not ready" — *not* the contents of any host file.
# Any 4xx/5xx is acceptable; what we're checking is that no bytes from
# outside the data dir come back.
out=$(run_py path-traversal)
status=$(echo "$out" | grep -oP 'STATUS=\K-?\d+')
case "$status" in 4*|5*) report PASS "Path traversal cannot read host paths" "status=$status (Go path-normalize then 4xx/5xx — no leak)" ;;
                    *)   report FAIL "Path traversal cannot read host paths" "status=$status" ;;
esac

echo
echo "== test 9: Install stamps a data-availability record =================="
ODAG="tst-${ts}-t9"
out=$(run_py installed-record "$ODAG")
local_status=$(echo "$out" | grep -oP 'LOCAL_STATUS=\K\d+')
local_body=$(echo "$out" | grep -oP 'LOCAL_BODY=\K.*')
remote_body=$(echo "$out" | grep -oP 'REMOTE_BODY=\K.*')
missing_status=$(echo "$out" | grep -oP 'MISSING_STATUS=\K\d+')
if [ "$local_status" = "200" ] && echo "$local_body" | grep -q '"source":"local"' \
   && echo "$local_body" | grep -q '"bytes":4096'; then
    report PASS "Local install records source + byte count" "$local_body"
else
    report FAIL "Local install records source + byte count" "status=$local_status body=$local_body"
fi
if echo "$remote_body" | grep -q '"source":"remote"' && echo "$remote_body" | grep -q '"fromNode":"othernode"'; then
    report PASS "Remote install attributed to sending node"
else
    report FAIL "Remote install attributed to sending node" "body=$remote_body"
fi
if [ "$missing_status" = "404" ]; then
    report PASS "Install record absent before install (404)"
else
    report FAIL "Install record absent before install (404)" "got=$missing_status"
fi

echo
echo "== test 10: SDK timing reports round-trip ============================="
ODAG="tst-${ts}-t10"
out=$(run_py timings-roundtrip "$ODAG")
put_status=$(echo "$out" | grep -oP 'PUT_STATUS=\K\d+')
get_body=$(echo "$out" | grep -oP 'GET_BODY=\K.*')
badjson=$(echo "$out" | grep -oP 'BADJSON_STATUS=\K\d+')
missing=$(echo "$out" | grep -oP 'MISSING_STATUS=\K\d+')
if [ "$put_status" = "200" ] && echo "$get_body" | grep -q '"computeSeconds":8.5'; then
    report PASS "Timing report stored and returned verbatim"
else
    report FAIL "Timing report stored and returned verbatim" "put=$put_status body=$get_body"
fi
if [ "$badjson" = "400" ]; then report PASS "Non-JSON timing body rejected (400)"; else report FAIL "Non-JSON timing body rejected (400)" "got=$badjson"; fi
if [ "$missing" = "404" ]; then report PASS "Absent timing report returns 404"; else report FAIL "Absent timing report returns 404" "got=$missing"; fi

echo
echo "== cleanup ============================================================"
# Drop test outputs on the data-agent's data dir.
kubectl -n "$NS" exec "$DA_POD" -- sh -c "rm -rf /data/tst-${ts}-* 2>&1" 2>/dev/null
kubectl -n "$NS" delete pod "$HELPER" --wait=false >/dev/null 2>&1
echo

echo "======================================================================"
echo "SUMMARY: $PASS/$((PASS+FAIL)) pass"
echo "======================================================================"
exit $FAIL
