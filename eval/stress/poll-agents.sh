#!/usr/bin/env bash
#
# Block 6a — resource overhead poller.
#
# Snapshots /metrics + kubectl top pod for every data-agent in the
# wl-system namespace, every INTERVAL seconds, until SIGINT. Emits
# CSV rows to OUT for off-line analysis.
#
#   ./poll-agents.sh [OUT=overhead.csv] [INTERVAL=5]
#
# Columns:
#   ts_s, node, agent_pod, cpu_m, mem_mb,
#   bytes_in, bytes_out, put_total, put_ok, put_idempotent,
#   put_conflict, put_checksum_mismatch,
#   push_attempts, push_success, push_failed, push_inflight,
#   disk_bytes, run_count, goroutines, mem_alloc_b, mem_sys_b, uptime_s
#
# CPU is millicores from kubectl top; the rest comes from /metrics inside
# the agent pod. Each row is one (agent, interval) sample.
set -uo pipefail

OUT="${1:-overhead.csv}"
INTERVAL="${2:-5}"
NS=wl-system

cols="ts_s,node,agent_pod,cpu_m,mem_mb,bytes_in,bytes_out,put_total,put_ok,put_idempotent,put_conflict,put_checksum_mismatch,push_attempts,push_success,push_failed,push_inflight,disk_bytes,run_count,goroutines,mem_alloc_b,mem_sys_b,uptime_s"
[[ -f "$OUT" ]] || echo "$cols" > "$OUT"

t0=$(date +%s)

# Build pod-IP map once so we don't kubectl-list every interval.
echo "Discovering data-agents..."
PODS=()
NODES=()
IPS=()
while read -r line; do
    [[ -z "$line" ]] && continue
    read -r pod node ip <<< "$line"
    PODS+=("$pod"); NODES+=("$node"); IPS+=("$ip")
    echo "  $pod  $node  $ip"
done < <(kubectl -n "$NS" get pods -l app=data-agent -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.spec.nodeName}{" "}{.status.podIP}{"\n"}{end}')

if [ ${#PODS[@]} -eq 0 ]; then echo "no data-agents found"; exit 1; fi

# Cache kubectl-top output once per cycle to avoid N calls.
poll_once() {
    local ts=$(($(date +%s) - t0))
    local top
    top=$(kubectl -n "$NS" top pod -l app=data-agent --no-headers 2>/dev/null)

    local n=${#PODS[@]}
    for ((i=0; i<n; i++)); do
        local pod="${PODS[i]}"
        local node="${NODES[i]}"
        local ip="${IPS[i]}"

        # CPU + memory from kubectl top (millicores + Mi). Default 0 if missing.
        local cpu mem
        read -r cpu mem <<< "$(echo "$top" | awk -v p="$pod" '$1==p {print $2, $3}')"
        cpu="${cpu:-0m}"
        mem="${mem:-0Mi}"
        cpu_m=${cpu%m}
        mem_mb=${mem%Mi}

        # /metrics from the agent (over cluster network — already on the node)
        local m
        m=$(kubectl -n "$NS" exec "$pod" -- wget -qO- http://localhost:8082/metrics 2>/dev/null)
        [[ -z "$m" ]] && continue

        python3 - "$ts" "$node" "$pod" "$cpu_m" "$mem_mb" <<PY >> "$OUT"
import json, sys
ts, node, pod, cpu_m, mem_mb = sys.argv[1:]
m = json.loads("""$m""")
t   = m.get("transfers", {})
p   = m.get("push", {})
d   = m.get("disk", {})
mem = m.get("memory", {})
row = [ts, node, pod, cpu_m, mem_mb,
       t.get("bytes_in", 0), p.get("bytes_out", 0),
       t.get("put_total", 0), t.get("put_ok", 0),
       t.get("put_idempotent", 0), t.get("put_conflict", 0),
       t.get("put_checksum_mismatch", 0),
       p.get("attempts", 0), p.get("success", 0), p.get("failed", 0), p.get("inflight", 0),
       d.get("bytes_used", 0), d.get("run_count", 0),
       m.get("goroutines", 0), mem.get("alloc_bytes", 0), mem.get("sys_bytes", 0),
       m.get("uptime_seconds", 0)]
print(",".join(str(x) for x in row))
PY
    done
}

echo "Polling every ${INTERVAL}s → $OUT  (Ctrl-C to stop)"
trap 'echo; echo "Polling stopped"; exit 0' INT TERM
while true; do
    poll_once
    sleep "$INTERVAL"
done
