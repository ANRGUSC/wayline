#!/usr/bin/env bash
#
# Wipe the profiler SQLite DB inside the odag-controller pod and
# restart it, giving the next config a clean EMA state. Needed
# between scheduler-config comparisons so warm/cold boundaries are
# reproducible.
set -euo pipefail

NS=${NS:-wl-system}
DEPLOY=odag-controller

echo "[profiler] wiping /data/wl-profiler.db{,-shm,-wal}..."
# MUST remove the -shm and -wal sidecar files too. If only the main .db
# is deleted, SQLite on the next start opens a fresh DB but the stale
# WAL/shared-memory files cause `disk I/O error (522)` during CREATE
# TABLE → profiling silently disables itself.
kubectl exec -n "$NS" deploy/$DEPLOY -- sh -c '
  rm -f /data/wl-profiler.db /data/wl-profiler.db-shm /data/wl-profiler.db-wal
' >/dev/null 2>&1 || true

echo "[profiler] restarting $DEPLOY..."
kubectl rollout restart -n "$NS" deploy/$DEPLOY >/dev/null
kubectl rollout status  -n "$NS" deploy/$DEPLOY --timeout=120s >/dev/null

# Verify profiler initialized cleanly — if not, something else is wrong
# and we want to fail loud rather than continue with profiling silently off.
# Target the NEWEST pod explicitly: right after a rollout the deployment
# label selector still matches terminating old replicas, and
# `kubectl logs deploy/` may pick one of those.
sleep 3
pod=$(kubectl get pods -n "$NS" -l app=$DEPLOY \
  --sort-by=.metadata.creationTimestamp -o name | tail -1)
if kubectl logs -n "$NS" "$pod" -c odag-controller 2>/dev/null | grep -q "profiler DB init failed"; then
  echo "[profiler] ERROR: DB init failed after restart. Aborting." >&2
  kubectl logs -n "$NS" "$pod" -c odag-controller 2>/dev/null | grep -i profiler >&2
  exit 1
fi
if ! kubectl logs -n "$NS" "$pod" -c odag-controller 2>/dev/null | grep -q "database ready"; then
  echo "[profiler] ERROR: no 'database ready' message seen in $pod. Aborting." >&2
  exit 1
fi
echo "[profiler] ready (verified)."
