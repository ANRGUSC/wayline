#!/usr/bin/env bash
# E1c: Argo+MinIO referent on the FLAT network for the three synthetic
# DAGs (iobt, hetero, wpf). Complements the shaped referent (E1a): gives
# the protocol/topology decomposition measured directly on both fabrics.
#
# Procedure: actively flatten tc on every UP board (the teardown script
# only removes pods; host qdiscs persist), verify no htb anywhere, run
# the e1 Argo arms N times each, then RESTORE the shaped matrix and
# verify htb is back before exiting.
set -uo pipefail
cd "$(dirname "$0")"
ROOT=$(cd ../.. && pwd)
N=${N:-20}
RES=${RES:-$HOME/e1c-results}
E1=$ROOT/eval/synthetic-dags/e1
SCHED=$ROOT/eval/synthetic-dags/scheduler
NODES="anrg-1 anrg-3 anrg-4 anrg-5 anrg-6 anrg-7 anrg-8 anrg-9"
mkdir -p "$RES"

# tc_state <node> — full qdisc listing on the node (all interfaces).
tc_state() {
  kubectl run tc-check-$1 --rm -i --restart=Never --image=alpine -n wl-system \
    --overrides='{"spec":{"nodeName":"'"$1"'","hostNetwork":true,"containers":[{"name":"tc-check","image":"alpine","command":["sh","-c","apk add -q iproute2 >/dev/null && tc qdisc show"],"securityContext":{"privileged":true}}]}}' \
    2>/dev/null
}

echo "=== E1C flattening tc on all UP boards $(date +%F' '%T) ==="
kubectl delete pods -n wl-system -l app=tc-setup --ignore-not-found >/dev/null 2>&1
for node in $NODES; do
  kubectl run tc-flat-$node --restart=Never --image=alpine -n wl-system \
    --overrides='{"spec":{"nodeName":"'"$node"'","hostNetwork":true,"containers":[{"name":"tc-flat","image":"alpine","command":["sh","-c","apk add -q iproute2 >/dev/null; for i in /sys/class/net/*; do tc qdisc del dev ${i##*/} root 2>/dev/null; done; echo flattened"],"securityContext":{"privileged":true}}]}}' \
    >/dev/null 2>&1
done
for node in $NODES; do
  kubectl wait pod/tc-flat-$node -n wl-system \
    --for=jsonpath='{.status.phase}'=Succeeded --timeout=120s >/dev/null 2>&1 \
    && echo "  $node: $(kubectl logs tc-flat-$node -n wl-system 2>/dev/null)" \
    || echo "  $node FLATTEN POD DID NOT COMPLETE"
  kubectl delete pod tc-flat-$node -n wl-system --ignore-not-found >/dev/null 2>&1
done
for node in $NODES; do
  if tc_state "$node" | grep -q htb; then
    echo "ABORT: htb still present on $node after flatten."; exit 1
  fi
done
echo "flat network verified (no htb on any UP board)"

# The e1 runner appends to results/argo/<bm>/summary.csv; archive the
# shaped-era (E1a) results so flat rows land in a fresh file.
if [ -d "$E1/results/argo" ] && [ ! -d "$E1/results/argo-shaped-e1a" ]; then
  mv "$E1/results/argo" "$E1/results/argo-shaped-e1a"
fi

for bm in iobt hetero wpf; do
  echo "=== ARM e1c-argo-$bm start $(date +%F' '%T) ==="
  # The Argo workflow-controller has a history of wedging mid-campaign
  # (workflows stay phase=Running after their pods complete, filling
  # the namespaceParallelism slots and postponing everything after).
  # Restart it and purge all workflows before every arm.
  kubectl -n argo rollout restart deploy/workflow-controller >/dev/null
  kubectl -n argo rollout status deploy/workflow-controller --timeout=120s >/dev/null
  kubectl -n argo delete wf --all --wait=false >/dev/null 2>&1
  sleep 10
  "$E1/run.sh" argo "$bm" "$N" 2>&1 | tee "$RES/e1c-argo-$bm.log"
  # Post-arm sanity: how many of this arm's runs actually succeeded.
  ok=$(awk -F, '$3=="Succeeded"' "$E1/results/argo/$bm/summary.csv" 2>/dev/null | wc -l)
  echo "ARM e1c-argo-$bm succeeded runs: $ok/$N"
  cp "$E1/results/argo/$bm/summary.csv" "$RES/e1c-argo-$bm-summary.csv" 2>/dev/null
  echo "=== ARM e1c-argo-$bm done $(date +%F' '%T) ==="
done

echo "=== E1C restoring shaped matrix $(date +%F' '%T) ==="
"$SCHED/setup-tc-matrix.sh"
# The tc-setup pods pull alpine + apk add before applying; give each
# node up to 3 minutes rather than racing a fixed sleep.
for node in $NODES; do
  ok=""
  for _ in $(seq 1 18); do
    if tc_state "$node" | grep -q htb; then ok=1; break; fi
    sleep 10
  done
  if [ -z "$ok" ]; then
    echo "RESTORE FAILED: no htb on $node. Fix before running shaped campaigns."
    exit 1
  fi
done
echo "shaped matrix restored and verified on all UP boards"
echo "E1C CAMPAIGN DONE $(date +%F' '%T)"
