#!/usr/bin/env bash
#
# Render + submit one Job per sensor node that generates synthetic
# mcmt clips into /var/lib/wl-workloads/aicity/cam-<i>/ on
# that node. Replaces dataset/prepare-synthetic.sh + dataset/stage-on-nodes.sh
# for environments where the dev host doesn't have ffmpeg or ssh access
# to nodes.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
TPL="$HERE/synthetic-clips-job.yml.tpl"

# Same cam→node mapping as wayline/render.py.
declare -a CAMS=(cam-1 cam-2 cam-3 cam-4)
declare -a NODES=(anrg-1 anrg-3 anrg-4 anrg-5)

for i in "${!CAMS[@]}"; do
    cam="${CAMS[$i]}"
    node="${NODES[$i]}"
    name="vemcmt-synth-${cam}"
    # Re-runnable: delete any prior Job of the same name.
    kubectl delete job -n default "$name" --ignore-not-found --wait=false >/dev/null
    sed -e "s|{{NAME}}|$name|g" -e "s|{{NODE}}|$node|g" -e "s|{{CAMERA}}|$cam|g" \
        "$TPL" | kubectl apply -f - >/dev/null
    echo "submitted $name → $node"
done

echo
echo "waiting for all Jobs to complete..."
ALL_DONE=0
for _ in $(seq 1 60); do
    sleep 5
    pending=$(kubectl get jobs -n default -l '!app' -o name 2>/dev/null \
        | grep vemcmt-synth | while read -r j; do
            status=$(kubectl get "$j" -n default -o jsonpath='{.status.succeeded}' 2>/dev/null)
            [ -z "$status" ] || [ "$status" = "0" ] && echo "$j" || true
        done | wc -l)
    n=$(kubectl get jobs -n default 2>/dev/null | grep -c vemcmt-synth || true)
    echo "  $(date +%T) jobs=$n pending=$pending"
    if [ "$pending" = "0" ]; then ALL_DONE=1; break; fi
done

if [ "$ALL_DONE" != "1" ]; then
    echo "WARN: some Jobs did not finish in time. Check:"
    echo "  kubectl get jobs -n default | grep vemcmt-synth"
    exit 1
fi

echo
echo "=== logs from one Job (cam-1) ==="
kubectl logs -n default -l job-name=vemcmt-synth-cam-1 --tail=20 2>&1 | head -25

echo
echo "Done. Clips live on each sensor node at /var/lib/wl-workloads/aicity/<camera>/clip_<d>s.mp4"
