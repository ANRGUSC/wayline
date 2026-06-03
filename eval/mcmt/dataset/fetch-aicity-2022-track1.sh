#!/usr/bin/env bash
#
# Submit the AI City 2022 Track 1 fetch+slice Job on anrg-9 and stream its
# logs to stdout. Produces /var/lib/wl-workloads/aicity-source/cam-<i>/
# clip_<d>s.mp4 on anrg-9. Run stage-aicity-on-nodes.sh next to copy
# clips out to each sensor node's hostPath.
#
# Env knobs (defaults shown):
#   VEMCMT_AICITY_GDRIVE_ID=13wNJpS_Oaoe-7y5Dzexg_Ol7bKu1OWuC
#   VEMCMT_AICITY_SCENE=S04
#   VEMCMT_AICITY_CAMS="c016 c017 c018 c019"
#   VEMCMT_AICITY_NODE=anrg-9
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
TPL="$HERE/fetch-aicity-2022-track1.yml.tpl"

GDRIVE_ID="${VEMCMT_AICITY_GDRIVE_ID:-13wNJpS_Oaoe-7y5Dzexg_Ol7bKu1OWuC}"
SCENE="${VEMCMT_AICITY_SCENE:-S04}"
CAMS="${VEMCMT_AICITY_CAMS:-c016 c017 c018 c019}"
NODE="${VEMCMT_AICITY_NODE:-anrg-9}"
NAME="vemcmt-aicity-fetch"

kubectl delete job -n default "$NAME" --ignore-not-found --wait=true >/dev/null

sed -e "s|{{NAME}}|$NAME|g" \
    -e "s|{{NODE}}|$NODE|g" \
    -e "s|{{GDRIVE_ID}}|$GDRIVE_ID|g" \
    -e "s|{{SCENE}}|$SCENE|g" \
    -e "s|{{CAMS}}|$CAMS|g" \
    "$TPL" | kubectl apply -f -

echo "Job submitted: $NAME on $NODE"
echo "Streaming logs..."
# Wait for the pod to schedule, then follow logs.
for _ in $(seq 1 30); do
    POD=$(kubectl get pods -n default -l job-name="$NAME" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    [ -n "$POD" ] && break
    sleep 1
done
[ -n "$POD" ] || { echo "ERR: pod for $NAME never appeared"; exit 2; }

kubectl logs -n default "$POD" -f
