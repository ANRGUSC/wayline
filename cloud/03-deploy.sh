#!/usr/bin/env bash
# Run on the server node from the wayline repo root: cloud/03-deploy.sh
# Renders every manifest with the cloud registry address, deploys the
# runtime, and applies the E9 templates (direct + store x 3 DAGs).
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
SERVER_IP=${SERVER_IP:-$(hostname -I | awk '{print $1}')}
. cloud/env.sh
NS=wl-system
OUT=cloud/rendered
mkdir -p "$OUT"

render(){ sed "s|192\.168\.1\.163:5000|$REG|g" "$1" > "$OUT/$(basename "$1")"; }

# Runtime.
kubectl apply -f deployments/namespace.yml
kubectl apply -f api/v1/odag-crd.yml -f api/v1/odagtemplate-crd.yml
kubectl apply -f deployments/odag-controller/rbac.yml
for f in deployments/data-agent/daemonset.yml deployments/odag-controller/deployment.yml; do
  render "$f"; kubectl apply -f "$OUT/$(basename "$f")"
done

# Flat 10G fabric profile: 1.25 GB/s default bandwidth. Replace with the
# iperf3-measured value after 04-campaign.sh prints it, then re-apply.
BW=${BW:-1250000000}
cat << YAML | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata: { name: wl-network-profile, namespace: $NS }
data:
  defaultBandwidth: "$BW"
YAML

# E9 templates: the three logical DAGs (direct) and their all-store
# augmentations, registry address swapped. Direct templates share the
# basename template-heft.yml, so render them under unique names.
for d in iot hetero-compute wide-pipeline-flex; do
  sed "s|192\.168\.1\.163:5000|$REG|g" \
    "eval/synthetic-dags/scheduler/$d/template-heft.yml" > "$OUT/$d-heft.yml"
done
for f in eval/policies/iot-store.yml eval/policies/hetero-compute-store.yml \
         eval/policies/wide-pipeline-flex-store.yml; do
  render "$f"
done
for f in "$OUT"/*.yml; do kubectl apply -f "$f"; done
kubectl -n $NS rollout status ds/wl-data-agent --timeout=300s
kubectl -n $NS rollout status deploy/odag-controller --timeout=300s
echo "deployed. nodes:"; kubectl get nodes -o wide
