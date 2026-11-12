#!/usr/bin/env bash
# Argo Workflows v3.5.5 (E16's version) for the E18 Argo arms, on eclipse.
#
#   argo/install.sh          install, or bring an install up to date (idempotent)
#   argo/install.sh down     remove it: namespace argo (workflows, the S3 repository),
#                            then the CRDs, cluster roles, bindings and priority class
#
# What it does:
#   1. mirrors the controller, executor and CLI images into 10.10.0.1:5000
#      (crane copies every platform, so argoexec runs on the Pis too) and the
#      S3 repository's rclone (1.75.1, every platform) as rclone:e16 if missing;
#   2. applies the release's install.yaml (cached in ~/testbed/argo) with its
#      images pointing at the mirror;
#   3. points the executor at the mirrored argoexec (the controller's
#      built-in default is quay.io);
#   4. grants the workflow pods' service account "argo" what the v3.5
#      executor needs (create/patch workflowtaskresults; install.yaml gives
#      that account only list/watch, and the wait container then fails to
#      report outputs);
#   5. keeps the workflow controller and argo-server on eclipse (tainted
#      control-plane), off the lanes' workers, from the first apply.
# The S3 repository itself (rclone serve s3 on eclipse's data address) is
# created per suite by argorun.repo_up() and removed by repo_down().
set -euo pipefail
VER=${ARGO_VERSION:-v3.5.5}
REG=${E18_REGISTRY:-10.10.0.1:5000}
CRANE=${CRANE:-$HOME/testbed/bin/crane}
RCLONE=${RCLONE_IMAGE:-docker.io/rclone/rclone:1.75.1}
K="k3s kubectl"
YAML=${ARGO_YAML:-$HOME/testbed/argo/install-$VER.yaml}

fetch_yaml() {
  if [ ! -s "$YAML" ]; then
    mkdir -p "$(dirname "$YAML")"
    curl -sfL -o "$YAML.tmp" "https://github.com/argoproj/argo-workflows/releases/download/$VER/install.yaml"
    mv "$YAML.tmp" "$YAML"
  fi
}

# install.yaml with the mirror's images, and the controller and server
# placed on eclipse from the first apply (patching afterwards lets the
# scheduler put the first replicas on any worker, the demo's included).
render() {
  sed "s#quay.io/argoproj/#$REG/argoproj/#g" "$YAML" | python3 -c '
import sys
s = sys.stdin.read()
old = "      nodeSelector:\n        kubernetes.io/os: linux\n"
new = old + ("        kubernetes.io/hostname: eclipse\n      tolerations:\n"
             "      - key: node-role.kubernetes.io/control-plane\n        operator: Exists\n        effect: NoSchedule\n")
assert s.count(old) == 2, "install.yaml layout changed: expected two Deployments with nodeSelector os=linux"
sys.stdout.write(s.replace(old, new))'
}
if [ "${1:-}" = down ]; then
  $K delete namespace argo --ignore-not-found --wait=true --timeout=180s
  fetch_yaml
  render | $K delete --ignore-not-found -f - || true
  echo "Argo removed"
  exit 0
fi

mirror() {   # src dst: copy unless the destination tag exists
  if ! "$CRANE" digest --insecure "$2" >/dev/null 2>&1; then
    "$CRANE" copy --insecure "$1" "$2"
  fi
}
for img in workflow-controller argoexec argocli; do
  mirror "quay.io/argoproj/$img:$VER" "$REG/argoproj/$img:$VER"
done
mirror "$RCLONE" "$REG/rclone:e16"

fetch_yaml
$K create namespace argo --dry-run=client -o yaml | $K apply -f -
render | $K -n argo apply -f -

# Executor image from the mirror.
$K -n argo patch configmap workflow-controller-configmap --type merge \
  -p "{\"data\":{\"executor\":\"image: $REG/argoproj/argoexec:$VER\\n\"}}"

# The executor's permissions (Argo docs, workflow-rbac): the wait container
# creates and patches a WorkflowTaskResult per pod.
$K apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: argo-executor, namespace: argo}
rules:
- apiGroups: [argoproj.io]
  resources: [workflowtaskresults]
  verbs: [create, patch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: argo-executor, namespace: argo}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: argo-executor}
subjects:
- {kind: ServiceAccount, name: argo, namespace: argo}
EOF

$K -n argo rollout status deploy/workflow-controller --timeout=180s
$K -n argo rollout status deploy/argo-server --timeout=180s
echo "Argo $VER installed (images from $REG)"
