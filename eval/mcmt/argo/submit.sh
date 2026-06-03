#!/usr/bin/env bash
#
# Render, apply, and submit one Argo Workflow from the mcmt template.
#
#   ./submit.sh <cameras> <duration_s>
#
# Examples:
#   ./submit.sh 4 60
#
set -euo pipefail

CAM="${1:-4}"
DUR="${2:-60}"

HERE="$(cd "$(dirname "$0")" && pwd)"
NAME="vemcmt-n${CAM}-d${DUR}-argo"

python3 "$HERE/render.py" --cameras "$CAM" --duration "$DUR" \
    --name "$NAME" -o "/tmp/${NAME}.yml"
kubectl apply -f "/tmp/${NAME}.yml"

# Submit a one-shot Workflow from the template.
kubectl -n argo create -f <(cat <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: ${NAME}-
  namespace: argo
spec:
  workflowTemplateRef:
    name: ${NAME}
EOF
)
