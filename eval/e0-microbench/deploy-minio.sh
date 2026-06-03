#!/usr/bin/env bash
#
# One-time MinIO setup for E0. Idempotent: re-running re-applies the
# manifest and re-creates the bench bucket if missing.
set -euo pipefail

E0_DIR="$(cd "$(dirname "$0")" && pwd)"
NS=e0-bench

kubectl apply -f "${E0_DIR}/minio/deployment.yml" >/dev/null

echo "[deploy-minio] waiting for MinIO to be ready..."
kubectl -n "$NS" wait --for=condition=Available deploy/minio --timeout=120s

# Create the bench bucket via a one-shot mc client pod.
echo "[deploy-minio] ensuring bucket e0-bench exists..."
kubectl -n "$NS" run mc-setup \
  --image=quay.io/minio/mc:latest \
  --restart=Never \
  --rm -i --tty=false \
  --quiet \
  --command -- sh -c '
    mc alias set local http://minio:9000 e0admin e0adminpw >/dev/null
    mc mb --ignore-existing local/e0-bench
    mc anonymous set none local/e0-bench >/dev/null
    echo "bucket ready"
  '

echo "[deploy-minio] done. Endpoint: http://minio.${NS}.svc.cluster.local:9000  bucket: e0-bench"
