#!/usr/bin/env bash
set -euo pipefail

E0_DIR="$(cd "$(dirname "$0")" && pwd)"
kubectl delete -f "${E0_DIR}/minio/deployment.yml" --ignore-not-found
echo "[teardown-minio] done"
