#!/usr/bin/env bash
# Images for the E16 baselines, built on anrg-2: the Argo task image, and
# rclone (its `serve s3` is the Argo arm's S3 repository; MinIO's images are
# no longer published) mirrored into the local registry.
# Local copies are removed after pushing (anrg-2's disk is tight).
set -euo pipefail
REG=192.168.1.163:5000
cd "$(dirname "$0")/argo"
docker build -q -t $REG/wl-e16-task:latest .
docker push -q $REG/wl-e16-task:latest
docker pull -q --platform linux/amd64 docker.io/rclone/rclone:latest
docker tag docker.io/rclone/rclone:latest $REG/rclone:e16
docker push -q $REG/rclone:e16
docker rmi $REG/wl-e16-task:latest $REG/rclone:e16 docker.io/rclone/rclone:latest >/dev/null
docker builder prune -af >/dev/null
df -h / | tail -1
