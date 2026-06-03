#!/usr/bin/env bash
set -uo pipefail
H=/home/anrg/wayline/eval/mcmt/scripts
for spec in "120 png" "30 jpg"; do read d fmt <<<"$spec"
  echo "######## DIST-MINIO cell d${d}-${fmt} ########"
  "$H/argo-distributed-minio.sh" wl-vemcmt-n4-d${d}-${fmt}-argo-dist \
    /home/anrg/wayline/eval/mcmt/results/distributed-minio-fair-d${d}-${fmt} 20 e0-bench-dist 2>&1 \
    | grep -E 'rep|Succeeded|Failed|makespan|mean|SUMMARY'
done
echo "DIST-MINIO DONE"
