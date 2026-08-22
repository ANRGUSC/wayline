#!/usr/bin/env bash
# Build + push all RAG-refresh images. Run ON anrg-2 from the repo root:
#   examples/rag-refresh-odag/build.sh
set -uo pipefail
R=${REGISTRY:-192.168.1.163:5000}
cd "$(dirname "$0")/../.."
for t in model ingest chunk embed index merge eval report; do
  echo "[$(date +%T)] building wl-rag-$t ..."
  if docker build -q -f examples/rag-refresh-odag/tasks/$t/Dockerfile \
       -t "$R/wl-rag-$t:latest" . >/tmp/wl-rag-$t.log 2>&1; then
    docker push -q "$R/wl-rag-$t:latest" >/dev/null && echo "[$(date +%T)] wl-rag-$t OK"
  else echo "[$(date +%T)] wl-rag-$t FAILED"; tail -5 /tmp/wl-rag-$t.log; fi
done
echo "ALL RAG BUILDS DONE"
