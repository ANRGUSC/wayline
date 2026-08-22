#!/usr/bin/env bash
# Render the physical-DAG variants used by the data-plane experiments
# (E1 generality tax, E3 coexistence, E6 vertex-overhead) from the logical
# templates. Idempotent; run from this directory.
#
#   ./gen-augmented.sh            # writes *.yml next to this script
set -euo pipefail
cd "$(dirname "$0")"
SCHED=../synthetic-dags/scheduler
PY="${PYTHON:-python3}"
export PYTHONPATH="../../sdk/python:${PYTHONPATH:-}"

STORE=anrg-9

# E1: all-store (Argo-mode) variants of the three benchmarks.
for d in iot hetero-compute wide-pipeline-flex; do
  $PY -m wl.augment "$SCHED/$d/template-heft.yml"  --store-node $STORE \
      > "$d-store.yml"
done

# E6: pod-realized arm (same physical DAG, vertices as containers).
$PY -m wl.augment "$SCHED/hetero-compute/template-heft.yml" \
    --store-node $STORE --pod-realized > hetero-compute-store-pod.yml

# E3: checkpoint-count sweep, largest payloads first, so the curve reads
# "cost tracks the bytes you route through the store". Endpoints k=0 (the
# direct arm) and k=all (the store arm) come from E1.
# hetero producers by size: train 100MB, preprocess 20MB, export 50MB*,
# evaluate 5MB (*export/evaluate feed report).
$PY -m wl.augment "$SCHED/hetero-compute/template-heft.yml" \
    --store-node $STORE --edges "train" --suffix=-ckpt1 > hetero-compute-ckpt1.yml
$PY -m wl.augment "$SCHED/hetero-compute/template-heft.yml" \
    --store-node $STORE --edges "train,export" --suffix=-ckpt2 > hetero-compute-ckpt2.yml
# wpf producers by size: source 100, merge 100, extract/scan/index 50 each,
# filter-a/b/c 30 each, aggregate 10.
$PY -m wl.augment "$SCHED/wide-pipeline-flex/template-heft.yml" \
    --store-node $STORE --edges "source" --suffix=-ckpt1 > wpf-ckpt1.yml
$PY -m wl.augment "$SCHED/wide-pipeline-flex/template-heft.yml" \
    --store-node $STORE --edges "source,merge,extract" --suffix=-ckpt3 > wpf-ckpt3.yml
$PY -m wl.augment "$SCHED/wide-pipeline-flex/template-heft.yml" \
    --store-node $STORE --edges "source,merge,extract,scan,index" --suffix=-ckpt5 > wpf-ckpt5.yml

echo "rendered: $(ls *.yml | tr '\n' ' ')"
