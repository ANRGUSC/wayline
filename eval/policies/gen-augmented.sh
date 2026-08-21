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

# E3: checkpoint sweep on hetero-compute (k = 1..N producers stored).
$PY -m wl.augment "$SCHED/hetero-compute/template-heft.yml" \
    --store-node $STORE --edges "train" --suffix=-ckpt1 > hetero-compute-ckpt1.yml
$PY -m wl.augment "$SCHED/hetero-compute/template-heft.yml" \
    --store-node $STORE --edges "train,preprocess" --suffix=-ckpt2 > hetero-compute-ckpt2.yml

echo "rendered: $(ls *.yml | tr '\n' ' ')"
