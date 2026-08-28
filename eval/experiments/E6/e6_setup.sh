#!/usr/bin/env bash
# E6 Part B setup: everything between "dataset fetched" and "pilot runnable".
#
#   1. stage clips from anrg-9's aicity-source onto the sensor nodes
#   2. rebuild the six MCMT images (wrappers now emit NAMED objects)
#   3. apply the bandwidth ConfigMap the scheduler reads
#   4. render + apply the sched template, freeze one HEFT schedule
#   5. render + apply the frozen and store templates from that freeze
#   6. render + apply the Argo WorkflowTemplate and its per-run stub
#
# Idempotent; safe to re-run. Run on anrg-2.
#   ./e6_setup.sh [n_cameras] [clip_s] [fmt]
set -uo pipefail
N=${1:-4}; D=${2:-120}; FMT=${3:-png}
ROOT=/home/anrg/wayline-build-vertex
MCMT=$ROOT/eval/mcmt
E6=$ROOT/eval/experiments/E6
FROZEN=${FROZEN:-/home/anrg/E6-frozen}
NS=wl-system

step() { echo; echo "=== $* ==="; }

step "1. stage clips onto sensor nodes"
bash "$MCMT/dataset/stage-aicity-on-nodes.sh" || {
  echo "STOP: staging failed"; exit 1; }

step "2. rebuild MCMT images (named-object wrappers)"
bash "$MCMT/build-wl-images.sh" || { echo "STOP: image build failed"; exit 1; }

step "3. bandwidth ConfigMap (same tiered matrix as the tc treatment)"
python3 "$ROOT/eval/experiments/E5/gen_e5.py" bwconfig \
  | kubectl apply -f - || { echo "STOP: bwconfig failed"; exit 1; }

step "4. freeze one HEFT schedule under the tiered matrix"
# The tc treatment must be live while freezing: HEFT's placement depends
# on the link rates, so a schedule frozen on an unshaped cluster would
# not be the one the arms reproduce.
"$E6/e6_net.sh" apply || { echo "STOP: could not apply shaping"; exit 1; }
"$E6/e6_net.sh" verify | tail -2
# Pin tie-breaking: HEFT moves under 9 of 10 hash seeds.
kubectl -n $NS set env deploy/odag-controller -c saga-sidecar PYTHONHASHSEED=0
kubectl -n $NS rollout status deploy/odag-controller --timeout=300s
python3 "$MCMT/wayline/render_e6.py" sched "$N" "$D" "$FMT" \
  --name e6-mcmt-sched -o /tmp/e6-mcmt-sched.yml
kubectl apply -f /tmp/e6-mcmt-sched.yml
mkdir -p "$FROZEN"
python3 "$E6/freeze_e6.py" e6-mcmt-sched heft "$FROZEN" || {
  echo "STOP: freeze failed"; exit 1; }

step "5. render frozen + store templates from that freeze"
python3 "$MCMT/wayline/render_e6.py" frozen "$N" "$D" "$FMT" \
  "$FROZEN/frozen-heft.json" --name e6-mcmt-frozen -o /tmp/e6-mcmt-frozen.yml
python3 "$MCMT/wayline/render_e6.py" store "$N" "$D" "$FMT" \
  "$FROZEN/frozen-heft.json" --name e6-mcmt-store -o /tmp/e6-mcmt-store.yml
kubectl apply -f /tmp/e6-mcmt-frozen.yml
kubectl apply -f /tmp/e6-mcmt-store.yml

step "6. Argo referent"
python3 "$MCMT/argo/render.py" --cameras "$N" --duration "$D" \
  --name e6-mcmt-argo -o /tmp/e6-mcmt-argo.yml
kubectl apply -f /tmp/e6-mcmt-argo.yml
cat > /tmp/e6-mcmt-argo-wf.yml <<EOF
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: e6-mcmt-argo-
  namespace: argo
spec:
  workflowTemplateRef:
    name: e6-mcmt-argo
EOF

step "7. clear shaping (the pilot applies it per run)"
"$E6/e6_net.sh" clear | tail -1

echo
echo "SETUP DONE. Frozen schedule:"
python3 - <<PY
import json
d = json.load(open("$FROZEN/frozen-heft.json"))
print("  hash", d["schedule_hash"][:16])
for n, ts in sorted(d["order"].items()):
    print(f"    {n:8} {' -> '.join(ts)}")
PY
