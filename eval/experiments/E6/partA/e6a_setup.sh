#!/usr/bin/env bash
# E6 Part A setup: image, templates, and one frozen HEFT schedule per
# workflow. Idempotent; run on anrg-2.
#
#   1. build + push the generic executor image (wl-e6a)
#   2. bandwidth ConfigMap (tiered matrix the scheduler reads)
#   3. render + apply the seven sched templates
#   4. shaping ON, PYTHONHASHSEED pinned, freeze each workflow from the
#      controller's own scheduling call (E5's freeze script: it STOPs if
#      any application task lands on the gateway, which is Part A's rule)
#   5. re-render frozen + store templates from the freezes, apply all 14
#   6. clear shaping (the harness applies it per run)
set -uo pipefail
ROOT=/home/anrg/wayline-build-vertex
A=$ROOT/eval/experiments/E6/partA
E6=$ROOT/eval/experiments/E6
FROZEN=${FROZEN:-/home/anrg/E6A-frozen}
TPL=${TPL:-/home/anrg/E6A-templates}
NS=wl-system
WFS="blast bwa cycles 1000genome montage seismology soykb"

step() { echo; echo "=== $* ==="; }

step "1. build + push wl-e6a"
cd "$ROOT"
docker build -q -f eval/experiments/E6/partA/tasks/Dockerfile \
  -t 192.168.1.163:5000/wl-e6a:latest . || { echo "STOP: build"; exit 1; }
docker push -q 192.168.1.163:5000/wl-e6a:latest >/dev/null \
  || { echo "STOP: push"; exit 1; }
echo "image pushed"

step "2. bandwidth ConfigMap"
python3 "$ROOT/eval/experiments/E5/gen_e5.py" bwconfig | kubectl apply -f - \
  || { echo "STOP: bwconfig"; exit 1; }

step "3. render + apply sched templates"
mkdir -p "$TPL" "$FROZEN"
python3 "$A/gen_parta.py" "$A/instances" "$TPL" || { echo "STOP: gen"; exit 1; }
for wf in $WFS; do kubectl apply -f "$TPL/e6a-$wf-sched.yml" >/dev/null; done
echo "7 sched templates applied"

step "4. freeze one HEFT schedule per workflow (shaping live, seed pinned)"
"$E6/e6_net.sh" apply | tail -1
kubectl -n $NS set env deploy/odag-controller -c saga-sidecar PYTHONHASHSEED=0
kubectl -n $NS rollout status deploy/odag-controller --timeout=300s
for wf in $WFS; do
  echo "--- freezing $wf"
  python3 "$ROOT/eval/experiments/E5/freeze_from_live.py" \
    "e6a-$wf-sched" "$wf" "$FROZEN" || { echo "STOP: freeze $wf"; exit 1; }
done

step "5. render frozen + store templates, apply all 14"
python3 "$A/gen_parta.py" "$A/instances" "$TPL" --frozen "$FROZEN" \
  || { echo "STOP: gen frozen"; exit 1; }
for wf in $WFS; do
  kubectl apply -f "$TPL/e6a-$wf-frozen.yml" >/dev/null || { echo "STOP: apply $wf"; exit 1; }
  kubectl apply -f "$TPL/e6a-$wf-store.yml" >/dev/null || { echo "STOP: apply $wf store"; exit 1; }
done
echo "14 arm templates applied"

step "6. clear shaping"
"$E6/e6_net.sh" clear | tail -1

echo; echo "SETUP DONE. Frozen schedules:"
python3 - <<PY
import json, glob
for f in sorted(glob.glob("$FROZEN/frozen-*.json")):
    d = json.load(open(f))
    if not d.get("placement"): continue
    nodes = sorted({n for n in d["placement"].values()})
    print(f"  {f.split('frozen-')[-1][:-5]:12} tasks={len(d['placement']):3} "
          f"nodes={len(nodes)} hash={d['schedule_hash'][:12]}")
PY
