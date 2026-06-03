#!/usr/bin/env bash
#
# End-to-end scheduler sweep for one ODAG.
#
# For each config in CONFIGS (see below), the driver:
#   1. applies the template,
#   2. wipes the profiler DB,
#   3. restarts the odag-controller,
#   4. kicks off N runs sequentially via repeat-template.sh,
#   5. dumps per-run JSON (makespan + placement + predictions) and a
#      summary CSV under results/<sweep-name>/<config>/.
#
# Usage:
#   ./sweep-scheduler.sh <odag-dir> [N]
#
#   <odag-dir>   e.g. iobt, hetero-compute, wide-pipeline-flex
#   N            number of runs per config (default: 20)
#
# Env knobs:
#   CONFIGS     space-separated list of config suffixes (default:
#               "random heft heft-eps"). For the iobt epsilon sweep,
#               set CONFIGS="random heft heft-eps05 heft-eps heft-eps20".
#   NS          namespace (default wl-system)
#   TIMEOUT     per-run timeout in seconds (default 300)
#
set -euo pipefail

EVAL_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$EVAL_DIR/../.." && pwd)"
Wayline="${REPO_ROOT}/bin/wayline"
REPEAT="${REPO_ROOT}/scripts/benchmarks/repeat-template.sh"

ODAG_DIR_ARG="${1:?usage: $0 <odag-dir> [N]}"
N="${2:-20}"
NS="${NS:-wl-system}"
TIMEOUT="${TIMEOUT:-300}"
CONFIGS="${CONFIGS:-random heft heft-eps}"

ODAG_DIR="${EVAL_DIR}/${ODAG_DIR_ARG}"
[[ -d "$ODAG_DIR" ]] || { echo "ERROR: $ODAG_DIR not found"; exit 1; }

SWEEP_NAME="$(basename "$ODAG_DIR")"
RESULTS_DIR="${EVAL_DIR}/results/${SWEEP_NAME}"
mkdir -p "$RESULTS_DIR"

sweep_start=$(date +%s)
echo "================================================================"
echo " Sweep:     $SWEEP_NAME"
echo " Configs:   $CONFIGS"
echo " Runs each: $N"
echo " Timeout:   ${TIMEOUT}s per run"
echo " Results:   $RESULTS_DIR"
echo "================================================================"

for cfg in $CONFIGS; do
  tpl="${ODAG_DIR}/template-${cfg}.yml"
  [[ -f "$tpl" ]] || { echo "SKIP: no template-${cfg}.yml in ${ODAG_DIR}"; continue; }

  cfg_dir="${RESULTS_DIR}/${cfg}"
  mkdir -p "$cfg_dir"

  echo ""
  echo "################################################################"
  echo "# [$SWEEP_NAME / $cfg]  applying template + resetting profiler"
  echo "################################################################"

  # Apply the template (pick up any edits; idempotent).
  kubectl apply -f "$tpl" >/dev/null

  # Parse template name (metadata.name) so we can wayline run it.
  tpl_name="$(awk '/^metadata:/{in_meta=1;next} in_meta && /^ *name:/{print $2; exit}' "$tpl")"
  [[ -n "$tpl_name" ]] || { echo "ERROR: could not parse template name from $tpl"; exit 1; }

  # Fresh profiler for this config.
  "$EVAL_DIR/reset-profiler.sh"

  # Brief settle after controller restart.
  sleep 3

  run_log="${cfg_dir}/repeat-template.log"
  summary_csv="${cfg_dir}/summary.csv"
  echo "iteration,run_name,phase,makespan,wall_s" > "$summary_csv"

  echo ""
  echo "# Kicking off $N runs of $tpl_name"
  "$REPEAT" "$tpl_name" "$N" "$NS" "$TIMEOUT" 2>&1 | tee "$run_log"

  # Extract per-run records into summary CSV and dump full ODAG JSON.
  # The log lines look like:
  #   -> <run-name>  phase=<p>  makespan=<m>s  wall=<w>s
  awk -F'[ =s]+' '
    /^-> /{
      gsub(/^-> /,"",$0);
      split($0,parts,"  ");
      run=parts[1];
      for (i=2; i<=length(parts); i++) {
        split(parts[i], kv, "=");
        kvs[kv[1]] = kv[2];
      }
      print run","kvs["phase"]","kvs["makespan"]","kvs["wall"];
      delete kvs;
    }' "$run_log" > "${cfg_dir}/raw-rows.tmp" || true

  i=1
  while IFS=, read -r name phase ms wall; do
    # Strip trailing "s" from makespan/wall if present
    ms="${ms%s}"; wall="${wall%s}"
    printf '%d,%s,%s,%s,%s\n' "$i" "$name" "$phase" "$ms" "$wall" >> "$summary_csv"
    # Dump full ODAG status for downstream analysis (placement, predictions).
    kubectl get odags.wl.io "$name" -n "$NS" -o json > "${cfg_dir}/${name}.json" 2>/dev/null || true
    i=$((i+1))
  done < "${cfg_dir}/raw-rows.tmp"
  rm -f "${cfg_dir}/raw-rows.tmp"

  # Snapshot profiler DB at end of this config.
  kubectl exec -n "$NS" deploy/odag-controller -- cat /data/wl-profiler.db > "${cfg_dir}/profiler-final.db" 2>/dev/null || true

  echo ""
  echo "# [$cfg] done — $(wc -l < "$summary_csv" | awk '{print $1-1}') rows in summary.csv"
done

sweep_wall=$(( $(date +%s) - sweep_start ))
echo ""
echo "================================================================"
echo " Sweep $SWEEP_NAME complete in ${sweep_wall}s."
echo "================================================================"
