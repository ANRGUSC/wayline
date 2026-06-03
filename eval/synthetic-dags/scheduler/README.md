# Network-Aware Scheduling Evaluation

End-to-end evaluation of Wayline's network-aware HEFT scheduler against a
random baseline, plus the ε-tolerant tie-breaking extension
(`spreadEpsilon`). Self-contained: this folder has every template,
task source, Dockerfile, and orchestration script needed to reproduce
the experiment on a fresh cluster.

## Benchmarks

| ODAG | Role | Tasks | Data profile | Where |
|---|---|---|---|---|
| `iobt` | Realistic IoBT ISR snapshot | 14 | 80–150 MB sensor bursts → ~1 MB detections → fused report | `iobt/` |
| `hetero-compute` | Minimal controlled microbenchmark | 5 | 5–100 MB mixed; explicit per-node `runtimeProfile` hints | `hetero-compute/` |
| `wide-pipeline-flex` | Structural stress test (fan-out × 2, fan-in × 2) | 10 | 10–100 MB per edge, two ingress contention points | `wide-pipeline-flex/` |

Each ODAG has three scheduler-config variants:

| Variant | Scheduler | `spreadEpsilon` | What it tests |
|---|---|---|---|
| `template-random.yml` | random | — | No placement intelligence — baseline |
| `template-heft.yml` | heft | 0 | Network-aware HEFT with strict EFT selection |
| `template-heft-eps.yml` | heft | 1.0 | HEFT + ε-tolerant tie-breaking (Contribution 5) |

For the iobt ε-sweep ablation figure, `iobt/` additionally ships
`template-heft-eps05.yml` (ε=0.5) and `template-heft-eps20.yml` (ε=2.0).

All variants share identical profiling settings so EMA behavior is
comparable:

```yaml
profiling:
  enabled: true
  warmupRuns: 0
  minSamples: 2
  emaAlpha: 0.7
  maxSamples: 50
  runtimeSource: profiler
  bandwidthSource: external
retention:
  maxRuns: 20
  data:
    policy: immediate         # delete run data as soon as the ODAG completes
```

`policy: immediate` keeps disks clean during long sweeps.

## Network matrix

All three benchmarks run under the same `tc`-shaped 8-node matrix:

```
         a-1   a-3   a-4   a-5   a-6   a-7   a-8   a-9
a-1       —    F     F     F     M     M     M     M
a-3       F    —     F     F     S     M     M     M
a-4       F    F     —     F     M     S     M     M
a-5       F    F     F     —     M     M     S     M
a-6       M    S     M     M     —     F     F     M
a-7       M    M     S     M     F     —     F     M
a-8       M    M     M     S     F     F     —     M
a-9       M    M     M     M     M     M     M     —
```

- **F** = 1 Gbps (same-tier; edge↔edge, compute↔compute)
- **M** = 100 Mbps (cross-tier generic)
- **S** = 50 Mbps (engineered bottlenecks: anrg-3↔6, anrg-4↔7, anrg-5↔8)

20× asymmetry between fastest and slowest pair; same-tier links are
10× faster than any cross-tier link. This matches the spirit of the
original network-aware eval: random placement is systematically slow
cross-tier, while HEFT hunts for same-tier shortcuts or the least-slow
cross-tier pair.

Results from an earlier matrix (F=1G/M=300M/S=100M) are preserved under
`results/archive-matrix-v1/` with a MATRIX.md explaining the configuration.

Apply with `./setup-tc-matrix.sh`, remove with `./teardown-tc-matrix.sh`.

## Running the sweep

### Pre-flight
```bash
# Clean cluster.
./cleanup-cluster.sh

# Apply tc shaping.
./setup-tc-matrix.sh

# Smoke-test each ODAG with HEFT once (stops on first failure).
for d in iobt hetero-compute wide-pipeline-flex; do
  kubectl apply -f $d/template-heft.yml
  ../../bin/wayline odag run "$(awk '/^metadata:/{in_meta=1;next} in_meta && /^ *name:/{print $2; exit}' $d/template-heft.yml)" -n wl-system
done
# Watch in UI or:
# kubectl get odag -n wl-system -w
```

### Full sweep (all three ODAGs, three configs each, 20 runs per config)

In a tmux session (so the SSH connection dying doesn't kill the sweep):

```bash
tmux new -s wl-eval
./cleanup-cluster.sh
./sweep-scheduler.sh iobt 20
./cleanup-cluster.sh
./sweep-scheduler.sh hetero-compute 20
./cleanup-cluster.sh
./sweep-scheduler.sh wide-pipeline-flex 20
# Detach: Ctrl-b d; re-attach: tmux attach -t wl-eval
```

Total wall-clock: ~2–3 hours (180 runs plus cleanup/reset overhead).

### ε-sweep ablation (iobt only)

```bash
./cleanup-cluster.sh
CONFIGS="random heft heft-eps05 heft-eps heft-eps20" ./sweep-scheduler.sh iobt 20
```

Adds 2 more configs × 20 runs on iobt (~40 min extra).

## What each script does

| Script | Role |
|---|---|
| `setup-tc-matrix.sh` | Apply 8-node tc bandwidth matrix (idempotent) |
| `teardown-tc-matrix.sh` | Remove tc-setup pods |
| `cleanup-cluster.sh` | Delete all ODAG/CDAG runs, task pods, per-run data on disk; leaves controllers / data-agents / mqtt-broker / nfs-server intact |
| `reset-profiler.sh` | Wipe profiler DB, restart odag-controller |
| `sweep-scheduler.sh <odag-dir> [N]` | End-to-end sweep: for each config, apply template → reset profiler → run N times → dump JSON + CSV |
| `plot-results.py` | Read `results/` and emit 4 figures under `figures/` |

## Results layout

After a sweep, `results/` contains:

```
results/
  iobt/
    random/
      summary.csv              # iteration,run_name,phase,makespan,wall_s
      repeat-template.log      # full driver output
      iobt-random-run-001.json # per-run ODAG status (placement + predictedSchedule)
      ...
      profiler-final.db        # profiler state at end of config
    heft/
      ...
    heft-eps/
      ...
  hetero-compute/
  wide-pipeline-flex/
```

`plot-results.py` reads this structure and writes figures to
`figures/`:

1. `makespan-distribution.png` — box plots of warm makespans per config
2. `makespan-convergence.png` — makespan vs run index, one line per config
3. `prediction-scatter.png` — predicted vs actual makespan (HEFT configs only)
4. `iobt-infer-placement.png` — share of infer-i placements per compute node

## Expected outcomes

Consistent with `docs/paper-notes.md` Contribution 1 and
Contribution 5:

- **HEFT vs random**: ~25–35% mean makespan reduction (depends on
  ODAG; largest on iobt where transfers dominate). Variance collapses
  substantially under HEFT once the profiler warms up (3–4 runs).
- **ε-HEFT vs strict HEFT**: mean makespan roughly unchanged, p95
  tightens, `infer-i` placement spreads across all three compute
  nodes instead of concentrating on one.

## Cluster assumptions

- k3s, `wl-system` namespace
- 8 schedulable workers: anrg-1, anrg-3..9 (anrg-2 is master, NoSchedule)
- `odag-controller`, `data-agent` DaemonSet, `ui-server` already deployed
- Image registry at `192.168.1.163:5000` with the following tags pushed:
  - `wl-iobt-{capture,preprocess,infer,fuse,report}:latest`
  - `multi-odag-task:latest` (used by hetero-compute and wide-pipeline-flex)

If any image is missing, build + push from each ODAG's `tasks/`
folder using the reference Dockerfiles.

## Reproducing from scratch

Fresh cluster, no pre-built images:

```bash
# 1. Build and push all task images.
REGISTRY=192.168.1.163:5000
cd eval/synthetic-dags/scheduler/iobt/tasks
for t in capture preprocess infer fuse report; do
  docker build -f $t/Dockerfile -t $REGISTRY/wl-iobt-$t:latest ../../../.. \
    && docker push $REGISTRY/wl-iobt-$t:latest
done
cd ../../hetero-compute/tasks
docker build -f Dockerfile -t $REGISTRY/multi-odag-task:latest ../../../.. \
  && docker push $REGISTRY/multi-odag-task:latest
# (wide-pipeline-flex reuses multi-odag-task, no separate build.)

# 2. Pre-flight + sweep as above.
```
