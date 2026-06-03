# Reproducing the Wayline evaluation

This directory is the artifact for the Wayline paper. Each subdirectory holds one
experiment: its run scripts, the committed result data, and a plot script that
regenerates the corresponding paper figures from that data. All results were
collected on the Wayline (`wl-system`) build.

```
e0-microbench/     E0 two-task data-plane microbenchmark (Wayline vs MinIO/NFS)
ray-microbench/    E0 with Ray's object store as a third comparator
mcmt/              AI City multi-camera tracking (the headline real workload)
  results/hero/      paired Wayline-vs-Argo makespan (the 1.6–2.1× result)
  results/ablation-main/  matched-placement static ablation (the 2.10× data-plane result)
  results/ablation-static-n4-d120-png/  HEFT spreadEpsilon sweep
  baselines/         distributed-MinIO (4-replica) and centralized-NFS baselines
synthetic-dags/
  e1/                full-system Wayline vs Argo+MinIO on iobt/hetero/wpf
  e2/                Argo + Kubernetes NetworkOverhead scheduler-plugin
  scheduler/         HEFT vs random placement under shaped tc topologies
stress/            K=3 concurrent ODAGs + data-agent CPU/RSS overhead
data-agent-tests/  correctness invariants + adversarial failure injection
```

## Two ways to reproduce

**(1) Regenerate the figures from the committed data — no cluster needed (seconds).**
```
make repro-figures        # from the repo root
```
This runs each experiment's plot script against its committed result CSVs and
rewrites the figures. Use it to confirm the paper's figures follow from the
shipped data.

**(2) Re-run the experiments end-to-end — requires the testbed (hours).**
Each directory has a `run.sh` / `sweep.sh`. See per-claim commands below.

## Requirements for end-to-end reproduction
- **Cluster:** 8-node x86 k3s — 1 master + 7–8 workers, Intel i3-N305 (8 cores,
  Xe-LP iGPU), 16 GB RAM, 1 GbE, plus a local registry (`<master-ip>:5000`).
- **Network shaping:** `tc/htb/netem`; the 8×8 matrix is applied by
  `synthetic-dags/scheduler/setup-tc-matrix.sh`.
- **MCMT only:** the AI City Challenge Track-1 dataset (registration required;
  fetch scripts in `mcmt/dataset/`) and the iGPU (`/dev/dri`) for VAAPI/OpenVINO.
- Convention: 20 paired reps/cell, warm window = runs 5–20, CPU governor `performance`.

## Claim → source → command → expected

| Paper artifact | dir | regenerate from data | expected |
|---|---|---|---|
| Fig/Tab E0 (e0-summary) | `e0-microbench/` | `cd e0-microbench && python3 plot.py` | same-100MB 2.74×, same-500MB **7.2×** Wayline vs MinIO |
| Tab e0-ray | `ray-microbench/` | data in `ray-e0.csv` (tc), `ray-e0-notc.csv` | Ray cross-node 500MB ≈ 401 s vs Wayline same-node ≈ 5.7 s |
| Tab/Fig aicity (hero) | `mcmt/results/hero/` | `cd mcmt && python3 scripts/plot-fair.py` | d120-png tc: Argo 225.9 s / Wayline 107.8 s (**2.10×**); 1.6–2.1× across cells |
| Tab static-ablation | `mcmt/results/ablation-main/` + `.../ablation-static-*` | (in plot-fair) | Argo 227.7 / Wayline-spread 108.2 (2.10×); ε=0/40/60 → 128.2/113.8/108.5 |
| Tab tuned-minio | `mcmt/baselines/` | — | distributed-MinIO ≈ 215 s, NFS ≈ 218 s (both ≈ 2.0× slower than Wayline) |
| Tab/Fig aicity-random | `mcmt/results/` (random nets) | `python3 scripts/plot-fair.py` | median ≈ 2.6× across 10 seeded topologies, wins all 10 |
| Fig/Tab E1 (e1-summary) | `synthetic-dags/e1/` | `cd synthetic-dags/e1 && python3 plot.py` | iobt 3.62×, hetero 1.72×, wpf 4.20× (Wayline vs Argo) |
| Fig/Tab E2 (e2-summary) | `synthetic-dags/e2/` | `cd synthetic-dags/e2 && python3 plot.py` | NetworkOverhead gives Argo ≤1.4%; gap to Wayline unchanged |
| Tab scheduler / heft-vs-random | `synthetic-dags/scheduler/` | `cd synthetic-dags/scheduler && python3 plot-results.py` | HEFT vs random: variance ↓ up to 6.1×, mean ↓ up to 11.8% |
| Tab concurrent | `stress/concurrent-k3-results.csv` | — | K=3 makespans 158/155/157 s; 1.70× concurrency gain |
| Tab overhead | `stress/overhead-{solo,k3-metrics}.csv` | — | solo d120-png peak 0.30 cores / 1.27 GB; K=3 0.14 cores / 0.315 GB |
| Tab agent-tests / failure-injection | `data-agent-tests/` | `bash correctness.sh` / `bash failure-injection.sh` | 11/11 invariants pass; 3/3 kill-and-recover pass |

End-to-end re-run: each dir's `run.sh`/`sweep.sh` (e.g. `cd e0-microbench && ./sweep.sh`,
`cd mcmt && less RUNBOOK.md`). Network experiments first apply
`synthetic-dags/scheduler/setup-tc-matrix.sh`.

> Note on naming: the project was renamed DSF→Wayline; identifiers here are all
> `wayline`/`wl-system`/`wl.io`. Material not in the paper (exploratory runs,
> superseded result copies, campaign workspaces) lives under `eval/_archive/`
> (gitignored, kept on disk) and in the git history.
