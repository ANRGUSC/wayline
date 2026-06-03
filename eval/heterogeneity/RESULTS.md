# Heterogeneity benchmark — results (v1)

20 nodes (8 Intel i3-N305 + 12 Pi 4), CPU clocks pinned (Intel 800–1800 MHz,
Pi 600–1200 MHz; ~15.7× compute spread), tiered tc network matrix. 27-task,
5-class synthetic DAG. Matrix: {resource-aware, classical} HEFT × {net on, off},
12 reps/cell, independent profile per cell (cold→converged).

## What worked
- **All 48 runs succeeded** (27/27 tasks each), both HEFT modes, both net conditions.
- **Profiling converges.** Per-(task,node) coverage grows each rep (≈26→120+ pairs)
  and the EMA settles (mean |Δruntime| vs previous rep falls to ≈0.04–0.09 s).
  Predicted-vs-actual makespan error shrinks monotonically as profiles fill in:
  ra-net −69%→−22%, ra-nonet −60%→−21%, classical-net −35%→−25%.

## Honest / surprising findings
1. **HEFT systematically under-predicts makespan by ~22% even when converged.**
   The cost model omits real execution overhead (pod create, data-agent install +
   500 ms readiness polling, k8s scheduling). Converged prediction error floors at
   ~−22%, it does not reach 0.
2. **Classical ≈ resource-aware** (warm: classical/ra = 0.91× shaped, 0.96× unshaped).
   Classical is *not* slower — and has notably lower variance (45–52 s vs 43–69 s).
3. **Network shaping barely moves makespan** (shaped/unshaped = 1.03× for RA).

## Why (2) and (3) are null — the workload is compute-bound
Warm per-class actual runtimes (s):

| class | mean | note |
|---|---|---|
| **sensor (Pi)** | **17–19** | dominates the critical path |
| heavy (Intel) | 5.8 | |
| shuffle | 3.3 | |
| aggregate | 2.5 | |
| report | 2.2 | |

Critical path ≈ sensor(17) → heavy(6) → shuffle(3) → aggregate(2.5) → report(2.2)
≈ 31 s of compute. Largest payload is ~12 MB → ~1–2 s even on the 50 Mbps
bottleneck. So **transfers are <10% of makespan; the slow Pi sensor compute
dominates.** That masks both the network effect (transfers negligible) and the
classical-vs-RA difference (the sensor layer runs in parallel across Pis in both
modes, and there isn't enough concurrent independent Intel work for RA's
co-scheduling to win). Classical's lower variance comes from removing
co-scheduling contention jitter — a real, if secondary, result.

## v2 proposal (to actually stress network + execution model)
- Sensors: **raise payload to ~80–120 MB** (feasible within Pi RAM) and **drop
  sensor compute** so transfer time (~120 MB / 50 Mbps ≈ 19 s) rivals compute →
  makespan becomes network-sensitive, exposing the shaped-matrix effect and
  network-aware placement value.
- Add a **wide layer of many independent Intel tasks** (e.g. 16–24) so RA's
  intra-node parallelism (8 cores) can overlap them while classical serializes →
  exposes the classical-vs-RA gap the way the model predicts.

Data: `results/<cell>/{makespan,tasks,profiles}.csv` + `runs/run-NN.json`.
Re-run: `REPS=12 ./campaign.sh`; analyze: `python3 analyze.py`.
