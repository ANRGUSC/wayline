# E13: Network heterogeneity, alone and with CPU heterogeneity

## Why

E11 and E12 vary only CPU speed over a clean, uniform network. E13 asks
what heterogeneity-aware placement gains when links differ, and when links
and CPUs differ together.

## Links (`net.py`)

Each worker has a link class; a pair of workers gets the slower end's rate.
B = 942 Mbit/s (E0).

| node | link | CPU class (scenario 2) |
|---|---|---|
| anrg-1 | B | fast |
| anrg-6 | B/4 | fast |
| anrg-7 | B/8 | fast |
| anrg-3 | B | medium |
| anrg-5 | B/2 | medium |
| anrg-8 | B/8 | medium |
| anrg-4 | B/2 | slow |
| anrg-9 | B/4 | slow |

Classes cut across the CPU classes, so the fastest CPU is not always on the
fastest link. Each worker shapes its egress per destination worker IPv4
address with HTB, which covers every data path between workers: Wayline's
agents (hostPort 8082), pod-to-pod VXLAN, and Ray's object transfers (host
network). Traffic to the control node is not shaped. `net.py verify`
measures TCP goodput on four pairs; unshaped every pair gives about 935
Mbit/s, shaped each lands at 95 to 96% of its planned rate. The schedulers'
bandwidth profile is each pair's rate times that measured goodput fraction,
so planned transfer times match the real ones.

## Scenarios

Same seeded 20-task DAG as E11/E12 with mixed 1/2/3-CPU tasks, edges scaled
4x (4 to 120 MB) so transfers matter (at 1x the largest edge takes 2 s even
at B/8, against 2 to 10 s tasks).

1. Network only: every CPU locked at 2.4 GHz, links shaped.
2. CPU and network: clocks locked at 2.4 / 1.2 / 0.8 GHz (E11 classes),
   links shaped.

Per scenario: built-in HEFT, SAGA HEFT, SAGA PEFT, each as Wayline pods,
Wayline warm and Ray pinned to the same placement; Ray's default three times.

## Run

```bash
# on anrg-2, from eval/experiments/E12 (E12's wrapper brings up the runners)
SUDO_PASS=... RES=~/E13-results python3 e12.py --schedulers heft saga/heft saga/peft \
    --default-reps 3 --net classes --size-scale 4 --cpu-classes uniform   # scenario 1
SUDO_PASS=... RES=~/E13-results python3 e12.py --schedulers heft saga/heft saga/peft \
    --default-reps 3 --net classes --size-scale 4 --cpu-classes hetero    # scenario 2
```

Links, clocks, the bandwidth profile and Ray are restored whatever happens.

## Results (2026-09-28, one run per arm)

A first attempt (`results/20260928T064727Z/`, kept for the record) is not
valid: deleted runs' data had filled anrg-1 and anrg-9 (disk pressure,
evictions, a hung run, failed arms), and the built-in HEFT sized every edge
at 1 MB. Both were fixed in the controller before the runs below.

### Network only (`results/20260928T074025Z/`, all CPUs at 2.4 GHz)

| scheduler | estimate | Wayline pods | Ray pinned | Wayline warm |
|---|---|---|---|---|
| built-in HEFT | 41.1 | 70.0 | 48.1 | 51.0 |
| SAGA HEFT | 41.5 | 57.4 | 49.1 | 53.2 |
| SAGA PEFT | 41.3 | 62.6 | 44.9 | 53.6 |
| Ray default | | | 61.6, 57.1, 49.6 (mean 56.1) | |

The planners move 0.7 to 0.9 GB between nodes, all of it over B and B/2
links; Ray's default moves 0.2 to 0.3 GB, all over B/4 or B/8 links.
Planned placement on Ray is 12 to 20% faster than Ray's default. Warm
Wayline is 3 to 9 s behind Ray at the same placement, the largest gap of any
experiment: on transfer-heavy runs Wayline's data path costs more than Ray's.

### CPU and network (`results/20260928T075247Z/`, 2.4 / 1.2 / 0.8 GHz)

| scheduler | estimate | Wayline pods | Ray pinned | Wayline warm |
|---|---|---|---|---|
| built-in HEFT | 49.2 | 77.3 | 61.3 | 68.0 |
| SAGA HEFT | 46.0 | 73.0 | 63.1 | 63.1 |
| SAGA PEFT | 47.0 | 58.5 | 56.6 | 54.1 |
| Ray default | | | 129.5, 120.1, 103.6 (mean 117.7) | |

The planners trade link speed for CPU speed: they put the work on the fast
CPUs (anrg-1, anrg-6, anrg-7) and move 0.8 to 1.1 GB, all over B/4 or B/8
links. Ray's default lands on slow CPUs. Planned placement on Ray is about
2x faster than Ray's default. Warm Wayline is within -2.5 to +6.7 s of Ray at
the same placement.

### What the estimates miss

Estimates are 10 to 17 s under Ray-pinned makespans in scenario 2 and 4 to
8 s in scenario 1. SAGA's standard model gives every transfer its link's
full rate; here up to a gigabyte crosses a few slow links at once. Next:
the same runs with `saga/contention_heft` (one transfer at a time per node
interface, merged into SAGA 2.2.0.dev1 and now deployed), and a look at the
agent's push path for the warm-vs-Ray gap on transfer-heavy runs.

## Contention-aware HEFT (2026-09-28)

`saga/contention_heft` (one transfer at a time per node interface; SAGA
2.2.0.dev1) against plain `saga/heft`, same run, same calibration and links.
"SAGA estimate" is the scheduler's own makespan estimate.

Network only (`results/20260928T094912Z/`):

| scheduler | SAGA estimate | Wayline pods | Ray pinned | Wayline warm |
|---|---|---|---|---|
| contention HEFT | 45.8 | 62.8 | 57.1 | 63.6 |
| SAGA HEFT | 42.8 | 69.9 | 57.0 | 67.1 |
| Ray default | | | 62.9, 54.0, 83.6 (mean 66.8) | |

CPU and network (`results/20260928T100033Z/`):

| scheduler | SAGA estimate | Wayline pods | Ray pinned | Wayline warm |
|---|---|---|---|---|
| contention HEFT | 49.3 | 64.1 | 52.1 | 65.0 |
| SAGA HEFT | 46.3 | 72.8 | 62.8 | 63.8 |
| Ray default | | | 137.8, 118.3, 60.8 (mean 105.6) | |

- With CPU and network heterogeneity, contention-aware HEFT's placement is
  17% faster on Ray (52.1 vs 62.8 s) and its estimate is 2.8 s under Ray,
  against 16.5 s for plain HEFT. It spreads inbound data over three nodes
  (0.98 GB) instead of pushing 0.68 GB into anrg-6's B/4 link.
- Network only, both place equally well on Ray (57 s); the contention
  estimate is closer (11.3 s under, against 14.2 s).
- Plain SAGA HEFT's network-only placement changed since the previous run
  (it now uses anrg-9's B/4 link; Ray pinned 57.0 s, was 49.1 s): with every
  CPU equal many candidates tie, and the merged determinism fix changed how
  ties break. Single runs; tie-heavy settings need repetitions.
- On these transfer-heavy runs warm Wayline trails Ray at the same placement
  by 1 to 13 s and is no faster than Wayline pods (62.8/64.1 pods vs
  63.6/65.0 warm for contention HEFT): moving data, not starting tasks, is
  Wayline's bottleneck here. Next: the agent's push path (serial per source,
  a cap on concurrent pushes per node) against Ray's parallel pulls.
