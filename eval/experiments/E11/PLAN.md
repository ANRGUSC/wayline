# E11: Real compute heterogeneity and looser constraints

Status: implemented (`e11.py`); first runs below. Follows E10.

## Why

HEFT-class schedulers earn their keep when nodes differ in compute speed and the
scheduler has real choices. E10 has neither: the Intel workers are identical,
runtime heterogeneity is emulated only for the gateway, and each task's CPU
request leaves one task per node. Under those conditions HEFT's advantage over
Ray's locality-driven placement is a lower bound.

## Changes relative to E10

1. **Real CPU heterogeneity.** Cap the CPU frequency per node, e.g. three speed
   classes over the eight workers (full, ~60%, ~30%), with
   the `scaling_max_freq` sysfs knob, set over ssh from anrg-2 with sudo.
   Record the achieved per-node speed with a fixed CPU benchmark before each
   block, and restore the defaults afterward.
2. **Real work, not sleep.** Use a CPU-bound task body (the
   `heterogeneous-compute` example's hashing loop, or a model inference) so
   runtime follows the frequency cap, and let Wayline's profiler learn the
   per-(task, node) costs across runs instead of declaring them.
3. **Looser constraints.** Each task may run on a random ~80% of the workers
   (seeded per task), and the CPU request is reduced so two or three tasks fit
   per node. The scheduler then has placements to choose between.
4. **More schedulers.** HEFT, CPoP, PEFT, MinMin via SAGA, pinned onto Ray the
   same way as E10's `ray-plan`, against Ray's default; plus the Wayline arms.
5. **Larger DAGs.** The IoT and wide-pipeline workloads and WfCommons
   instances, where the placement space is larger.

## Expected result

The gap between heterogeneity-aware placement (on either runtime) and Ray's
default should grow with the spread of node speeds and with the scheduler's
freedom, while Wayline's per-task overhead (E9) stays constant, so Wayline's
net position against Ray should improve as tasks and heterogeneity grow.

## Implementation

- `gen_e11.py`: seeded layered DAG (layers 1-5-5-5-3-1, 20 tasks, 36 named
  edges of 1-30 MB, work 2-10 s at full clock), each task allowed on a random
  6 of the 8 workers. Emits the template (`runtimeProfile` from calibrated
  rates) and a uniform 942 Mbit/s `wl-network-profile`.
- `tasks/`: image `wl-e11`, a fixed number of SHA-256 iterations per task, so
  runtime follows the node's clock; `--bench` prints the node's rate.
- `e11_ray.py`: the same body on Ray. `plan` pins each task
  (NodeAffinity); `default` lets Ray choose within the allowed set
  (`NodeLabelSchedulingStrategy`, workers labeled `wl-node=<name>`).
- `e11.py`: caps clocks (fast 3.8 GHz: anrg-1/6/7; medium 1.9: anrg-3/5/8;
  slow 0.9: anrg-4/9), calibrates with a bench pod per node, runs every
  scheduler on Wayline and pins its placement onto Ray, then Ray's default.
  Clocks, the bandwidth profile and Ray are restored in a `finally`.
  The sudo password comes from `SUDO_PASS` and is never written.

Calibrated rates are 2.5-2.6, 1.26-1.34 and 0.61-0.63 Mhash/s, a
1 : 0.51 : 0.24 ratio.

## First runs (2026-09-25/26, 2-CPU tasks, enactOrder order)

`results/<stamp>/`. Makespans in seconds; Wayline is first submission to last
output close.

| scheduler | Wayline run 1 | Ray-plan run 1 | Wayline run 2 | Ray-plan run 2 |
|---|---|---|---|---|
| HEFT | 74.8 | 72.5 | 76.8 | 75.9 |
| CPoP | 75.1 | 70.5 | 98.5 | 96.4 |
| PEFT | 107.7 | 105.1 | 106.1 | 105.5 |
| MinMin | 59.1 | 55.4 | 78.4 | 72.7 |
| random | 105.0 | 103.2 | 103.2 | 98.3 |
| Ray default | | 84.3 | | 58.0 |

A HEFT-only smoke run before these gave Ray-plan 73.7 and Ray default 169.9.

What the runs show:

- At identical placement Wayline is 1 to 6 s behind Ray, per-task pod start
  on the critical path, as in E9 and E10.
- Ray's default is heterogeneity-blind and packs by locality, so its outcome
  depends on which class it starts in: 18 of 20 tasks on the slow nodes gave
  169.9 s, 19 on medium 84.3 s, and 19 on fast 58.0 s. Mean 104 s, spread 3x.
- The SAGA schedulers model a node as running one task at a time, but 2-CPU
  tasks let each 8-CPU node run four. They spill work to medium and slow
  nodes that a slot-aware schedule would keep on the fast ones, which is how
  Ray's lucky all-fast run beat every SAGA placement.
- Scheduler outputs move between runs (CPoP 70 to 96 s on Ray) because the
  calibrated rates differ slightly between runs.

## Matched setting (2026-09-26, 5-CPU tasks, one per node)

Here each node runs one task at a time on both runtimes, the machine model
the SAGA schedulers assume. One Wayline run per scheduler, three Ray-default
samples. `results/20260926T005239Z/`.

| scheduler | estimate | Wayline | Ray pinned |
|---|---|---|---|
| PEFT | 47.4 | 61.6 | 52.5 |
| CPoP | 70.9 | 82.2 | 81.1 |
| MinMin | 74.9 | 92.5 | 81.6 |
| HEFT | 85.6 | 98.6 | 92.4 |
| random | 101.9 | 111.1 | 99.4 |
| Ray default | | | 129.7, 78.6, 137.8 (mean 115.4) |

- Every SAGA placement pinned onto Ray beats Ray's default mean; PEFT by
  2.2x, HEFT by 1.25x. Random placement also beats it, because default Ray
  packs onto whichever class it starts in.
- Wayline trails Ray at the same placement by 1 to 12 s. With 5-CPU pods, a
  node's next pod waits for the previous one to exit (E10), so the pod
  overhead grows with the number of same-node successors.
- The scheduler ranking is not stable across runs yet (HEFT led PEFT in the
  2-CPU runs); more repetitions are needed before ranking schedulers.

Next: slot-aware machine models so schedulers can use multi-task nodes, and
E12 (warm runners) to remove the pod overhead.
