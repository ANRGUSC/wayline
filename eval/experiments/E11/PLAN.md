# E11 (planned): Real compute heterogeneity and looser constraints

Status: not started. Follows E10.

## Why

HEFT-class schedulers earn their keep when nodes differ in compute speed and the
scheduler has real choices. E10 has neither: the Intel workers are identical,
runtime heterogeneity is emulated only for the gateway, and each task's CPU
request leaves one task per node. Under those conditions HEFT's advantage over
Ray's locality-driven placement is a lower bound.

## Changes relative to E10

1. **Real CPU heterogeneity.** Cap the CPU frequency per node, e.g. three speed
   classes over the eight workers (full, ~60%, ~30%), with
   `cpupower frequency-set -u <freq>` (or the `scaling_max_freq` sysfs knob) from
   a privileged DaemonSet, since direct ssh to the workers is not available.
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
