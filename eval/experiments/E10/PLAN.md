# E10: Wayline vs Ray on the same DAG, with and without Wayline's placement

## Question

How much of Wayline's advantage over a warm-worker runtime comes from
heterogeneity-aware placement, and how much does pod-based execution cost at
identical placement? E10 answers both by running one DAG three ways.

| arm | runtime | placement | data plane |
|---|---|---|---|
| `wayline` | pods (Wayline) | HEFT via SAGA (`saga/heft`) | Wayline agents |
| `ray-plan` | Ray warm workers | Wayline's realized HEFT placement, pinned with `NodeAffinitySchedulingStrategy`, submitted in HEFT order | Ray object store |
| `ray-default` | Ray warm workers | Ray's own (locality-aware hybrid) | Ray object store |

- `ray-default` → `ray-plan`: the value of heterogeneity-aware placement, runtime held fixed.
- `ray-plan` → `wayline`: the cost of pod-based execution and of the data plane, placement held fixed (compare with E9's per-task overhead).

## Workload

E5's seven-task DAG, unchanged: every edge a named object (1–60 MB),
`runtime(t, n) = WORK[t] / SPEED[n]` emulated in the task, 5 CPUs per task on
8-CPU workers. The Ray driver (`e10_ray.py`) reproduces the same sizes, the same
sleep model, and the same CPU request.

Nodes: `anrg-1, 3, 4, 5, 9` (anrg-6/7/8 excluded until their node IPs are
reachable from the control node again). Heterogeneity is therefore only the
gateway (`anrg-9`, speed 0.25); see E11 for the stronger setting.

Clean network in the pilot. For shaped runs, both data planes must be shaped the
same way: Wayline's agents on port 8082, Ray's object manager on the pinned
port 8076.

## Infrastructure

Ray 2.58.0 in `wl-ray-baseline:2.58.0` (`ray/Dockerfile`), run as hostNetwork
pods: head on `anrg-2` with no CPUs, one worker per node (`ray/*.tmpl`). The
orchestrator (`e10.py`) brings the cluster up, runs the Wayline arm, hands its
placement and HEFT order to `ray-plan`, then runs `ray-default`.

## Measurement

- `wayline`: wall time from `wayline run` to the run observed Succeeded (250 ms
  poll), plus `status.makespan` (pod timestamps, 1 s resolution) and HEFT's
  estimate.
- Ray arms: driver time from the first submission to the last task's result.

## Run

```bash
# on anrg-2, from the experiment directory
RES=~/E10-results python3 e10.py --reps 1          # add --keep-ray to leave the Ray pods up
```

## Pilot (2026-09-25, one run per arm, clean network)

| arm | makespan (s) |
|---|---|
| wayline | 89.1 wall (87 pod-based; HEFT estimate 80.3) |
| ray-plan | 82.2 |
| ray-default | 113.7 |

Placement-aware Ray is 28% faster than default Ray (default put `c` and `j2` on
the quarter-speed gateway). At identical placement, Wayline is 7 s (8%) slower
than Ray, consistent with E9's ~1 s of pod overhead per critical-path task.
