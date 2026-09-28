# E12: Warm runners vs pods vs Ray

## Why

E9 put Wayline's per-task pod overhead at about 1 s, and E10 showed it is
most of the remaining gap to Ray at identical placement. Warm runners
(`runner:` on a task, see the README) remove the pod from the task path.
E12 measures how much of the gap they close, on E11's heterogeneous
setting, where placement matters.

## Setup

Everything from E11: clock caps (three classes), per-node calibration,
the seeded 20-task DAG with 80% node constraints, uniform 942 Mbit/s
bandwidth profile, `enactOrder: order`, 2-CPU tasks. Per scheduler (HEFT,
MinMin by default):

| arm | what runs |
|---|---|
| `wayline` | a pod per task (image `wl-e11:v2`) |
| `wayline-warm` | the same tasks as calls on `wl-runner-e12` (same image, `--script task.py --slots 4`) |
| `ray-plan` | the cold arm's placement and order pinned onto Ray |
| `ray-default` | Ray's own placement within each task's allowed nodes, once per repetition |

Slots are 4 per node, matching Ray's 8 CPUs over 2-CPU tasks. Makespan for
Wayline arms is first submission to the last task's output close (SDK
timings); for Ray it is driver submission to the last result.

## Run

```bash
# on anrg-2
SUDO_PASS=... RES=~/E12-results python3 e12.py --reps 1
```

The runner DaemonSet is created at the start and deleted at the end
(`--keep-runners` keeps it). Clocks and the bandwidth profile are restored
by E11's harness.

## First run (2026-09-26, one repetition)

`results/20260926T011811Z/`. Makespan in seconds.

| scheduler | estimate | Wayline pods | Ray pinned | Wayline warm |
|---|---|---|---|---|
| HEFT | 62.6 | 74.3 | 69.6 | 65.1 |
| MinMin | 56.9 | 65.6 | 62.2 | 61.0 |
| Ray default | | | 83.9 | |

Launch delay per task, from its last input being sealed to the task
starting:

| arm | median | mean | max |
|---|---|---|---|
| pods, HEFT | 1.07 | 1.20 | 3.16 |
| warm, HEFT | 0.22 | 0.27 | 0.68 |
| pods, MinMin | 0.90 | 0.93 | 1.40 |
| warm, MinMin | 0.13 | 0.27 | 0.96 |

- Warm runners cut per-task launch delay about fourfold and take Wayline from
  behind Ray to ahead of it at identical placement (HEFT 4.5 s ahead, MinMin
  1.2 s ahead).
- Warm HEFT lands 2.5 s over the scheduler's estimate, against 11.7 s with
  pods.
- The remaining warm delay is mostly the dispatch path (agent state polling
  at 50 ms and the controller's readiness checks), not execution.
- One repetition only; the Ray-default sample is a single draw from a
  high-variance distribution (see E11).

## With slots and free-capacity planning (2026-09-27)

`results/20260927T081549Z/`: `slots: auto`, Wayline tasks 1.8 CPU, Ray 2,
four tasks per node on both runtimes. Makespan in seconds.

| scheduler | estimate | Wayline pods | Ray pinned | Wayline warm |
|---|---|---|---|---|
| built-in HEFT | 40.0 | 54.5 | 47.4 | 47.4 |
| SAGA MinMin | 51.5 | 62.4 | 57.6 | 56.6 |
| SAGA HEFT | 47.3 | 64.4 | 57.6 | 63.1 |
| Ray default | | | 83.7, 49.7, 50.5 | |

- Best overall: built-in HEFT, warm or pinned onto Ray, at 47.4 s. The two
  values match to the millisecond by coincidence (independent runs 48 s
  apart; the controller's own figure for the warm run is 47.05 s).
- Warm matches or beats Ray at the same placement for built-in HEFT and
  MinMin; SAGA HEFT warm was 5.5 s behind Ray in this run.
- Default Ray again ranged widely, 49.7 to 83.7 s.

## After the bridge fix: exact runtimes and constraints (2026-09-27)

`results/20260927T084123Z/`. Same setting as above, with the SAGA bridge
now planning with exact per-(task, node) runtimes and honoring constraints
while scheduling (no post-scheduling moves). Makespan in seconds.

| scheduler | estimate | Wayline pods | Ray pinned | Wayline warm |
|---|---|---|---|---|
| SAGA HEFT | 40.1 | 54.3 | 48.9 | 48.8 |
| SAGA PEFT | 40.1 | 56.2 | 47.6 | 48.6 |
| SAGA MinMin | 40.1 | 57.1 | 51.4 | 49.4 |
| built-in HEFT | 40.1 | 61.8 | 51.9 | 52.3 |
| Ray default | | | 59.3, 106.1, 81.2 (mean 82.2) | |

All four schedulers converge on almost the same placement (MinMin and
built-in HEFT identical to SAGA HEFT, PEFT differs on two tasks off the
critical path): with four slots per fast node, the fast nodes absorb nearly
the whole DAG.

### Where estimate and reality differ

`critpath.py` walks the realized critical path back from the last task.
SAGA HEFT, 8.7 s (warm) and 14.3 s (pods) over the estimate:

| component on the critical path | warm | pods |
|---|---|---|
| compute over plan | +5.5 | +7.1 |
| launch (parent sealed to task start) | 1.5 | 5.4 |
| of which planned transfer | 0.3 | 0.3 |
| output handoff and close | 1.5 | 1.6 |
| input read | 0.2 | 0.2 |

The compute excess is turbo. Calibration benchmarks one busy core; fast
nodes (3.8 GHz cap) run slower as more tasks share them, on Wayline and
Ray alike (actual / planned compute over all runs):

| fast node, tasks at once | 1 | 2 | 3 | 4 |
|---|---|---|---|---|
| actual / planned | 1.03 | 1.17 | 1.32 | 1.40 |

Medium nodes (1.9 GHz cap, below the all-core clock) stay at 1.02 to 1.03.
The remaining warm overhead is about 0.2 s launch and 0.25 s handoff per
critical-path task; pods add about 0.65 s more launch per task.

Fix for the model: calibrate each node with as many concurrent benchmark
processes as it has slots, or cap fast nodes below the all-core clock.

## Capacity-aware scheduling, mixed CPU requests, locked clocks (2026-09-28)

`results/20260928T003132Z/`. SAGA plans with node capacity (free CPU) and
each task's own CPU request (seeded 1, 2 or 3 cores; DAG unchanged), exact
runtimes and constraints natively (SAGA feature/capacity). Clocks locked at
2.4 / 1.2 / 0.8 GHz. Ray gets the same CPU per task and 7 CPUs per worker.

| scheduler | estimate | Wayline pods | Ray pinned | Wayline warm |
|---|---|---|---|---|
| SAGA HEFT | 40.0 | 51.0 | 44.6 | 44.1 |
| SAGA PEFT | 40.0 | 50.7 | 44.3 | 43.5 |
| SAGA MinMin | 40.0 | 49.8 | 45.1 | 43.5 |
| built-in HEFT | 40.0 | 54.8 | 43.8 | 44.3 |
| Ray default | | | 133.7, 64.0, 102.9 (mean 100.2) | |

- Warm Wayline is 3.5 to 4.3 s over the estimate (was 8.7 s with an unlocked
  clock) and matches or beats Ray at the same placement.
- Aware placement on Ray averages 2.3x faster than Ray's default.
- `contention.py`: peak CPU requested by tasks actually computing on one node
  is 7 (plan 7.4, 8 cores) in every arm; compute over plan averages 1.02 to
  1.03 warm and 1.05 to 1.09 with pods (max 1.25; other pods starting on the
  node take CPU nobody requests).

## Pods and warm tasks in one run (2026-09-28)

`results/20260928T004715Z/`. Built-in HEFT, the same placement run three
ways; "mixed" runs every other task warm and the rest as pods.

| arm | makespan | peak CPU on a node | compute / plan (mean, max) |
|---|---|---|---|
| pods | 51.6 | 6 | 1.09, 1.23 |
| warm | 44.1 | 7 | 1.03, 1.06 |
| mixed | 47.3 | 6 | 1.05, 1.28 |

Mixed runs correctly and lands between the two. Open: pods are admitted by
the kubelet (pod requests only) and warm calls by the runner (its own
budget), so neither sees the other's tasks. Nothing overbooked here because
execution followed the plan closely, but a node could run more than its
capacity when tasks drift from plan. A controller-side capacity gate over
both kinds of task would close it.
