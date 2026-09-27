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
